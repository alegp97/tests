def run(
  sourcedb: String,
  targetdb: String,
  targetTable: String,
  targetTableOptionalName: String,
  entities: List[IngestEntity],
  extra_filter: String,
  sourceTable: String,
  process: String,
  is_incremental: String
)(implicit spark: SparkSession): Unit = {

  // ------------------- Resolución de nombres y diccionario (flujo lineal) -------------------
  val table  = dstTableName(targetdb, targetTable, targetTableOptionalName)
  val source = s"$sourcedb.$sourceTable"
  val value  = latestFieldsDictValue(sourcedb)

  // ------------------- Construcción de query de columnas desde fields_dict ------------------
  val columnsVariablesDF = loadColumnsVariablesDF(sourcedb, sourceTable, targetTable, process, value)
  val query              = buildQuery(columnsVariablesDF)
  log.info(s"[SAST] Query size: ${query.size}")

  // ------------------- Carga de origen + exec_id + ORIGINAL_VALUE opcional ------------------
  val filterTable                   = spark.sqlContext.table(source)
  val (sa_exec_id_column, hasStamp) = stampColumnAndFlag(filterTable) // (lista de cols, flag)
  val original_value_colist         = originalValueColIfNeeded(filterTable, sourceTable, targetTable)
                                      .getOrElse(query)

  // ------------------- Filtro por entidades/incremental sin if en línea --------------------
  val baseDF = entitiesPredicate(entities, is_incremental, hasStamp)
    .map(pred => filterTable.where(pred))
    .getOrElse(filterTable)

  // ------------------- Proyección de columnas + extra_filter opcional ----------------------
  // Seleccionar (sa_exec_id_column ++ original_value_colist) funciona aunque la primera esté vacía
  val withExecId = baseDF.select((sa_exec_id_column ++ original_value_colist): _*)
  val toSave0    = Option(extra_filter).map(_.trim).filter(_.nonEmpty) match {
    case Some(f) =>
      log.info(s"[SAST] Aplicando filtro extra : $f")
      withExecId.where(f)
    case None    => withExecId
  }

  // ------------------- Particiones y alineación de esquema ---------------------------------
  val partitionsName = partitionsForWrite(targetdb, targetTable, targetTableOptionalName)
  log.info(s"[SAST] partitionsName: $partitionsName")
  val targetTableDF  = spark.sqlContext.table(table)
  val toSave1        = alignedToTargetSchema(toSave0, targetTableDF)

  // ------------------- Reglas especiales vía estrategia (sin if/else cadenas) --------------
  // Mantiene la lógica original: MKT_ET_DATA_ST -> anti-join por process_id
  //                             EDITED_INPUT -> DATA_INPUT -> anti-join por claves compuestas
  val ruleKey = s"${sourceTable.trim.toUpperCase}->${targetTable.trim.toUpperCase}"

  val businessRules: Map[String, (DataFrame, DataFrame) => DataFrame] = Map(
    s"MKT_ET_DATA_ST->${targetTable.trim.toUpperCase}" -> BusinessRules.antiByProcessId,
    "EDITED_INPUT->DATA_INPUT"                         -> BusinessRules.antiByCompositeKeys
  ).withDefaultValue(BusinessRules.identity)

  // Logs equivalentes a los de tu código original, pero sin ramificación en línea:
  val ruleLog: Map[String, String] = Map(
    s"MKT_ET_DATA_ST->${targetTable.trim.toUpperCase}" -> s"[STRESSTEST] - Filtramos datos cuyos process_id ya existan en $table en business.",
    "EDITED_INPUT->DATA_INPUT"                         -> s"[SAST] Case EDITED_INPUT -> DATA_INPUT"
  )
  ruleLog.get(ruleKey).foreach(msg => log.info(msg))

  val toSave = businessRules(ruleKey)(toSave1, targetTableDF)

  // ------------------- Guard clause de escritura (sin else) --------------------------------
  // Evita count(); mismo efecto práctico que "count()>0" con menor coste y complejidad.
  val hayDatos = toSave.take(1).nonEmpty
  if (!hayDatos) {
    log.info(s"[STRESSTEST] - Todos los datos a guardar ya existen en $table (o ninguno nuevo).")
    return
  }

  log.info(s"[STRESSTEST] - Encontrados datos nuevos a guardar en $table")
  toSave
    .repartition(1)
    .write
    .format("parquet")
    .mode(SaveMode.Append)
    .partitionBy(partitionsName: _*)
    .saveAsTable(table)
}
