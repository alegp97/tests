val fields_dict_DF = spark.sqlContext.table(s"$sourcedb.fields_dict")
  .where(col("data_date_part") === value)
  .where(lower(col("target_table")) === prefix_table.toLowerCase())
  .where(lower(col("src_name")).isin("execution_def", "st_metrics_input"))






object PuntosPartidaJob {

  private val log = LogManager.getLogger(getClass.getName)

  // ------------------------------- RUN (≤ 15 CC) ---------------------------------------------
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

    // 1) Nombres base y diccionario
    val table  = dstTableName(targetdb, targetTable, targetTableOptionalName)
    val source = s"$sourcedb.$sourceTable"
    val value  = latestFieldsDictValue(sourcedb)

    // 2) Query de columnas desde fields_dict
    val columnsVariablesDF = loadColumnsVariablesDF(sourcedb, sourceTable, targetTable, process, value)
    val query              = buildQuery(columnsVariablesDF)
    log.info(s"[SAST] Query size: ${query.size}")

    // 3) Origen + exec_id + ORIGINAL_VALUE opcional
    val filterTable                   = spark.sqlContext.table(source)
    val (sa_exec_id_column, hasStamp) = stampColumnAndFlag(filterTable)
    val original_value_colist         = originalValueColIfNeeded(filterTable, sourceTable, targetTable)
      .getOrElse(query)

    // 4) Predicado de entidades / incremental (sin if inline)
    val baseDF = entitiesPredicate(entities, is_incremental, hasStamp)
      .map(pred => filterTable.where(pred))
      .getOrElse(filterTable)

    // 5) Proyección + filtro extra opcional
    val withExecId = baseDF.select((sa_exec_id_column ++ original_value_colist): _*)
    val toSave0    = Option(extra_filter).map(_.trim).filter(_.nonEmpty) match {
      case Some(f) =>
        log.info(s"[SAST] Aplicando filtro extra : $f")
        withExecId.where(f)
      case None    => withExecId
    }

    // 6) Particiones y alineación
    val partitionsName = partitionsForWrite(targetdb, targetTable, targetTableOptionalName)
    log.info(s"[SAST] partitionsName: $partitionsName")
    val targetTableDF  = spark.sqlContext.table(table)
    val toSave1        = alignedToTargetSchema(toSave0, targetTableDF)

    // 7) Reglas especiales como estrategias (sin cadenas de if/else)
    val ruleKey = s"${sourceTable.trim.toUpperCase}->${targetTable.trim.toUpperCase}"
    val toSave  = businessRules(ruleKey)(toSave1, targetTableDF)
    ruleLogs.get(ruleKey).foreach(msg => log.info(msg))

    // 8) Guard clause de escritura (evita else y evita count())
    if (!hasAnyRow(toSave)) {
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
  // -------------------------------------------------------------------------------------------

  // ----------------------------- Estrategias de negocio --------------------------------------
  private type Rule = (DataFrame, DataFrame) => DataFrame

  private val businessRules: Map[String, Rule] = Map(
    // Caso 1: MKT_ET_DATA_ST -> (cualquier destino actual). Anti-join por process_id
    // Nota: clave la parte izquierda (sourceTable) tal como sale en tu lógica
    "MKT_ET_DATA_ST->MKT_ET_DATA_ST" -> antiByProcessId, // por si destino coincide
    "MKT_ET_DATA_ST->DATA_INPUT"     -> antiByProcessId,
    // Caso 2: EDITED_INPUT -> DATA_INPUT. Anti-join por claves compuestas
    "EDITED_INPUT->DATA_INPUT"       -> antiByCompositeKeys
  ).withDefaultValue(identityRule)

  private val ruleLogs: Map[String, String] = Map(
    "MKT_ET_DATA_ST->MKT_ET_DATA_ST" -> "[STRESSTEST] - Filtramos datos cuyos process_id ya existan en business.",
    "MKT_ET_DATA_ST->DATA_INPUT"     -> "[STRESSTEST] - Filtramos datos cuyos process_id ya existan en business.",
    "EDITED_INPUT->DATA_INPUT"       -> "[SAST] Case EDITED_INPUT -> DATA_INPUT"
  )

  private val identityRule: Rule = (df, _) => df

  private val antiByProcessId: Rule = (df, tgt) => {
    val right = tgt.select(col("process_id")).distinct()
    df.join(right, Seq("process_id"), "left_anti")
  }

  private val antiByCompositeKeys: Rule = (df, tgt) => {
    val keys = Seq(
      "country","business_unit","exercise","input_version","datagen_timestamp",
      "edit_version","edit_timestamp","end_date","dataset","dataset_family",
      "granularity_type","granularity","context_id"
    )
    df.join(tgt, keys, "left_anti")
  }

  // ----------------------------- Helpers (según tus capturas) -------------------------------

  /** Nombre final de tabla destino (mantiene tu semántica). */
  private def dstTableName(targetdb: String, targetTable: String, targetTableOptionalName: String): String =
    if (Option(targetTableOptionalName).forall(_.isEmpty)) s"$targetdb.$targetTable"
    else s"$targetdb.$targetTableOptionalName"

  /** Último valor de partición en fields_dict (data_date_part). */
  private def latestFieldsDictValue(sourcedb: String)(implicit spark: SparkSession): Option[String] =
    Try {
      spark.sqlContext
        .sql(s"show partitions $sourcedb.fields_dict")
        .orderBy(col("partition").desc)
        .limit(1)
        .collect()
        .headOption
        .map(_.getString(0).split("=", 2)(1))
    }.toOption.flatten

  /** Carga y normaliza columnas desde fields_dict con filtros por tabla origen/destino/proceso. */
  private def loadColumnsVariablesDF(
    sourcedb: String,
    sourceTable: String,
    targetTable: String,
    process: String,
    value: Option[String]
  )(implicit spark: SparkSession): DataFrame = {
    val base = spark.sqlContext.table(s"$sourcedb.fields_dict")
      .where(lower(col("target_table")) === lit(targetTable))
      .where(lower(col("src_name")) === lit(sourceTable))
      .where(lower(col("process")) === lit(process))

    val withDate = value.map(v => base.where(col("data_date_part") === lit(v))).getOrElse(base)

    withDate.select(
      trim(lower(col("fld_name"))).as("fld_name"),
      trim(lower(col("src_fld_header"))).as("src_fld_header"),
      trim(lower(col("src_data_dim1"))).as("src_data_dim1"),
      trim(lower(col("src_data_dim1_value"))).as("src_data_dim1_value"),
      trim(lower(col("src_data_dim2"))).as("src_data_dim2"),
      trim(lower(col("src_data_dim2_value"))).as("src_data_dim2_value"),
      trim(lower(col("src_data_dim3"))).as("src_data_dim3"),
      trim(lower(col("src_data_dim3_value"))).as("src_data_dim3_value"),
      trim(lower(col("src_data_dim4"))).as("src_data_dim4"),
      trim(lower(col("src_data_dim4_value"))).as("src_data_dim4_value"),
      trim(lower(col("src_data_dim5"))).as("src_data_dim5"),
      trim(lower(col("src_data_dim5_value"))).as("src_data_dim5_value")
    ).distinct()
  }

  /** Construye la lista de columnas de consulta (usa tu util BoardGenericUtil). */
  private def buildQuery(colsDF: DataFrame): List[Column] =
    colsDF
      .collect()
      .groupBy(_.getAs[String]("fld_name"))
      .map { case (_, rows) => BoardGenericUtil.buildCasesQuery(rows) }
      .toList

  /** Devuelve (colsExecId, tieneColumnaDeStamp) según exista data_timestamp_part / data_date_part. */
  private def stampColumnAndFlag(tbl: DataFrame): (List[Column], Boolean) = {
    val hasTs  = tbl.columns.contains("data_timestamp_part")
    val hasDay = tbl.columns.contains("data_date_part")
    val chosen =
      if (hasTs) "data_timestamp_part"
      else if (hasDay) "data_date_part"
      else ""
    val cols = if (chosen.isEmpty) Nil else List(col(chosen).as("sa_exec_id"))
    (cols, hasTs || hasDay)
  }

  /** ORIGINAL_VALUE solo para (SAE_OVERRIDE_EVENT_ST -> AUDIT_TABLE). */
  private def originalValueColIfNeeded(filterTable: DataFrame, sourceTable: String, targetTable: String): Option[List[Column]] = {
    val isAudit = "SAE_OVERRIDE_EVENT_ST".equalsIgnoreCase(sourceTable) && "AUDIT_TABLE".equalsIgnoreCase(targetTable)
    if (!isAudit) None
    else {
      val attrs = filterTable.columns.filter(_.startsWith("attribute_"))
      Some(casesOriginalValue(attrs) :: Nil)
    }
  }

  /** Construye la columna ORIGINAL_VALUE con fold (sin var/if anidados). */
  private def casesOriginalValue(columns: Array[String]): Column = {
    val base = when(lower(col("original_value_field")) === lit(columns.head.toLowerCase), col(columns.head))
    columns.tail.foldLeft(base) { (acc, c) =>
      acc.when(lower(col("original_value_field")) === lit(c.toLowerCase), col(c))
    }.otherwise(lit(null)).as("original_value")
  }

  /** Predicado de entidades: OR de valores por timestamp o date según exista columna. */
  private def entitiesPredicate(entities: List[IngestEntity], is_incremental: String, hasStampCol: Boolean): Option[Column] = {
    if (entities.isEmpty || is_incremental != "true") None
    else {
      val disj = entities.map { e =>
        if (hasStampCol) col("data_timestamp_part") === e.getDataTimestampPart
        else             col("data_date_part")      === e.getDataDatePart
      }.reduce(_ or _)
      Some(disj)
    }
  }

  /** Alinea esquema al destino: añade nulos, reordena columnas, hace distinct(). */
  private def alignedToTargetSchema(df: DataFrame, dst: DataFrame): DataFrame = {
    val srcCols = df.columns.map(_.toLowerCase).toList
    val dstCols = dst.columns.map(_.toLowerCase).toList
    val missing = dstCols.diff(srcCols)

    val filled = df
      .select(srcCols.map(c => col(s"`$c`")) ++ missing.map(c => lit(null).as(c)): _*)
      .select(dstCols.map(c => col(s"`$c`")): _*)

    filled.distinct()
  }

  /** Particiones para escribir, delegando en HiveUtil (según opcion de nombre). */
  private def partitionsForWrite(targetdb: String, targetTable: String, targetTableOptionalName: String): List[String] = {
    val opt = Option(targetTableOptionalName).filter(_.nonEmpty)
    opt.fold(HiveUtil.getPartitions(targetdb, targetTable))(n => HiveUtil.getPartitions(targetdb, n))
  }

  /** Chequeo de existencia sin count() para bajar complejidad y coste. */
  private def hasAnyRow(df: DataFrame): Boolean = df.take(1).nonEmpty
}
