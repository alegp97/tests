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

  val table         = resolveTargetTable(targetdb, targetTable, targetTableOptionalName)
  val source        = s"$sourcedb.$sourceTable"
  val targetTableDF = spark.sqlContext.table(table)

  val valueOpt = lastFieldsDictPartition(sourcedb)
  if (valueOpt.isEmpty) {
    log.info(s"[STRESSTEST] - No hay nada en el diccionario para la tabla $table")
    return
  }

  val columnsVariablesDF = loadColumnsVariablesDF(sourcedb, sourceTable, targetTable, process, valueOpt.get)
  if (isEmpty(columnsVariablesDF)) {
    log.info(s"[STRESSTEST] - No hay nada en el diccionario para la tabla $table")
    return
  }

  val query       = buildQuery(columnsVariablesDF)
  val filterTable = spark.sqlContext.table(source)
  val execCols    = selectExecIdCols(filterTable)
  val flgTS       = filterTable.columns.contains("data_timestamp_part")
  val origCols    = originalValueColsIfAudit(sourceTable, targetTable, filterTable)

  var toSave = buildToSave(filterTable, entities, is_incremental, flgTS, execCols, origCols, query)
  toSave     = applyExtraFilter(toSave, extra_filter)
  toSave     = alignSchemaIfNeeded(toSave, targetTableDF, query)
  toSave     = applyMarketRuleIfNeeded(sourceTable, targetTable, targetTableDF, toSave)
  toSave     = applyEditedInputRuleIfNeeded(sourceTable, targetTable, targetTableDF, toSave)

  val partitionsName = resolvePartitions(targetdb, targetTable, targetTableOptionalName)
  writeIfNotEmpty(toSave, table, partitionsName, targetdb, targetTableOptionalName)
}



// ========================= Helpers privados =========================

/** Resuelve la tabla destino en función de targetTableOptionalName */
private def resolveTargetTable(db: String, tbl: String, opt: String): String =
  if (opt == null || opt.isEmpty) s"$db.$tbl" else s"$db.$opt"

/** Última partición del diccionario fields_dict */
private def lastFieldsDictPartition(sourcedb: String)(implicit spark: SparkSession): Option[String] =
  spark.sqlContext
    .sql(s"show partitions $sourcedb.fields_dict")
    .orderBy(org.apache.spark.sql.functions.col("partition").desc)
    .limit(1)
    .collect()
    .headOption.map(_.getString(0).split("=").apply(1))

/** Carga el diccionario de campos para la combinación source/target/proceso */
private def loadColumnsVariablesDF(
  sourcedb: String, sourceTable: String, targetTable: String, process: String, value: String
)(implicit spark: SparkSession): DataFrame = {
  import org.apache.spark.sql.functions._
  spark.sqlContext.table(s"$sourcedb.fields_dict")
    .where(col("data_date_part") === value && lower(col("target_table")) === targetTable && lower(col("src_name")) === sourceTable)
    .where(lower(col("process")) === process)
    .select(
      trim(lower(col("fld_name"))).as("fld_name"),
      trim(lower(col("src_fld_header"))).as("src_fld_header"),
      trim(lower(col("src_data_dim"))).as("src_data_dim"),
      trim(lower(col("src_data_dim1"))).as("src_data_dim1"),
      trim(lower(col("src_data_dim2"))).as("src_data_dim2"),
      trim(lower(col("src_data_dim3"))).as("src_data_dim3"),
      trim(lower(col("src_data_dim4"))).as("src_data_dim4"),
      trim(lower(col("src_data_dim5"))).as("src_data_dim5"),
      trim(lower(col("src_data_dim1_value"))).as("src_data_dim1_value"),
      trim(lower(col("src_data_dim2_value"))).as("src_data_dim2_value"),
      trim(lower(col("src_data_dim3_value"))).as("src_data_dim3_value"),
      trim(lower(col("src_data_dim4_value"))).as("src_data_dim4_value"),
      trim(lower(col("src_data_dim5_value"))).as("src_data_dim5_value")
    ).distinct()
}

/** Comprueba si un DataFrame está vacío */
private def isEmpty(df: DataFrame): Boolean = df.limit(1).count() == 0

/** Construye la query de casos a partir del diccionario */
private def buildQuery(columnsVariablesDF: DataFrame): List[Column] = {
  import org.apache.spark.sql.Row
  columnsVariablesDF.collect()
    .groupBy(_.getAs[String]("fld_name"))
    .map { case (_, rows: Array[Row]) => BoardGenericUtil.buildCasesQuery(rows) }
    .toList
}

/***Consultar si la tabla tiene la columna data_timestamp_part o data_date_part (si la tiene en staging es incremental)*/
private def selectExecIdCols(filterTable: DataFrame): List[Column] = {
  import org.apache.spark.sql.functions._
  if (filterTable.columns.contains("data_timestamp_part"))
    List(col("data_timestamp_part").as("sa_exec_id"))
  else if (filterTable.columns.contains("data_date_part"))
    List(col("data_date_part").as("sa_exec_id"))
  else List()
}

/** //SAST-1590 -SAST-1544: AUDIT_TABLE, lógica de campo ORIGINAL_VALUE */
private def originalValueColsIfAudit(
  sourceTable: String, targetTable: String, filterTable: DataFrame
): List[Column] = {
  if (sourceTable.equalsIgnoreCase("SAVE_OVERRIDE_EVENT_ST") && targetTable.equalsIgnoreCase("AUDIT_TABLE")) {
    val columnsDynamic = filterTable.columns.filter(_.contains("_attribute_"))
    val original_value_column = casesOriginalValue(columnsDynamic)
    if (original_value_column == null) List() else List(original_value_column)
  } else List()
}

/***toSave: Si usamos SparkLauncherActionDatabricks entities vendrá vacio,
 * si usamos SparkLauncherStresstestAction el valor de entities no es vacio.
 * Ambas son correctas */
private def buildToSave(
  filterTable: DataFrame,
  entities: List[IngestEntity],
  is_incremental: String,
  flg_partition_stg_timestamp: Boolean,
  sa_exec_id_column: List[Column],
  original_value_colist: List[Column],
  query: List[Column]
): DataFrame = {
  import org.apache.spark.sql.functions._
  if (!entities.isEmpty && is_incremental == "true") {
    def entityPredicate(a: IngestEntity): Column =
      if (flg_partition_stg_timestamp)
        col("data_timestamp_part") === a.getDataTimestampPart
      else
        col("data_date_part") === a.getDataDatePart

    entities.foreach(a => log.info("[SAST] entity to process: [" + a.toString() + "]"))
    val where = entities.map(entityPredicate).reduceOption(_ or _).get
    filterTable.where(where).select((sa_exec_id_column ++ original_value_colist ++ query): _*)
  } else {
    log.info("[SAST] entities.isEmpty or is_incremental = false")
    filterTable.select((original_value_colist ++ query): _*)
  }
}

/** Aplica filtro extra si viene informado */
private def applyExtraFilter(df: DataFrame, extra_filter: String): DataFrame =
  if (extra_filter != null && extra_filter.trim != "") {
    log.info("[SAST] Aplicando filtro extra : " + extra_filter)
    df.where(extra_filter)
  } else df

/** Resuelve las particiones de la tabla destino */
private def resolvePartitions(
  targetdb: String, targetTable: String, targetTableOptionalName: String
): List[String] =
  if (targetTableOptionalName == null || targetTableOptionalName.isEmpty)
    HiveUtil.getPartitions(targetdb, targetTable)
  else
    HiveUtil.getPartitions(targetdb, targetTableOptionalName)

/** Alinea esquema de toSave con el de la tabla destino */
private def alignSchemaWithTarget(toSave: DataFrame, targetTableDF: DataFrame): DataFrame = {
  import org.apache.spark.sql.functions._
  val esquemaToSave    = toSave.columns.map(_.toLowerCase).toList
  log.info("[SAST] esquemaToSave: " + esquemaToSave)
  val esquemaTablaDest = targetTableDF.columns.map(_.toLowerCase).toList
  log.info("[SAST] esquemaTablaDest: " + esquemaTablaDest)
  val columnasSinValor = esquemaTablaDest.diff(esquemaToSave)
  log.info("[SAST] columnasSinValor: " + columnasSinValor)

  toSave.distinct
    .select(esquemaToSave.map(x => col("`" + x + "`")) ++ columnasSinValor.map(x => org.apache.spark.sql.functions.lit(null).as(x)) : _*)
    .select(esquemaTablaDest.map(x => col("`" + x + "`")): _*)
}

/** //SAST-1589: Filtramos datos cuyos process_id ya existan en tablón de business */
private def applyMarketRuleIfNeeded(
  sourceTable: String, targetTable: String, targetTableDF: DataFrame, toSave: DataFrame
): DataFrame = {
  import org.apache.spark.sql.functions._
  if (sourceTable.equalsIgnoreCase("MKT_EI_DATA_ST") && targetTable.equalsIgnoreCase("MKT_EI_DATA_ST")) {
    if (targetTableDF.select(col("process_id")).distinct().limit(1).count() > 0) {
      log.info(s"[STRESSTEST] - Datos encontrados en ${targetTable} en business.")
      val existing = targetTableDF.select(col("process_id")).distinct()
      toSave.join(existing, Seq("process_id"), "left_anti")
    } else {
      log.info(s"[STRESSTEST] - Sin datos encontrados en ${targetTable} en business.")
      toSave
    }
  } else toSave
}

/** //SAST-6951: lógica de filtrado EDITED_INPUT -> DATA_INPUT */
private def applyEditedInputRuleIfNeeded(
  sourceTable: String, targetTable: String, targetTableDF: DataFrame, toSave: DataFrame
): DataFrame = {
  import org.apache.spark.sql.functions._
  if (sourceTable.equalsIgnoreCase("EDITED_INPUT") && targetTable.equalsIgnoreCase("DATA_INPUT")) {
    log.info("[SAST] Case EDITED_INPUT -> DATA_INPUT")
    toSave.join(
      targetTableDF.select(
        col("country"), col("business_unit"), col("exercise"), col("input_version"),
        col("datagen_timestamp"), col("edit_version"), col("edit_timestamp"),
        col("end_date"), col("dataset"), col("dataset_family"),
        col("granularity_type"), col("granularity"), col("context_id")
      ).distinct(),
      Seq(
        "country","business_unit","exercise","input_version","datagen_timestamp",
        "edit_version","edit_timestamp","end_date","dataset","dataset_family",
        "granularity_type","granularity","context_id"
      ),
      "left_anti"
    )
  } else toSave
}

/** Escritura final si hay datos */
private def writeIfNotEmpty(
  df: DataFrame, table: String, partitions: Seq[String], targetdb: String, targetTableOptionalName: String
): Unit = {
  if (df.count() > 0) {
    log.info(s"[STRESSTEST] - Encontrados datos nuevos a guardar en ${table} en business.")
    log.info(s"[STRESSTEST] - TABLE ("+targetdb+"."+targetTableOptionalName+") partitionsName: "+partitions)
    df.repartition(1)
      .write
      .format("parquet")
      .mode(SaveMode.Append)
      .partitionBy(partitions: _*)
      .saveAsTable(table)
  } else {
    log.info(s"[STRESSTEST] - Todos los datos a guardar han sido encontrados en ${table} en business.")
  }
}
