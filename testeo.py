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

  val table = if (targetTableOptionalName == null || targetTableOptionalName.isEmpty) {
    s"${targetdb}.${targetTable}"
  } else {
    s"${targetdb}.${targetTableOptionalName}"
  }
  val source = s"${sourcedb}.${sourceTable}"
  val targetTableDF = spark.sqlContext.table(table)

  // ------------------------------------------------------------
  // Partición más reciente del diccionario
  // ------------------------------------------------------------
  val maxFieldsDict = spark.sqlContext
    .sql(s"show partitions ${sourcedb}.fields_dict")
    .orderBy(col("partition").desc)
    .limit(1)
    .collect()

  if (maxFieldsDict.length != 1) {
    log.info(s"[STRESSTEST] - No hay nada en el diccionario para la tabla ${table}")
    return
  }
  val value = maxFieldsDict(0).getString(0).split("=")(1)

  // ------------------------------------------------------------
  // Diccionario de campos y query
  // ------------------------------------------------------------
  val columnsVariablesDF =
    spark.sqlContext.table(s"${sourcedb}.fields_dict")
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

  if (columnsVariablesDF.limit(1).count() == 0) {
    log.info(s"[STRESSTEST] - No hay nada en el diccionario para la tabla ${table}")
    return
  }

  import org.apache.spark.sql.Row
  var query = columnsVariablesDF
    .collect()
    .groupBy(_.getAs[String]("fld_name"))
    .map { case (_, rows: Array[Row]) => BoardGenericUtil.buildCasesQuery(rows) }
    .toList

  log.info("[SAST] Query: " + query)

  val filterTable = spark.sqlContext.table(source)

  /***Consultar si la tabla tiene la columna data_timestamp_part o data_date_part (si la tiene en staging es incremental)*/
  val sa_exec_id_column =
    if (filterTable.columns.contains("data_timestamp_part")) {
      List(col("data_timestamp_part").as("sa_exec_id"))
    } else if (filterTable.columns.contains("data_date_part")) {
      List(col("data_date_part").as("sa_exec_id"))
    } else { List() }
  log.info("[SAST] sa_exec_id_column: " + sa_exec_id_column)

  val flg_partition_stg_timestamp = filterTable.columns.contains("data_timestamp_part")
  log.info("[SAST] flg_partition_stg_timestamp : " + flg_partition_stg_timestamp)

  // //SAST-1590 -SAST-1544: AUDIT_TABLE, lógica de campo ORIGINAL_VALUE
  var original_value_column: Column = null
  if ("SAVE_OVERRIDE_EVENT_ST".equalsIgnoreCase(sourceTable) && "AUDIT_TABLE".equalsIgnoreCase(targetTable)) {
    val columnsDynamic = filterTable.columns.filter(p => p.contains("_attribute_"))
    original_value_column = casesOriginalValue(columnsDynamic)
  }
  val original_value_colist = if (original_value_column == null) List() else List(original_value_column)
  log.info("[SAST] original_value_colist: " + original_value_colist)

  log.info("[SAST] entities: " + entities)

  /***toSave: Si usamos SparkLauncherActionDatabricks entities vendrá vacio, si usamos SparkLauncherStresstestAction el valor de entities no es vacio. Ambas son correctas*/
  var toSave = {
    if (!entities.isEmpty && is_incremental == "true") {
      // helper para cada entidad
      def entityPredicate(a: IngestEntity): Column =
        if (flg_partition_stg_timestamp)
          col("data_timestamp_part") === a.getDataTimestampPart
        else
          col("data_date_part") === a.getDataDatePart

      // logs por entidad
      entities.foreach(a => log.info("[SAST] entity to process: [" + a.toString() + "]"))

      // combinación de predicados con OR
      val where = entities.map(entityPredicate).reduceOption(_ or _).get

      filterTable.where(where).select((sa_exec_id_column ++ original_value_colist ++ query): _*)
    } else {
      log.info("[SAST] entities.isEmpty or is_incremental = false")
      filterTable.select((original_value_colist ++ query): _*)
    }
  }

  log.info("[SAST] toSave: " + toSave)

  if (extra_filter.trim() != "") {
    log.info("[SAST] Aplicando filtro extra : " + extra_filter)
    toSave = toSave.where(extra_filter)
  }

  log.info("[SAST] toSave2: " + toSave)

  var partitionsName: List[String] = List()
  if (targetTableOptionalName == null || targetTableOptionalName.isEmpty) {
    partitionsName = HiveUtil.getPartitions(targetdb, targetTable)
  } else {
    partitionsName = HiveUtil.getPartitions(targetdb, targetTableOptionalName)
  }
  log.info(s"[SAST] getPartitions: targetDb(${targetdb}) targetTable(${targetTable}) targetTableOptionalName(${targetTableOptionalName}) --> partitionsName: ${partitionsName}")

  if (query.size > 0) {
    val esquemaToSave   = toSave.columns.map(x => x.toLowerCase()).toList
    log.info("[SAST] esquemaToSave: " + esquemaToSave)
    val esquemaTablaDest = targetTableDF.columns.map(x => x.toLowerCase()).toList
    log.info("[SAST] esquemaTablaDest: " + esquemaTablaDest)
    val columnasSinValor = esquemaTablaDest.diff(esquemaToSave)
    log.info("[SAST] columnasSinValor: " + columnasSinValor)

    toSave = toSave
      .distinct
      .select(esquemaToSave.map(x => col("`" + x + "`")) ++ columnasSinValor.map(x => lit(null).as(x)) : _*)
      .select(esquemaTablaDest.map(x => col("`" + x + "`")): _*)
  }

  // //SAST-1589: Filtramos datos cuyos process_id ya existan en tablón de business
  if ("MKT_EI_DATA_ST".equalsIgnoreCase(sourceTable) && "MKT_EI_DATA_ST".equalsIgnoreCase(targetTable)) {
    if (targetTableDF.select(col("process_id")).distinct().limit(1).count() > 0) {
      log.info(s"[STRESSTEST] - Datos encontrados en ${targetTable} en business.")
      val existing = targetTableDF.select(col("process_id")).distinct()
      toSave = toSave.join(existing, Seq("process_id"), "left_anti")
    } else {
      log.info(s"[STRESSTEST] - Sin datos encontrados en ${targetTable} en business.")
    }
  }

  // //SAST-6951
  if ("EDITED_INPUT".equalsIgnoreCase(sourceTable) && "DATA_INPUT".equalsIgnoreCase(targetTable)) {
    log.info("[SAST] Case EDITED_INPUT -> DATA_INPUT")
    toSave = toSave.join(
      targetTableDF.select(
        col("country"), col("business_unit"), col("exercise"), col("input_version"),
        col("datagen_timestamp"), col("edit_version"), col("edit_timestamp"),
        col("end_date"), col("dataset"), col("dataset_family"),
        col("granularity_type"), col("granularity"), col("context_id")
      ).distinct(),
      Seq("country","business_unit","exercise","input_version","datagen_timestamp",
        "edit_version","edit_timestamp","end_date","dataset","dataset_family",
        "granularity_type","granularity","context_id"),
      "left_anti"
    )
  }

  if (toSave.count() > 0) {
    log.info(s"[STRESSTEST] - Encontrados datos nuevos a guardar en ${table} en business.")
    log.info(s"[STRESSTEST] - TABLE ("+targetdb+"."+targetTableOptionalName+") partitionsName: "+partitionsName)
    toSave.repartition(1)
      .write
      .format("parquet")
      .mode(SaveMode.Append)
      .partitionBy(partitionsName: _*)
      .saveAsTable(table)
  } else {
    log.info(s"[STRESSTEST] - Todos los datos a guardar han sido encontrados en ${table} en business.")
  }
}
