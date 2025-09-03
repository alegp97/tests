private def resolveTargetTable(targetdb: String, targetTable: String, targetTableOptionalName: String): String = {
  if (targetTableOptionalName == null || targetTableOptionalName.isEmpty) {
    s"${targetdb}.${targetTable}"
  } else {
    s"${targetdb}.${targetTableOptionalName}"
  }
}


private def lastFieldsDictPartition(sourcedb: String)(implicit spark: SparkSession): Option[String] = {
  val maxFieldsDict = spark.sqlContext
    .sql(s"show partitions ${sourcedb}.fields_dict")
    .orderBy(col("partition").desc)
    .limit(1)
    .collect()

  if (maxFieldsDict.length != 1) {
    None
  } else {
    Some(maxFieldsDict(0).getString(0).split("=")(1))
  }
}



private def loadColumnsVariablesDF(
  sourcedb: String,
  sourceTable: String,
  targetTable: String,
  process: String,
  value: String
)(implicit spark: SparkSession): DataFrame = {
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
}



private def isEmpty(df: DataFrame): Boolean = {
  df.limit(1).count() == 0
}


private def buildQuery(columnsVariablesDF: DataFrame): List[Column] = {
  import org.apache.spark.sql.Row
  columnsVariablesDF
    .collect()
    .groupBy(_.getAs[String]("fld_name"))
    .map { case (_, rows: Array[Row]) =>
      BoardGenericUtil.buildCasesQuery(rows)
    }.toList
}


private def selectExecIdCols(filterTable: DataFrame): List[Column] = {
  if (filterTable.columns.contains("data_timestamp_part")) {
    List(col("data_timestamp_part").as("sa_exec_id"))
  } else if (filterTable.columns.contains("data_date_part")) {
    List(col("data_date_part").as("sa_exec_id"))
  } else {
    List()
  }
}



private def originalValueColsIfAudit(
  sourceTable: String,
  targetTable: String,
  filterTable: DataFrame
): List[Column] = {
  var original_value_column: Column = null
  if ("SAVE_OVERRIDE_EVENT_ST".equalsIgnoreCase(sourceTable) &&
      "AUDIT_TABLE".equalsIgnoreCase(targetTable)) {
    val columnsDynamic = filterTable.columns.filter(_.contains("_attribute_"))
    original_value_column = casesOriginalValue(columnsDynamic)
  }
  if (original_value_column == null) List() else List(original_value_column)
}



private def buildToSave(
  filterTable: DataFrame,
  entities: List[IngestEntity],
  is_incremental: String,
  flg_partition_stg_timestamp: Boolean,
  sa_exec_id_column: List[Column],
  original_value_colist: List[Column],
  query: List[Column]
): DataFrame = {
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


private def applyExtraFilter(toSave: DataFrame, extra_filter: String): DataFrame = {
  if (extra_filter.trim() != "") {
    log.info("[SAST] Aplicando filtro extra : " + extra_filter)
    toSave.where(extra_filter)
  } else {
    toSave
  }
}


private def resolvePartitions(
  targetdb: String,
  targetTable: String,
  targetTableOptionalName: String
): List[String] = {
  if (targetTableOptionalName == null || targetTableOptionalName.isEmpty) {
    HiveUtil.getPartitions(targetdb, targetTable)
  } else {
    HiveUtil.getPartitions(targetdb, targetTableOptionalName)
  }
}


private def alignSchemaIfNeeded(
  toSave: DataFrame,
  targetTableDF: DataFrame,
  query: List[Column]
): DataFrame = {
  if (query.size > 0) {
    val esquemaToSave   = toSave.columns.map(x => x.toLowerCase()).toList
    log.info("[SAST] esquemaToSave: " + esquemaToSave)
    val esquemaTablaDest = targetTableDF.columns.map(x => x.toLowerCase()).toList
    log.info("[SAST] esquemaTablaDest: " + esquemaTablaDest)
    val columnasSinValor = esquemaTablaDest.diff(esquemaToSave)
    log.info("[SAST] columnasSinValor: " + columnasSinValor)

    toSave
      .distinct
      .select(esquemaToSave.map(x => col("`" + x + "`")) ++ columnasSinValor.map(x => lit(null).as(x)) : _*)
      .select(esquemaTablaDest.map(x => col("`" + x + "`")): _*)
  } else {
    toSave
  }
}



private def applyMarketRuleIfNeeded(
  sourceTable: String,
  targetTable: String,
  targetTableDF: DataFrame,
  toSave: DataFrame
): DataFrame = {
  if ("MKT_EI_DATA_ST".equalsIgnoreCase(sourceTable) &&
      "MKT_EI_DATA_ST".equalsIgnoreCase(targetTable)) {
    if (targetTableDF.select(col("process_id")).distinct().limit(1).count() > 0) {
      log.info(s"[STRESSTEST] - Datos encontrados en ${targetTable} en business.")
      val existing = targetTableDF.select(col("process_id")).distinct()
      toSave.join(existing, Seq("process_id"), "left_anti")
    } else {
      log.info(s"[STRESSTEST] - Sin datos encontrados en ${targetTable} en business.")
      toSave
    }
  } else {
    toSave
  }
}



private def applyEditedInputRuleIfNeeded(
  sourceTable: String,
  targetTable: String,
  targetTableDF: DataFrame,
  toSave: DataFrame
): DataFrame = {
  if ("EDITED_INPUT".equalsIgnoreCase(sourceTable) &&
      "DATA_INPUT".equalsIgnoreCase(targetTable)) {
    log.info("[SAST] Case EDITED_INPUT -> DATA_INPUT")
    toSave.join(
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
  } else {
    toSave
  }
}


private def writeIfNotEmpty(
  toSave: DataFrame,
  table: String,
  partitionsName: Seq[String],
  targetdb: String,
  targetTableOptionalName: String
): Unit = {
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


