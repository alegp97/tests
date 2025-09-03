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
