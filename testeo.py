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

  // --- 1) Resolve table names & dictionary context (pure, no branching) -----------------------
  val table  = dstTableName(targetdb, targetTable, targetTableOptionalName)
  val source = s"$sourcedb.$sourceTable"
  val dictValueOpt = latestFieldsDictValue(sourcedb) // e.g., last data_date_part from fields_dict

  // --- 2) Build column-select query from fields_dict (linear flow) ----------------------------
  val columnsVarsDF = loadColumnsVariablesDF(sourcedb, sourceTable, targetTable, process, dictValueOpt)
  val queryCols     = buildQuery(columnsVarsDF)

  // --- 3) Load source & compute execId stamp + optional ORIGINAL_VALUE column -----------------
  val filterTable               = spark.sqlContext.table(source)
  val (saExecCols, hasStampCol) = stampColumnAndFlag(filterTable) // returns (execIdCols, hasTsOrDay)
  val selectCols                = originalValueColIfNeeded(filterTable, sourceTable, targetTable)
                                  .getOrElse(queryCols)

  // --- 4) Apply entity/incremental predicate when provided (no inline if) ---------------------
  val baseDF = entitiesPredicate(entities, is_incremental, hasStampCol)
    .map(pred => filterTable.where(pred))
    .getOrElse(filterTable)

  // --- 5) Project execId + business columns; add extra_filter when present --------------------
  // selecting (saExecCols ++ selectCols) works whether saExecCols is empty or not
  val projected  = baseDF.select((saExecCols ++ selectCols): _*)
  val filtered   = Option(extra_filter).map(_.trim).filter(_.nonEmpty)
                      .map(expr => projected.where(expr))
                      .getOrElse(projected)

  // --- 6) Align with target schema and compute partitions -------------------------------------
  val partitionsName = partitionsForWrite(targetdb, targetTable, targetTableOptionalName)
  val targetTableDF  = spark.sqlContext.table(table)
  val alignedDF      = alignedToTargetSchema(filtered, targetTableDF)

  // --- 7) Apply special business rules via strategy registry (no if/else chains) --------------
  // MKT_ET_DATA_ST  -> anti-join by process_id
  // EDITED_INPUT -> DATA_INPUT  -> anti-join by composite keys
  val ruleKey = s"${sourceTable.trim.toUpperCase}->${targetTable.trim.toUpperCase}"

  val businessRules: Map[String, (DataFrame, DataFrame) => DataFrame] = Map(
    s"MKT_ET_DATA_ST->${
      targetTable.trim.toUpperCase
    }" -> BusinessRules.antiByProcessId,
    "EDITED_INPUT->DATA_INPUT"         -> BusinessRules.antiByCompositeKeys
  ).withDefaultValue(BusinessRules.identity)

  val toSave = businessRules(ruleKey)(alignedDF, targetTableDF)

  // --- 8) Guard clause for write; avoid count() cost & extra branching ------------------------
  // Use head(1) to test for new rows with minimal action and no else branch.
  val hasRows = toSave.take(1).nonEmpty
  if (!hasRows) {
    log.info(s"[STRESSTEST] - No new rows to write for $table.")
    return
  }

  // --- 9) Persist ---------------------------------------------------------------------------
  toSave
    .repartition(1)
    .write
    .format("parquet")
    .mode(SaveMode.Append)
    .partitionBy(partitionsName: _*)
    .saveAsTable(table)

  log.info(s"[STRESSTEST] - Written new rows into $table.")
}
