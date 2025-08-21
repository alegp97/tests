package com.santander.puntospartida

import org.apache.spark.sql.{Column, Row, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import scala.util.Try

object PuntosPartidaJob {

  private val log = org.apache.log4j.LogManager.getLogger(getClass.getName)

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

    // Sonarqube less cognitive complexity: encapsula decisión en helper, evita repetir condición
    def dstTableName: String =
      if (Option(targetTableOptionalName).forall(_.isEmpty))
        s"$targetdb.$targetTable"
      else
        s"$targetdb.$targetTableOptionalName"

    // Sonarqube less cognitive complexity: usa Option y Try, elimina if anidado y null checks
    def latestFieldsDictValue: Option[String] =
      Try {
        spark.sqlContext
          .sql(s"show partitions $sourcedb.fields_dict")
          .orderBy(col("partition").desc)
          .limit(1)
          .collect()
          .headOption
          .map(_.getString(0).split("=")(1))
      }.toOption.flatten

    // Sonarqube less cognitive complexity: concentra filtros y normaliza columnas en un solo paso
    def loadColumnsVariablesDF(value: Option[String]) = {
      val base = spark.sqlContext.table(s"$sourcedb.fields_dict")
        .where(lower(col("target_table")) === lit(targetTable))
        .where(lower(col("src_name")) === lit(sourceTable))
        .where(lower(col("process")) === lit(process))

      val withDate =
        value.map(v => base.where(col("data_date_part") === lit(v))).getOrElse(base)

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

    // Sonarqube less cognitive complexity: usa map en vez de for anidado, reduce ramificación
    def buildQuery(colsDF: org.apache.spark.sql.DataFrame): List[Column] = {
      colsDF
        .collect()
        .groupBy(_.getAs[String]("fld_name"))
        .map { case (_, rows) => BoardGenericUtil.buildCasesQuery(rows) }
        .toList
    }

    // Sonarqube less cognitive complexity: encapsula lógica en tupla, evita if/else repetidos
    def stampColumnAndFlag(tbl: org.apache.spark.sql.DataFrame): (List[Column], Boolean) = {
      val hasTs  = tbl.columns.contains("data_timestamp_part")
      val hasDay = tbl.columns.contains("data_date_part")
      val chosen = if (hasTs) "data_timestamp_part" else if (hasDay) "data_date_part" else ""
      val list   = if (chosen.isEmpty) Nil else List(col(chosen).as("sa_exec_id"))
      (list, hasTs || hasDay)
    }

    // Sonarqube less cognitive complexity: usa Option para encapsular lógica audit, evita null
    def originalValueColIfNeeded(filterTable: org.apache.spark.sql.DataFrame): Option[Column] = {
      val isAudit = "SAE_OVERRIDE_EVENT_ST".equalsIgnoreCase(sourceTable) &&
                    "AUDIT_TABLE".equalsIgnoreCase(targetTable)
      if (!isAudit) None
      else {
        val attrs = filterTable.columns.filter(_.startsWith("attribute_"))
        Some(casesOriginalValue(attrs))
      }
    }

    // Sonarqube less cognitive complexity: reduce bucles e ifs combinando con reduce
    def entitiesPredicate(hasStamp: Boolean): Option[Column] =
      if (entities.isEmpty || is_incremental != "true") None
      else {
        val disj = entities.map { e =>
          if (hasStamp) col("data_timestamp_part") === e.getDataTimestampPart
          else          col("data_date_part")      === e.getDataDatePart
        }.reduce(_ or _)
        Some(disj)
      }

    // Sonarqube less cognitive complexity: alinea columnas en un bloque único, evita ifs anidados
    def alignedToTargetSchema(df: org.apache.spark.sql.DataFrame, dst: org.apache.spark.sql.DataFrame): org.apache.spark.sql.DataFrame = {
      val srcCols = df.columns.map(_.toLowerCase).toList
      val dstCols = dst.columns.map(_.toLowerCase).toList
      val missing = dstCols.diff(srcCols)

      val filled = df
        .select(srcCols.map(c => col(s"`$c`")) ++ missing.map(c => lit(null).as(c)): _*)
        .select(dstCols.map(c => col(s"`$c`")): _*)

      filled.distinct()
    }

    // Sonarqube less cognitive complexity: encapsula en función, evita duplicar getPartitions
    def partitionsForWrite: List[String] = {
      val opt = Option(targetTableOptionalName).filter(_.nonEmpty)
      opt.fold(HiveUtil.getPartitions(targetdb, targetTable)) { tton =>
        HiveUtil.getPartitions(targetdb, tton)
      }
    }

    // ---------- pipeline ----------
    val table = dstTableName
    val source = s"$sourcedb.$sourceTable"
    val value  = latestFieldsDictValue

    val columnsVariablesDF = loadColumnsVariablesDF(value)
    val queryCols          = buildQuery(columnsVariablesDF)

    log.info(s"[SAST] Query size: ${queryCols.size}")

    val filterTable = spark.sqlContext.table(source)
    val (saExecCol, hasStampCol) = stampColumnAndFlag(filterTable)

    val selectCols = originalValueColIfNeeded(filterTable)
      .map(ov => ov :: queryCols)
      .getOrElse(queryCols)

    val baseDF = entitiesPredicate(hasStampCol)
      .map(pred => filterTable.where(pred))
      .getOrElse(filterTable)

    val withExecId = if (saExecCol.nonEmpty) baseDF.select((saExecCol ++ selectCols): _*)
                     else baseDF.select(selectCols: _*)

    val toSave0 =
      if (extra_filter.trim.nonEmpty) withExecId.where(extra_filter) else withExecId

    val partitionsName = partitionsForWrite
    log.info(s"[SAST] partitionsName: $partitionsName")

    val targetTableDF = spark.sqlContext.table(table)
    var toSave = alignedToTargetSchema(toSave0, targetTableDF)

    // Sonarqube less cognitive complexity: maneja reglas específicas con if planos, no anidados
    if ("MKT_ET_DATA_ST".equalsIgnoreCase(targetTable)) {
      val anti = targetTableDF.select(col("process_id")).distinct()
      toSave = toSave.join(anti, Seq("process_id"), "left_anti")
    }

    if ("EDITED_INPUT".equalsIgnoreCase(sourceTable) && "DATA_INPUT".equalsIgnoreCase(targetTable)) {
      val keys = Seq(
        "country","business_unit","exercise","input_version","datagen_timestamp",
        "edit_version","edit_timestamp","end_date","dataset","dataset_family",
        "granularity_type","granularity","context_id"
      )
      toSave = toSave.join(targetTableDF, keys, "left_anti")
    }

    // Sonarqube less cognitive complexity: usa guard clause para escritura, evita else innecesario
    if (toSave.count() > 0) {
      log.info(s"[STRESSTEST] - Encontrados datos nuevos a guardar en $table")
      toSave
        .repartition(1)
        .write
        .format("parquet")
        .mode(SaveMode.Append)
        .partitionBy(partitionsName: _*)
        .saveAsTable(table)
      return
    }
    log.info(s"[STRESSTEST] - Todos los datos a guardar ya existen en $table (o ninguno nuevo).")
  }

  // Sonarqube less cognitive complexity: pliega condiciones con foldLeft, evita ifs anidados
  private def casesOriginalValue(columns: Array[String]): Column = {
    val folded = columns.foldLeft(Option.empty[Column]) { (acc, f) =>
      val cond = lower(col("original_value_field")) === lit(f)
      val sel  = col(f)
      acc.map(_.when(cond, sel)).orElse(Some(when(cond, sel)))
    }
    folded.getOrElse(lit(null)).otherwise(null).as("original_value")
  }
}
