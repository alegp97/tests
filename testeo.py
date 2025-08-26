import org.apache.log4j.LogManager
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.hadoop.fs.{FileSystem, Path}

// ADAPTADO A BDRUtils: usa BDRUtils.<campo>._1 (nuevo nombre) y literales cond_* / tablas.
object BDRFlowsJob {

  val log = LogManager.getLogger(getClass.getName)

  // Atajo: columna por “nuevo nombre” definido en BDRUtils (tupla (nuevo, original))
  private def c(t: (String, String)): Column = col(t._1)

  // ------------------------------ RUN (CC ≲ 15) ---------------------------------------------
  def run(sourcedb: String,
          targetdb: String,
          targetTableOptionalName: String,
          entities: List[IngestEntity],
          extra_filter: String,
          sourceTable: String,
          process: String,
          is_incremental: String)
         (implicit spark: SparkSession): Unit = {

    import spark.implicits._

    val source = s"$sourcedb.$sourceTable"
    val target = s"$targetdb.$targetTableOptionalName"
    val processType = s"$process"
    val isIncremental = s"$is_incremental"

    log.info(s"[SAST] source  : $source")
    log.info(s"[SAST] target  : $target")
    log.info(s"[SAST] isIncremental : $isIncremental")
    log.info(s"[SAST] process : $processType")

    // Cargamos contrato desde tmpPath (igual que tu código)
    val spContractDF = spark.sqlContext.read.format("parquet").load(BDRUtils.tmpPath)

    // PARTICIONES por tabla de starting_points_contract → rango [min,max]
    val fechasParticiones = fechasDeParticion(targetdb)
    val (minFecha, maxFecha) = (fechasParticiones.head, fechasParticiones.last)

    // DATETIME en SCALA → intervalo mensual (BDRUtils.INTERVALO_CALCULO)
    val fechasFiltro = construirFechasFiltro(fechasParticiones, BDRUtils.INTERVALO_CALCULO)

    // Nos quedamos con los registros de las fechas calculadas
    val dfFechas = spContractDF.where(c(BDRUtils.fecha).isin(fechasFiltro: _*))

    // Duplicamos el registro de fecha mínima por contrato y desplazamos su fecha a la siguiente
    val dfMinFix   = duplicarMinYDesplazarFecha(dfFechas)

    // Metemos row_number y “momento” (beg/end) por contrato
    val dfMomento  = addRowNumberYMomento(dfMinFix)

    // Cambiamos stage del beg por el del end anterior (lag)
    val dfStage    = recalcularStageBegConLag(dfMomento)

    // EAD/PROVOS con lag según beg/end (usa ead_ifrs/prov_ifrs de BDRUtils)
    val dfEads     = calcularEadsYProvisiones(dfStage)

    // Flujos S1↔S2, S1/S3 y S2/S3, y S3↔S1/2 (compactados)
    val dfFlows12  = flujosS1S2(dfEads)
    val dfFlows13  = flujosS1S3yS2S3(dfFlows12)
    val dfFlows31  = flujosS3S1yS3S2(dfFlows13)

    // Preview_TR: first/last EAD por contrato y first/lastStage
    val dfPrevTR   = calcularPreviewTR(dfFlows31)

    // Transferencias TR (todas *_FLOW_TR_BEG/END con condición base común)
    val dfFlowsTR  = flujosTransferenciasTR(dfPrevTR)

    // Segmentación COREP (texto COREP original + última fecha del intervalo)
    val dfCorep    = segmentacionCorep(dfFlowsTR)

    // Reordenación de columnas y sort final
    val dfFinal    = reordenarYOrdenar(dfCorep)

    // Guardamos en destino
    println(s"RESULTADO GUARDADO EN : $source")
    dfFinal.write.mode(SaveMode.Overwrite).format("parquet").saveAsTable(s"$source")

    // Borramos tmp HDFS
    borrarTmpHDFS(BDRUtils.tmpPath)
  }
  // -------------------------------------------------------------------------------------------

  // ============================ HELPERS (pequeños y puros) ===================================

  /** Lee show partitions y devuelve lista de fechas ascendente (yyyy-MM-dd). */
  private def fechasDeParticion(targetdb: String)(implicit spark: SparkSession): List[String] =
    spark.sqlContext
      .sql(s"show partitions $targetdb.${BDRUtils.starting_points_contract}")
      .map(r => r.getString(0))
      .collect()
      .toList
      .flatMap(_.split("=", 2).lift(1))
      .filter(_.matches("\\d{4}-\\d{2}-\\d{2}"))
      .sorted

  /** Genera las fechas del intervalo con paso mensual (o el paso que dicte INTERVALO_CALCULO). */
  private def construirFechasFiltro(fechasPart: List[String], mesesPaso: Int): List[String] = {
    val fmt = java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd")
    val minD = java.time.LocalDate.parse(fechasPart.head, fmt)
    val maxD = java.time.LocalDate.parse(fechasPart.last, fmt)
    Iterator.iterate(minD)(_.plusMonths(mesesPaso.toLong))
      .takeWhile(!_.isAfter(maxD))
      .map(_.format(fmt))
      .toList
  }

  /** Duplica min(fecha) por contrato, marca 'dup' y mueve esa fecha al siguiente día de partición. */
  private def duplicarMinYDesplazarFecha(df: DataFrame)(implicit spark: SparkSession): DataFrame = {
    val wContrato = Window.partitionBy(c(BDRUtils.id_contrato))
    val minFecha = min(c(BDRUtils.fecha)).over(wContrato)
    val wOrden   = wContrato.orderBy(c(BDRUtils.fecha).asc)

    val dfDup = df.withColumn("dup", when(c(BDRUtils.fecha) === minFecha, lit("DUPLICAR")))
    val dfWithNext = dfDup.withColumn("nextDate", lead(c(BDRUtils.fecha), 1).over(wOrden))

    dfWithNext
      .withColumn("newFecha", when(col("dup") === "DUPLICAR", col("nextDate")).otherwise(c(BDRUtils.fecha)))
      .drop("dup", "nextDate")
      .withColumnRenamed("newFecha", BDRUtils.fecha._1)
  }

  /** Añade row_number por contrato y columna 'momento' beg/end. */
  private def addRowNumberYMomento(df: DataFrame): DataFrame = {
    val w = Window.partitionBy(c(BDRUtils.id_contrato)).orderBy(c(BDRUtils.fecha).asc)
    df.withColumn("row_number", row_number().over(w))
      .withColumn("momento", when(col("row_number") % 2 === 0, lit("beg")).otherwise(lit("end")))
  }

  /** Cambia stage del beg por el del end anterior (lag) y deja stageFinal. */
  private def recalcularStageBegConLag(df: DataFrame): DataFrame = {
    val wAsc = Window.partitionBy(c(BDRUtils.id_contrato)).orderBy(c(BDRUtils.fecha).asc)
    val lagStage = lag(c(BDRUtils.stage), 1).over(wAsc)
    df.withColumn("stageAux",  when(col("momento") === "beg", lagStage).otherwise(c(BDRUtils.stage)))
      .withColumn("stageFinal",when(col("momento") === "beg", col("stageAux")).otherwise(c(BDRUtils.stage)))
      .drop("stageAux")
  }

  /** Calcula columnas EAD y PROVOS según beg/end con lag correspondiente. */
  private def calcularEadsYProvisiones(df: DataFrame): DataFrame = {
    val w = Window.partitionBy(c(BDRUtils.id_contrato)).orderBy(col("row_number").asc)
    val lagEad  = lag(c(BDRUtils.ead_ifrs), 1).over(w)
    val lagProv = lag(c(BDRUtils.prov_ifrs), 1).over(w)

    df.withColumn("EAD_S1_IFRS", when(col("momento") === "beg", lagEad).otherwise(c(BDRUtils.ead_ifrs)))
      .withColumn("EAD_S2_IFRS", when(col("momento") === "beg", lagEad).otherwise(c(BDRUtils.ead_ifrs)))
      .withColumn("EAD_S3_IFRS", when(col("momento") === "beg", lagEad).otherwise(c(BDRUtils.ead_ifrs)))
      .withColumn("PROV_S1_IFRS", when(col("momento") === "beg", lagProv).otherwise(c(BDRUtils.prov_ifrs)))
      .withColumn("PROV_S2_IFRS", when(col("momento") === "beg", lagProv).otherwise(c(BDRUtils.prov_ifrs)))
      .withColumn("PROV_S3_IFRS", when(col("momento") === "beg", lagProv).otherwise(c(BDRUtils.prov_ifrs)))
      // Limpieza de NULLS
      .withColumn("EAD_S1_IFRS", when(col("EAD_S1_IFRS").isNull, lit(0)).otherwise(col("EAD_S1_IFRS")))
      .withColumn("EAD_S2_IFRS", when(col("EAD_S2_IFRS").isNull, lit(0)).otherwise(col("EAD_S2_IFRS")))
      .withColumn("EAD_S3_IFRS", when(col("EAD_S3_IFRS").isNull, lit(0)).otherwise(col("EAD_S3_IFRS")))
  }

  // ---------- Flujos compactados (S1/S2, S1/S3-S2/S3, S3/S1-S3/S2) ---------------------------

  private def flujosS1S2(df: DataFrame): DataFrame = {
    val w = Window.partitionBy(c(BDRUtils.id_contrato)).orderBy(col("row_number").asc)
    val lagStage = lag(c(BDRUtils.stage), 1).over(w)

    df.withColumn("ind_S2_S1", when(col("momento") === "end" && c(BDRUtils.stage) === 1 && (lagStage === 2), lit(1)).otherwise(lit(0)))
      .withColumn("s1_s2_FLOW", when(col("momento") === "end" && col("ind_S2_S1") === 1, col("EAD_S1_IFRS")).otherwise(lit(0)))
      .withColumn("ind_S1_S2", when(col("momento") === "end" && c(BDRUtils.stage) === 2 && (lagStage === 1), lit(1)).otherwise(lit(0)))
      .withColumn("s1_S2_FLOW", when(col("momento") === "end" && col("ind_S1_S2") === 1, col("EAD_S2_IFRS")).otherwise(lit(0)))
  }

  private def flujosS1S3yS2S3(df: DataFrame): DataFrame = {
    val w = Window.partitionBy(c(BDRUtils.id_contrato)).orderBy(col("row_number").asc)
    val lagStage = lag(c(BDRUtils.stage), 1).over(w)

    df.withColumn("ind_S1_S3", when(col("momento") === "end" && c(BDRUtils.stage) === 3 && (lagStage === 1), lit(1)).otherwise(lit(0)))
      .withColumn("s1_s3_FLOW", when(col("momento") === "end" && col("ind_S1_S3") === 1, col("EAD_S3_IFRS")).otherwise(lit(0)))
      .withColumn("ind_S2_S3", when(col("momento") === "end" && c(BDRUtils.stage) === 3 && (lagStage === 2), lit(1)).otherwise(lit(0)))
      .withColumn("s2_s3_FLOW", when(col("momento") === "end" && col("ind_S2_S3") === 1, col("EAD_S3_IFRS")).otherwise(lit(0)))
  }

  private def flujosS3S1yS3S2(df: DataFrame): DataFrame = {
    val w = Window.partitionBy(c(BDRUtils.id_contrato)).orderBy(col("row_number").asc)
    val lagStage = lag(c(BDRUtils.stage), 1).over(w)

    df.withColumn("ind_S3_S1", when(col("momento") === "end" && c(BDRUtils.stage) === 1 && (lagStage === 3), lit(1)).otherwise(lit(0)))
      .withColumn("s3_s1_FLOW", when(col("momento") === "end" && col("ind_S3_S1") === 1, lag(col("EAD_S3_IFRS"), 1).over(w)).otherwise(lit(0)))
      .withColumn("ind_S3_S2", when(col("momento") === "end" && c(BDRUtils.stage) === 2 && (lagStage === 3), lit(1)).otherwise(lit(0)))
      .withColumn("s3_s2_FLOW", when(col("momento") === "end" && col("ind_S3_S2") === 1, lag(col("EAD_S2_IFRS"), 1).over(w)).otherwise(lit(0)))
  }

  // ---------- Preview_TR (first/last por ventana beg/end) -----------------------------------

  private def calcularPreviewTR(df: DataFrame): DataFrame = {
    val wBeg = Window.partitionBy(c(BDRUtils.id_contrato)).orderBy(col("momento") === "beg")
    val wEnd = Window.partitionBy(c(BDRUtils.id_contrato)).orderBy(col("momento") === "end")

    df.withColumn("firstBegS1", first(col("EAD_S1_IFRS")).over(wBeg))
      .withColumn("firstBegS2", first(col("EAD_S2_IFRS")).over(wBeg))
      .withColumn("firstBegS3", first(col("EAD_S3_IFRS")).over(wBeg))
      .withColumn("lastEndS1", last(col("EAD_S1_IFRS")).over(wEnd))
      .withColumn("lastEndS2", last(col("EAD_S2_IFRS")).over(wEnd))
      .withColumn("lastEndS3", last(col("EAD_S3_IFRS")).over(wEnd))
      .withColumn("firstStage", first(c(BDRUtils.stage)).over(wBeg))
      .withColumn("lastStage",  last(c(BDRUtils.stage)).over(wEnd))
  }

  // ---------- Transferencias TR (todas las *_FLOW_TR_BEG/END compactadas) --------------------

  private def flujosTransferenciasTR(df: DataFrame): DataFrame = {
    val minC = lit(BDRUtils.cond_minFeperac)
    val maxC = lit(BDRUtils.cond_maxFeperac)

    def condTR(stFirst: Int, stLast: Int, momentoBeg: Boolean): Column = {
      val base = (col("minFeperac") === minC) && (col("maxFeperac") === maxC) &&
                 (col("firstStage") === lit(stFirst)) && (col("lastStage") === lit(stLast))
      if (momentoBeg) base && (col("momento") === "beg") else base && (col("momento") === "end")
    }

    df
      .withColumn("S1_S2_FLOW_TR_BEG", when(condTR(1, 2, momentoBeg = true),  col("firstBegS1")).otherwise(lit(0)))
      .withColumn("S1_S2_FLOW_TR_END", when(condTR(1, 2, momentoBeg = false), col("lastEndS2")).otherwise(lit(0)))
      .withColumn("S1_S3_FLOW_TR_BEG", when(condTR(1, 3, momentoBeg = true),  col("firstBegS1")).otherwise(lit(0)))
      .withColumn("S1_S3_FLOW_TR_END", when(condTR(1, 3, momentoBeg = false), col("lastEndS3")).otherwise(lit(0)))
      .withColumn("S2_S3_FLOW_TR_BEG", when(condTR(2, 3, momentoBeg = true),  col("firstBegS2")).otherwise(lit(0)))
      .withColumn("S2_S3_FLOW_TR_END", when(condTR(2, 3, momentoBeg = false), col("lastEndS3")).otherwise(lit(0)))
      .withColumn("S2_S1_FLOW_TR_BEG", when(condTR(2, 1, momentoBeg = true),  col("firstBegS2")).otherwise(lit(0)))
      .withColumn("S2_S1_FLOW_TR_END", when(condTR(2, 1, momentoBeg = false), col("lastEndS1")).otherwise(lit(0)))
      .withColumn("S3_S1_FLOW_TR_BEG", when(condTR(3, 1, momentoBeg = true),  col("firstBegS3")).otherwise(lit(0)))
      .withColumn("S3_S1_FLOW_TR_END", when(condTR(3, 1, momentoBeg = false), col("lastEndS1")).otherwise(lit(0)))
      .withColumn("S3_S2_FLOW_TR_BEG", when(condTR(3, 2, momentoBeg = true),  col("firstBegS3")).otherwise(lit(0)))
      .withColumn("S3_S2_FLOW_TR_END", when(condTR(3, 2, momentoBeg = false), col("lastEndS2")).otherwise(lit(0)))
  }

  // ---------- Segmentación COREP -------------------------------------------------------------

  private def segmentacionCorep(df: DataFrame): DataFrame = {
    val segCorepText =
      when(c(BDRUtils.categoria) === 16 && c(BDRUtils.subcategoria) === 16 && c(BDRUtils.flag_gar) > 0, lit(BDRUtils.segCorep_text_SM_Sec))
        .when(c(BDRUtils.categoria) === 16 && c(BDRUtils.subcategoria) === 16 && c(BDRUtils.flag_gar) === 0, lit(BDRUtils.segCorep_text_SM_NSec))
        .when(c(BDRUtils.categoria) === 16 && c(BDRUtils.subcategoria) === 0  && c(BDRUtils.flag_gar) > 0, lit(BDRUtils.segCorep_text_Other_Sec))
        .when(c(BDRUtils.categoria) === 16 && c(BDRUtils.subcategoria) === 0  && c(BDRUtils.flag_gar) === 0, lit(BDRUtils.segCorep_text_Other_NSec))
        .when(c(BDRUtils.flag_sme) === 1, lit(BDRUtils.segCorep_text_Other_SME))
        .otherwise(lit(BDRUtils.segCorep_text_Other))

    // “Fecha final” para COREP en el fin del intervalo
    val maxC = lit(BDRUtils.cond_maxFeperac)
    df.withColumn("seg_COREP", segCorepText)
      .withColumn("seg_COREP_DateFinal",
        when(c(BDRUtils.fecha) === maxC && col("momento") === "end", c(BDRUtils.fecha)).otherwise(lit(null))
      )
  }

  // ---------- Orden/selección de columnas -----------------------------------------------------

  private def reordenarYOrdenar(df: DataFrame): DataFrame = {
    val reorderedColumnNames: Array[String] = Array(
      BDRUtils.fecha._1,"momento","empresa",BDRUtils.id_contrato._1,"stage",
      "seg_COREP","seg_COREP_Final","EAD_S1_IFRS","EAD_S2_IFRS","EAD_S3_IFRS",
      "PROV_S1_IFRS","PROV_S2_IFRS","PROV_S3_IFRS",
      "ind_S1_S2","s1_S2_FLOW","ind_S1_S3","s1_s3_FLOW","ind_S2_S3","s2_s3_FLOW",
      "S1_S2_FLOW_TR_BEG","S1_S2_FLOW_TR_END","S1_S3_FLOW_TR_BEG","S1_S3_FLOW_TR_END",
      "S2_S3_FLOW_TR_BEG","S2_S3_FLOW_TR_END","S2_S1_FLOW_TR_BEG","S2_S1_FLOW_TR_END",
      "S3_S1_FLOW_TR_BEG","S3_S1_FLOW_TR_END","S3_S2_FLOW_TR_BEG","S3_S2_FLOW_TR_END",
      "row_number"
    )
    df.sort(asc(BDRUtils.id_contrato._1), asc(BDRUtils.fecha._1), asc("row_number"))
      .select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)
  }

  // ---------- Limpieza tmp HDFS ---------------------------------------------------------------

  private def borrarTmpHDFS(tmpPath: String)(implicit spark: SparkSession): Unit = {
    val fs = org.apache.hadoop.fs.FileSystem.get(spark.sparkContext.hadoopConfiguration)
    val pathFile = new Path(tmpPath)
    if (fs.exists(pathFile)) fs.delete(pathFile, true)
  }
}
