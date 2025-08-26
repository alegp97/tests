object BDRFlowsJob {

  val log = LogManager.getLogger(getClass.getName)

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

    // Cargamos tmpPath del contrato en parquet (igual que tu código)
    val spContractDF = spark.sqlContext.read.format("parquet").load(BDRUtils.tmpPath)

    // PARTICIONES por foeperac → obtenemos fechas ordenadas y el rango [min,max]
    val fechasParticiones = fechasDeParticion(targetdb)
    val (minFecha, maxFecha) = (fechasParticiones.head, fechasParticiones.last)

    // DATETIME en SCALA → construimos los hitos del intervalo con paso mensual (INTERVALO_CALCULO)
    val fechasFiltro = construirFechasFiltro(fechasParticiones, BDRUtils.INTERVALO_CALCULO)

    // Nos quedamos con los registros de las fechas calculadas
    val dfFechas = spContractDF.where(col(BDRUtils.fecha_1).isin(fechasFiltro: _*))

    // Duplicamos el registro con la fecha mínima por contrato y desplazamos fecha duplicada a la siguiente fecha
    val dfMinFix   = duplicarMinYDesplazarFecha(dfFechas)

    // Metemos row_number y “momento” (beg/end) por contrato ordenado por fecha
    val dfMomento  = addRowNumberYMomento(dfMinFix)

    // Cambiamos el stage de beg por el del end de la fecha anterior + renombrados auxiliares
    val dfStage    = recalcularStageBegConLag(dfMomento)

    // Lag EAD IFRS y PRVOS (se aplican sobre beg/end según reglas)
    val dfEads     = calcularEadsYProvisiones(dfStage)

    // Metemos flujos S1_S2 y S1_S2_FLOW + S1_S3/S2_S3 + S3_S1/S3_S2, etc. (bloques compactados)
    val dfFlows12  = flujosS1S2(dfEads)
    val dfFlows13  = flujosS1S3yS2S3(dfFlows12)
    val dfFlows31  = flujosS3S1yS3S2(dfFlows13)

    // Flujos Preview_TR: first/last de EAD por contrato entre beg/end
    val dfPrevTR   = calcularPreviewTR(dfFlows31)

    // Reglas de FLUJO *_FLOW_TR_BEG/END (cond_minFeperac / cond_maxFeperac, firstStage/lastStage)
    val dfFlowsTR  = flujosTransferenciasTR(dfPrevTR)

    // Segmento COREP (original + última fecha) y columnas auxiliares de fecha/país final
    val dfCorep    = segmentacionCorep(dfFlowsTR)

    // Reordenación de columnas y sort final (manteniendo tu lista)
    val dfFinal    = reordenarYOrdenar(dfCorep)

    // Guardamos en destino (mismos logs)
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
  private def duplicarMinYDesplazarFecha(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.Window
    val wByContrato = Window.partitionBy(BDRUtils.id_contrato_1)
    val minFecha = min(col(BDRUtils.fecha_1)).over(wByContrato)
    val dfDup = df.withColumn("dup", when(col(BDRUtils.fecha_1) === minFecha, lit("DUPLICAR")))
    val dfWithNext = dfDup.withColumn("nextDate",
      lead(col(BDRUtils.fecha_1), 1).over(wByContrato.orderBy(col(BDRUtils.fecha_1).asc))
    )
    dfWithNext
      .withColumn("newFecha",
        when(col("dup") === "DUPLICAR", col("nextDate")).otherwise(col(BDRUtils.fecha_1))
      )
      .drop("dup", "nextDate")
      .withColumnRenamed("newFecha", BDRUtils.fecha_1)
  }

  /** Añade row_number por contrato y columna 'momento' beg/end. */
  private def addRowNumberYMomento(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.Window
    val w = Window.partitionBy(BDRUtils.id_contrato_1).orderBy(col(BDRUtils.fecha_1).asc)
    df.withColumn("row_number", row_number().over(w))
      .withColumn("momento", when(col("row_number") % 2 === 0, lit("beg")).otherwise(lit("end")))
  }

  /** Cambia stage del beg por el del end anterior (lag) y deja stageFinal, ead ifrs aux, etc. */
  private def recalcularStageBegConLag(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.Window
    val wByContratoAsc = Window.partitionBy(BDRUtils.id_contrato_1).orderBy(col(BDRUtils.fecha_1).asc)
    val lagStage = lag(col(BDRUtils.stage_1), 1).over(wByContratoAsc)
    df.withColumn("stageAux",
        when(col("momento") === "beg", lagStage).otherwise(col(BDRUtils.stage_1))
      )
      .withColumn("stageFinal",
        when(col("momento") === "beg", col("stageAux")).otherwise(col(BDRUtils.stage_1))
      )
      .drop("stageAux")
  }

  /** Calcula columnas EAD y PROVOS según beg/end con lag correspondiente. */
  private def calcularEadsYProvisiones(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.Window
    val w = Window.partitionBy(BDRUtils.id_contrato_1).orderBy(col("row_number").asc)
    val lagEad    = lag(col(BDRUtils.ead_ifrs_1), 1).over(w)
    val lagProvOS = lag(col(BDRUtils.prov_ifrs_), 1).over(w)

    df.withColumn("EAD_S1_IFRS", when(col("momento") === "beg", lagEad).otherwise(col(BDRUtils.ead_ifrs_1)))
      .withColumn("EAD_S2_IFRS", when(col("momento") === "beg", lagEad).otherwise(col(BDRUtils.ead_ifrs_2)))
      .withColumn("EAD_S3_IFRS", when(col("momento") === "beg", lagEad).otherwise(col(BDRUtils.ead_ifrs_3)))
      .withColumn("PROV_S1_IFRS", when(col("momento") === "beg", lagProvOS).otherwise(col(BDRUtils.prov_ifrs_1)))
      .withColumn("PROV_S2_IFRS", when(col("momento") === "beg", lagProvOS).otherwise(col(BDRUtils.prov_ifrs_2)))
      .withColumn("PROV_S3_IFRS", when(col("momento") === "beg", lagProvOS).otherwise(col(BDRUtils.prov_ifrs_3)))
      // Limpieza de NULLS (mantengo tu intención)
      .withColumn("EAD_S1_IFRS", when(col("EAD_S1_IFRS").isNull, lit(0)).otherwise(col("EAD_S1_IFRS")))
      .withColumn("EAD_S2_IFRS", when(col("EAD_S2_IFRS").isNull, lit(0)).otherwise(col("EAD_S2_IFRS")))
      .withColumn("EAD_S3_IFRS", when(col("EAD_S3_IFRS").isNull, lit(0)).otherwise(col("EAD_S3_IFRS")))
  }

  // ---------- Bloques de flujos compactados (S1/S2, S1/S3-S2/S3, S3/S1-S3/S2) ----------------

  private def flujosS1S2(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.Window
    val w = Window.partitionBy(BDRUtils.id_contrato_1).orderBy(col("row_number").asc)
    val lagStage = lag(col(BDRUtils.stage_1), 1).over(w)

    df.withColumn("ind_S2_S1", when(col("momento") === "end" && col(BDRUtils.stage_1) === 1 && (lagStage === 2), lit(1)).otherwise(lit(0)))
      .withColumn("s1_s2_FLOW", when(col("momento") === "end" && col("ind_S2_S1") === 1, col("EAD_S1_IFRS")).otherwise(lit(0)))
      .withColumn("ind_S1_S2", when(col("momento") === "end" && col(BDRUtils.stage_1) === 2 && (lagStage === 1), lit(1)).otherwise(lit(0)))
      .withColumn("s1_S2_FLOW", when(col("momento") === "end" && col("ind_S1_S2") === 1, col("EAD_S2_IFRS")).otherwise(lit(0)))
  }

  private def flujosS1S3yS2S3(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.Window
    val w = Window.partitionBy(BDRUtils.id_contrato_1).orderBy(col("row_number").asc)
    val lagStage = lag(col(BDRUtils.stage_1), 1).over(w)

    df.withColumn("ind_S1_S3", when(col("momento") === "end" && col(BDRUtils.stage_1) === 3 && (lagStage === 1), lit(1)).otherwise(lit(0)))
      .withColumn("s1_s3_FLOW", when(col("momento") === "end" && col("ind_S1_S3") === 1, col("EAD_S3_IFRS")).otherwise(lit(0)))
      .withColumn("ind_S2_S3", when(col("momento") === "end" && col(BDRUtils.stage_1) === 3 && (lagStage === 2), lit(1)).otherwise(lit(0)))
      .withColumn("s2_s3_FLOW", when(col("momento") === "end" && col("ind_S2_S3") === 1, col("EAD_S3_IFRS")).otherwise(lit(0)))
  }

  private def flujosS3S1yS3S2(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.Window
    val w = Window.partitionBy(BDRUtils.id_contrato_1).orderBy(col("row_number").asc)
    val lagStage = lag(col(BDRUtils.stage_1), 1).over(w)

    df.withColumn("ind_S3_S1", when(col("momento") === "end" && col(BDRUtils.stage_1) === 1 && (lagStage === 3), lit(1)).otherwise(lit(0)))
      .withColumn("s3_s1_FLOW", when(col("momento") === "end" && col("ind_S3_S1") === 1, lag(col("EAD_S3_IFRS"), 1).over(w)).otherwise(lit(0)))
      .withColumn("ind_S3_S2", when(col("momento") === "end" && col(BDRUtils.stage_1) === 2 && (lagStage === 3), lit(1)).otherwise(lit(0)))
      .withColumn("s3_s2_FLOW", when(col("momento") === "end" && col("ind_S3_S2") === 1, lag(col("EAD_S2_IFRS"), 1).over(w)).otherwise(lit(0)))
  }

  // ---------- Preview_TR (first/last por ventana beg/end) -----------------------------------

  private def calcularPreviewTR(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.Window
    val wBeg = Window.partitionBy(BDRUtils.id_contrato_1).orderBy(col("momento") === "beg")
    val wEnd = Window.partitionBy(BDRUtils.id_contrato_1).orderBy(col("momento") === "end")

    df.withColumn("firstBegS1", first(col("EAD_S1_IFRS")).over(wBeg))
      .withColumn("firstBegS2", first(col("EAD_S2_IFRS")).over(wBeg))
      .withColumn("firstBegS3", first(col("EAD_S3_IFRS")).over(wBeg))
      .withColumn("lastEndS1", last(col("EAD_S1_IFRS")).over(wEnd))
      .withColumn("lastEndS2", last(col("EAD_S2_IFRS")).over(wEnd))
      .withColumn("lastEndS3", last(col("EAD_S3_IFRS")).over(wEnd))
      .withColumn("firstStage", first(col(BDRUtils.stage_1)).over(wBeg))
      .withColumn("lastStage",  last(col(BDRUtils.stage_1)).over(wEnd))
  }

  // ---------- Transferencias TR (todas las *_FLOW_TR_BEG/END compactadas) --------------------

  private def flujosTransferenciasTR(df: DataFrame): DataFrame = {
    // factor común de condición TR por rango y stage
    def condTR(minC: Column, maxC: Column, stFirst: Int, stLast: Int): Column =
      (col("minFeperac") === minC) && (col("maxFeperac") === maxC) &&
        (col("firstStage") === lit(stFirst)) && (col("lastStage") === lit(stLast)) &&
        (col("momento") === "beg")

    df
      .withColumn("S1_S2_FLOW_TR_BEG", when(condTR(BDRUtils.cond_minFeperac, BDRUtils.cond_maxFeperac, 1, 2),  lit(col("firstBegS1"))).otherwise(lit(0)))
      .withColumn("S1_S2_FLOW_TR_END", when(condTR(BDRUtils.cond_minFeperac, BDRUtils.cond_maxFeperac, 1, 2) && col("momento")==="end", lit(col("lastEndS2"))).otherwise(lit(0)))
      .withColumn("S1_S3_FLOW_TR_BEG", when(condTR(BDRUtils.cond_minFeperac, BDRUtils.cond_maxFeperac, 1, 3),  lit(col("firstBegS1"))).otherwise(lit(0)))
      .withColumn("S1_S3_FLOW_TR_END", when(condTR(BDRUtils.cond_minFeperac, BDRUtils.cond_maxFeperac, 1, 3) && col("momento")==="end", lit(col("lastEndS3"))).otherwise(lit(0)))
      .withColumn("S2_S3_FLOW_TR_BEG", when(condTR(BDRUtils.cond_minFeperac, BDRUtils.cond_maxFeperac, 2, 3),  lit(col("firstBegS2"))).otherwise(lit(0)))
      .withColumn("S2_S3_FLOW_TR_END", when(condTR(BDRUtils.cond_minFeperac, BDRUtils.cond_maxFeperac, 2, 3) && col("momento")==="end", lit(col("lastEndS3"))).otherwise(lit(0)))
      .withColumn("S2_S1_FLOW_TR_BEG", when(condTR(BDRUtils.cond_minFeperac, BDRUtils.cond_maxFeperac, 2, 1),  lit(col("firstBegS2"))).otherwise(lit(0)))
      .withColumn("S2_S1_FLOW_TR_END", when(condTR(BDRUtils.cond_minFeperac, BDRUtils.cond_maxFeperac, 2, 1) && col("momento")==="end", lit(col("lastEndS1"))).otherwise(lit(0)))
      .withColumn("S3_S1_FLOW_TR_BEG", when(condTR(BDRUtils.cond_minFeperac, BDRUtils.cond_maxFeperac, 3, 1),  lit(col("firstBegS3"))).otherwise(lit(0)))
      .withColumn("S3_S1_FLOW_TR_END", when(condTR(BDRUtils.cond_minFeperac, BDRUtils.cond_maxFeperac, 3, 1) && col("momento")==="end", lit(col("lastEndS1"))).otherwise(lit(0)))
      .withColumn("S3_S2_FLOW_TR_BEG", when(condTR(BDRUtils.cond_minFeperac, BDRUtils.cond_maxFeperac, 3, 2),  lit(col("firstBegS3"))).otherwise(lit(0)))
      .withColumn("S3_S2_FLOW_TR_END", when(condTR(BDRUtils.cond_minFeperac, BDRUtils.cond_maxFeperac, 3, 2) && col("momento")==="end", lit(col("lastEndS2"))).otherwise(lit(0)))
  }

  // ---------- Segmentación COREP y cálculo de fecha/país final --------------------------------

  private def segmentacionCorep(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.expressions.Window
    val w = Window.partitionBy(BDRUtils.id_contrato_1)

    val segCorepText =
      when(col("categoria") === 16 && col("subcategoria") === 16 && col("flag_gar") > 0, lit(BDRUtils.segCorep_text_SM_Sec))
        .when(col("categoria") === 16 && col("subcategoria") === 16 && col("flag_gar") === 0, lit(BDRUtils.segCorep_text_SM_NSec))
        .when(col("categoria") === 16 && col("subcategoria") === 0  && col("flag_gar") > 0, lit(BDRUtils.segCorep_text_Other_Sec))
        .when(col("categoria") === 16 && col("subcategoria") === 0  && col("flag_gar") === 0, lit(BDRUtils.segCorep_text_Other_NSec))
        .when(col("flag_sme") === 1, lit(BDRUtils.segCorep_text_Other_SME))
        .otherwise(lit(BDRUtils.segCorep_text_Other))

    df.withColumn("seg_COREP", segCorepText)
      .withColumn("seg_COREP_DateFinal",
        when(col(BDRUtils.fecha_1) === BDRUtils.cond_maxFeperac && col("momento") === "end", col(BDRUtils.fecha_1))
          .otherwise(when(col(BDRUtils.fecha_1) === BDRUtils.cond_maxFeperac && col("momento") === "end", col(BDRUtils.segCorep_text_Other)))
      )
      .withColumn("id_pais_dateFinal", max(col("id_pais_final")).over(w))
  }

  // ---------- Orden/selección de columnas -----------------------------------------------------

  private def reordenarYOrdenar(df: DataFrame): DataFrame = {
    val reorderedColumnNames: Array[String] = Array(
      "fecha","momento","empresa","id_contrato","stage","seg_COREP","seg_COREP_Final",
      "EAD_S1_IFRS","EAD_S2_IFRS","EAD_S3_IFRS","EAD_S1_cap","EAD_S2_cap","EAD_S3_cap",
      "PROV_S1_IFRS","PROV_S2_IFRS","PROV_S3_IFRS",
      "ind_S1_S2","s1_S2_FLOW","ind_S1_S3","s1_s3_FLOW","ind_S2_S3","s2_s3_FLOW",
      "S1_S2_FLOW_TR_BEG","S1_S2_FLOW_TR_END","S1_S3_FLOW_TR_BEG","S1_S3_FLOW_TR_END",
      "S2_S3_FLOW_TR_BEG","S2_S3_FLOW_TR_END","S2_S1_FLOW_TR_BEG","S2_S1_FLOW_TR_END",
      "S3_S1_FLOW_TR_BEG","S3_S1_FLOW_TR_END","S3_S2_FLOW_TR_BEG","S3_S2_FLOW_TR_END",
      "row_number"
    )
    df.sort(asc(BDRUtils.id_contrato_1), asc(BDRUtils.fecha_1), asc("row_number"))
      .select(reorderedColumnNames.head, reorderedColumnNames.tail: _*)
  }

  // ---------- Limpieza tmp HDFS ---------------------------------------------------------------

  private def borrarTmpHDFS(tmpPath: String): Unit = {
    val fs = HDFSHandler.getFileSystem(BDRUtils.tmpPath)
    val pathFile = new Path(tmpPath)
    if (fs.exists(pathFile)) fs.delete(pathFile, true)
  }
}
