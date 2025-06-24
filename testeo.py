  test("BDRFlowsJob.run ejecuta correctamente y cubre la lógica principal") {
    // ---------- Spark & SQL ----------
    implicit val spark: SparkSession = mock[SparkSession](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))
    val sqlContext = mock[SQLContext]
    when(spark.sqlContext).thenReturn(sqlContext)

    // ---------- Lectura parquet ----------
    val readerMock = mock[DataFrameReader](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))
    when(sqlContext.read).thenReturn(readerMock)
    val dfMock = mock[DataFrame](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))
    when(readerMock.format("parquet")).thenReturn(readerMock)
    when(readerMock.load(any[String])).thenReturn(dfMock)

    // ---------- Stubs genéricos DataFrame ----------
    when(dfMock.withColumn(any[String], any[Column])).thenReturn(dfMock)
    when(dfMock.drop(any[String])).thenReturn(dfMock)
    when(dfMock.sort(any(classOf[Array[Column]]): _*)).thenReturn(dfMock)
    when(dfMock.select(any(classOf[Array[Column]]): _*)).thenReturn(dfMock)
    when(dfMock.where(any[Column])).thenReturn(dfMock)
    when(dfMock.unionAll(any[DataFrame])).thenReturn(dfMock)

    // ---------- Write ----------
    val writerMock = mock[DataFrameWriter[Row]](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))
    when(dfMock.write).thenReturn(writerMock)
    when(writerMock.mode(SaveMode.Overwrite)).thenReturn(writerMock)
    when(writerMock.format("parquet")).thenReturn(writerMock)
    when(writerMock.saveAsTable(any[String])).thenReturn(())

    // ---------- HDFS ----------
    val fsMock = mock[FileSystem]
    val staticHdfs: MockedStatic[HDFSHandler] = mockStatic(classOf[HDFSHandler])
    staticHdfs.when(() => HDFSHandler.getFileSystem(any[String])).thenReturn(fsMock)
    when(fsMock.exists(any(classOf[Path]))).thenReturn(true)
    when(fsMock.delete(any(classOf[Path]), meq(java.lang.Boolean.TRUE))).thenReturn(true)

    // ---------- Mock constantes BDRUtils ----------
    val staticBDR: MockedStatic[BDRUtils] = mockStatic(classOf[BDRUtils])
    staticBDR.when(() => BDRUtils.tmpPath).thenReturn("/tmp/test/path")
    staticBDR.when(() => BDRUtils.starting_points_contract).thenReturn("starting_points_contract")
    staticBDR.when(() => BDRUtils.fecha._1).thenReturn("fecha")
    staticBDR.when(() => BDRUtils.INTERVALO_CALCULO).thenReturn(1)

    // ---------- Mock "show partitions … .map …" ----------
    val dsRowMock = mock[Dataset[Row]](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))
    when(sqlContext.sql(any[String])).thenReturn(dsRowMock)

    val dsStringMock = mock[Dataset[String]](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))
    when(dsRowMock.map[String](any[Function1[Row, String]]())(any[Encoder[String]]())).thenReturn(dsStringMock)
    when(dsStringMock.collect()).thenReturn(Array("2025-01-01"))

    // ---------- Ejecución ----------
    BDRFlowsJob.run(
      sourcecb = "sourcedb",
      targetdb = "targetdb",
      targetTableOptionalName = "agg_table",
      entities = List.empty,
      extra_filter = "",
      sourceTable = "starting_points_contract",
      process = "FULL",
      is_incremental = "false"
    )

    // ---------- Verificaciones ----------
    verify(writerMock, atLeastOnce()).saveAsTable(any[String])

    // ---------- Cierre mocks estáticos ----------
    staticHdfs.close()
    staticBDR.close()
  }
