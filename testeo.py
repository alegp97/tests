test("run ejecuta correctamente sin lanzar errores") {

    // Arrange
    val sparkMock = mock[SparkSession]
    val scMock = mock[SparkContext]
    val confMock = mock[SparkConf]
    val sqlContextMock = mock[SQLContext]

    val sourcedb = "test_source"
    val targetdb = "test_target"
    val data_timestamp_part = "20240605"
    val entities = List(mock[IngestEntity])

    // Mocks esenciales del entorno
    when(sparkMock.sparkContext).thenReturn(scMock)
    when(scMock.hadoopConfiguration).thenReturn(confMock)
    when(sparkMock.sqlContext).thenReturn(sqlContextMock)

    // Mock del DataFrame general
    val anyDF = mock[DataFrame]
    val row = mock[Row]

    // DataFrameWriter mock (pero sabiendo que puede fallar)
    val writerMock = mock[DataFrameWriter[Row]](RETURNS_DEEP_STUBS)
    try {
      when(anyDF.write).thenReturn(writerMock)
    } catch {
      case e: Exception =>
        // En caso de fallo por ser clase final, ignoramos
    }

    when(writerMock.mode(SaveMode.Overwrite)).thenReturn(writerMock)
    when(writerMock.saveAsTable(any[String])).thenReturn(())

    when(row.getString(0)).thenReturn("partition=20240605")
    when(anyDF.collect()).thenReturn(Array(row))
    when(anyDF.columns).thenReturn(Array("col1", "col2"))
    when(anyDF.select(any[Seq[Column]])).thenReturn(anyDF)
    when(anyDF.select(any[Column])).thenReturn(anyDF)
    when(anyDF.selectExpr(any[String])).thenReturn(anyDF)
    when(anyDF.withColumn(any[String], any[Column])).thenReturn(anyDF)
    when(anyDF.withColumnRenamed(any[String], any[String])).thenReturn(anyDF)
    when(anyDF.drop(any[Column])).thenReturn(anyDF)
    when(anyDF.where(any[Column])).thenReturn(anyDF)
    when(anyDF.join(any[DataFrame], any[Column])).thenReturn(anyDF)
    when(anyDF.join(any[DataFrame])).thenReturn(anyDF)
    when(anyDF.count()).thenReturn(1L)
    when(anyDF.repartition(any[Int])).thenReturn(anyDF)

    // Mocks de tablas y SQLContext
    when(sqlContextMock.sql(any[String])).thenReturn(anyDF)
    when(sqlContextMock.table(any[String])).thenReturn(anyDF)

    // Mocks para funciones estáticas dentro de objetos (si son usadas)
    val helper = mock[HistoricalExercisesJob.type]
    when(helper.searchDuplicates(any[List[String]])).thenReturn(List.empty)
    when(helper.deleteDuplicatesInSource(any[List[String]], any[List[String]])).thenReturn(List.empty)

    // Act & Assert
    try {
      HistoricalExercisesJob.run(sourcedb, targetdb, data_timestamp_part, entities)(sparkMock)
    } catch {
      case e: Exception =>
        fail(s"No debería lanzar excepción, pero lanzó: ${e.getMessage}")
    }
  }
