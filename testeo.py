test("run ejecuta correctamente sin lanzar errores") {
  // Arrange
  val sourcedb = "test_source"
  val targetdb = "test_target"
  val data_timestamp_part = "20240605"
  val entities = List(mock[IngestEntity])

  val sparkMock = mock[SparkSession]
  val sqlContextMock = mock[SQLContext]
  when(sparkMock.sqlContext).thenReturn(sqlContextMock)

  // Mocks genéricos
  val anyDF = mock[DataFrame]
  val row = mock[Row]
  when(row.getString(0)).thenReturn("partition=20240605")

  // Particiones
  val dfPartitions = mock[DataFrame]
  when(sqlContextMock.sql(startsWith("show partitions"))).thenReturn(dfPartitions)
  when(dfPartitions.orderBy(any[Column])).thenReturn(anyDF)
  when(anyDF.limit(1)).thenReturn(anyDF)
  when(anyDF.collect()).thenReturn(Array(row))

  // Tablas simuladas necesarias
  when(sqlContextMock.table(contains("fields_dict"))).thenReturn(anyDF)
  when(sqlContextMock.table(contains("exercise_inventory"))).thenReturn(anyDF)
  when(sqlContextMock.table(contains("execution_def"))).thenReturn(anyDF)
  when(sqlContextMock.table(contains("st_metrics_input"))).thenReturn(anyDF)
  when(sqlContextMock.table(contains("data_output"))).thenReturn(anyDF)
  when(sqlContextMock.table(contains("contexts_st"))).thenReturn(anyDF)
  when(sqlContextMock.table(contains("granularities_map"))).thenReturn(anyDF)

  when(anyDF.where(any[Column])).thenReturn(anyDF)
  when(anyDF.select(any[Seq[Column]]: _*)).thenReturn(anyDF)
  when(anyDF.select(any[Column])).thenReturn(anyDF)
  when(anyDF.selectExpr(any[String])).thenReturn(anyDF)
  when(anyDF.collect()).thenReturn(Array(row))
  when(anyDF.count()).thenReturn(1L)
  when(anyDF.columns).thenReturn(Array("col1", "col2"))
  when(anyDF.map(any)).thenReturn(List("col1", "col2"))
  when(anyDF.toDF()).thenReturn(anyDF)
  when(anyDF.distinct()).thenReturn(anyDF)
  when(anyDF.withColumn(any[String], any[Column])).thenReturn(anyDF)
  when(anyDF.withColumnRenamed(any[String], any[String])).thenReturn(anyDF)
  when(anyDF.drop(any[Column])).thenReturn(anyDF)
  when(anyDF.repartition(1)).thenReturn(anyDF)

  val writer = mock[DataFrameWriter[Row]]
  when(anyDF.write).thenReturn(writer)
  when(writer.mode(SaveMode.Overwrite)).thenReturn(writer)
  when(writer.saveAsTable(any[String])).thenReturn(())

  // Simula renombrado de tabla temporal
  when(sqlContextMock.sql(contains("ALTER TABLE"))).thenReturn(anyDF)
  when(sqlContextMock.sql(contains("DROP TABLE"))).thenReturn(anyDF)

  // Mocks para searchDuplicates y deleteDuplicatesInSource
  val helper = mock[HistoricalExercisesJob.type]
  when(helper.searchDuplicates(any[Seq[String]])).thenReturn(Set.empty)
  when(helper.deleteDuplicatesInSource(any[Seq[String]], any[Seq[String]])).thenReturn(Seq.empty)

  // Act & Assert: simplemente comprobamos que no lanza
  noException shouldBe thrownBy {
    HistoricalExercisesJob.run(sourcedb, targetdb, data_timestamp_part, entities)(sparkMock)
  }
}
