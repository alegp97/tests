test("run ejecuta correctamente sin lanzar errores") {
  // Arrange
  val sourcedb = "test_source"
  val targetdb = "test_target"
  val data_timestamp_part = "20240605"
  val entities = List(mock[IngestEntity])

  val sparkMock = mock[SparkSession]
  val sqlContextMock = mock[SQLContext]
  when(sparkMock.sqlContext).thenReturn(sqlContextMock)

  val dummyDF = mock[DataFrame]
  val row = mock[Row]
  when(row.getString(0)).thenReturn("partition=20240605")

  // show partitions
  val partitionsDF = mock[DataFrame]
  when(sqlContextMock.sql(org.mockito.ArgumentMatchers.eq(s"show partitions $sourcedb.fields_dict")))
    .thenReturn(partitionsDF)
  when(partitionsDF.orderBy(org.mockito.Matchers.any[Column])).thenReturn(dummyDF)
  when(dummyDF.limit(1)).thenReturn(dummyDF)
  when(dummyDF.collect()).thenReturn(Array(row))

  // mocks comunes para todas las tablas
  when(sqlContextMock.table(org.mockito.ArgumentMatchers.any[String])).thenReturn(dummyDF)
  when(dummyDF.where(org.mockito.Matchers.any[Column])).thenReturn(dummyDF)
  when(dummyDF.select(org.mockito.Matchers.any[Seq[Column]]: _*)).thenReturn(dummyDF)
  when(dummyDF.select(org.mockito.Matchers.any[Column])).thenReturn(dummyDF)
  when(dummyDF.selectExpr(org.mockito.Matchers.any[String])).thenReturn(dummyDF)
  when(dummyDF.collect()).thenReturn(Array(row))
  when(dummyDF.count()).thenReturn(1L)
  when(dummyDF.columns).thenReturn(Array("col1", "col2"))
  when(dummyDF.distinct()).thenReturn(dummyDF)
  when(dummyDF.withColumn(org.mockito.Matchers.any[String], org.mockito.Matchers.any[Column])).thenReturn(dummyDF)
  when(dummyDF.withColumnRenamed(org.mockito.Matchers.any[String], org.mockito.Matchers.any[String])).thenReturn(dummyDF)
  when(dummyDF.drop(org.mockito.Matchers.any[Column])).thenReturn(dummyDF)
  when(dummyDF.repartition(1)).thenReturn(dummyDF)
  when(dummyDF.toDF()).thenReturn(dummyDF)
  when(dummyDF.map(org.mockito.Matchers.any())).thenReturn(dummyDF)

  // .write y .saveAsTable simulados
  val writer = mock[org.apache.spark.sql.DataFrameWriter[Row]]
  when(dummyDF.write).thenReturn(writer)
  when(writer.mode("overwrite")).thenReturn(writer)
  when(writer.saveAsTable(org.mockito.Matchers.any[String])).thenReturn(())

  // ALTER y DROP TABLE
  when(sqlContextMock.sql(org.mockito.ArgumentMatchers.argThat((s: String) => s.startsWith("ALTER TABLE")))).thenReturn(dummyDF)
  when(sqlContextMock.sql(org.mockito.ArgumentMatchers.argThat((s: String) => s.startsWith("DROP TABLE")))).thenReturn(dummyDF)

  // Act
  HistoricalExercisesJob.run(sourcedb, targetdb, data_timestamp_part, entities)(sparkMock)

  // Assert implícito: si llega aquí sin excepción, pasa
  assert(true)
}
