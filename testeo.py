test("getSourceFilter should apply filter when entities are not empty and partitionType is valid") {
  val sqlContext = mock[SQLContext]
  val dfMock = mock[DataFrame]
  val filteredDfMock = mock[DataFrame]
  val colMock = mock[Column]

  val partitionType = "data_date_part"
  val source = "my_table"
  val query = List(mock[Column])

  val entity = mock[IngestEntity]
  when(entity.getDataDatePart).thenReturn("2023-01-01")
  when(entity.getDataTimestampPart()).thenReturn("2023-01-01 00:00:00")

  when(sqlContext.table(source)).thenReturn(dfMock)

  when(dfMock.where(any[Column])).thenReturn(filteredDfMock)

  val result = BoardDataUtil.getSourceFilter(partitionType, source, query, List(entity), sqlContext)

  assert(result eq filteredDfMock)
}

test("removeDuplicatesFromDataFrame should drop rows with duplicate partitionDateColumn") {
  val dfMock = mock[DataFrame]
  val resultDfMock = mock[DataFrame]
  val withColumnDfMock = mock[DataFrame]
  val droppedDfMock = mock[DataFrame]
  val filteredDfMock = mock[DataFrame]
  val colMock = mock[Column]

  val windowSpec = Window.partitionBy(colMock)

  when(dfMock.withColumn(
    ArgumentMatchers.eq("maxDataDatePart"),
    ArgumentMatchers.any[Column]
  )).thenReturn(withColumnDfMock)

  when(withColumnDfMock.where(colMock === col("maxDataDatePart"))).thenReturn(filteredDfMock)
  when(filteredDfMock.drop("maxDataDatePart")).thenReturn(droppedDfMock)

  val result = BoardDataUtil.removeDuplicatesFromDataFrame(colMock, dfMock, colMock)

  assert(result eq droppedDfMock)
}


test("repairTable should execute MSCK REPAIR TABLE SQL") {
  val sqlContext = mock[SQLContext]

  BoardDataUtil.repairTable("target_table", sqlContext)

  verify(sqlContext).sql("set hive.msck.path.validation=ignore")
  verify(sqlContext).sql("msck repair table target_table")
}
