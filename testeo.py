test("createTmpTable should handle extraFilter not null and StringType with empty where") {
  val sqlContext = mock[SQLContext](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))
  val sourceDF = mock[DataFrame](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))
  val filteredDF = mock[DataFrame](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))
  val tmpDF = mock[DataFrame](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))
  val distinctDF = mock[DataFrame](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))

  val extraFilter = mock[Column]
  val column = col("dummy_column")
  val partList = List("part_col")

  val mockRow = mock[Row]
  when(mockRow.get(0)).thenAnswer(_ => "dummy_value")
  val collected = Array(mockRow)

  // Schema mock
  val structType = StructType(Seq(StructField("part_col", StringType)))
  when(filteredDF.schema("part_col").dataType.getClass.getSimpleName).thenReturn("StringType$")
  when(filteredDF.schema).thenReturn(structType)

  // Path mock
  val path = "/tmp/fake_path"
  val finalPath = new Path(path + "/tmp_windowsFunction_" + Calendar.getInstance().getTimeInMillis)

  // Source filter returns filteredDF
  when(sourceDF.where(extraFilter)).thenReturn(sourceDF)
  when(sourceDF.where(extraFilter).select(any[Seq[Column]])).thenReturn(filteredDF)

  // Select logic
  when(filteredDF.select(any[Seq[Column]])).thenReturn(filteredDF)
  when(filteredDF.persist(StorageLevel.MEMORY_AND_DISK)).thenReturn(filteredDF)
  when(filteredDF.select(col("`part_col`")).distinct().collect()).thenReturn(collected)
  when(filteredDF.select(col("`part_col`"))).thenReturn(filteredDF)
  when(filteredDF.unpersist()).thenReturn(filteredDF)

  // tmpDF and distinct write
  when(sqlContext.sql(any[String])).thenReturn(tmpDF)
  when(tmpDF.select(any[Seq[Column]])).thenReturn(tmpDF)
  when(tmpDF.distinct()).thenReturn(distinctDF)

  val resultPath = BoardDataUtil.createTmpTable(
    sourceDF,
    "target_table",
    partList,
    extraFilter,
    column,
    sqlContext,
    path
  )

  assert(resultPath.toString.contains("tmp_windowsFunction"))
}
