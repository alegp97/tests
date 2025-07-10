test("createTmpTable should generate and save filtered tmp table") {
    val sqlContext = mock[SQLContext](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))

    val df = mock[DataFrame](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))
    val filteredDF = mock[DataFrame](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))
    val tmpDF = mock[DataFrame](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))
    val distinctDF = mock[DataFrame](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))

    val column = col("dummy_column")
    val partList = List("part_col")

    val mockRow = mock[Row]
    when(mockRow.get(0)).thenReturn("value")
    val collected = Array(mockRow)

    // simulate schema
    val structType = StructType(Seq(StructField("part_col", StringType)))
    when(df.schema).thenReturn(structType)
    when(df.schema("part_col").dataType).thenReturn(StringType)

    // simulate path
    val path = "/tmp/fake_path"
    val finalPath = new Path(path + "/tmp_windowsFunction_" + Calendar.getInstance().getTimeInMillis)

    // simulate logic
    when(df.select(any[Seq[Column]]: _*)).thenReturn(df)
    when(df.where(any[Column])).thenReturn(filteredDF)
    when(filteredDF.persist(any())).thenReturn(filteredDF)
    when(filteredDF.schema("part_col").dataType.getClass.getSimpleName).thenReturn("StringType")
    when(filteredDF.select(any[Column])).thenReturn(filteredDF)
    when(filteredDF.select(any[Column]).distinct().collect()).thenReturn(collected)
    when(filteredDF.unpersist()).thenReturn(filteredDF)

    when(sqlContext.sql(any[String])).thenReturn(tmpDF)
    when(tmpDF.select(any[Seq[Column]]: _*)).thenReturn(tmpDF)
    when(tmpDF.distinct()).thenReturn(distinctDF)

    // Ejecutar la función
    val resultPath = BoardDataUtil.createTmpTable(
      sourceFilter = df,
      target = "target_table",
      particionesDest = partList,
      extraFilter = column,
      sqlContext = sqlContext,
      path = path
    )

    assert(resultPath.contains("tmp_windowsFunction_"))
  }
