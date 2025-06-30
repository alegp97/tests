test("columnNotIn should return elements from big not in small") {
  val big = List("a", "b", "c")
  val small = List("b", "d")
  val result = BoardDataUtil.columnNotIn(big, small)
  assert(result == List("a", "c"))
}


test("columnNotInColumn should return columns from big not in small") {
  import org.apache.spark.sql.functions._

  val big = List(col("a"), col("b"), col("c"))
  val small = List(col("b"), col("d"))
  val result = BoardDataUtil.columnNotInColumn(big, small)

  // Por cómo funciona `diff`, se espera `a` y `c`
  assert(result.map(_.toString()) == List("a", "c"))
}


test("calculateRepartition should assign correct number of partitions") {
  assert(BoardDataUtil.calculateRepartition(500000) == 1)
  assert(BoardDataUtil.calculateRepartition(1000000) == 1)
  assert(BoardDataUtil.calculateRepartition(1500000) == 2)
  assert(BoardDataUtil.calculateRepartition(80000000) == 80)
  assert(BoardDataUtil.calculateRepartition(9999999999L) == 100)
}


test("getMaxRowCount should return max row count from entities") {
  val entity1 = mock[IngestEntity]
  val entity2 = mock[IngestEntity]
  when(entity1.getRowCount).thenReturn(100L)
  when(entity2.getRowCount).thenReturn(200L)

  val result = BoardDataUtil.getMaxRowCount(List(entity1, entity2))
  assert(result == 200L)
}

test("columnNotIn should return elements from big not in small") {
  val big = List("a", "b", "c")
  val small = List("b", "d")
  val result = BoardDataUtil.columnNotIn(big, small)
  assert(result == List("a", "c"))
}

test("columnNotInColumn should return columns from big not in small") {
  import org.apache.spark.sql.functions._

  val big = List(col("a"), col("b"), col("c"))
  val small = List(col("b"), col("d"))
  val result = BoardDataUtil.columnNotInColumn(big, small)

  // Por cómo funciona `diff`, se espera `a` y `c`
  assert(result.map(_.toString()) == List("a", "c"))
}


test("calculateRepartition should assign correct number of partitions") {
  assert(BoardDataUtil.calculateRepartition(500000) == 1)
  assert(BoardDataUtil.calculateRepartition(1000000) == 1)
  assert(BoardDataUtil.calculateRepartition(1500000) == 2)
  assert(BoardDataUtil.calculateRepartition(80000000) == 80)
  assert(BoardDataUtil.calculateRepartition(9999999999L) == 100)
}

test("getMaxRowCount should return max row count from entities") {
  val entity1 = mock[IngestEntity]
  val entity2 = mock[IngestEntity]
  when(entity1.getRowCount).thenReturn(100L)
  when(entity2.getRowCount).thenReturn(200L)

  val result = BoardDataUtil.getMaxRowCount(List(entity1, entity2))
  assert(result == 200L)
}


test("generateAlterSentence should return correct SQL alter statement") {
  import spark.implicits._

  val df = Seq(
    ("name1", "string"),
    ("name2", "int")
  ).toDF("name", "data_type")

  val sql = BoardDataUtil.generateAlterSentence("my_table", df)
  assert(sql == "ALTER TABLE my_table ADD COLUMNS (`name1` string, `name2` int)")
}


def getDataDatePartIngestEntity(entities: List[IngestEntity]): List[String] = {
  var d_d_p: List[String] = List()
  for (e <- entities) {
    d_d_p = d_d_p :+ e.getDataDatePart
  }
  d_d_p
}


test("buildCasesQuery should return when condition for given rows") {
  val row1 = Row(BigDecimal(1), "col1", "New_Name_1")
  val row2 = Row(BigDecimal(2), "col2", "New_Name_2")

  val schema = StructType(List(
    StructField("scenario_type", DecimalType(38, 0), true),
    StructField("column_name", StringType, true),
    StructField("name", StringType, true)
  ))

  val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(row1, row2)), schema)
  val column = BoardDataUtil.buildCasesQuery(df.collect())

  val testDF = Seq((1, "value1", "value2"), (2, "value3", "value4")).toDF("scenario_type", "col1", "col2")
    .withColumn("result", column)

  val results = testDF.select("result").as[String].collect()
  assert(results.length == 2)
}


test("coalesceByColumns should return coalesced columns with aliases") {
  val df1 = Seq(("a", "x")).toDF("col1", "col2")
  val df2 = Seq(("b", "y")).toDF("col1", "col2")

  val cols = BoardDataUtil.coalesceByColumns(df1, df2, Array("col1", "col2"))
  val dfResult = df1.join(df2).select(cols: _*)

  assert(dfResult.columns sameElements Array("col1", "col2"))
}


test("generateCreate should return correct CREATE EXTERNAL TABLE sentence") {
  val fields = List(Fields("col1", "string"), Fields("col2", "int"))
  val result = BoardDataUtil.generateCreate("my_table", fields, "/path/to/table")

  val expected = "CREATE EXTERNAL TABLE IF NOT EXISTS my_table (`col1` string, `col2` int) STORED AS PARQUET LOCATION '/path/to/table'"
  assert(result == expected)
}


test("generateAlter should return correct ALTER TABLE sentence") {
  val fields = List(Fields("col1", "string"), Fields("col2", "int"))
  val result = BoardDataUtil.generateAlter("target_table", fields)

  assert(result == "ALTER TABLE target_table ADD COLUMNS (`col1` string, `col2` int)")
}

