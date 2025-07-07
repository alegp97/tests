val df = spark.createDataFrame(Seq(
  Tuple1("X"),
  Tuple1("Y")
)).toDF("country")
