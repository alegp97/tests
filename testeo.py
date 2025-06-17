val row = Row("fecha=2023-10-01")
val schema = StructType(Seq(StructField("partition", StringType, nullable = true)))
val df = spark.createDataFrame(spark.sparkContext.parallelize(Seq(row)), schema)

when(sparkMock.sqlContext.sql(startsWith("show partitions"))).thenReturn(df)
