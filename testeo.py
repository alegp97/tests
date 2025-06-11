test("run ejecuta todo correctamente hasta el punto de escritura") {

  // Arrange
  val sparkMock = mock[SparkSession]
  val scMock = mock[SparkContext]
  val confMock = mock[org.apache.hadoop.conf.Configuration]
  val sqlContextMock = mock[SQLContext]
  val anyDF = mock[DataFrame]
  val row = mock[Row]

  val sourcedb = "test_source"
  val targetdb = "test_target"
  val data_timestamp_part = "20240605"
  val entities = List(mock[IngestEntity])

  // Configuración del entorno Spark
  when(sparkMock.sparkContext: SparkContext).thenReturn(scMock)
  when(scMock.hadoopConfiguration: org.apache.hadoop.conf.Configuration).thenReturn(confMock)
  doReturn(sqlContextMock).when(sparkMock).sqlContext

  // Verificación clave
  assert(sparkMock.sqlContext eq sqlContextMock)

  // Mock específico para maxPartition
  val dfPartitions = mock[DataFrame]
  val rowPartition = mock[Row]
  when(rowPartition.getString(0)).thenReturn("partition=20240605")

  when(dfPartitions.orderBy(any[Array[Column]](): _*)).thenReturn(dfPartitions)
  when(dfPartitions.limit(1)).thenReturn(dfPartitions)
  when(dfPartitions.collect()).thenReturn(Array(rowPartition))
  when(sqlContextMock.sql(startsWith("show partitions"))).thenReturn(dfPartitions)

  // Simulación general de DF
  when(row.getString(0)).thenReturn("partition=20240605")
  when(anyDF.collect()).thenReturn(Array(row))
  when(anyDF.count()).thenReturn(1L)
  when(anyDF.columns).thenReturn(Array("col1", "col2"))
  when(anyDF.select(any[Array[Column]](): _*)).thenReturn(anyDF)
  when(anyDF.selectExpr(any[String])).thenReturn(anyDF)
  when(anyDF.withColumn(any[String], any[Column])).thenReturn(anyDF)
  when(anyDF.withColumnRenamed(any[String], any[String])).thenReturn(anyDF)
  when(anyDF.drop(any[Column])).thenReturn(anyDF)
  when(anyDF.where(any[Column])).thenReturn(anyDF)
  when(anyDF.join(any[DataFrame])).thenReturn(anyDF)
  when(anyDF.join(any[DataFrame], any[Column])).thenReturn(anyDF)
  when(anyDF.join(any[DataFrame], any[Column], any[String])).thenReturn(anyDF)
  when(anyDF.repartition(any[Int])).thenReturn(anyDF)
  when(anyDF.distinct()).thenReturn(anyDF)
  when(anyDF.toDF()).thenReturn(anyDF)

  // Tablas simuladas necesarias
  when(sqlContextMock.table(contains("fields_dict"))).thenReturn(anyDF)
  when(sqlContextMock.table(contains("exercise_inventory"))).thenReturn(anyDF)
  when(sqlContextMock.table(contains("execution_def"))).thenReturn(anyDF)
  when(sqlContextMock.table(contains("st_metrics_input"))).thenReturn(anyDF)
  when(sqlContextMock.table(contains("data_output"))).thenReturn(anyDF)
  when(sqlContextMock.table(contains("contexts_st"))).thenReturn(anyDF)
  when(sqlContextMock.table(contains("granularities_map"))).thenReturn(anyDF)
  when(sqlContextMock.sql(any[String])).thenReturn(anyDF)
