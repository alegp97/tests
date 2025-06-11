  test("run avanza hasta .write sin NPE en maxPartition") {

    // -----------------------------------------------------------------------
    // 1.  Mocks base de Spark
    // -----------------------------------------------------------------------
    val sparkMock       = mock[SparkSession](RETURNS_DEEP_STUBS)
    val scMock          = mock[SparkContext]
    val hadoopConfMock  = mock[org.apache.hadoop.conf.Configuration]
    val sqlCtxMock      = mock[SQLContext]

    when(sparkMock.sparkContext).thenReturn(scMock)
    when(scMock.hadoopConfiguration).thenReturn(hadoopConfMock)
    when(sparkMock.sqlContext).thenAnswer(_ => sqlCtxMock)   // evita ambigüedad

    // -----------------------------------------------------------------------
    // 2.  Stub de la consulta de particiones (maxPartition)
    // -----------------------------------------------------------------------
    val dfPartitions = mock[DataFrame]
    val rowPart      = mock[Row]

    when(rowPart.getString(0)).thenReturn("partition=20240605")

    // orderBy(...)  y  limit(1)  deben devolver el mismo DF
    when(dfPartitions.orderBy(any[Array[Column]](): _*)).thenReturn(dfPartitions)
    when(dfPartitions.limit(anyInt())).thenReturn(dfPartitions)
    when(dfPartitions.collect()).thenReturn(Array(rowPart))

    // La SQL exacta que usa run()
    when(sqlCtxMock.sql(startsWith("show partitions"))).thenReturn(dfPartitions)

    // -----------------------------------------------------------------------
    // 3.  DataFrame “comodín” para el resto de tablas / consultas
    // -----------------------------------------------------------------------
    val anyDF = mock[DataFrame](RETURNS_DEEP_STUBS)

    when(anyDF.columns).thenReturn(Array("col1", "col2"))
    when(anyDF.collect()).thenReturn(Array(mock[Row]))
    when(anyDF.count()).thenReturn(1L)

    when(sqlCtxMock.table(contains("fields_dict")))       .thenReturn(anyDF)
    when(sqlCtxMock.table(contains("exercise_inventory"))).thenReturn(anyDF)
    when(sqlCtxMock.table(contains("execution_def")))     .thenReturn(anyDF)
    when(sqlCtxMock.table(contains("st_metrics_input")))  .thenReturn(anyDF)
    when(sqlCtxMock.table(contains("data_output")))       .thenReturn(anyDF)
    when(sqlCtxMock.table(contains("contexts_st")))       .thenReturn(anyDF)
    when(sqlCtxMock.table(contains("granularities_map"))) .thenReturn(anyDF)

    // Fallback para cualquier otra SQL
    when(sqlCtxMock.sql(any[String])).thenReturn(anyDF)

    // -----------------------------------------------------------------------
    // 4.  Datos de entrada para run()
    // -----------------------------------------------------------------------
    val sourcedb = "test_source"
    val targetdb = "test_target"
    val tsPart   = "20240605"
    val entities = List(mock[IngestEntity])
