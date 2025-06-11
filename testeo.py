  test("run avanza hasta .write sin NPE en maxPartition") {

   // ─── 1. mocks mínimos de Spark ─────────────────────────────────────────
    val sparkMock  = mock[SparkSession](RETURNS_DEEP_STUBS)
    val sqlCtxMock = mock[SQLContext]
    when(sparkMock.sqlContext).thenReturn(sqlCtxMock)
  
    // ─── 2. stub show partitions → orderBy → limit → collect ───────────────
    val dfPart  = mock[DataFrame]
    val rowPart = mock[Row]
    when(rowPart.getString(0)).thenReturn("partition=20240605")
  
    when(
      dfPart.orderBy(any[Column]).limit(anyInt()).collect()
    ).thenReturn(Array(rowPart))
  
    when(sqlCtxMock.sql(startsWith("show partitions"))).thenReturn(dfPart)
  
    // ─── 3. DataFrame comodín para el resto de tablas/consultas ────────────
    val anyDF = mock[DataFrame](RETURNS_DEEP_STUBS)
    when(anyDF.columns).thenReturn(Array("c1","c2"))
    when(anyDF.collect()).thenReturn(Array(mock[Row]))
    when(anyDF.count()).thenReturn(1L)
    when(sqlCtxMock.table(any[String])).thenReturn(anyDF)
    when(sqlCtxMock.sql(any[String])).thenReturn(anyDF)

    // Fallback para cualquier otra SQL
    when(sqlCtxMock.sql(any[String])).thenReturn(anyDF)

    // -----------------------------------------------------------------------
    // 4.  Datos de entrada para run()
    // -----------------------------------------------------------------------
    val sourcedb = "test_source"
    val targetdb = "test_target"
    val tsPart   = "20240605"
    val entities = List(mock[IngestEntity])
