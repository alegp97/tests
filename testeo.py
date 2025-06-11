  // ─── mocks de clase ───────────────────────────────────────────────────
  private val sparkMock   = mock[SparkSession]
  private val sqlCtxMock  = mock[SQLContext]

  override def beforeAll(): Unit = {
    super.beforeAll()
    when(sparkMock.sqlContext).thenReturn(sqlCtxMock)
  }

  test("run avanza hasta .write sin NPE") {

    // 1) DataFrame para show partitions -----------------------------------
    val dfPart  = mock[DataFrame]
    val rowPart = mock[Row]
    when(rowPart.getString(0)).thenReturn("partition=20240605")

    when(sqlCtxMock.sql(startsWith("show partitions"))).thenReturn(dfPart)
    when(dfPart.orderBy(any[Column]))              .thenReturn(dfPart)
    when(dfPart.orderBy(any[Array[Column]](): _*)) .thenReturn(dfPart)
    when(dfPart.limit(anyInt()))                   .thenReturn(dfPart)
    when(dfPart.collect())                         .thenReturn(Array(rowPart))

    // 2) DataFrame comodín para el resto de tablas/consultas --------------
    val anyDF  = mock[DataFrame](RETURNS_DEEP_STUBS)
    val anyRow = mock[Row]
    when(anyRow.getString(anyInt())).thenReturn("")
    when(anyDF.collect()).thenReturn(Array(anyRow))
    when(anyDF.count()).thenReturn(1L)
    when(anyDF.columns).thenReturn(Array("c1","c2"))
    when(sqlCtxMock.table(any[String])) .thenReturn(anyDF)
    when(sqlCtxMock.sql(any[String]))   .thenReturn(anyDF)

    // 3) IngestEntity ------------------------------------------------------
    val entity = mock[IngestEntity]
    when(entity.getDataTimestampPart).thenReturn("20240605")
