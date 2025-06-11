  test("run avanza hasta .write sin NullPointer") {

    // ── 1.  SparkSession + SQLContext ────────────────────────────────────
    val sparkMock  = mock[SparkSession]
    val sqlCtxMock = mock[SQLContext]
    when(sparkMock.sqlContext).thenReturn(sqlCtxMock)

    // ── 2.  Cadena show partitions ▸ orderBy ▸ limit ▸ collect() ─────────
    val dfShow   = mock[DataFrame]          //  show partitions …
    val dfOrder  = mock[DataFrame]          //  after orderBy
    val dfLimit  = mock[DataFrame]          //  after limit(1)
    val rowPart  = mock[Row]

    when(rowPart.getString(0)).thenReturn("partition=20240605")

    //  show partitions …  →  dfShow
    when(sqlCtxMock.sql(startsWith("show partitions"))).thenReturn(dfShow)

    //  dfShow.orderBy(... ) → dfOrder         (sobrecarga 1-col + var-args)
    when(dfShow.orderBy(any[Column])).thenReturn(dfOrder)
    when(dfShow.orderBy(any[Array[Column]](): _*)).thenReturn(dfOrder)

    //  dfOrder.limit(1)     → dfLimit
    when(dfOrder.limit(anyInt())).thenReturn(dfLimit)

    //  dfLimit.collect()    → Array(rowPart)
    when(dfLimit.collect()).thenReturn(Array(rowPart))

    // ── 3.  DataFrame genérico para todas las demás tablas / SQL ─────────
    val anyDF  = mock[DataFrame](RETURNS_DEEP_STUBS)
    val anyRow = mock[Row]
    when(anyRow.getString(anyInt())).thenReturn("")
    when(anyDF.collect()).thenReturn(Array(anyRow))
    when(anyDF.columns).thenReturn(Array("c1","c2"))
    when(anyDF.count()).thenReturn(1L)
    when(sqlCtxMock.table(any[String])).thenReturn(anyDF)
    when(sqlCtxMock.sql(any[String])).thenReturn(anyDF)

    // ── 4.  IngestEntity válido ──────────────────────────────────────────
    val entity = mock[IngestEntity]
    when(entity.getDataTimestampPart).thenReturn("20240605")
