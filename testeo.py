    // 1. SparkSession y SQLContext
    val sparkMock  = mock[SparkSession]
    val sqlCtxMock = mock[SQLContext]
    when(sparkMock.sqlContext).thenReturn(sqlCtxMock)

    // 2. show partitions ▸ orderBy ▸ limit ▸ collect
    val dfShow  = mock[DataFrame]   // A
    val dfOrd   = mock[DataFrame]   // B
    val dfLim   = mock[DataFrame]   // C
    val rowPart = mock[Row]

    when(rowPart.getString(0)).thenReturn("partition=20240605")

    // sql(...) -> A
    when(sqlCtxMock.sql(startsWith("show partitions"))).thenReturn(dfShow)

    // A.orderBy(...) -> B
    when(dfShow.orderBy(any[Column])).thenReturn(dfOrd)
    when(dfShow.orderBy(any[Array[Column]](): _*)).thenReturn(dfOrd)

    // B.limit(1)     -> C
    when(dfOrd.limit(anyInt())).thenReturn(dfLim)

    // C.collect()    -> fila simulada
    when(dfLim.collect()).thenReturn(Array(rowPart))
