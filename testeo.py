 // 1. SparkSession / SQLContext
    val sparkMock  = mock[SparkSession]
    val sqlCtxMock = mock[SQLContext]
    when(sparkMock.sqlContext).thenReturn(sqlCtxMock)

    // 2. show partitions → orderBy → limit → collect
    val dfPart  = mock[DataFrame]
    val rowPart = mock[Row]
    when(rowPart.getString(0)).thenReturn("partition=20240605")

    when(sqlCtxMock.sql(contains("show partitions")))  .thenReturn(dfPart)
    when(dfPart.orderBy(any[Column]))                  .thenReturn(dfPart)
    when(dfPart.orderBy(any[Array[Column]](): _*))     .thenReturn(dfPart)
    when(dfPart.limit(anyInt()))                       .thenReturn(dfPart)
    when(dfPart.collect())                             .thenReturn(Array(rowPart))

    // 3. DataFrame comodín para cualquier otra tabla / SQL
    val anyDF = mock[DataFrame](RETURNS_DEEP_STUBS)
    when(anyDF.collect()).thenReturn(Array(mock[Row]))
    when(anyDF.columns).thenReturn(Array("c1","c2"))
    when(sqlCtxMock.table(any[String])) .thenReturn(anyDF)
    when(sqlCtxMock.sql(any[String]))   .thenReturn(anyDF)

    // 4. IngestEntity válido
    val ent = mock[IngestEntity]
    when(ent.getDataTimestampPart).thenReturn("20240605")
