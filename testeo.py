 // 1. Spark mocks
    val sparkMock  = mock[SparkSession]
    val sqlCtxMock = mock[SQLContext]
    when(sparkMock.sqlContext).thenReturn(sqlCtxMock)

    // 2. show partitions  → orderBy → limit → collect
    val dfPart  = mock[DataFrame]
    val rowPart = mock[Row]
    when(rowPart.getString(0)).thenReturn("partition=20240605")

    when(dfPart.orderBy(any[Column]))                .thenAnswer(returnsSelf)
    when(dfPart.orderBy(any[Array[Column]](): _*))   .thenAnswer(returnsSelf)
    when(dfPart.limit(anyInt()))                     .thenAnswer(returnsSelf)
    when(dfPart.collect())                           .thenReturn(Array(rowPart))

    when(sqlCtxMock.sql(startsWith("show partitions"))).thenReturn(dfPart)

    // 3. cualquier tabla / sql → DataFrame comodín
    val anyDF = mock[DataFrame](RETURNS_DEEP_STUBS)
    when(anyDF.collect()).thenReturn(Array(mock[Row]))
    when(anyDF.columns).thenReturn(Array("c1","c2"))
    when(anyDF.count()).thenReturn(1L)
    when(sqlCtxMock.table(any[String]))  .thenReturn(anyDF)
    when(sqlCtxMock.sql(any[String]))    .thenReturn(anyDF)

    // 4. IngestEntity
    val entity = mock[IngestEntity]
    when(entity.getDataTimestampPart).thenReturn("20240605")
