// Mock de Row que representa una partición
val mockRow = mock[Row]
when(mockRow.getString(0)).thenReturn("fecha=2023-10-01")

// Mock del DataFrame que devolverá esas particiones
val mockPartitionsDF = mock[DataFrame](withSettings().defaultAnswer(RETURNS_DEEP_STUBS))

// Cuando se hace el show partitions desde spark.sqlContext.sql(...)
when(sparkMock.sqlContext).thenReturn(sqlContext)
when(sqlContext.sql(startsWith("show partitions"))).thenReturn(mockPartitionsDF)
when(mockPartitionsDF.collect()).thenReturn(Array(mockRow))
