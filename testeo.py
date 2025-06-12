val joinedDF = mock[DataFrame]
when(sourceDF.join(any[DataFrame], any[Column], any[String])).thenReturn(joinedDF)
when(joinedDF.drop(any[Column])).thenReturn(joinedDF)
when(joinedDF.select(any[List[Column]])).thenReturn(joinedDF)
when(joinedDF.withColumnRenamed(any[String], any[String])).thenReturn(joinedDF)
when(joinedDF.dropDuplicates()).thenReturn(joinedDF)
