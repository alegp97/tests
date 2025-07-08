val mockMinRow = mock[Row]
val mockMaxRow = mock[Row]
val mockMinAgg = mock[DataFrame]
val mockMaxAgg = mock[DataFrame]

when(mockMinRow.getString(0)).thenReturn("2025-01-01")
when(mockMaxRow.getString(0)).thenReturn("2025-01-31")

when(mockMinAgg.head()).thenReturn(mockMinRow)
when(mockMaxAgg.head()).thenReturn(mockMaxRow)

when(mockDF.agg(col("end_date").as("end_date"))).thenReturn(mockMinAgg) // para min_end_date
when(mockDF.select(col("end_date"))).thenReturn(mockMaxAgg)             // para max_end_date
