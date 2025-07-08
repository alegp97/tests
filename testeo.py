test("validation_period dispatcher e inner funcionan correctamente con mocks") {
  // Stubs adicionales necesarios
  val mockDFGranularityFiltered = mock[DataFrame](RETURNS_DEEP_STUBS)
  val mockAggDF                 = mock[DataFrame](RETURNS_DEEP_STUBS)
  val mockWriter                = mock[DataFrameWriter[Row]]
  val mockRow                   = mock[Row]
  val mockDFSelected            = mock[DataFrame](RETURNS_DEEP_STUBS)

  // selectPkValue
  when(ValFunUtil.selectPkValue(mockDF, "colX")).thenReturn(mockDF)

  // filter → persist
  when(mockDF.filter(any[Column])).thenReturn(mockDFGranularityFiltered)
  when(mockDFGranularityFiltered.persist(any())).thenReturn(mockDFGranularityFiltered)

  // agg → para min_end_date y max_end_date
  when(mockDFGranularityFiltered.agg(any[Column])).thenReturn(mockAggDF)
  when(mockAggDF.head()).thenReturn(mockRow)
  when(mockRow.getString(0)).thenReturn("2025-01-01")

  // select → distinct → orderBy → limit → head → getString → para max_end_date
  when(mockDFGranularityFiltered.select(any[Column])).thenReturn(mockDFSelected)
  when(mockDFSelected.distinct()).thenReturn(mockDFSelected)
  when(mockDFSelected.orderBy(any[Column])).thenReturn(mockDFSelected)
  when(mockDFSelected.limit(anyInt())).thenReturn(mockDFSelected)
  when(mockDFSelected.head()).thenReturn(mockRow)
  when(mockRow.getString(0)).thenReturn("2025-01-31")

  // count() del DF filtrado con granularidad
  when(mockDFGranularityFiltered.count()).thenReturn(2L)

  // where, join y demás stubs genéricos para campos
  when(mockDFGranularityFiltered.where(any[Column])).thenReturn(mockDFGranularityFiltered)
  when(mockDFGranularityFiltered.join(any[DataFrame], any[Column])).thenReturn(mockDFGranularityFiltered)

  // write de Drilldown
  when(mockDF.columns).thenReturn(Array("colX"))
  when(mockDF.write).thenReturn(mockWriter)
  when(mockWriter.insertInto(any[String])).thenReturn(())

  // llamada real
  ValFunUtil.validation_period(
    st_metrics_input = mockDF,
    dateLoad         = "20250101",
    timestamp        = "20250101120000",
    targetdb         = "targetdb",
    targetTable      = "targetTable",
    fields           = mutable.HashMap("daily" -> List("colX")),
    type_period      = "MIDDLE",
    variables        = List("daily")
  )
}
