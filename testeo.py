test("validation_end_date con granularidad QUARTERLY y condición falsa") {
  val mockDF = mock(classOf[DataFrame], RETURNS_DEEP_STUBS)
  val mockFilteredDF = mock(classOf[DataFrame], RETURNS_DEEP_STUBS)
  val mockFieldVal = mock(classOf[DataFrame])
  val mockWriter = mock(classOf[DataFrameWriter[Row]])

  val variables = List("var1")
  val field = "fecha"
  val type_end_date = "QUARTERLY"

  // Simula .filter(...) => mockFilteredDF
  when(mockDF.filter(any[Column])).thenReturn(mockFilteredDF)

  // Simula selectPkValue y writeDrilldown
  when(ValFunUtil.selectPkValue(mockFilteredDF, field)).thenReturn(mockFieldVal)
  when(mockFieldVal.write).thenReturn(mockWriter)
  when(mockWriter.insertInto(any[String])).thenReturn(())

  // Ejecutar
  ValFunUtil.validation_end_date(
    mockDF,
    targetdb = "miBD",
    targetTable = "miTabla",
    field = field,
    variables = variables,
    type_end_date = type_end_date,
    dateLoad = "2025-07-09",
    timestamp = "12:00"
  )

  // Verificación
  verify(mockDF).filter(any[Column])
  verify(ValFunUtil).selectPkValue(mockFilteredDF, field)
  verify(mockWriter).insertInto(contains("END_DATE_QUARTERLY"))
}
