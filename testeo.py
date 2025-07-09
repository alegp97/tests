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
  // Llamadas con los distintos tipos de validación
  ValFunUtil.validation_end_date(mockDF, "db", "table", "fecha", List("var1"), "QUARTERLY", "2025-07-09", "12:00")
  ValFunUtil.validation_end_date(mockDF, "db", "table", "fecha", List("var1"), "YEARLY", "2025-07-09", "12:00")
  ValFunUtil.validation_end_date(mockDF, "db", "table", "fecha", List("var1"), "MONTHLY", "2025-07-09", "12:00")
  ValFunUtil.validation_end_date(mockDF, "db", "table", "fecha", List("var1"), "WEEKLY", "2025-07-09", "12:00")

  // Verificación
  verify(mockDF).filter(any[Column])
  verify(ValFunUtil).selectPkValue(mockFilteredDF, field)
  verify(mockWriter).insertInto(contains("END_DATE_QUARTERLY"))
}
