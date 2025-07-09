test("validation_end_date cubre todos los casos de tipo_end_date") {
  val mockDF = mock(classOf[DataFrame], RETURNS_DEEP_STUBS)
  val mockFilteredDF = mock(classOf[DataFrame], RETURNS_DEEP_STUBS)
  val mockFieldVal = mock(classOf[DataFrame], RETURNS_DEEP_STUBS)
  val mockWriter = mock(classOf[DataFrameWriter[Row]], RETURNS_DEEP_STUBS)

  // Columnas simuladas para concat_ws
  when(mockFieldVal.columns).thenReturn(Array("col1", "col2"))
  when(mockFieldVal.apply("col1")).thenReturn(col("col1"))
  when(mockFieldVal.apply("col2")).thenReturn(col("col2"))

  // Encadenamiento de DataFrame para los filtros y selects
  when(mockDF.filter(any[Column])).thenReturn(mockFilteredDF)
  when(mockFilteredDF.filter(any[Column])).thenReturn(mockFieldVal)
  when(mockFieldVal.select(any[Column])).thenReturn(mockFieldVal)
  when(mockFieldVal.write).thenReturn(mockWriter)
  when(mockWriter.insertInto(any[String])).thenReturn(())

  // Stub del método selectPkValue para devolver el DataFrame procesado
  when(ValFunUtil.selectPkValue(mockFilteredDF, "fecha")).thenReturn(mockFieldVal)

  val tipos = Seq("QUARTERLY", "YEARLY", "MONTHLY", "WEEKLY") // WEEKLY cubre el else final

  tipos.foreach { tipo =>
    ValFunUtil.validation_end_date(
      st_metrics_input = mockDF,
      targetdb = "targetdb",
      targetTable = "targetTable",
      field = "fecha",
      variables = List("var1"),
      type_end_date = tipo,
      dateLoad = "20250101",
      timestamp = "20250101120000"
    )
  }
}
