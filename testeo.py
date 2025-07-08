test("validation_unique_period dispatcher e inner funcionan con mocks sin NPE") {
  // ─────────────────────────────────────────────
  // Preparación de mocks adicionales si faltan
  // ─────────────────────────────────────────────
  val mockDFSelected = mock(classOf[DataFrame], RETURNS_DEEP_STUBS)
  val mockDFWriter   = mock(classOf[DataFrameWriter[Row]], RETURNS_DEEP_STUBS)

  // select con múltiples columnas → selectPkValue
  when(mockDF.select(*[Column])).thenReturn(mockDFSelected)
  when(mockDFSelected.distinct()).thenReturn(mockDF)

  // Encadenamientos Spark comunes
  when(mockDF.select(any[Column])).thenReturn(mockDF)
  when(mockDF.distinct()).thenReturn(mockDF)
  when(mockDF.count()).thenReturn(2L)
  when(mockDF.columns).thenReturn(Array("c1", "c2"))
  when(mockDF.write).thenReturn(mockDFWriter)
  when(mockDFWriter.insertInto(any[String])).thenReturn(())

  // Filtros y persistencias de validation_unique_period
  when(mockDF.filter(any[Column])).thenReturn(mockDF)
  when(mockDF.where(any[Column])).thenReturn(mockDF)
  when(mockDF.persist(any())).thenReturn(mockDF)
  when(mockDF.unpersist()).thenReturn(mockDF)

  // Stub selectPkValue directo (no static mock necesario)
  when(ValFunUtil.selectPkValue(mockDF, "colA")).thenReturn(mockDF)
  when(ValFunUtil.selectPkValue(mockDF, "colB")).thenReturn(mockDF)

  // ─────────────────────────────────────────────
  // 1️⃣ Llamamos a la variante dispatcher
  // ─────────────────────────────────────────────
  ValFunUtil.validation_unique_period(
    st_metrics_input = mockDF,
    dateLoad         = "20250101",
    timestamp        = "20250101120000",
    targetdb         = "targetdb",
    targetTable      = "targetTable",
    fields           = HashMap(
                         "daily"   -> List("colA"),
                         "monthly" -> List("colB")
                       ),
    variables        = List("daily", "monthly")
  )

  // ─────────────────────────────────────────────
  // 2️⃣ Llamamos también a la inner directamente
  // ─────────────────────────────────────────────
  ValFunUtil.validation_unique_period(
    st_metrics_input = mockDF,
    dateLoad         = "20250101",
    timestamp        = "20250101120000",
    targetdb         = "targetdb",
    targetTable      = "targetTable",
    fields           = List("colA"),
    variables        = List("daily")
  )
}
