test("validation_period dispatcher e inner funcionan correctamente con mocks") {

  // ───── Stubs adicionales necesarios ─────
  val mockDFSelected = mock(classOf[DataFrame], RETURNS_DEEP_STUBS)
  val mockWriter     = mock(classOf[DataFrameWriter[Row]], RETURNS_DEEP_STUBS)
  val mockAggDF      = mock(classOf[DataFrame], RETURNS_DEEP_STUBS)

  // select múltiple para selectPkValue
  when(mockDF.select(*[Column])).thenReturn(mockDFSelected)
  when(mockDFSelected.distinct()).thenReturn(mockDF)
  when(mockDFSelected.orderBy(any[Column])).thenReturn(mockDF)
  when(mockDF.limit(any[Int])).thenReturn(mockDF)

  // .head.getString(0) → simula una fecha cualquiera
  when(mockDF.head()).thenReturn(mock(classOf[Row]))
  when(mockDF.head().getString(0)).thenReturn("2025-01-01")

  // persistencia y filtros
  when(mockDF.persist(any())).thenReturn(mockDF)
  when(mockDF.unpersist()).thenReturn(mockDF)
  when(mockDF.where(any[Column])).thenReturn(mockDF)
  when(mockDF.filter(any[Column])).thenReturn(mockDF)
  when(mockDF.join(any[DataFrame], any[Column])).thenReturn(mockDF)
  when(mockDF.count()).thenReturn(2L)

  // .agg(...) para min_end_date
  when(mockDF.agg(any[Column])).thenReturn(mockAggDF)
  when(mockAggDF.as(any[String])).thenReturn(mockAggDF)

  // columnas, write
  when(mockDF.columns).thenReturn(Array("c1", "c2"))
  when(mockDF.write).thenReturn(mockWriter)
  when(mockWriter.insertInto(any[String])).thenReturn(())

  // selectPkValue
  when(ValFunUtil.selectPkValue(mockDF, "colX")).thenReturn(mockDF)

  // ───── 1. Llamada a la variante dispatcher ─────
  ValFunUtil.validation_period(
    st_metrics_input = mockDF,
    dateLoad         = "20250101",
    timestamp        = "20250101120000",
    targetdb         = "targetdb",
    targetTable      = "targetTable",
    fields           = scala.collection.immutable.HashMap("DAILY" -> List("colX")),
    type_period      = "MIDDLE",              // para que entre en lógica real
    variables        = List("DAILY")
  )

  // ───── 2. Llamada directa a la variante inner ─────
  ValFunUtil.validation_period(
    st_metrics_input = mockDF,
    min_end_date     = mockDF,
    max_end_date     = mockDF,
    dateLoad         = "20250101",
    timestamp        = "20250101120000",
    targetdb         = "targetdb",
    targetTable      = "targetTable",
    fields           = List("colX"),
    type_period      = "MIDDLE",
    variables        = List("DAILY")
  )
}
