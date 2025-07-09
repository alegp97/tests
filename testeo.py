test("validation_period - FIRST_AND_SECOND ejecuta correctamente join y drop") {
  // Mocks necesarios
  val mockFilteredDF      = mock[DataFrame](RETURNS_DEEP_STUBS)
  val mockFirstTwoEndDate = mock[DataFrame](RETURNS_DEEP_STUBS)
  val mockJoinedDF        = mock[DataFrame](RETURNS_DEEP_STUBS)
  val mockDroppedDF       = mock[DataFrame](RETURNS_DEEP_STUBS)
  val mockWriter          = mock[DataFrameWriter[Row]]
  val mockRow             = mock[Row]

  // st_metrics_input.filter → devuelve filtrado
  when(mockDF.filter(any[Column])).thenReturn(mockFilteredDF)
  when(mockFilteredDF.count()).thenReturn(1L) // Para entrar al if

  // firstTwoEndDate = select(...).distinct().orderBy(...).limit(2)
  when(mockFilteredDF.select(any[Column])).thenReturn(mockFirstTwoEndDate)
  when(mockFirstTwoEndDate.distinct()).thenReturn(mockFirstTwoEndDate)
  when(mockFirstTwoEndDate.orderBy(any[Column])).thenReturn(mockFirstTwoEndDate)
  when(mockFirstTwoEndDate.limit(2)).thenReturn(mockFirstTwoEndDate)

  // join de stMetricsInputWithGranularity.join(firstTwoEndDate, cond)
  when(mockFilteredDF.col("end_date")).thenReturn(mock[Column])
  when(mockFirstTwoEndDate.col("end_date")).thenReturn(mock[Column])
  when(mockFilteredDF.join(eqTo(mockFirstTwoEndDate), any[Column])).thenReturn(mockJoinedDF)

  // drop(...)
  when(mockJoinedDF.drop(any[Column])).thenReturn(mockDroppedDF)

  // Simular persistencia y escritura
  when(mockDroppedDF.persist(any())).thenReturn(mockDroppedDF)
  when(mockDroppedDF.where(any[Column])).thenReturn(mockDroppedDF)
  when(mockDroppedDF.count()).thenReturn(1L)
  when(ValFunUtil.selectPkValue(mockDroppedDF, "colX")).thenReturn(mockDroppedDF)

  when(mockDroppedDF.columns).thenReturn(Array("colX"))
  when(mockDroppedDF.write).thenReturn(mockWriter)
  when(mockWriter.insertInto(any[String])).thenReturn(())

  // Mock fecha en head()
  when(mockDroppedDF.head()).thenReturn(mockRow)
  when(mockRow.getString(0)).thenReturn("2025-01-01")

  // Ejecutar
  ValFunUtil.validation_period(
    st_metrics_input = mockDF,
    min_end_date     = mockDroppedDF,
    max_end_date     = mockDroppedDF,
    dateLoad         = "20250101",
    timestamp        = "20250101120000",
    targetdb         = "targetdb",
    targetTable      = "targetTable",
    fields           = List("colX"),
    type_period      = "FIRST_AND_SECOND",
    variables        = List("daily")
  )

// Este where es el que se ejecuta en el bucle de validación de campos
val mockDFToCheck = mock[DataFrame]
when(mockDroppedDF.where(any[Column])).thenReturn(mockDFToCheck)
when(mockDFToCheck.count()).thenReturn(1L) // hace que entre al if

// Simulación de selectPkValue y escritura final
when(ValFunUtil.selectPkValue(mockDFToCheck, "colX")).thenReturn(mockDF)
when(mockDF.write).thenReturn(mockWriter)
when(mockWriter.insertInto(any[String])).thenReturn(())

}
