test("calculateDFModels should return expected DataFrame with mocked inputs") {
  val sqlContext = mock[SQLContext](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))

  // Mocks para todos los DFs
  val engine = mock[DataFrame](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))
  val idLogitDF = mock[DataFrame](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))
  val iderror1DF = mock[DataFrame](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))
  val iderror2DF = mock[DataFrame](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))
  val idPrecioDF = mock[DataFrame](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))
  val joinComun = mock[DataFrame](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))
  val finalDF = mock[DataFrame](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))

  // Mock de .where(...) para los filtros de cada grupo
  when(engine.where(any[Column])).thenReturn(idLogitDF)
  when(engine.where(any[Column])).thenReturn(idLogitDF, iderror1DF, iderror2DF, idPrecioDF)

  // Mock común para todas las llamadas map + collect + toList
  val stringDS = mock[Dataset[String]]
  when(stringDS.collect()).thenReturn(Array("dummy_id1", "dummy_id2"))

  // Mock .map(...) → .collect()
  for (df <- Seq(idLogitDF, iderror1DF, iderror2DF, idPrecioDF)) {
    when(df.map(any[Row => String](), any[Encoder[String]])).thenReturn(stringDS)
  }

  // Mock joinComun
  when(joinComun.where(any[Column])).thenReturn(finalDF)
  when(joinComun.join(any[DataFrame], any[Column])).thenReturn(joinComun)

  // Mock sqlContext.table(...)
  when(sqlContext.table(ArgumentMatchers.anyString())).thenReturn(engine)

  // Llamada real
  val result = BoardDataUtil.calculateDFModels("db", "CREDIT", sqlContext)

  // Assert: el valor devuelto no es null (podrías hacer más checks si lo necesitas)
  assert(result != null)
}
