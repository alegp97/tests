// ── (2)  Crea un Dataset[String] mock para después del map ───────────
val dsString = mock[Dataset[String]](
  withSettings().defaultAnswer(org.mockito.Answers.RETURNS_DEEP_STUBS)
)
// Cuando se llame collect() sobre él → devuelve tu array real
when(dsString.collect()).thenReturn(Array("dummy_id1", "dummy_id2"))

// ── (3)  map(...)(encoder)   →   dsString  ───────────────────────────
when(
  idLogitDF
    .map(any[Row => String]())(any[Encoder[String]]())
).
