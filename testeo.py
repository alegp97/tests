// 2) Stub de map-scala + collect
when(
  idLogitDF
    .map(any[Function1[Row, String]]())          // 1er parámetro
    (any[Encoder[String]]())                     // 2º parámetro (implícito)
    .collect()
).thenReturn(Array("dummy_id1", "dummy_id2"))
