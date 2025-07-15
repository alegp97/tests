when(
  engine
    .where(anyArg[Column]())
    // 1ª lista de parámetros
    .map(anyArg[Function1[Row, _]]())          // r => …   (tipo laxo)
    // 2ª lista implícita  (Encoder)
    (Encoders.STRING.asInstanceOf[Encoder[String]])
    .collect()
).thenReturn(Array("dummy_id1", "dummy_id2"))
