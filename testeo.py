when(
  idLogitDF.map(
    any[Function1[Row, String]]()      // la lambda Scala
  )(                                     // paréntesis del parámetro implícito
    any[Encoder[String]]()              // el encoder implícito
  )
).thenReturn(dsString)

// 3. Stub de collect() sobre ese mismo Dataset[String]
when(dsString.collect()).thenReturn(Array("dummy_id1", "dummy_id2"))
