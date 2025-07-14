when(
  idLogitDF.where(any[Column])
           .map(any[Row ⇒ String])
           .collect()
).thenReturn(Array("dummy_id1", "dummy_id2"))
