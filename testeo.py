val ds = mock[Dataset[String]]
when(idLogitDF
       .map(any[Function1[Row, String]]())   // variante Scala
       (any())                               // cualquier Encoder[String]
).thenReturn(ds)

// 3️⃣  ds.collect()  →  array real
when(ds.collect()).thenReturn(Array("dummy_id1", "dummy_id2"))
