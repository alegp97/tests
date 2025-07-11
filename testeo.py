val ds = mock[Dataset[String]]
when(ds.collect()).thenReturn(Array("id1", "id2"))
when(idLogitDF.map(any[Row ⇒ String], any[Encoder[String]])).thenReturn(ds)
