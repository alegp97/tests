when(idLogitDF.select(any[Column])).thenReturn(idLogitDF)   // si en tu código hay .select(...)
when(idLogitDF.distinct()).thenReturn(idLogitDF)            // si hay .distinct()

// ────────── 4.  Dataset[String] que saldrá del map ─────────────────────────
val dsString = mock[Dataset[String]]()                       // tampoco deep-stubs
when(dsString.collect()).thenReturn(Array("dummy_id1","dummy_id2"))

// ────────── 5.  map(Java)  →  dsString ─────────────────────────────────────
when(
  idLogitDF.map(
    any[MapFunction[Row,String]](),      // λ  r => r(0).toString
    any[Encoder[String]]()               // Encoders.STRING
  )
).thenReturn(dsString)
