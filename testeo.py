val dsString = mock[Dataset[String]](withSettings().defaultAnswer(org.mockito.Answers.RETURNS_DEEP_STUBS))
when(dsString.collect()).thenReturn(Array("dummy_id1","dummy_id2"))

// 2. map (…)  →  dsString   (elige la sobrecarga correcta)
when(
  idLogitDF.map(any[MapFunction[Row,String]](), any[Encoder[String]]())
).thenReturn(dsString)
