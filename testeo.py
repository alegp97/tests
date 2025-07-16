val idFixed = mock[Dataset[Row]]
val mappedDataset = mock[Dataset[String]]

// stub para .map(...)
when(idFixed.map(
  mockAny[Function1[Row, String]](),
  mockAny[Encoder[String]]()
)).thenReturn(mappedDataset)

// stub para .collect()
when(mappedDataset.collect()).thenReturn(Array("dummy_id1", "dummy_id2"))
