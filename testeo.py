// 4. Mock para MAP
val stringDsMock = mock[Dataset[String]]
val encoder = Encoders.STRING
when(selectedMock.map(any[MapFunction[Row, String]])(any[Encoder[String]]()))
  .thenReturn(stringDsMock)

// 5. Mock para COLLECT
when(stringDsMock.collect()).thenReturn(Array("id_001", "id_002"))
