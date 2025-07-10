val mockRow = mock[Row](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))

when(mockRow.get(0)).thenReturn("dummy_value")
when(mockRow.apply(0)).thenReturn("dummy_value")
when(mockRow.toString()).thenReturn("dummy_value")

val collected = Array(mockRow)
