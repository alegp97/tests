// Mock raíz del DataFrame que devuelve sql()
val camposOriginals = mock[DataFrame](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))

// Simulación del resultado de sqlContext.sql(...)
when(sqlContext.sql(ArgumentMatchers.startsWith("select a.name"))).thenReturn(camposOriginals)

// Mockeo de toda la cadena: .distinct().collect()
val rowMock = mock[Row]
when(rowMock.getAs[String]("name")).thenReturn("campo_1")

// Puedes añadir más si quieres varias filas:
val rows = Array(rowMock)

when(camposOriginals.distinct()).thenReturn(camposOriginals)
when(camposOriginals.collect()).thenReturn(rows)
