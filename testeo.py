// Mock principal
val j0 = mock[DataFrame](withSettings().defaultAnswer(RETURNS_DEEP_STUBS))

// Stub para el join inicial
when(lastScenarioDataTable.join(any[DataFrame], any[Column], any[String])).thenReturn(j0)

// Stub de columnas para el map que hace: j0.columns.map(x => col("`" + x + "`"))
when(j0.columns).thenReturn(Array("dummy_col1", "dummy_col2"))

// Stub de select con columnas
when(j0.select(any[Seq[Column]]: _*)).thenReturn(j0)

// Stub de col()
when(j0.col(any[String])).thenReturn(mock[Column])

// Stub de drop().drop().drop() en cadena
when(j0.drop(any[Column]())).thenReturn(j0)

// Stub de join posteriores (j1.join(j2...))
when(j0.join(any[DataFrame], any[Column], any[String])).thenReturn(j0)

// Stub de distinct final
when(j0.distinct()).thenReturn(j0)
