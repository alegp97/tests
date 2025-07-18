// Mocks principales
val lastScenarioDataTable = mock[DataFrame]
val peque = mock[DataFrame]
val j0 = mock[DataFrame]
val dummyCol1 = mock[Column]
val dummyCol2 = mock[Column]
val dummyColScenario = mock[Column]

// Stub para el .col(...) en peque
when(peque.col("partition_key")).thenReturn(dummyCol1)
when(peque.col("period_id")).thenReturn(dummyCol2)
when(peque.col("scenario_id")).thenReturn(dummyColScenario)

// Stub para los drop() encadenados en peque
when(peque.drop(dummyCol1)).thenReturn(peque)
when(peque.drop(dummyCol2)).thenReturn(peque)
when(peque.drop(dummyColScenario)).thenReturn(peque)
