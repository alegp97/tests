val engine = mock[DataFrame]
val engineWhere = mock[DataFrame]
val engineSelect = mock[DataFrame]
val engineDistinct = mock[DataFrame]

when(sqlContext.table(s"$sourcedb.sae_engine")).thenReturn(engine)
when(engine.where(any[Column])).thenReturn(engineWhere)
when(engineWhere.select(any[Column])).thenReturn(engineSelect)
when(engineSelect.distinct()).thenReturn(engineDistinct)
