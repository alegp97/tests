    // -----------------------------------------------------------------------------
    // Mock para fields_dict del targetdb  (evita NPE en línea 52)
    // -----------------------------------------------------------------------------
    val columnsVariablesDF   = mock[DataFrame]
    val distinctVarsDF       = mock[DataFrame]
    val fkRow                = mock[Row]

    when(sqlContext.table(s"$targetdb.fields_dict")).thenReturn(columnsVariablesDF)
    when(columnsVariablesDF.where(any[Column])).thenReturn(columnsVariablesDF)
    when(columnsVariablesDF.select(any[Array[Column]](): _*)).thenReturn(columnsVariablesDF)
    when(columnsVariablesDF.distinct()).thenReturn(distinctVarsDF)
    when(columnsVariablesDF.count()).thenReturn(1L)
    when(fkRow.getString(0)).thenReturn("partition_key")
    when(distinctVarsDF.collect()).thenReturn(Array(fkRow))
