// Transformaciones
    val df_stmetrics_partitionkey = mock[DataFrame]
    val df_afterDrop = mock[DataFrame]
    val df_castedNumeric = mock[DataFrame]
    val df_castedWeight = mock[DataFrame]

    when(finalDF.drop(eqTo("data_date_part"))).thenReturn(df_afterDrop)
    when(df_afterDrop.columns).thenReturn(Array("num_col"))
    when(df_afterDrop.withColumn(eqTo("num_col"), any[Column])).thenReturn(df_castedNumeric)
    when(df_castedNumeric.withColumn(eqTo("weight_inout"), any[Column])).thenReturn(df_castedWeight)
    when(df_castedWeight.count()).thenReturn(10L)
    when(df_castedWeight.write).thenReturn(writer)
    doNothing().when(writer).saveAsTable(eqTo(s"$targetdb.$outputTable"))
    when(renamedDF.select(any[Array[Column]](): _*)).thenReturn(renamedDF)
    when(renamedDF.withColumn(any[String], any[Column])).thenReturn(renamedDF)
    when(renamedDF.drop(eqTo("data_date_part"))).thenReturn(renamedDF)
    when(renamedDF.columns).thenReturn(Array("num_col", "weight_inout"))

    when(renamedDF.withColumn(eqTo("row_count"), any[Column])).thenReturn(withRowCountDF)
    when(withRowCountDF.withColumn(any[String], any[Column])).thenReturn(castedDF)
    when(castedDF.withColumn(any[String], any[Column])).thenReturn(finalDF)
    when(finalDF.columns).thenReturn(Array("x"))

    when(renamedDF.count()).thenReturn(10L)
    when(finalDF.count()).thenReturn(10L)
