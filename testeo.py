    // partition_key detectado
    // ---- row_count + drop(data_date_part) chain ----
    when(renamedDF.withColumn(eqTo("row_count"), any[Column])).thenReturn(withRowCountDF)
    when(withRowCountDF.drop(eqTo("data_date_part"))).thenReturn(df_partition)
    when(withRowCountDF.columns).thenReturn(Array("num_col","weight_inout"))
    when(withRowCountDF.withColumn(eqTo("num_col"), any[Column])).thenReturn(df_partition)
    when(withRowCountDF.withColumn(eqTo("weight_inout"), any[Column])).thenReturn(df_partition)

    when(renamedDF.select(any[Array[Column]](): _*)).thenReturn(withPKDF)
    val pkRow = mock[Row]
    when(pkRow.getString(0)).thenReturn("value")
    when(withPKDF.collect()).thenReturn(Array(pkRow))
