    // ---------- bloque final transformaciones ----------
    val df_partition = mock[DataFrame](withSettings().defaultAnswer(RETURNS_DEEP_STUBS))

    when(renamedDF.drop(eqTo("data_date_part"))).thenReturn(df_partition)
    when(df_partition.columns).thenReturn(Array("num_col","weight_inout"))
    when(df_partition.withColumn(eqTo("num_col"), any[Column])).thenReturn(df_partition)
    when(df_partition.withColumn(eqTo("weight_inout"), any[Column])).thenReturn(df_partition)
    when(df_partition.withColumn(eqTo("row_count"), any[Column])).thenReturn(df_partition)
    // encadenado extra para select / withColumnRenamed / dropDuplicates
    when(df_partition.select(any[Array[Column]](): _*)).thenReturn(df_partition)
    when(df_partition.withColumnRenamed(any[String], any[String])).thenReturn(df_partition)
    when(df_partition.dropDuplicates()).thenReturn(df_partition)
    when(df_partition.count()).thenReturn(10L)
