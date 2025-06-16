// ─────────────── DataFrame final y transformaciones ────────────────
    val df_partition =
      mock[DataFrame](withSettings().defaultAnswer(RETURNS_DEEP_STUBS))

    // row_count → devuelve directamente df_partition
    when(pkStmiDF.withColumn(eqTo("row_count"), any[Column])).thenReturn(df_partition)

    // drop y casts sobre df_partition
    when(df_partition.drop(eqTo("data_date_part"))).thenReturn(df_partition)
    when(df_partition.drop(any[String])).thenReturn(df_partition)
    when(df_partition.columns).thenReturn(Array("num_col","weight_inout"))

    val schemaMock = StructType(Seq(
      StructField("num_col",      DoubleType),
      StructField("weight_inout", DoubleType)
    ))
    when(df_partition.schema).thenReturn(schemaMock)

    when(df_partition.withColumn(eqTo("num_col"),      any[Column])).thenReturn(df_partition)
    when(df_partition.withColumn(eqTo("weight_inout"), any[Column])).thenReturn(df_partition)
    when(df_partition.select(any[Array[Column]](): _*)).thenReturn(df_partition)
    when(df_partition.withColumnRenamed(anyString(), anyString())).thenReturn(df_partition)
    when(df_partition.dropDuplicates()).thenReturn(df_partition)
    when(df_partition.count()).thenReturn(10L)

    // ─────────────── DROP TABLE previo ────────────────
    when(sqlContext.sql(contains("drop table"))).thenReturn(mock[DataFrame])
