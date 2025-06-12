test("run - caso completo con partition_key presente y columnas transformadas") {
    val sqlContext = mock[SQLContext]
    val settings   = mock[BoardsArgs]

    val sourcedb   = "src_db"
    val targetdb   = "tgt_db"
    val sourceTable= "my_source"
    val timestamp  = "20240601"
    val outputTable= s"st_metrics_input_pk_$timestamp"

    when(settings.sourcedb).thenReturn(sourcedb)
    when(settings.targetdb).thenReturn(targetdb)
    when(settings.sourceTable).thenReturn(sourceTable)
    when(settings.data_timestamp_part).thenReturn(timestamp)

    // -------- DataFrames base --------
    val sourceDF  = mock[DataFrame]
    val contextDF = mock[DataFrame]
    val joinedDF  = mock[DataFrame]
    val selectedDF= mock[DataFrame]
    val renamedDF = mock[DataFrame]
    val withPKDF  = mock[DataFrame]
    val withRowCountDF = mock[DataFrame]
    val castedDF  = mock[DataFrame]

    when(sqlContext.table(s"$sourcedb.$sourceTable")).thenReturn(sourceDF)
    when(sourceDF.where(any[Column])).thenReturn(sourceDF)
    when(sourceDF.drop("supra_source")).thenReturn(sourceDF)
    when(sourceDF.drop("data_timestamp_part")).thenReturn(sourceDF)
    when(sourceDF.columns).thenReturn(Array("report_date","workspace","col_a","num_col","weight_inout"))

    when(sqlContext.table(s"$targetdb.contexts_st")).thenReturn(contextDF)
    when(contextDF.drop("supra_source")).thenReturn(contextDF)

    when(sourceDF.col(eqTo("workspace"))).thenReturn(col("workspace"))
    when(sourceDF.col(any[String])).thenReturn(col("dummy"))
    when(contextDF.col(any[String])).thenAnswer(inv => col(inv.getArgument(0)))

    // join – overload de 2 y 3 args
    when(sourceDF.join(any[DataFrame], any[Column])).thenReturn(joinedDF)
    when(sourceDF.join(any[DataFrame], any[Column], any[String])).thenReturn(joinedDF)

    when(joinedDF.drop(any[Column])).thenReturn(joinedDF)
    when(joinedDF.select(any[Array[Column]](): _*)).thenReturn(selectedDF)
    when(selectedDF.withColumnRenamed(any[String], any[String])).thenReturn(renamedDF)
    when(renamedDF.dropDuplicates()).thenReturn(renamedDF)

    // ---------- show partitions mock ----------
    val partitionsDF  = mock[DataFrame]
    val partitionsRow = mock[Row]
    when(partitionsRow.getString(0)).thenReturn("20240601=value")
    when(sqlContext.sql(contains("show partitions"))).thenReturn(partitionsDF)
    when(partitionsDF.orderBy(any[Column])).thenReturn(partitionsDF)
    when(partitionsDF.limit(any[Int])).thenReturn(partitionsDF)
    when(partitionsDF.collect()).thenReturn(Array(partitionsRow))

    // ---------- fields_dict mock ----------
    val columnsVariablesDF = mock[DataFrame]
    val distinctVarsDF     = mock[DataFrame]
    val fkRow              = mock[Row]

    when(sqlContext.table(s"$targetdb.fields_dict")).thenReturn(columnsVariablesDF)
    when(columnsVariablesDF.where(any[Column])).thenReturn(columnsVariablesDF)
    when(columnsVariablesDF.select(any[Array[Column]](): _*)).thenReturn(columnsVariablesDF)
    when(columnsVariablesDF.distinct()).thenReturn(distinctVarsDF)
    when(columnsVariablesDF.count()).thenReturn(1L)
    when(fkRow.getString(0)).thenReturn("partition_key")
    when(distinctVarsDF.collect()).thenReturn(Array(fkRow))

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

    // ---------- bloque final transformaciones ----------
    val df_partition = mock[DataFrame](withSettings().defaultAnswer(RETURNS_DEEP_STUBS))

    when(renamedDF.drop(eqTo("data_date_part"))).thenReturn(df_partition)
    when(df_partition.columns).thenReturn(Array("num_col","weight_inout"))
    // mock schema to avoid NPE when code accesses df_partition.schema
    val schemaMock = StructType(Seq(
      StructField("num_col", DoubleType),
      StructField("weight_inout", DoubleType)
    ))
    when(df_partition.schema).thenReturn(schemaMock)
    when(df_partition.withColumn(eqTo("num_col"), any[Column])).thenReturn(df_partition)
    when(df_partition.withColumn(eqTo("weight_inout"), any[Column])).thenReturn(df_partition)
    when(df_partition.withColumn(eqTo("row_count"), any[Column])).thenReturn(df_partition)
    // encadenado extra para select / withColumnRenamed / dropDuplicates
    when(df_partition.select(any[Array[Column]](): _*)).thenReturn(df_partition)
    when(df_partition.withColumnRenamed(any[String], any[String])).thenReturn(df_partition)
    when(df_partition.dropDuplicates()).thenReturn(df_partition)
    when(df_partition.count()).thenReturn(10L)

    // DROP table
    when(sqlContext.sql(contains("drop table"))).thenReturn(mock[DataFrame])

    // Ejecutar
    GeneratePartitionKeyJob.run(sqlContext, settings)
  }
