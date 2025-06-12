  test("run - caso completo con partition_key presente y columnas transformadas") {
    val sqlContext = mock[SQLContext]
    val settings = mock[BoardsArgs]

    val sourcedb = "src_db"
    val targetdb = "tgt_db"
    val sourceTable = "my_source"
    val timestamp = "20240601"
    val outputTable = s"st_metrics_input_pk_$timestamp"

    when(settings.sourcedb).thenReturn(sourcedb)
    when(settings.targetdb).thenReturn(targetdb)
    when(settings.sourceTable).thenReturn(sourceTable)
    when(settings.data_timestamp_part).thenReturn(timestamp)

    // Mocks
    val sourceDF = mock[DataFrame]
    val contextDF = mock[DataFrame]
    val joinedDF = mock[DataFrame]
    val selectedDF = mock[DataFrame]
    val renamedDF = mock[DataFrame]
    val withPKDF = mock[DataFrame]
    val withRowCountDF = mock[DataFrame]
    val castedDF = mock[DataFrame]
    val finalDF = mock[DataFrame]

    when(sqlContext.table(s"$sourcedb.$sourceTable")).thenReturn(sourceDF)
    when(sourceDF.where(any[Column])).thenReturn(sourceDF)
    when(sourceDF.drop("supra_source")).thenReturn(sourceDF)
    when(sourceDF.drop("data_timestamp_part")).thenReturn(sourceDF)
    when(sourceDF.columns).thenReturn(Array("report_date", "workspace", "col_a", "num_col", "weight_inout"))

    when(sqlContext.table(s"$targetdb.contexts_st")).thenReturn(contextDF)
    when(contextDF.drop("supra_source")).thenReturn(contextDF)

    when(sourceDF.col(any[String])).thenAnswer(inv => col(inv.getArgument(0)))
    when(contextDF.col(any[String])).thenAnswer(inv => col(inv.getArgument(0)))

    val df2 = mock[DataFrame]
    val joinCond = mock[Column]
    val joinType = "inner"

    when(sourceDF.join(df2, joinCond, joinType)).thenReturn(joinedDF)
    when(joinedDF.drop(any[Column])).thenReturn(joinedDF)
    when(joinedDF.select(any[Array[Column]])).thenReturn(selectedDF)
    when(selectedDF.withColumnRenamed(any[String], any[String])).thenReturn(renamedDF)
    when(renamedDF.dropDuplicates()).thenReturn(renamedDF)

    // Partition key presente
    when(renamedDF.select(any[Array[Column]])).thenReturn(withPKDF)
    val partitionRow = mock[Row]
    when(partitionRow.getString(0)).thenReturn("value")
    when(withPKDF.collect()).thenReturn(Array(partitionRow))

    // Transformaciones
    when(renamedDF.select(any[Array[Column]])).thenReturn(renamedDF)
    when(renamedDF.withColumn(any[String], any[Column])).thenReturn(renamedDF)
    when(renamedDF.drop(eqTo("data_date_part"))).thenReturn(renamedDF)
    when(renamedDF.columns).thenReturn(Array("num_col", "weight_inout"))

    when(renamedDF.withColumn(eqTo("row_count"), any[Column])).thenReturn(withRowCountDF)
    when(withRowCountDF.withColumn(any[String], any[Column])).thenReturn(castedDF)
    when(castedDF.withColumn(any[String], any[Column])).thenReturn(finalDF)
    when(finalDF.columns).thenReturn(Array("x"))

    when(renamedDF.count()).thenReturn(10L)
    when(finalDF.count()).thenReturn(10L)

    // DROP y saveAsTable
    when(sqlContext.sql(contains("drop table"))).thenReturn(mock[DataFrame])
    val writer = mock(classOf[DataFrameWriter[Row]])
    doReturn(writer: DataFrameWriter[Row]).when(finalDF).write
    doNothing().when(writer).saveAsTable(eqTo(s"$targetdb.$outputTable"))

    // Ejecutar
    GeneratePartitionKeyJob.run(sqlContext, settings)
  }
}
