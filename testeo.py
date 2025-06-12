  test("run - fallback cuando fields_dict está vacío y partition_key no existe") {
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

    val sourceDF = mock[DataFrame]
    val contextDF = mock[DataFrame]
    val joinedDF = mock[DataFrame]
    val selectedDF = mock[DataFrame]
    val renamedDF = mock[DataFrame]
    val withRowCountDF = mock[DataFrame]
    val castedDF = mock[DataFrame]

    when(sqlContext.table(s"$sourcedb.$sourceTable")).thenReturn(sourceDF)
    when(sourceDF.where(any())).thenReturn(sourceDF)
    when(sourceDF.drop("supra_source")).thenReturn(sourceDF)
    when(sourceDF.drop("data_timestamp_part")).thenReturn(sourceDF)
    when(sourceDF.columns).thenReturn(Array("report_date", "workspace"))

    when(sqlContext.table(s"$targetdb.contexts_st")).thenReturn(contextDF)
    when(contextDF.drop("supra_source")).thenReturn(contextDF)

    when(sourceDF.col(any[String])).thenAnswer(inv => col(inv.getArgument(0)))
    when(contextDF.col(any[String])).thenAnswer(inv => col(inv.getArgument(0)))
    when(sourceDF.join(any(), any(), any())).thenReturn(joinedDF)
    when(joinedDF.drop("workspace")).thenReturn(joinedDF)
    when(joinedDF.select(any[List[org.apache.spark.sql.Column]])).thenReturn(selectedDF)
    when(selectedDF.withColumnRenamed(any(), any())).thenReturn(renamedDF)

    // fields_dict está vacío
    val partitionsDF = mock[DataFrame]
    when(sqlContext.sql(contains("show partitions"))).thenReturn(partitionsDF)
    when(partitionsDF.orderBy(any[org.apache.spark.sql.Column])).thenReturn(partitionsDF)
    when(partitionsDF.limit(any[Int])).thenReturn(partitionsDF)
    when(partitionsDF.collect()).thenReturn(Array.empty[Row])

    // columnas -> no incluye partition_key, fallback
    when(renamedDF.columns).thenReturn(Array("report_date"))
    when(renamedDF.select(any())).thenReturn(mock[DataFrame])
    when(renamedDF.withColumn("row_count", any())).thenReturn(withRowCountDF)
    when(withRowCountDF.withColumn(any(), any())).thenReturn(castedDF)
    when(castedDF.columns).thenReturn(Array("x"))
    when(castedDF.count()).thenReturn(1L)

    // DROP y saveAsTable
    when(sqlContext.sql(contains("drop table"))).thenReturn(mock[DataFrame])
    val writer = mock[DataFrame#DataFrameWriter[Row]]
    when(castedDF.write).thenReturn(writer)
    when(writer.saveAsTable(s"$targetdb.$outputTable")).thenReturn(())

    GeneratePartitionKeyJob.run(sqlContext, settings)
  }
}
