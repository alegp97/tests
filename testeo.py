  test("run should build the view when fields_dict has content and partitions are present") {
    val sqlContext = mock[SQLContext]
    val settings = mock[BoardsArgs]

    // Configuración básica
    val sourceDB = "src_db"
    val targetDB = "tgt_db"
    val prefix = "mytable"
    val inputExecDef = s"${prefix}_input"
    val outputExecDef = s"${prefix}_output"

    when(settings.sourcedb).thenReturn(sourceDB)
    when(settings.targetdb).thenReturn(targetDB)
    when(settings.sourceTable).thenReturn(prefix)

    // Mock para exec_def_in_DF y exec_def_ou_DF
    val inputDF = mock[DataFrame]
    val outputDF = mock[DataFrame]

    when(sqlContext.table(s"$targetDB.$inputExecDef")).thenReturn(inputDF)
    when(sqlContext.table(s"$targetDB.$outputExecDef")).thenReturn(outputDF)
    when(inputDF.columns).thenReturn(Array("col_a", "col_b"))
    when(outputDF.columns).thenReturn(Array("col_a", "col_b"))

    // 🔴 Mock completo de la línea 47: show partitions ...
    val partitionsDF = mock[DataFrame]
    val orderedDF = mock[DataFrame]
    val limitedDF = mock[DataFrame]
    val partitionRow = mock[Row]

    when(partitionRow.getString(0)).thenReturn("20240601=value")
    when(sqlContext.sql(contains("show partitions"))).thenReturn(partitionsDF)
    when(partitionsDF.orderBy(any[org.apache.spark.sql.Column])).thenReturn(orderedDF)
    when(orderedDF.limit(1)).thenReturn(limitedDF)
    when(limitedDF.collect()).thenReturn(Array(partitionRow))

    // Mock para fields_dict
    val fieldsDictDF = mock[DataFrame]
    val filteredDF = mock[DataFrame]
    val selectedDF = mock[DataFrame]
    val distinctedDF = mock[DataFrame]
    val rowField = Row("col_a")

    when(sqlContext.table(s"$sourceDB.fields_dict")).thenReturn(fieldsDictDF)
    when(fieldsDictDF.where(any[org.apache.spark.sql.Column])).thenReturn(filteredDF)
    when(filteredDF.where(any[org.apache.spark.sql.Column])).thenReturn(filteredDF) // por si hay dos where
    when(filteredDF.select(any[org.apache.spark.sql.Column])).thenReturn(selectedDF)
    when(selectedDF.distinct()).thenReturn(distinctedDF)
    when(distinctedDF.collect()).thenReturn(Array(rowField))

    // Mock de los schemas
    val schema = StructType(Seq(
      StructField("col_a", DecimalType(10, 2)),
      StructField("col_b", IntegerType)
    ))
    when(inputDF.schema).thenReturn(schema)
    when(outputDF.schema).thenReturn(schema)

    // Mock para sqlContext.sql(...) de DROP y CREATE
    when(sqlContext.sql(startsWith("DROP VIEW"))).thenReturn(mock[DataFrame])
    when(sqlContext.sql(startsWith("CREATE VIEW"))).thenReturn(mock[DataFrame])

    // Ejecutar
    GenerateExecutionDefViewJob.run(sqlContext, settings)
  }
