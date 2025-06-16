  test("run debe entrar en ambos else (JEJEJEJEJE y JAJAJAJAJAJAJA)") {

    // -- Mocks base ----------------------------------------------------------
    val sqlContext = mock[SQLContext]
    val settings   = mock[BoardsArgs]

    val sourceDB     = "src_db"
    val targetDB     = "tgt_db"
    val prefix       = "mytable"
    val inputExecDef = s"${prefix}_input"
    val outputExecDef= s"${prefix}_output"

    when(settings.sourcedb).thenReturn(sourceDB)
    when(settings.targetdb).thenReturn(targetDB)
    when(settings.sourceTable).thenReturn(prefix)

    val inputDF  = mock[DataFrame]
    val outputDF = mock[DataFrame]

    when(sqlContext.table(s"$targetDB.$inputExecDef")).thenReturn(inputDF)
    when(sqlContext.table(s"$targetDB.$outputExecDef")).thenReturn(outputDF)

    when(inputDF.columns).thenReturn(Array("col_a", "col_b"))
    when(outputDF.columns).thenReturn(Array("col_a", "col_b"))

    // -- Mock SHOW PARTITIONS ------------------------------------------------
    val partitionsDF = mock[DataFrame]
    val orderedDF    = mock[DataFrame]
    val limitedDF    = mock[DataFrame]
    val partitionRow = mock[Row]

    when(partitionRow.getString(0)).thenReturn("20240601=value")

    when(sqlContext.sql(contains("show partitions"))).thenReturn(partitionsDF)
    when(partitionsDF.orderBy(any[org.apache.spark.sql.Column])).thenReturn(orderedDF)
    when(orderedDF.limit(1)).thenReturn(limitedDF)
    when(limitedDF.collect()).thenReturn(Array(partitionRow))

    // -- Mock fields_dict ----------------------------------------------------
    val fieldsDictDF  = mock[DataFrame]
    val filteredDF    = mock[DataFrame]
    val selectedDF    = mock[DataFrame]
    val distinctedDF  = mock[DataFrame]

    when(sqlContext.table(s"$sourceDB.fields_dict")).thenReturn(fieldsDictDF)
    when(fieldsDictDF.where(any[org.apache.spark.sql.Column])).thenReturn(filteredDF)
    when(filteredDF.where(any[org.apache.spark.sql.Column])).thenReturn(filteredDF)
    when(filteredDF.select(any[org.apache.spark.sql.Column])).thenReturn(selectedDF)
    when(selectedDF.distinct()).thenReturn(distinctedDF)

    //  >>> Array vacío ← exec_in_columns = Seq.empty  → check = false
    when(distinctedDF.collect()).thenReturn(Array.empty[Row])

    // -- Schema sin Decimal/Integer para forzar el segundo else --------------
    val schema = StructType(Seq(
      StructField("col_a", StringType),
      StructField("col_b", BooleanType)
    ))

    when(inputDF.schema).thenReturn(schema)
    when(outputDF.schema).thenReturn(schema)

    // -- Ignoramos ejecución real de DROP / CREATE --------------------------
    when(sqlContext.sql(startsWith("DROP VIEW"))).thenReturn(mock[DataFrame])
    when(sqlContext.sql(startsWith("CREATE VIEW"))).thenReturn(mock[DataFrame])

    // -- Ejecutar el job -----------------------------------------------------
    GenerateExecutionDefViewJob.run(sqlContext, settings)
  }
}
