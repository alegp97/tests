class GenerateExecutionDefViewJobTest extends AnyFunSuite with MockitoSugar {

  test("run should create the execution_def view when fields_dict is not empty") {
    val sqlContext = mock[SQLContext]
    val settings = mock[BoardsArgs]

    val sourceDB = "src_db"
    val targetDB = "tgt_db"
    val prefix = "mytable"
    val inputExecDef = s"${prefix}_input"
    val outputExecDef = s"${prefix}_output"

    when(settings.sourcedb).thenReturn(sourceDB)
    when(settings.targetdb).thenReturn(targetDB)
    when(settings.sourceTable).thenReturn(prefix)

    // Tabla input/output mock
    val inputDF = mock[DataFrame]
    val outputDF = mock[DataFrame]

    when(sqlContext.table(s"$targetDB.$inputExecDef")).thenReturn(inputDF)
    when(sqlContext.table(s"$targetDB.$outputExecDef")).thenReturn(outputDF)
    when(inputDF.columns).thenReturn(Array("col_a", "col_b"))
    when(outputDF.columns).thenReturn(Array("col_a", "col_b"))

    // Simulación de show partitions
    val partitionRow = mock[Row]
    when(partitionRow.getString(0)).thenReturn("20240601=whatever")
    val partitionsDF = mock[DataFrame]
    when(partitionsDF.collect()).thenReturn(Array(partitionRow))
    when(sqlContext.sql(contains("show partitions"))).thenReturn(partitionsDF)

    // Simulación de fields_dict
    val fieldsDictDF = mock[DataFrame]
    val selectedDF = mock[DataFrame]
    when(sqlContext.table(s"$sourceDB.fields_dict")).thenReturn(fieldsDictDF)
    when(fieldsDictDF.where(any[org.apache.spark.sql.Column])).thenReturn(fieldsDictDF)
    when(fieldsDictDF.select(any[org.apache.spark.sql.Column])).thenReturn(selectedDF)
    when(selectedDF.distinct()).thenReturn(selectedDF)

    // Mock de collect de campos del dict
    val rowField = Row("col_a")
    val rowSchema = StructType(Seq(StructField("fld_name", StringType)))
    val rowRDD = org.apache.spark.sql.SparkSession.builder.getOrCreate().sparkContext.parallelize(Seq(rowField))
    val rowDF = org.apache.spark.sql.SparkSession.builder.getOrCreate().createDataFrame(rowRDD, rowSchema)
    when(selectedDF.collect()).thenReturn(rowDF.collect())

    // Mock del schema con tipos
    val schema = StructType(Seq(
      StructField("col_a", DecimalType(10, 2)),
      StructField("col_b", IntegerType)
    ))
    when(inputDF.schema).thenReturn(schema)
    when(outputDF.schema).thenReturn(schema)

    // Simula sqlContext.sql(...)
    when(sqlContext.sql(startsWith("DROP VIEW"))).thenReturn(mock[DataFrame])
    when(sqlContext.sql(startsWith("CREATE VIEW"))).thenReturn(mock[DataFrame])

    // Run
    GenerateExecutionDefViewJob.run(sqlContext, settings)
  }
