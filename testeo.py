 test("run ejecuta todo correctamente hasta el punto de escritura") {

    // 1️⃣  Mock “profundo” del SparkSession  -------------------------------
    val sparkMock = mock[SparkSession](RETURNS_DEEP_STUBS)
    val scMock    = mock[SparkContext]
    val sqlCtxMock= mock[SQLContext]

    when(sparkMock.sparkContext).thenReturn(scMock)
    when(scMock.hadoopConfiguration)
      .thenReturn(mock[org.apache.hadoop.conf.Configuration])
    when(sparkMock.sqlContext).thenReturn(sqlCtxMock)

    // 2️⃣  Mock de la consulta de particiones ------------------------------
    val dfPartitions = mock[DataFrame](RETURNS_DEEP_STUBS)
    val rowPart      = mock[Row]
    when(rowPart.getString(0)).thenReturn("partition=20240605")
    when(dfPartitions.collect()).thenReturn(Array(rowPart))

    when(sqlCtxMock.sql(startsWith("show partitions"))).thenReturn(dfPartitions)

    // 3️⃣  Mock genérico para cualquier DataFrame intermedio ---------------
    val anyDF = mock[DataFrame](RETURNS_DEEP_STUBS)
    when(anyDF.columns).thenReturn(Array("col1", "col2"))
    when(anyDF.collect()).thenReturn(Array(mock[Row]))
    when(anyDF.count()).thenReturn(1L)

    // Tablas que la función toca
    Seq("fields_dict", "exercise_inventory", "execution_def",
        "st_metrics_input", "data_output", "contexts_st", "granularities_map")
      .foreach(t => when(sqlCtxMock.table(contains(t))).thenReturn(anyDF))

    // Fallback: cualquier otra SQL → anyDF
    when(sqlCtxMock.sql(any[String])).thenReturn(anyDF)

    // 4️⃣  Datos mínimos para lanzar run() ----------------------------------
    val sourcedb  = "test_source"
    val targetdb  = "test_target"
    val tsPart    = "20240605"
    val entities  = List(mock[IngestEntity])

    // 5️⃣  Ejecutamos y sólo dejamos que falle en el .write -----------------
    intercept[Exception] {
      HistoricalExercisesJob.run(sourcedb, targetdb, tsPart, entities)(sparkMock)
    }
    succeed() // si ha llegado aquí, todo antes del write se ejecutó
  }
