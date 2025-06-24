class BDRPreviewFlowsJobTest extends AnyFunSuite with MockitoSugar with BeforeAndAfterAll {

  // ---------- Spark & SQL ----------
  implicit val spark: SparkSession = mock[SparkSession](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))
  private val sqlContext: SQLContext  = mock[SQLContext]
  when(spark.sqlContext).thenReturn(sqlContext)

  // ---------- Lectura parquet ----------
  private val readerMock: DataFrameReader = mock[DataFrameReader](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))
  when(sqlContext.read).thenReturn(readerMock)

  private val dfMock: DataFrame = mock[DataFrame](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))
  when(readerMock.format("parquet")).thenReturn(readerMock)
  when(readerMock.load(any[String])).thenReturn(dfMock)

  // Acceso directo a tabla origen
  when(sqlContext.table(any[String])).thenReturn(dfMock)

  // Persist / unpersist
  when(dfMock.persist(StorageLevel.MEMORY_AND_DISK)).thenReturn(dfMock)
  when(dfMock.unpersist()).thenReturn(dfMock)

  // ---------- Stubs genéricos de DataFrame ----------
  // 129 columnas para activar el ajuste de block‑size
  when(dfMock.columns).thenReturn(Array.fill(129)("col"))
  when(dfMock.withColumn(any[String], any[classOf[org.apache.spark.sql.Column]])).thenReturn(dfMock)
  when(dfMock.drop(any[String])).thenReturn(dfMock)
  when(dfMock.sort(any[Seq[org.apache.spark.sql.Column]]: _*)).thenReturn(dfMock)
  when(dfMock.select(any[org.apache.spark.sql.Column])).thenReturn(dfMock)
  when(dfMock.select(any[Array[org.apache.spark.sql.Column]]: _*)).thenReturn(dfMock)
  when(dfMock.where(any[org.apache.spark.sql.Column])).thenReturn(dfMock)
  when(dfMock.unionAll(any[DataFrame])).thenReturn(dfMock)

  // ---------- Mock "show partitions … .map …" ----------
  private val dsRowMock: Dataset[Row] = mock[Dataset[Row]](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))
  when(sqlContext.sql(org.mockito.ArgumentMatchers.startsWith("show partitions"))).thenReturn(dsRowMock)

  private val dsStringMock: Dataset[String] = mock[Dataset[String]](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))
  when(dsRowMock.map[String](any[Function1[Row, String]])(any[org.apache.spark.sql.Encoder[String]])).thenReturn(dsStringMock)
  // Partición única para que el while sea ejecutado al menos una vez
  when(dsStringMock.collect()).thenReturn(Array("2025-01-01"))

  // ---------- Escritura parquet ----------
  private val dfWriterMock: DataFrameWriter[Row] = mock[DataFrameWriter[Row]](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))
  when(dfMock.write).thenReturn(dfWriterMock)
  when(dfWriterMock.mode(SaveMode.Overwrite)).thenReturn(dfWriterMock)
  when(dfWriterMock.format("parquet")).thenReturn(dfWriterMock)
  when(dfWriterMock.partitionBy(any[String])).thenReturn(dfWriterMock)
  doNothing().when(dfWriterMock).parquet(any[String])

  // ---------- Mock HDFS ----------
  private val fsMock = mock[org.apache.hadoop.fs.FileSystem]
  private val staticHdfsMock: MockedStatic[HDFSHandler] = mockStatic(classOf[HDFSHandler])
  staticHdfsMock.when(() => HDFSHandler.getFileSystem(any[String])).thenReturn(fsMock)
  when(fsMock.exists(any[Path])).thenReturn(true)
  when(fsMock.delete(any[Path], eqTo(true))).thenReturn(true)

  override protected def afterAll(): Unit = {
    staticHdfsMock.close()
  }

  // ----------------------------------------------------------------------------------
  //                                         TEST
  // ----------------------------------------------------------------------------------

  test("BDRPreviewFlowsJob.run se ejecuta sin excepciones y realiza interacciones críticas") {

    // ---------------- Act ----------------
    noException shouldBe thrownBy {
      BDRPreviewFlowsJob.run(
        sourcedb                = "sourcedb",
        targetdb                = "targetdb",
        targetTableOptionalName = "agg_table",
        entities                = List.empty,
        extra_filter            = "*",
        sourceTable             = "starting_points_contract",
        process                 = "FULL",
        is_incremental          = "false"
      )
    }

    // ---------------- Assert -------------
    // Verificamos que se tocaron los puntos clave del flujo
    verify(sqlContext).table(org.mockito.ArgumentMatchers.contains("sp_contract")) // lectura tabla
    verify(dfMock).persist(StorageLevel.MEMORY_AND_DISK)                            // persistencia
    verify(fsMock).delete(any[Path], eqTo(true))                                    // limpieza HDFS
    verify(dfWriterMock).partitionBy(any[String])                                   // particionamiento
    verify(dfWriterMock).parquet(any[String])                                       // escritura parquet
    // Ajuste de bloque —> columnas > 128
    verify(spark.sqlContext.sparkContext.hadoopConfiguration)
      .setInt(eqTo("parquet.block.size"), any[Int])
  }
}
