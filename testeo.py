test("Debe ejecutar run con datos simulados (sin entrar en el if)") {
  // --- Mocks base de Spark ---
  val sparkMock         = mock[SparkSession]
  val sqlContextMock    = mock[SQLContext]
  when(sparkMock.sqlContext).thenReturn(sqlContextMock)

  // DataFrames: usar RETURNS_SELF para encadenados y evitar deep-stub cascada
  val versionDf    = mock[org.apache.spark.sql.DataFrame](withSettings().defaultAnswer(Answers.RETURNS_SELF))
  val historicalDf = mock[org.apache.spark.sql.DataFrame](withSettings().defaultAnswer(Answers.RETURNS_SELF))
  val joinedDf     = mock[org.apache.spark.sql.DataFrame](withSettings().defaultAnswer(Answers.RETURNS_SELF))

  // Tablas que pide run
  when(sqlContextMock.table("validation_db.version_notification")).thenReturn(versionDf)
  when(sqlContextMock.table("common_db.historical_data")).thenReturn(historicalDf)

  // Encadenados en versionDf
  when(versionDf.where(any[org.apache.spark.sql.Column]())).thenReturn(versionDf)
  when(versionDf.cache()).thenReturn(versionDf)
  when(versionDf.alias(any[String])).thenReturn(versionDf)

  // Encadenados en historicalDf
  when(historicalDf.select(any[org.apache.spark.sql.Column](), any[org.apache.spark.sql.Column](), any[org.apache.spark.sql.Column]()))
    .thenReturn(historicalDf)
  when(historicalDf.distinct()).thenReturn(historicalDf)
  when(historicalDf.alias(any[String])).thenReturn(historicalDf)

  // Join y post-join → devolvemos siempre el mismo DF mock “joinedDf”
  when(versionDf.join(any[org.apache.spark.sql.Dataset[org.apache.spark.sql.Row]](), any[org.apache.spark.sql.Column]()))
    .thenReturn(joinedDf)
  when(joinedDf.drop(any[org.apache.spark.sql.Column]())).thenReturn(joinedDf)
  when(joinedDf.distinct()).thenReturn(joinedDf)

  // PUNTO CLAVE: collect vacío → no entra en el if
  when(joinedDf.collect()).thenReturn(Array.empty[org.apache.spark.sql.Row])

  // --- HDFS ---
  val fsMock = mock[org.apache.hadoop.fs.FileSystem]
  val staticFs: org.mockito.MockedStatic[HDFSHandler] =
    org.mockito.Mockito.mockStatic(classOf[HDFSHandler])
  staticFs.when(() => HDFSHandler.getFileSystem(any[String])).thenReturn(fsMock)
  when(fsMock.exists(any[org.apache.hadoop.fs.Path])).thenReturn(false)
  when(fsMock.delete(any[org.apache.hadoop.fs.Path], any[Boolean])).thenReturn(true)

  // --- mailServer y parámetros (no se usan porque no se entra al if, pero pasamos mocks seguros) ---
  val mailServer = mock[MailServerConfig]
  val params = ParametersTransposeAndNotificationHD(
    common_db          = "common_db",
    validation_db      = "validation_db",
    staging_db         = "staging_db",
    message            = mailServer,                 // si tu case class lo incluye
    sql_wharehouse_url = "jdbc:dummy",
    path               = "/dummy/path/Historical.xlsx",
    environment        = "DEV"
  )

  // --- Ejecutar run ---
  implicit val spark: SparkSession = sparkMock
  TransposeAndNotificationHD.run(
    "20230101",                // data_date_part
    "20230301123456",          // data_timestamp_part
    mailServer,                // mailServer
    "20230301123456",          // last_timestamp_version
    params
  )

  // --- Verificaciones suaves: NO se entra al if => NO hay llamadas a nada de mail/Excel ---
  // joinedDf.collect() sí se llamó
  verify(joinedDf, times(1)).collect()

  // liberar estáticos
  staticFs.close()
}
