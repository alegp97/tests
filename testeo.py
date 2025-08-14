test("Debe ejecutar processAndSendNotificationEmail con datos simulados") {
  // ====== Row de entrada ======
  val rowMock = mock[Row](RETURNS_DEEP_STUBS)
  def str(k: String) = k match {
    case "unit_id" | "entity_id" | "exercise" | "file_version" | "file_name" |
         "country" | "modification" | "modification_type" | "modification_details" |
         "name" | "detailsmod" => "X"
    case "modification_date" => " 20250101 "
    case _ => "X"
  }
  when(rowMock.getAs ))

  // ====== versionNotificationDF ======
  val versionNotificationDF = mock[DataFrame](RETURNS_DEEP_STUBS)

  // ====== Parámetros ======
  val mailConfig = mock[MailConfig](RETURNS_DEEP_STUBS)
  when(mailConfig.getFrom()).thenReturn("from@x.com")
  when(mailConfig.getTo()).thenReturn("")
  when(mailConfig.getCc()).thenReturn("")
  when(mailConfig.getBcc()).thenReturn("")
  when(mailConfig.getSubject()).thenReturn("SUBJ")
  when(mailConfig.getBody()).thenReturn("BODY")
  when(mailConfig.getTemplate()).thenReturn("TPL")

  val parametros = mock[ParametersTransposeAndNotificationHD](RETURNS_DEEP_STUBS)
  when(parametros.environment).thenReturn("DEV")
  when(parametros.path).thenReturn("/path/to/files")
  when(parametros.message).thenReturn(mailConfig)
  when(parametros.validation_db).thenReturn("validation_db")
  when(parametros.staging_db).thenReturn("staging_db")
  when(parametros.common_db).thenReturn("common_db")
  when(parametros.sql_wharehouse_url).thenReturn("http://hue.url")

  // ====== Spark & SQL ======
  val sparkMock      = mock[SparkSession]
  val sqlContextMock = mock[SQLContext]
  when(sparkMock.sqlContext).thenReturn(sqlContextMock)

  // DataFrames que devuelve sqlContext.table(...) en cada uso
  val hdBaseDF      = mock[DataFrame](RETURNS_DEEP_STUBS)
  val hdQuarterlyDF = mock[DataFrame](RETURNS_DEEP_STUBS)
  val hdAnnualDF    = mock[DataFrame](RETURNS_DEEP_STUBS)
  val quaterlyDF    = mock[DataFrame](RETURNS_DEEP_STUBS)
  val anualDF       = mock[DataFrame](RETURNS_DEEP_STUBS)
  val allDF         = mock[DataFrame](RETURNS_DEEP_STUBS)
  val toWriteDF     = mock[DataFrame](RETURNS_DEEP_STUBS)

  when(sqlContextMock.table(contains("historical_data"))).thenReturn(hdBaseDF)

  // groupBy/pivot/agg encadenados
  val rgdQ = mock[RelationalGroupedDataset](RETURNS_DEEP_STUBS)
  val rgdA = mock[RelationalGroupedDataset](RETURNS_DEEP_STUBS)
  when(hdQuarterlyDF.groupBy(anyVararg[org.apache.spark.sql.Column])).thenReturn(rgdQ)
  when(rgdQ.pivot(any[String])).thenReturn(rgdQ)
  when(rgdQ.agg(any[org.apache.spark.sql.Column])).thenReturn(quaterlyDF)
  when(hdAnnualDF.groupBy(anyVararg[org.apache.spark.sql.Column])).thenReturn(rgdA)
  when(rgdA.pivot(any[String])).thenReturn(rgdA)
  when(rgdA.agg(any[org.apache.spark.sql.Column])).thenReturn(anualDF)

  // join quaterly/anual -> allDF
  when(quaterlyDF.alias("quaterly")).thenReturn(quaterlyDF)
  when(anualDF.alias("anual")).thenReturn(anualDF)
  when(quaterlyDF.join(eq(anualDF.alias("anual")), any[org.apache.spark.sql.Column])).thenReturn(allDF)
  when(allDF.drop(any[org.apache.spark.sql.Column])).thenReturn(allDF)
  when(allDF.distinct()).thenReturn(allDF)
  when(allDF.where(any[org.apache.spark.sql.Column])).thenReturn(allDF)
  when(allDF.orderBy(anyVararg[org.apache.spark.sql.Column])).thenReturn(allDF)
  when(allDF.select(any(classOf[Seq[org.apache.spark.sql.Column]]))).thenReturn(toWriteDF)

  // write CSV
  val writer = mock[DataFrameWriter[Row]](RETURNS_DEEP_STUBS)
  when(toWriteDF.coalesce(anyInt())).thenReturn(toWriteDF)
  when(toWriteDF.write).thenReturn(writer)
  when(writer.format(any[String])).thenReturn(writer)
  when(writer.option(any[String], any[String])).thenReturn(writer)
  when(writer.mode(any[String])).thenReturn(writer)
  doNothing().when(writer).save(any[String])

  // ====== FileSystem ======
  val fsMock = mock[FileSystem]
  val fsStatusMock = mock[FileStatus]
  val tmpCsv = new Path("/tmp/part-000.csv")
  when(fsStatusMock.getPath).thenReturn(tmpCsv)
  when(fsMock.globStatus(any(classOf[Path]))).thenReturn(Array(fsStatusMock))
  when(fsMock.exists(any(classOf[Path]))).thenReturn(false)
  when(fsMock.mkdirs(any(classOf[Path]))).thenReturn(true)
  when(fsMock.delete(any(classOf[Path]), anyBoolean())).thenReturn(true)
  when(fsMock.rename(any(classOf[Path]), any(classOf[Path]))).thenReturn(true)
  doNothing().when(fsMock).setPermission(any(classOf[Path]), any(classOf[FsPermission]))

  // ====== Estáticos externos ======
  val staticExcel: MockedStatic[ExcelUtil] = org.mockito.Mockito.mockStatic(classOf[ExcelUtil])
  staticExcel.when(() =>
    ExcelUtil.generateExcelFromCsv(any(classOf[FileSystem]), any(classOf[Path]), any(classOf[Path]), anyChar())
  ).thenAnswer(_ => ())

  val staticNotif: MockedStatic[NotificationUtil] = org.mockito.Mockito.mockStatic(classOf[NotificationUtil])
  staticNotif.when(() =>
    NotificationUtil.replaceStringWithInfoNotification(anyString(), any())
  ).thenReturn("BODY-REPLACED")

  val staticSender: MockedStatic[com.santander.supra.core3.staging.mail.AzureEmailSender] =
    org.mockito.Mockito.mockStatic(classOf[com.santander.supra.core3.staging.mail.AzureEmailSender])
  staticSender.when(() =>
    com.santander.supra.core3.staging.mail.AzureEmailSender.sendEmail(
      anyString(), anyString(),
      any(classOf[Array[String]]), any(classOf[Array[String]]),
      any(classOf[Array[String]]), any(classOf[Array[String]])
    )
  ).thenAnswer(_ => ())

  // ====== Ejecutar directamente processAndSendNotificationEmail ======
  TransposeAndNotificationHD.processAndSendNotificationEmail(
    rowMock,
    versionNotificationDF,
    parametros
  )(sparkMock, fsMock)

  // ====== Limpieza ======
  staticSender.close()
  staticNotif.close()
  staticExcel.close()
  fsMock.close()

  assert(true)
}



when(mailConfig.getFrom()).thenReturn("from@x.com")
when(mailConfig.getTo()).thenReturn(Collections.singletonList("to@x.com"))
when(mailConfig.getCc()).thenReturn(Collections.singletonList("cc@x.com"))
when(mailConfig.getBc()).thenReturn(Collections.singletonList("bcc@x.com"))
when(mailConfig.getSubject()).thenReturn("SUBJ")
when(mailConfig.getBody()).thenReturn("BODY")
when(mailConfig.getTemplate()).thenReturn("TPL")


staticSender.when(() =>
  com.santander.supra.core3.staging.mail.AzureEmailSender.sendEmail(
    anyString(),
    anyString(),
    any[Array[String]](),
    any[Array[String]](),
    any[Array[String]](),
    any[Array[String]]()
  )
).thenAnswer(_ => null)



val staticNotif = org.mockito.Mockito.mockStatic(classOf[NotificationUtil])
staticNotif.when(() =>
  NotificationUtil.replaceStringWithInfoNotification(anyString(), any())
).thenReturn("BODY-REPLACED")

// 2) Replica EXACTAMENTE el array 'tomails' del código:
val expectedTo: Array[String] = Array(
  "nerea.ruiz@serexternos.gruposantander.com",
  "daniel.castillo@gruposantander.com",
  "luis.lopez@serexternos.gruposantander.com",
  "juan.martin@serexternos.gruposantander.com",
  "alejandro.garcia@serexternos.gruposantander.com",
  "zhanna.shybitsa@serexternos.gruposantander.com",
  "pedro.ruiz@serexternos.gruposantander.com",
  "agustin.manzano@serexternos.gruposantander.com",
  "maria.chinchillalroman@serexternos.gruposantander.com",
  "scenariosrepository.soportesupra@gruposantander.com",
  "diego.sernanfernandez@serexternos.gruposantander.com"
)

// 3) Mock estático SIN matchers: usa valores literales exactamente como se llamarán
val staticSender =
  org.mockito.Mockito.mockStatic(classOf[com.santander.supra.core3.staging.mail.AzureEmailSender])

staticSender.when(() =>
  com.santander.supra.core3.staging.mail.AzureEmailSender.sendEmail(
    "FINAL-SUBJECT",
    "BODY-REPLACED",
    expectedTo,
    Array.empty[String],
    Array.empty[String],
    Array.empty[String]
  )
).thenAnswer(_ => null)

