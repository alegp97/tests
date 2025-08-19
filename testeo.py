import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.mockito.MockitoSugar._
import org.mockito.ArgumentMatchers.{ any => anyArg, _ }
import org.mockito.{ Answers, MockedStatic }
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.RelationalGroupedDataset
import org.apache.hadoop.fs._
import org.apache.spark.sql.SaveMode

class TransposeAndNotificationHD_Run_EnterIf_Test extends AnyWordSpec with Matchers {

  "TransposeAndNotificationHD.run" should {
    "entrar en el if y recorrer el flujo completo con mocks (coverage)" in {

      // ---------- Spark ----------
      val sparkMock      = mock[SparkSession]
      val sqlContextMock = mock[SQLContext]
      when(sparkMock.sqlContext).thenReturn(sqlContextMock)

      // DataFrames encadenables
      def dfSelf(): DataFrame =
        mock[DataFrame](withSettings().defaultAnswer(Answers.RETURNS_SELF))

      val versionDf = dfSelf()
      val histDf    = dfSelf()     // reutilizado para getHd* dentro del método
      val joinedDf  = dfSelf()
      val allDf     = dfSelf()
      val toWriteDf = dfSelf()

      // Tablas usadas por run
      when(sqlContextMock.table("validation_db.version_notification")).thenReturn(versionDf)
      when(sqlContextMock.table("common_db.historical_data")).thenReturn(histDf)

      when(versionDf.where(any[Column]())).thenReturn(versionDf)
      when(versionDf.cache()).thenReturn(versionDf)
      when(versionDf.alias(anyString())).thenReturn(versionDf)

      when(histDf.distinct()).thenReturn(histDf)
      when(histDf.alias(anyString())).thenReturn(histDf)

      // Join principal del run
      when(versionDf.join(any[Dataset[Row]](), any[Column]())).thenReturn(joinedDf)
      when(joinedDf.drop(any[Column]())).thenReturn(joinedDf)
      when(joinedDf.distinct()).thenReturn(joinedDf)

      // ---------- Row con schema para provocar la entrada en el IF ----------
      val schema = StructType(Seq(
        StructField("unit_id",             StringType),
        StructField("entity_id",           StringType),
        StructField("exercise",            StringType),
        StructField("file_version",        StringType),
        StructField("file_name",           StringType),
        StructField("modification_date",   StringType),
        StructField("country",             StringType),
        StructField("modification_type",   StringType),
        StructField("modification_details",StringType),
        StructField("name",                StringType),
        StructField("detailsmod",          StringType)
      ))
      val rowOk = new GenericRowWithSchema(Array[Any](
        "U1","E1","EX1","v1","HISTORICAL_DATA_001.csv","2025-01-01","ES","MT","DET","NOMBRE","DMOD"
      ), schema)
      when(joinedDf.collect()).thenReturn(Array(rowOk))   // <-- entra en el if

      // ---------- Dentro de processAndSendNotificationEmail ----------

      // A) getHd* ⇒ columnas y groupBy/pivot/agg
      when(histDf.columns).thenReturn(Array(
        "Country","Name","Description","Detail","Transformation","Adjustment",
        "Source","Original_Source","Code","Condition","ejeY","value",
        // por si se usan en where previos:
        "unit_id","entity_id","dataTimestampPart","timeline","year"
      ))
      val rgd = mock[RelationalGroupedDataset](withSettings().defaultAnswer(Answers.RETURNS_SELF))
      // varargs helpers
      doReturn(rgd).when(histDf).groupBy(anyArg[Seq[Column]](): _*)
      when(rgd.pivot(anyString())).thenReturn(rgd)
      when(rgd.agg(any[Column]())).thenReturn(histDf)
      // union/join de quarterly + annual
      when(histDf.join(any[Dataset[Row]](), any[Column]())).thenReturn(allDf)
      when(allDf.alias(anyString())).thenReturn(allDf)
      when(allDf.drop(any[Column]())).thenReturn(allDf)
      when(allDf.distinct()).thenReturn(allDf)
      when(allDf.col(anyString())).thenReturn(lit(1))
      when(allDf.where(any[Column]())).thenReturn(allDf)
      doReturn(allDf).when(allDf).orderBy(anyArg[Seq[Column]](): _*)
      doReturn(toWriteDf).when(allDf).select(anyArg[Seq[Column]](): _*)

      // B) writer del CSV temporal
      val writer = mock[DataFrameWriter[Row]](withSettings().defaultAnswer(Answers.RETURNS_SELF))
      when(toWriteDf.coalesce(1)).thenReturn(toWriteDf)
      when(toWriteDf.write).thenReturn(writer)
      when(writer.format(anyString())).thenReturn(writer)
      when(writer.option(anyString(), anyString())).thenReturn(writer)
      when(writer.mode(any[SaveMode]())).thenReturn(writer)
      doNothing().when(writer).save(anyString())

      // C) FileSystem/HDFS
      val fsMock = mock[FileSystem]
      val staticFs: MockedStatic[HDFSHandler] =
        org.mockito.Mockito.mockStatic(classOf[HDFSHandler])
      staticFs.when(() => HDFSHandler.getFileSystem(anyString())).thenReturn(fsMock)

      when(fsMock.exists(any[Path])).thenReturn(false)
      when(fsMock.mkdirs(any[Path])).thenReturn(true)
      when(fsMock.delete(any[Path], anyBoolean())).thenReturn(true)
      when(fsMock.rename(any[Path], any[Path])).thenReturn(true)
      doNothing().when(fsMock).setPermission(any[Path], any[FsPermission])

      val status = mock[FileStatus]
      when(status.getPath).thenReturn(new Path("/tmp/part-00000.csv"))
      when(fsMock.globStatus(argThat[Path](_.toString.endsWith("/part*"))))
        .thenReturn(Array(status))

      // D) Excel y Email (estáticos)
      val excelStatic = org.mockito.Mockito.mockStatic(classOf[ExcelUtil])
      excelStatic.when(() => ExcelUtil.generateExcelFromCsv(any(), any(), any(), anyChar())).thenAnswer(_ => ())

      val emailStatic = org.mockito.Mockito.mockStatic(classOf[com.santander.supra.core3.staging.mail.AzureEmailSender])
      emailStatic.when(() =>
        com.santander.supra.core3.staging.mail.AzureEmailSender.sendEmail(
          anyString(), anyString(), any(classOf[Array[String]]), any(), any(), any(classOf[Array[String]])
        )
      ).thenAnswer(_ => ())

      // E) Hive/Notification utils + usuarios
      val hiveStatic = org.mockito.Mockito.mockStatic(classOf[HiveUtilWrapper])
      hiveStatic.when(() => HiveUtilWrapper.tableExists(eqTo("staging_db"), eqTo("users_eresresearch"))).thenReturn(true)

      val notifStatic = org.mockito.Mockito.mockStatic(classOf[NotificationUtil])
      notifStatic.when(() =>
        NotificationUtil.getEmisorNotificacionWithDefault(any(), eqTo("staging_db"), anyString(), anyString())
      ).thenReturn("noreply@test.com")
      notifStatic.when(() => NotificationUtil.getDireccionesSoporte(any(), eqTo("staging_db")))
        .thenReturn(List("soporte@test.com"))
      notifStatic.when(() => NotificationUtil.replaceStringWithInfoNotification(anyString(), any()))
        .thenAnswer(inv => inv.getArgument)

      val userEmailsDf = dfSelf()
      when(sqlContextMock.table("staging_db.users_eresresearch")).thenReturn(userEmailsDf)
      when(userEmailsDf.where(any[Column]())).thenReturn(userEmailsDf)
      when(userEmailsDf.select(any[Column](), any[Column](), any[Column](), any[Column]()))
        .thenReturn(userEmailsDf)
      when(userEmailsDf.select(any[Column]())).thenReturn(userEmailsDf)
      when(userEmailsDf.distinct()).thenReturn(userEmailsDf)
      val ueSchema = StructType(Seq(StructField("user_email", StringType)))
      val ueRow = new GenericRowWithSchema(Array[Any]("user@test.com"), ueSchema)
      when(userEmailsDf.collect()).thenReturn(Array(ueRow))

      // F) Insert en notification_sent sobre el DF de version
      val writer2 = mock[DataFrameWriter[Row]](withSettings().defaultAnswer(Answers.RETURNS_SELF))
      when(versionDf.repartition(1)).thenReturn(versionDf)
      when(versionDf.write).thenReturn(writer2)
      when(writer2.mode(any[SaveMode]())).thenReturn(writer2)
      doNothing().when(writer2).insertInto(anyString())

      // ---------- Parámetros ----------
      val mailServer = mock[MailServerConfig]
      when(mailServer.getFrom()).thenReturn("from@test.com")
      when(mailServer.getTo()).thenReturn(Array("to@test.com"))
      when(mailServer.getCc()).thenReturn(Array("cc@test.com"))
      when(mailServer.getBc()).thenReturn(Array("bcc@test.com"))
      when(mailServer.getSubject()).thenReturn("Asunto")
      when(mailServer.getBody()).thenReturn("Cuerpo")

      val params = ParametersTransposeAndNotificationHD(
        common_db          = "common_db",
        validation_db      = "validation_db",
        staging_db         = "staging_db",
        message            = mailServer,
        sql_wharehouse_url = "jdbc:dummy",
        path               = "/exports/Historical_data_U1_E1.xlsx",
        environment        = "DEV"
      )

      // ---------- Ejecutar ----------
      implicit val spark: SparkSession = sparkMock
      TransposeAndNotificationHD.run(
        data_date_part        = "20250101",
        data_timestamp_part   = "20250301123456",
        mailServer            = mailServer,
        last_timestamp_version= "20250301123456",
        parametros            = params
      )

      // ---------- Verificaciones mínimas ----------
      org.mockito.Mockito.verify(emailStatic, org.mockito.Mockito.times(1))
      org.mockito.Mockito.verify(writer2,  org.mockito.Mockito.times(1)).insertInto(eqTo("validation_db.notification_sent"))

      // ---------- Cierre estáticos ----------
      emailStatic.close()
      excelStatic.close()
      hiveStatic.close()
      notifStatic.close()
      staticFs.close()
    }
  }
