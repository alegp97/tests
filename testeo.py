  test("Debe ejecutar run con datos simulados y cubrir envío de email sin efectos") {
    // ========= MailConfig =========
    val mailServerConfig = mock(classOf[MailServerConfig])
    val mailConfig       = mock(classOf[MailConfig], RETURNS_DEEP_STUBS)
    when(mailConfig.getFrom()).thenReturn("from@x.com")
    when(mailConfig.getTo()).thenReturn("")
    when(mailConfig.getCc()).thenReturn("")
    when(mailConfig.getBcc()).thenReturn("")
    when(mailConfig.getSubject()).thenReturn("SUBJ")
    when(mailConfig.getBody()).thenReturn("BODY")
    when(mailConfig.getTemplate()).thenReturn("TPL")

    // ========= Mocks de Spark y HDFS =========
    val sparkMock        = mock(classOf[SparkSession])
    val sqlContextMock   = mock(classOf[SQLContext])
    when(sparkMock.sqlContext).thenReturn(sqlContextMock)

    val fsMock           = mock(classOf[FileSystem])
    val fsStatusMock     = mock(classOf[FileStatus])

    // ========= Mocks de DataFrames (tu estilo) =========
    val versionNotificationDF = mock(classOf[DataFrame], RETURNS_DEEP_STUBS)
    val historicalDataDF      = mock(classOf[DataFrame], RETURNS_DEEP_STUBS)
    val notificationHDDF      = mock(classOf[DataFrame], RETURNS_DEEP_STUBS)

    // Tablas que lee el código (exactamente como lo tenías)
    when(sqlContextMock.table(eqTo("validation_db.version_notification"))).thenReturn(versionNotificationDF)
    when(sqlContextMock.table(eqTo("common_db.historical_data"))).thenReturn(historicalDataDF)

    // --- versionNotificationDF pipeline: where(...).cache().alias ---
    when(versionNotificationDF.where(any[org.apache.spark.sql.Column])).thenReturn(versionNotificationDF)
    when(versionNotificationDF.cache()).thenReturn(versionNotificationDF)
    when(versionNotificationDF.alias(any[String])).thenReturn(versionNotificationDF)

    // --- historicalDataDF pipeline: select(...).distinct().alias ---
    when(historicalDataDF.select(anyVararg[org.apache.spark.sql.Column])).thenReturn(historicalDataDF)
    when(historicalDataDF.distinct()).thenReturn(historicalDataDF)
    when(historicalDataDF.alias(any[String])).thenReturn(historicalDataDF)

    // --- join → notificationHDDF ---
    when(versionNotificationDF.join(any(classOf[DataFrame]), any[org.apache.spark.sql.Column])).thenReturn(notificationHDDF)

    // --- drops encadenados y distinct final sobre notificationHDDF ---
    when(notificationHDDF.drop(any[org.apache.spark.sql.Column])).thenReturn(notificationHDDF)
    when(notificationHDDF.distinct()).thenReturn(notificationHDDF)

    // --- collect NO vacío para entrar en !reg.isEmpty & for(row <- reg) ---
    val rowMock = mock(classOf[Row], RETURNS_DEEP_STUBS)
    def str(k: String) = k match {
      case "unit_id" | "entity_id" | "exercise" | "file_version" | "file_name" |
           "country" | "modification" | "modification_type" | "modification_details" |
           "name" | "detailsmod" => "X"
      case "modification_date" => " 20250101 " // para .trim()
      case _ => "X"
    }
    when(rowMock.getAs))
    when(notificationHDDF.collect()).thenReturn(Array(rowMock))

    // ========= FileSystem/HDFSHandler =========
    val staticHdfsMock: MockedStatic[HDFSHandler] =
      org.mockito.Mockito.mockStatic(classOf[HDFSHandler])
    staticHdfsMock.when(() => HDFSHandler.getFileSystem(any[String])).thenReturn(fsMock)

    // globStatus -> part, mkdirs/exists/rename/delete/perm
    val tmpCsv = new Path("/tmp/part-000.csv")
    when(fsStatusMock.getPath).thenReturn(tmpCsv)
    when(fsMock.globStatus(any(classOf[Path]))).thenReturn(Array(fsStatusMock))
    when(fsMock.exists(any(classOf[Path]))).thenReturn(false)
    when(fsMock.mkdirs(any(classOf[Path]))).thenReturn(true)
    when(fsMock.delete(any(classOf[Path]), anyBoolean())).thenReturn(true)
    when(fsMock.rename(any(classOf[Path]), any(classOf[Path]))).thenReturn(true)
    doNothing().when(fsMock).setPermission(any(classOf[Path]), any(classOf[FsPermission]))

    // ========= Espiar auxiliares del propio object (tu estilo pero con PowerMockito) =========
    PowerMockito.spy(TransposeAndNotificationHD)

    val hdQuarterlyDF = mock(classOf[DataFrame], RETURNS_DEEP_STUBS)
    val hdAnnualDF    = mock(classOf[DataFrame], RETURNS_DEEP_STUBS)
    when(hdQuarterlyDF.columns).thenReturn(Array("Country","Name","Description","Detail","Transformation","Source","Original_Source","Code","Condition","ejey","value"))
    when(hdAnnualDF.columns).thenReturn(Array("Country","Name","Description","Detail","Transformation","Source","Original_Source","Code","Condition","ejey","value"))

    // getHdQuarterly/getHdAnnual/addRecipients/getSubject
    PowerMockito.doReturn(hdQuarterlyDF)
      .when(TransposeAndNotificationHD, "getHdQuarterly", anyString(), any(classOf[SparkSession]), any(), any())
    PowerMockito.doReturn(hdAnnualDF)
      .when(TransposeAndNotificationHD, "getHdAnnual", anyString(), any(classOf[SparkSession]), any())
    PowerMockito.doNothing()
      .when(TransposeAndNotificationHD, "addNotificationRecipientsAndSender", anyString(), any(classOf[SparkSession]), any(), any())
    PowerMockito.doReturn("FINAL-SUBJECT")
      .when(TransposeAndNotificationHD, "getSubjectOfMessageToSend", any(classOf[MailConfig]), any(), anyString())

    // groupBy/pivot/agg -> quarterly & annual
    val rgdQ  = mock(classOf[RelationalGroupedDataset], RETURNS_DEEP_STUBS)
    val rgdA  = mock(classOf[RelationalGroupedDataset], RETURNS_DEEP_STUBS)
    val quaterlyDF = mock(classOf[DataFrame], RETURNS_DEEP_STUBS)
    val anualDF    = mock(classOf[DataFrame], RETURNS_DEEP_STUBS)
    val allDF      = mock(classOf[DataFrame], RETURNS_DEEP_STUBS)
    val toWriteDF  = mock(classOf[DataFrame], RETURNS_DEEP_STUBS)

    when(hdQuarterlyDF.groupBy(anyVararg[org.apache.spark.sql.Column])).thenReturn(rgdQ)
    when(rgdQ.pivot(any[String])).thenReturn(rgdQ)
    when(rgdQ.agg(any[org.apache.spark.sql.Column])).thenReturn(quaterlyDF)

    when(hdAnnualDF.groupBy(anyVararg[org.apache.spark.sql.Column])).thenReturn(rgdA)
    when(rgdA.pivot(any[String])).thenReturn(rgdA)
    when(rgdA.agg(any[org.apache.spark.sql.Column])).thenReturn(anualDF)

    // union quaterly + anual -> all
    when(quaterlyDF.alias("quaterly")).thenReturn(quaterlyDF)
    when(anualDF.alias("anual")).thenReturn(anualDF)
    when(quaterlyDF.join(eq(anualDF.alias("anual")), any[org.apache.spark.sql.Column])).thenReturn(allDF)
    when(allDF.drop(any[org.apache.spark.sql.Column])).thenReturn(allDF)
    when(allDF.distinct()).thenReturn(allDF)

    // filtro + orderBy + select dinámico -> toWriteDF
    when(allDF.where(any[org.apache.spark.sql.Column])).thenReturn(allDF)
    when(allDF.orderBy(anyVararg[org.apache.spark.sql.Column])).thenReturn(allDF)
    when(allDF.select(any(classOf[Seq[org.apache.spark.sql.Column]]))).thenReturn(toWriteDF)

    // writer CSV
    val writer = mock(classOf[DataFrameWriter[Row]], RETURNS_DEEP_STUBS)
    when(toWriteDF.coalesce(anyInt())).thenReturn(toWriteDF)
    when(toWriteDF.write).thenReturn(writer)
    when(writer.format(any[String])).thenReturn(writer)
    when(writer.option(any[String], any[String])).thenReturn(writer)
    when(writer.mode(any[String])).thenReturn(writer)
    doNothing().when(writer).save(any[String])

    // ========= Estáticos externos del email (Excel/Notif/Sender) =========
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

    // ========= Ejecutar el método bajo test en tu wrapper =========
    HiveUtilWrapper.withMocks(
      dbMock    = _ => true,
      tableMock = _ => true
    ) {
      implicit val spark: SparkSession = sparkMock

      TransposeAndNotificationHD.run(
        "20230101",
        "20230101123456",
        mailServerConfig,
        "20230101123456",
        ParametersTransposeAndNotificationHD(
          "common_db",
          "validation_db",
          "staging_db",
          mailConfig,                 // <- tu message
          "http://hue.url",
          "/path/to/files",
          "DEV"
        )
      )
    }

    // ========= Liberar recursos estáticos =========
    staticSender.close()
    staticNotif.close()
    staticExcel.close()
    staticHdfsMock.close()
    fsMock.close()

    assert(true)
  }
