  test("setup mocks para cubrir todas las líneas de run (sin ejecutarlo)") {
    // ==== 1) Mocks base (Spark + SQLContext) ====
    implicit val sparkMock: SparkSession = mock(classOf[SparkSession], RETURNS_DEEP_STUBS)
    val sqlCtxMock: SQLContext          = mock(classOf[SQLContext],  RETURNS_DEEP_STUBS)
    when(sparkMock.sqlContext).thenReturn(sqlCtxMock)

    // ==== 2) Mocks de DataFrames ====
    val versionNotificationDF = mock(classOf[DataFrame], RETURNS_DEEP_STUBS)
    val historicalDataDF      = mock(classOf[DataFrame], RETURNS_DEEP_STUBS)
    val notificationHDDF      = mock(classOf[DataFrame], RETURNS_DEEP_STUBS)

    // Tablas que lee el código
    when(sqlCtxMock.table(eqTo("validation.version_notification")))
      .thenReturn(versionNotificationDF)
    when(sqlCtxMock.table(eqTo("common.historical_data")))
      .thenReturn(historicalDataDF)

    // --- versionNotificationDF pipeline: where(...).cache() ---
    when(versionNotificationDF.where(any())).thenReturn(versionNotificationDF)
    when(versionNotificationDF.cache()).thenReturn(versionNotificationDF)

    // alias para el join
    when(versionNotificationDF.alias(any[String])).thenReturn(versionNotificationDF)

    // --- historicalDataDF pipeline: select(...).distinct() ---
    // acepta cualquier combinación de columnas
    when(historicalDataDF.select(anyVararg[org.apache.spark.sql.Column]))
      .thenReturn(historicalDataDF)
    when(historicalDataDF.distinct()).thenReturn(historicalDataDF)
    when(historicalDataDF.alias(any[String])).thenReturn(historicalDataDF)

    // --- join → notificationHDDF ---
    // join(left: DF, cond: Column) → devuelve el DF del join
    when(versionNotificationDF.join(any(classOf[DataFrame]), any()))
      .thenReturn(notificationHDDF)

    // --- drops encadenados y distinct final sobre notificationHDDF ---
    when(notificationHDDF.drop(any[org.apache.spark.sql.Column])).thenReturn(notificationHDDF)
    when(notificationHDDF.distinct()).thenReturn(notificationHDDF)

    // --- collect final para que !reg.isEmpty sea TRUE ---
    when(notificationHDDF.collect()).thenReturn(Array(Row(1)))

    // ==== 3) Mocks de parámetros externos ====
    val parametros = mock(classOf[TransposeAndNotificationHD.ParametersTransposeAndNotificationHD])
    when(parametros.environment).thenReturn("dev")
    when(parametros.path).thenReturn("/tmp/xx")
    when(parametros.message).thenReturn("hola")
    when(parametros.validation_db).thenReturn("validation")
    when(parametros.staging_db).thenReturn("staging")
    when(parametros.common_db).thenReturn("common")
    when(parametros.sql_wharehouse_url).thenReturn("jdbc://dummy")

    val mailCfg = mock(classOf[MailServerConfig])

    // ==== 4) FileSystem/HDFSHandler (si el método lo usa) ====
    // si tu run hace: val fs = HDFSHandler.getFileSystem(path)
    // deja preparado el stub:
    val fsMock = mock(classOf[FileSystem])
    // when(HDFSHandler.getFileSystem(any())).thenReturn(fsMock)
    // (descomenta la línea de arriba si `getFileSystem` es invocado; si es un 'object' Scala
    // y necesitas mockear método estático, usa PowerMockito en tu suite real.)

    // ==== 5) Evitar efectos del envío de correo (opcional) ====
    // Si el run llama a processAndSendNotificationEmail y no quieres efectos,
    // puedes espiar el object y hacer doNothing(). Lo dejo a tu decisión.
    // PowerMockito.spy(TransposeAndNotificationHD)
    // PowerMockito.doNothing().when(TransposeAndNotificationHD, "processAndSendNotificationEmail",
    //   any(classOf[Row]), any(), any()
    // )(any(), any())
