private def dfMock(name: String): DataFrame =
    mock[DataFrame](withSettings().name(name).defaultAnswer(RETURNS_DEEP_STUBS))

  test("run debe generar newNotifications y escribirlas") {

    // ---------- Spark / SQLContext ----------
    val sparkMock        = mock[SparkSession](RETURNS_DEEP_STUBS)
    val sqlContextMock   = mock[SQLContext](RETURNS_DEEP_STUBS)
    when(sparkMock.sqlContext).thenReturn(sqlContextMock)

    // ---------- DataFrames intermedios ----------
    val versionsDF              = dfMock("versionsDF")
    val timestampDF             = dfMock("timestampDF")
    val versionMaxDF            = dfMock("versionMaxDF")
    val versionMinDF            = dfMock("versionMinDF")
    val onlyNewDF               = dfMock("onlyNewDF")
    val dfSave                  = dfMock("dfSave")
    val notificationsEnviadasDF = dfMock("notificationsEnviadasDF")
    val newNotificationsDF      = dfMock("newNotificationsDF")

    val dfWriter                = mock[org.apache.spark.sql.DataFrameWriter[Row]](RETURNS_DEEP_STUBS)

    // ---------- Stub de tablas ----------
    // 1ª llamada: obtiene ALL unit/entity      → versionsDF
    // 2ª llamada: dentro del loop (timestamps) → timestampDF
    // 3ª y 4ª llamada: particiones concretas   → versionMaxDF, versionMinDF
    when(sqlContextMock.table(startsWith("commondb.versions")))
      .thenReturn(versionsDF, timestampDF, versionMaxDF, versionMinDF)

    when(sqlContextMock.table(equalTo("targetdb.notification_sent")))
      .thenReturn(notificationsEnviadasDF)

    // ---------- Stubs genéricos para los DF ----------
    // 1. Métodos que devuelven el propio DF (encadenables)
    for (df <- Seq(
          versionsDF, timestampDF, versionMaxDF, versionMinDF,
          onlyNewDF, dfSave, notificationsEnviadasDF, newNotificationsDF
        )) {
      when(df.alias(any[String])).thenReturn(df)
      when(df.select(any[Seq[Column]]: _*)).thenReturn(df)
      when(df.select(any[Column])).thenReturn(df)
      when(df.where(any[Column])).thenReturn(df)
      when(df.distinct()).thenReturn(df)
      when(df.orderBy(any[Column])).thenReturn(df)
      when(df.limit(any[Int])).thenReturn(df)
      when(df.except(any[DataFrame])).thenReturn(df)
      when(df.union(any[DataFrame])).thenReturn(df)
      when(df.unionAll(any[DataFrame])).thenReturn(df)
      when(df.repartition(any[Int])).thenReturn(df)
    }

    // 2. Métodos que devuelven valores escalares
    when(versionsDF.collect())
      .thenReturn(Array(mock[Row])) // simula un solo (unit_id, entity_id)

    when(timestampDF.collect())
      .thenReturn(Array(
        { val r = mock[Row]; when(r.getAs[Long]("dataTimestampPart")).thenReturn(20230202L); r },
        { val r = mock[Row]; when(r.getAs[Long]("dataTimestampPart")).thenReturn(20230101L); r }
      ))

    for (df <- Seq(versionsDF, timestampDF, versionMaxDF, versionMinDF,
                   onlyNewDF, dfSave, newNotificationsDF))
      when(df.columns).thenReturn(Array("colA", "colB"))

    when(dfSave.count()).thenReturn(1)
    when(notificationsEnviadasDF.count()).thenReturn(0)
    when(newNotificationsDF.count()).thenReturn(1)

    // 3. Operaciones clave en la ruta:
    //    versionsDF.distinct -> versionsDF    (ya stubbed)
    //    timestampDF.orderBy -> timestampDF   (ya stubbed)
    //    timestampDF.limit   -> timestampDF   (ya stubbed)
    //    timestampDF.select  -> timestampDF   (ya stubbed)
    //    timestampDF.distinct-> timestampDF   (ya stubbed)
    //    timestampDF.collect -> definido arriba

    // ---------- Unión entre versionMax/min ----------
    when(versionMaxDF.except(versionMinDF)).thenReturn(onlyNewDF)

    // ---------- Construcción de dfSave ----------
    when(dfSave.unionAll(onlyNewDF)).thenReturn(dfSave)

    // ---------- Join contra notificaciones enviadas ----------
    // join → newNotificationsDF
    when(dfSave.join(
           eqTo(notificationsEnviadasDF),
           any[Column],
           eqTo("left")
         )).thenReturn(newNotificationsDF)

    // where(col(...).isNull) → newNotificationsDF
    when(newNotificationsDF.where(any[Column])).thenReturn(newNotificationsDF)

    // select(...) → newNotificationsDF
    when(newNotificationsDF.select(any[Seq[Column]]: _*)).thenReturn(newNotificationsDF)

    // distinct() → newNotificationsDF  (ya stubbed)

    // ---------- Write ----------
    when(newNotificationsDF.write).thenReturn(dfWriter)
    when(dfWriter.mode(SaveMode.Append)).thenReturn(dfWriter)

    // ---------- Ejecutar ----------
    implicit val spark: SparkSession = sparkMock
    GenerateNotification.run(
      data_date_part      = "20230101",
      data_timestamp_part = "20230202123456",
      commondb            = "commondb",
      targetdb            = "targetdb"
    )

    // ---------- Verificaciones ----------
    verify(sqlContextMock, times(1)).table(startsWith("commondb.versions"))
    verify(sqlContextMock, times(1)).table("targetdb.notification_sent")
    verify(dfWriter).mode(SaveMode.Append)
    verify(newNotificationsDF).repartition(1)            // o el número que use tu implementación
    verify(dfWriter).save()                              // si tu código llama a save()

    succeed()  // Si llegó aquí, no hubo NPE ⇢ test OK
  }
}







          test("run debe construir newNotifications y escribirlas (con mocks)") {

    // ---------- Spark / SQLContext ----------
    val sparkMock      = mock[SparkSession](RETURNS_DEEP_STUBS)
    val sqlContextMock = mock[SQLContext](RETURNS_DEEP_STUBS)
    when(sparkMock.sqlContext).thenReturn(sqlContextMock)

    // ---------- DataFrames ----------
    val versionsDF              = dfMock("versionsDF")
    val timestampDF             = dfMock("timestampDF")
    val versionMaxDF            = dfMock("versionMaxDF")
    val versionMinDF            = dfMock("versionMinDF")
    val onlyNewDF               = dfMock("onlyNewDF")
    val dfSave                  = dfMock("dfSave")
    val notificationsEnviadasDF = dfMock("notificationsEnviadasDF")
    val newNotificationsDF      = dfMock("newNotificationsDF")
    val dfWriter                = mock[DataFrameWriter[Row]](RETURNS_DEEP_STUBS)

    // ---------- Tabla commondb.versions: 4 accesos en orden ----------
    when(sqlContextMock.table(anyArg[String]))
      .thenAnswer { inv =>
        val tbl = inv.getArgument
        if (tbl.startsWith("commondb.versions"))
          Seq(versionsDF, timestampDF, versionMaxDF, versionMinDF).headOption.get
        else notificationsEnviadasDF
      }
      .thenReturn(versionsDF, timestampDF, versionMaxDF, versionMinDF)   // 4 devoluciones
    // 5º acceso: notification_sent
    when(sqlContextMock.table("targetdb.notification_sent"))
      .thenReturn(notificationsEnviadasDF)

    // ---------- Stubs comunes ----------
    for (df <- Seq(versionsDF, timestampDF, versionMaxDF, versionMinDF,
                   onlyNewDF, dfSave, notificationsEnviadasDF, newNotificationsDF)) {

      when(df.alias(anyArg[String])).thenReturn(df)
      when(df.select(anyArg[Seq[Column]]: _*)).thenReturn(df)
      when(df.select(anyArg[Column])).thenReturn(df)
      when(df.where(anyArg[Column])).thenReturn(df)
      when(df.distinct()).thenReturn(df)
      when(df.orderBy(anyArg[Column])).thenReturn(df)
      when(df.limit(anyArg[Int])).thenReturn(df)
      when(df.except(anyArg[DataFrame])).thenReturn(df)
      when(df.union(anyArg[DataFrame])).thenReturn(df)
      when(df.unionAll(anyArg[DataFrame])).thenReturn(df)
      when(df.repartition(anyArg[Int])).thenReturn(df)
      when(df.columns).thenReturn(Array("colA", "colB"))
    }

    // --- Datos simulados que necesita la lógica ---
    // Un único par (unit, entity)
    val rowUnit = mock[Row]
    when(rowUnit.getAs[String]("unit_id")).thenReturn("U1")
    when(rowUnit.getAs[String]("entity_id")).thenReturn("E1")
    when(versionsDF.collect()).thenReturn(Array(rowUnit))

    // Dos timestamps: max = 20230202L, min = 20230101L
    def tsRow(v: Long) = {
      val r = mock[Row]; when(r.getAs[Long]("dataTimestampPart")).thenReturn(v); r
    }
    when(timestampDF.collect()).thenReturn(Array(tsRow(20230202L), tsRow(20230101L)))

    // max – min → onlyNewDF
    when(versionMaxDF.except(same(versionMinDF))).thenReturn(onlyNewDF)

    // Accumulador dfSave = onlyNewDF
    when(dfSave.unionAll(same(onlyNewDF))).thenReturn(dfSave)

    // join con notification_sent → newNotificationsDF
    when(dfSave.join(
           same(notificationsEnviadasDF),     // DF
           anyArg[Column],                    // condición
           argEq("left")                      // tipo join
         )).thenReturn(newNotificationsDF)

    // chain posterior
    when(newNotificationsDF.write).thenReturn(dfWriter)
    when(dfWriter.mode(SaveMode.Append)).thenReturn(dfWriter)

    // ---------- Ejecutar ----------
    implicit val spark: SparkSession = sparkMock

    GenerateNotification.run(
      data_date_part      = "20230101",
      data_timestamp_part = "20230202123456",
      commondb            = "commondb",
      targetdb            = "targetdb"
    )
