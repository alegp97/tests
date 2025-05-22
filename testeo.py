 test("getEmisorNotificacionWithDefault debe devolver emisor de unidad/entidad cuando existe") {

    // ------------------------ Mocks base ------------------------
    val sqlContextMock        = mock[SQLContext]
    val usersEmisores         = mock[DataFrame]
    val usersEmisoresFiltered = mock[DataFrame]
    val rowEmisor             = mock[Row]

    // ------------------------ Stubbing ------------------------
    when(sqlContextMock.table("staging_db.users_ecresearch"))
      .thenReturn(usersEmisores)

    // select (Column*) es varargs ⇒ usar anyVararg
    when(usersEmisores.select(any[Column], anyVararg[Column]()))
      .thenReturn(usersEmisores)

    // El primer .where(...) devuelve el DataFrame filtrado por unidad/entidad
    when(usersEmisores.where(any[Column]))
      .thenReturn(usersEmisoresFiltered)

    // Filtro de unidad/entidad tiene resultado
    when(usersEmisoresFiltered.limit(anyInt())).thenReturn(usersEmisoresFiltered)
    when(usersEmisoresFiltered.collect()).thenReturn(Array(rowEmisor))

    // Simula un Row con el campo user_email esperado
    when(rowEmisor.getAs[String]("user_email"))
      .thenReturn("emisor.unidad@test.com")

    // ------------------------ Ejecución ------------------------
    val resultado =
      com.santander.eresearch.excel.transpose.notifications.NotificationUtil
        .getEmisorNotificacionWithDefault(
          sqlContextMock,
          "staging_db",
          "TEST_UNIT",
          "TEST_ENTITY"
        )

    // ------------------------ Verificación ------------------------
    resultado shouldEqual "emisor.unidad@test.com"
  }
