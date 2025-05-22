 test("getEmisorNotificacionWithDefault debe devolver emisor de unidad/entidad cuando existe") {

    // --- Mocks principales ---
    val sqlContextMock        = mock[SQLContext]
    val usersEmisores         = mock[DataFrame]
    val usersEmisoresFiltered = mock[DataFrame]
    val usersEmisoresLimited  = mock[DataFrame]
    val rowEmisor             = mock[Row]

    // --- Stubbing de la tabla y select (Column*) ---
    when(sqlContextMock.table("staging_db.users_ecresearch"))
      .thenReturn(usersEmisores)

    when(usersEmisores.select(any[Seq[Column]]: _*))
      .thenReturn(usersEmisores)

    // --- Simular búsqueda por unidad/entidad con resultado ---
    when(usersEmisores.where(any[Column]))
      .thenReturn(usersEmisoresFiltered)

    when(usersEmisoresFiltered.limit(anyInt()))
      .thenReturn(usersEmisoresLimited)

    when(usersEmisoresLimited.collect())
      .thenReturn(Array(rowEmisor))

    when(rowEmisor.getAs[String]("user_email"))
      .thenReturn("emisor.unidad@test.com")
