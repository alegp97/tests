 // --- Mocks ---
    val sqlContextMock         = mock[SQLContext]
    val usersEmisores          = mock[DataFrame] // tabla sin procesar
    val usersEmisoresSelect    = mock[DataFrame] // tras select(...)
    val usersEmisoresFiltered  = mock[DataFrame] // tras where(...) con filtro unit/entity
    val usersEmisoresLimited   = mock[DataFrame] // tras limit(1)
    val rowEmisor              = mock[Row]

    // --- Stub para sqlContext.table(...) ---
    when(sqlContextMock.table("staging_db.users_ecresearch"))
      .thenReturn(usersEmisores)

    // --- Stub para .select(...) → devuelve otro DataFrame (intermedio) ---
    when(usersEmisores.select(any[Seq[Column]]: _*))
      .thenReturn(usersEmisoresSelect)

    // --- Stub para .where(...) después de select → devuelve el DF real de emisores ---
    when(usersEmisoresSelect.where(any[Column]))
      .thenReturn(usersEmisores)

    // --- Stub para where(unit/entity) → devuelve DF filtrado ---
    when(usersEmisores.where(any[Column]))
      .thenReturn(usersEmisoresFiltered)

    // --- Stub para limit(1) tras filtro ---
    when(usersEmisoresFiltered.limit(anyInt()))
      .thenReturn(usersEmisoresLimited)

    // --- Stub para collect() que devuelve fila válida ---
    when(usersEmisoresLimited.collect())
      .thenReturn(Array(rowEmisor))

    // --- Stub para obtener el email del emisor ---
    when(rowEmisor.getAs[String]("user_email"))
      .thenReturn("emisor.unidad@test.com")
