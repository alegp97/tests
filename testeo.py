 when(sqlContextMock.table("staging_db.users_ecresearch"))
      .thenReturn(usersEmisores)

    // select(Column*) es var-args ⇒ hay que indicárselo a Mockito
    when(usersEmisores.select(any[Column], anyVararg[Column]))
      .thenReturn(usersEmisores)

    // Cadena where -> (1) sin resultados  |  (2) resultados por defecto
    when(usersEmisores.where(any[Column]))
      .thenReturn(usersEmisoresFiltered)   // 1ª llamada: búsqueda específica
      .thenReturn(usersEmisoresDefault)    // 2ª llamada: búsqueda por defecto

    // --- búsqueda específica (vacía) ---
    when(usersEmisoresFiltered.limit(anyInt())).thenReturn(usersEmisoresFiltered)
    when(usersEmisoresFiltered.collect()).thenReturn(Array.empty[Row])

    // --- búsqueda por defecto (tiene 1 fila) ---
    when(usersEmisoresDefault.limit(anyInt())).thenReturn(usersEmisoresDefault)
    when(usersEmisoresDefault.collect()).thenReturn(Array(rowEmisorDefault))
    when(rowEmisorDefault.getAs[String]("user_email"))
      .thenReturn("emisor.default@test.com")
