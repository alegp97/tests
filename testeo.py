test("getEmisorNotificacion debe devolver un emisor válido cuando existe") {
  // ───── Configuración de los mocks ─────
  when(sqlContextMock.table("staging_db.users_ecresearch"))
    .thenReturn(usersTable)

  when(usersTable.where(any[Column]))
    .thenReturn(dfUsersAndRols)

  when(dfUsersAndRols.select(any[Seq[Column]]: _*))
    .thenReturn(dfUsersAndRols)

  // Permitir acceso dinámico a columnas
  when(dfUsersAndRols.col(anyString())).thenAnswer { inv =>
    val colName = inv.getArgument
    col(colName)
  }

  // Simular el filtro por unidad y el limit(1)
  when(dfUsersAndRols.where(any[Column])).thenReturn(userUnidad)
  when(userUnidad.where(any[Column])).thenReturn(userUnidad)
  when(userUnidad.limit(anyInt())).thenReturn(userUnidad)

  // Simular un único emisor
  val rowEmisor = mock[Row]
  when(rowEmisor.getAs[String]("user_email"))
    .thenReturn(" Emisor@Test.com ")
  when(userUnidad.collect()).thenReturn(Array(rowEmisor))

  // ───── Ejecución ─────
  val resultado = NotificationUtil.getEmisorNotificacion(
    sqlContextMock,
    "staging_db",
    "TEST_UNIT"
  )

  // ───── Verificación ─────
  assertEquals("emisor@test.com", resultado)   // String, no lista
}
