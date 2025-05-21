test("getEmisorNotificacion debe devolver null cuando no encuentra emisor") {
  val sqlContextMock = mock[SQLContext]
  val usersTable = mock[DataFrame]
  val dfUsersAndRols = mock[DataFrame]
  val userUnidad = mock[DataFrame]

  when(sqlContextMock.table("staging_db.users_ecresearch")).thenReturn(usersTable)
  when(usersTable.where(any[Column])).thenReturn(dfUsersAndRols)
  when(dfUsersAndRols.select(any[Seq[Column]]: _*)).thenReturn(dfUsersAndRols)

  when(dfUsersAndRols.col(any[String])).thenAnswer(new Answer[Column] {
    override def answer(inv: InvocationOnMock): Column = col(inv.getArgument(0))
  })

  when(dfUsersAndRols.where(any[Column])).thenReturn(userUnidad)
  when(userUnidad.where(any[Column])).thenReturn(userUnidad)
  when(userUnidad.limit(anyInt())).thenReturn(userUnidad)

  // 🔴 Aquí el punto clave:
  when(userUnidad.collect()).thenReturn(Array.empty[Row])

  val result = NotificationUtil.getEmisorNotificacion(sqlContextMock, "staging_db", "TEST_UNIT")
  assert(result == null)
}
