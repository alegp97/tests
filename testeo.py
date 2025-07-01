  test("columnNotInColumn devuelve las columnas de big que no están en small") {

    /* ---------- 1. Creamos el mock de Builder y de SparkSession ---------- */
    val sessionMock = Mockito.mock(classOf[SparkSession], RETURNS_DEEP_STUBS)
    val builderMock  = Mockito.mock(classOf[SparkSession.Builder], RETURNS_DEEP_STUBS)

    // hacemos que toda la API fluida devuelva el builder­Mock otra vez
    Mockito.when(builderMock.appName(anyString())).thenReturn(builderMock)
    Mockito.when(builderMock.config(anyString(), anyString())).thenReturn(builderMock)
    Mockito.when(builderMock.enableHiveSupport()).thenReturn(builderMock)
    Mockito.when(builderMock.getOrCreate()).thenReturn(sessionMock)

    /* ---------- 2. Mockeamos la llamada estática SparkSession.builder ---------- */
    val staticSpark = Mockito.mockStatic(classOf[SparkSession])
    staticSpark.when(() => SparkSession.builder).thenReturn(builderMock)

    try {
      /* ---------- 3. Preparamos Column mocks para evitar col("x") real ---------- */
      val colA, colB, colC, colD = {
        val c = Mockito.mock(classOf[Column])
        // toString es lo que usa internamente el método diff
        Mockito.when(c.toString).thenReturn(java.util.UUID.randomUUID().toString) 
        c
      }
      Mockito.when(colA.toString).thenReturn("a")
      Mockito.when(colB.toString).thenReturn("b")
      Mockito.when(colC.toString).thenReturn("c")
      Mockito.when(colD.toString).thenReturn("d")

      val big   = List(colA, colB, colC)
      val small = List(colB, colD)

      val res = BoardDataUtil.columnNotInColumn(big, small)

      assert(res.map(_.toString) == List("a", "c"))
    } finally {
      /* ---------- 4. Cerramos el mock estático para no ensuciar otros tests ---------- */
      staticSpark.close()
    }
  }
