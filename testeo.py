  test("columnNotInColumn devuelve las columnas de big que no están en small") {

    // 1. Creamos los mocks
    val sessionMock = mock(classOf[SparkSession], Answers.RETURNS_DEEP_STUBS)
    val builderMock = mock(classOf[SparkSession.Builder], Answers.RETURNS_DEEP_STUBS)

    // 2. Encadenamos la API fluida
    when(builderMock.appName(anyString())).thenReturn(builderMock)
    when(builderMock.config(anyString(), anyString())).thenReturn(builderMock)
    when(builderMock.enableHiveSupport()).thenReturn(builderMock)
    when(builderMock.getOrCreate()).thenReturn(sessionMock)

    // 3. Mockeamos SparkSession.builder()
    val staticSpark: MockedStatic[SparkSession] = mockStatic(classOf[SparkSession])
    staticSpark.when(() => SparkSession.builder()).thenReturn(builderMock)

    try {
      // 4. Mocks de Columnas individuales (cada uno único)
      val colA = mock(classOf[Column])
      val colB = mock(classOf[Column])
      val colC = mock(classOf[Column])
      val colD = mock(classOf[Column])

      when(colA.toString).thenReturn("a")
      when(colB.toString).thenReturn("b")
      when(colC.toString).thenReturn("c")
      when(colD.toString).thenReturn("d")

      val big = List(colA, colB, colC)
      val small = List(colB, colD)

      val result = BoardDataUtil.columnNotInColumn(big, small)

      assert(result.map(_.toString) == List("a", "c"))
    } finally {
      staticSpark.close() // Muy importante
    }
  }
