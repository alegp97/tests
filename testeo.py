test("run should execute reduceBySize on each assigned path") {
    val mockSqlContext = mock[SQLContext]

    // Rutas simuladas que devolverá getAllPathsOfParent
    val allPaths = ListBuffer(
      Ruta("/tmp/x", 0L, 0L),
      Ruta("/tmp/y", 0L, 0L),
      Ruta("/tmp/z", 0L, 0L)
    )
    val path      = "/tmp"
    val numThreads = 3
    val sizeFile   = 128

    // Mock estático de HdfsUtil (requiere mockito-inline en build.sbt)
    val staticMock: MockedStatic[HdfsUtil] = mockStatic(classOf[HdfsUtil])
    try {
      // Stub de métodos estáticos
      staticMock.when(() => HdfsUtil.getAllPathsOfParent(eq(path), eq(mockSqlContext))).thenReturn(allPaths)
      staticMock.when(() => HdfsUtil.reduceBySize(any[com.santander.stresstest.util.Ruta], anyInt(), any[SQLContext]))
        .thenAnswer(_ => ())

      // Usamos una instancia anónima que delega en la lógica real
      object TestCompaction extends CompactationBySizeProcess.type {
        override def getMapPath(all: ListBuffer[Ruta], threads: Int) = super.getMapPath(all, threads)
      }

      // Ejecutar
      TestCompaction.run(mockSqlContext, path, numThreads, sizeFile)

      // Verificar que reduceBySize se llamó EXACTAMENTE allPaths.size veces
      staticMock.verify(() => HdfsUtil.reduceBySize(any[com.santander.stresstest.util.Ruta], eq(sizeFile), eq(mockSqlContext)), times(allPaths.size))

    } finally {
      staticMock.close()
    }
  }
}
