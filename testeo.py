  test("getMapPath should split paths evenly across threads") {
    val allPaths = List(
      Ruta("/tmp/a"),
      Ruta("/tmp/b"),
      Ruta("/tmp/c"),
      Ruta("/tmp/d"),
      Ruta("/tmp/e")
    )
    val numThreads = 3

    val result = CompactationBySizeProcess.getMapPath(allPaths, numThreads)

    result.keySet should contain allElementsOf (0 until numThreads)
    val allAssigned = result.values.flatten.toList
    allAssigned.sorted shouldEqual allPaths.sorted
  }
