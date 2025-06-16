 val expectedKeys = (0 until numThreads).toSet
    val actualKeys = result.keySet.collect { case i: Int => i }
    actualKeys.subsetOf(expectedKeys) shouldBe true // NOTE: Se asume que no todos los hilos deben tener rutas asignadas, por eso no se fuerza contain allElementsOf
