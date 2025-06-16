 test("isEmpty cubre null, vacío, espacios y texto") {
    assert(GeneratePartitionKeyJob.isEmpty(null)     === true)
    assert(GeneratePartitionKeyJob.isEmpty("")       === true)
    assert(GeneratePartitionKeyJob.isEmpty("   ")    === true)
    assert(GeneratePartitionKeyJob.isEmpty("algo")   === false)
  }
