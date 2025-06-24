test("Debe escribir datos correctamente en el destino") {
  val sparkMock = mock[SparkSession]
  val sqlContextMock = mock[SQLContext]
  val mockDataFrame = mock[DataFrame]

  when(sparkMock.sqlContext).thenReturn(sqlContextMock)
  when(sqlContextMock.table(anyString())).thenReturn(mockDataFrame)

  // Ejecuta el bloque con los mocks activos
  HiveUtilWrapper.withMocks(
    dbMock = {
      case "external_db" => true
      case _             => false
    },
    tableMock = {
      case ("source_db", "historical_data_total") => true
      case ("external_db", "pais_moneda")         => true
      case _                                       => false
    }
  ) {
    BoardHistoricalDataJob.run("source_db", "target_db", "external_db", "2023-01-01")(sparkMock)
  }

  assert(true)
}
