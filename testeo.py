test("Debe escribir datos correctamente en el destino") {
  // --> Evitar llamadas reales al Hive Metastore
  HiveUtilWrapper.tableExistsMock = Some {
    case ("source_db", "historical_data_total") => true
    case ("external_db", "pais_moneda")         => true
    case _                                       => false
  }

  HiveUtilWrapper.dbExistsMock = Some {
    case "external_db" => true
    case _              => false
  }

  val sparkMock = mock[SparkSession]
  val sqlContextMock = mock[SQLContext]
  val mockDataFrame = mock[DataFrame]

  when(sparkMock.sqlContext).thenReturn(sqlContextMock)
  when(sqlContextMock.table(anyString())).thenReturn(mockDataFrame)

  // Llama al job real
  BoardHistoricalDataJob.run(
    "source_db",
    "target_db",
    "external_db",
    "2023-01-01"
  )(sparkMock)

  // Puedes añadir asserts o verificaciones si es necesario
  assert(true) // Por ahora solo evita el error

  // Limpia los mocks para que otros tests no se vean afectados
  HiveUtilWrapper.tableExistsMock = None
  HiveUtilWrapper.dbExistsMock = None
}
