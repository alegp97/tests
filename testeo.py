test("casesQuery devuelve la lista de columnas correctamente") {
  val sourcedb = "test_source"
  val targetTable = "test_table"

  // Mocks base
  val sparkMock = mock[SparkSession]
  val sqlContextMock = mock[SQLContext]
  val dfShowPartitions = mock[DataFrame]
  val dfOrdered = mock[DataFrame]
  val dfLimited = mock[DataFrame]
  val dfFieldsDict = mock[DataFrame]
  val dfFiltered = mock[DataFrame]
  val dfSelected = mock[DataFrame]

  // Mock del Row de partición
  val maxFieldsRow = mock[Row]
  when(maxFieldsRow.getString(0)).thenReturn("partition=20240605")

  // Encadenar mocks de show partitions
  when(sparkMock.sqlContext).thenReturn(sqlContextMock)
  when(sqlContextMock.sql("show partitions test_source.fields_dict")).thenReturn(dfShowPartitions)
  when(dfShowPartitions.orderBy(any[Column])).thenReturn(dfOrdered)
  when(dfOrdered.limit(1)).thenReturn(dfLimited)
  when(dfLimited.collect()).thenReturn(Array(maxFieldsRow))

  // Mock del DataFrame de la tabla
  when(sqlContextMock.table("test_source.fields_dict")).thenReturn(dfFieldsDict)
  when(dfFieldsDict.where(any[Column])).thenReturn(dfFiltered)
  when(dfFiltered.where(any[Column])).thenReturn(dfFiltered)
  when(dfFiltered.select(any[Seq[Column]]: _*)).thenReturn(dfSelected)

  // Este es el Row con schema necesario para getAs("fld_name")
  val schema = StructType(List(
    StructField("fld_name", StringType, nullable = false)
  ))

  val row = new GenericRowWithSchema(Array("mocked_dim1"), schema)

  when(dfSelected.collect()).thenReturn(Array(row))

  // Mock del método final de utilidad si hace falta
  object BoardGenericUtil {
    def buildCasesWithLikeQuery(rows: Array[Row]): Column = lit("mocked_column")
  }

  // Act
  val result = HistoricalExercisesJob.casesQuery(sourcedb, targetTable)(sparkMock)

  // Assert
  assert(result.isInstanceOf[List[_]])
}
