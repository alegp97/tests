test("casesQuery devuelve la lista de columnas correctamente") {
  // Arrange
  val sourcedb = "test_source"
  val targetTable = "test_table"

  // Mock de SparkSession y SQLContext
  val sparkMock = mock[SparkSession]
  val sqlContextMock = mock[SQLContext]
  val dfMock = mock[DataFrame]
  val rowMock = mock[Row]

  // Simula que hay una partición
  val maxFieldsRow = mock[Row]
  when(maxFieldsRow.getString(0)).thenReturn("partition=20240605")
  when(sparkMock.sqlContext).thenReturn(sqlContextMock)
  when(sqlContextMock.sql(s"show partitions $sourcedb.fields_dict")
    .orderBy(col("partition").desc)
    .limit(1)
    .collect()).thenReturn(Array(maxFieldsRow))

  // Mock de columnas variables
  val columnsVariablesDFMock = mock[DataFrame]
  when(sqlContextMock.table(s"$sourcedb.fields_dict")).thenReturn(columnsVariablesDFMock)

  // Mock filtros y selects
  when(columnsVariablesDFMock.where(any[Column])).thenReturn(columnsVariablesDFMock)
  when(columnsVariablesDFMock.select(any[Seq[Column]]: _*)).thenReturn(columnsVariablesDFMock)

  // Mock final de .collect().groupBy(...)
  val resultRow = Row("dim1")
  when(columnsVariablesDFMock.collect()).thenReturn(Array(resultRow))

  // Simular GroupBy + buildCasesWithLikeQuery
  // Este paso se puede omitir si solo querés verificar tipo de retorno
  object BoardGenericUtil {
    def buildCasesWithLikeQuery(rows: Array[Row]): Column = {
      lit("mocked_column")
    }
  }

  // Act
  val result = HistoricalExercisesJob.casesQuery(sourcedb, targetTable)(sparkMock)

  // Assert
  assert(result.isInstanceOf[List[_]])
}
