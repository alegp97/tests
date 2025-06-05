test("casesQuery devuelve la lista de columnas correctamente") {
  // Arrange
  val sourcedb = "test_source"
  val targetTable = "test_table"

  val sparkMock = mock[SparkSession]
  val sqlContextMock = mock[SQLContext]
  val dfShowPartitions = mock[DataFrame]
  val dfOrdered = mock[DataFrame]
  val dfLimited = mock[DataFrame]
  val dfFieldsDict = mock[DataFrame]
  val dfFiltered = mock[DataFrame]
  val dfSelected = mock[DataFrame]

  // Mock partición: show partitions ...
  val partitionRow = mock[Row]
  when(partitionRow.getString(0)).thenReturn("partition=20240605")
  when(sparkMock.sqlContext).thenReturn(sqlContextMock)
  when(sqlContextMock.sql("show partitions test_source.fields_dict")).thenReturn(dfShowPartitions)
  when(dfShowPartitions.orderBy(any[Column])).thenReturn(dfOrdered)
  when(dfOrdered.limit(1)).thenReturn(dfLimited)
  when(dfLimited.collect()).thenReturn(Array(partitionRow))

  // Mock tabla fields_dict y filtros
  when(sqlContextMock.table("test_source.fields_dict")).thenReturn(dfFieldsDict)
  when(dfFieldsDict.where(any[Column])).thenReturn(dfFiltered)
  when(dfFiltered.where(any[Column])).thenReturn(dfFiltered)
  when(dfFiltered.select(any[Seq[Column]]: _*)).thenReturn(dfSelected)

  // Crear schema con todas las columnas necesarias
  val schema = StructType(List(
    StructField("fld_name", StringType, false),
    StructField("src_fld_header", StringType, true),
    StructField("src_data_dim1", StringType, true),
    StructField("src_data_dim1_value", DecimalType(38, 23), true),
    StructField("calculation_method", StringType, true),
    StructField("src_data_dim2", StringType, true),
    StructField("src_data_dim2_value", DecimalType(38, 23), true),
    StructField("src_data_dim3", StringType, true),
    StructField("src_data_dim3_value", DecimalType(38, 23), true),
    StructField("src_data_dim4", StringType, true),
    StructField("src_data_dim4_value", DecimalType(38, 23), true),
    StructField("src_data_dim5", StringType, true),
    StructField("src_data_dim5_value", DecimalType(38, 23), true)
  ))

  // Fila de prueba compatible con el schema
  val row: Row = new GenericRowWithSchema(
    Array[AnyRef](
      "dim1",          // fld_name
      "header1",       // src_fld_header
      "value1",        // src_data_dim1
      BigDecimal("1.0"), // src_data_dim1_value
      "calc",          // calculation_method
      "v2", BigDecimal("2.0"),
      "v3", BigDecimal("3.0"),
      "v4", BigDecimal("4.0"),
      "v5", BigDecimal("5.0")
    ), schema
  )

  when(dfSelected.collect()).thenReturn(Array(row))

  // Mock del método de utilidad si no es llamado internamente
  object BoardGenericUtil {
    def buildCasesWithLikeQuery(rows: Array[Row]): Column = lit("mocked_column")
  }

  // Act
  val result = HistoricalExercisesJob.casesQuery(sourcedb, targetTable)(sparkMock)

  // Assert
  assert(result.isInstanceOf[List[_]])
  assert(result.nonEmpty)
}
