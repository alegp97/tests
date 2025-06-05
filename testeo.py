test("casesQuery devuelve la lista de columnas correctamente") {
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

  // Mock partición
  val partitionRow = mock[Row]
  when(partitionRow.getString(0)).thenReturn("partition=20240605")
  when(sparkMock.sqlContext).thenReturn(sqlContextMock)
  when(sqlContextMock.sql("show partitions test_source.fields_dict")).thenReturn(dfShowPartitions)
  when(dfShowPartitions.orderBy(any[Column])).thenReturn(dfOrdered)
  when(dfOrdered.limit(1)).thenReturn(dfLimited)
  when(dfLimited.collect()).thenReturn(Array(partitionRow))

  // Mock tabla y filtros
  when(sqlContextMock.table("test_source.fields_dict")).thenReturn(dfFieldsDict)
  when(dfFieldsDict.where(any[Column])).thenReturn(dfFiltered)
  when(dfFiltered.where(any[Column])).thenReturn(dfFiltered)
  when(dfFiltered.select(any[Seq[Column]]: _*)).thenReturn(dfSelected)

  // Schema compatible con uso de getAs[String]
  val schema = StructType(List(
    StructField("fld_name", StringType, false),
    StructField("src_fld_header", StringType, true),
    StructField("src_data_dim1", StringType, true),
    StructField("src_data_dim1_value", StringType, true),
    StructField("calculation_method", StringType, true),
    StructField("src_data_dim2", StringType, true),
    StructField("src_data_dim2_value", StringType, true),
    StructField("src_data_dim3", StringType, true),
    StructField("src_data_dim3_value", StringType, true),
    StructField("src_data_dim4", StringType, true),
    StructField("src_data_dim4_value", StringType, true),
    StructField("src_data_dim5", StringType, true),
    StructField("src_data_dim5_value", StringType, true)
  ))

  // Fila con valores como Strings para evitar ClassCastException
  val row: Row = new GenericRowWithSchema(
    Array[AnyRef](
      "dim1", "header1", "d1", "1.0", "calc",
      "d2", "2.0", "d3", "3.0", "d4", "4.0", "d5", "5.0"
    ), schema
  )

  when(dfSelected.collect()).thenReturn(Array(row))

  // Act
  val result = HistoricalExercisesJob.casesQuery(sourcedb, targetTable)(sparkMock)

  // Assert
  assert(result.isInstanceOf[List[_]])
  assert(result.nonEmpty)
}
