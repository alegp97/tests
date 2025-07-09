test("Validation - Basic and Granularity Dataset Functions") {
  val mockMap: HashMap[String, List[String]] = HashMap("type" -> List("val1"))
  val mockList: List[String] = List("val1")
  val mockFields: List[String] = List("field1", "field2")

  ValFunUtil.validation_workspace_business_unit(mockDF, "20250101", "20250101120000", "targetdb", "targetTable", mockMap, mockList)
  ValFunUtil.validation_granularity_input_dataset(mockDF, "targetdb", "targetTable", "field1", mockList, "20250101", "20250101120000")
  ValFunUtil.validation_granularity_input_dataset_inclusive(mockDF, "targetdb", "targetTable", "field1", mockList, "20250101", "20250101120000")
  ValFunUtil.validation_granularity_input_dataset_lowercase(mockDF, "targetdb", "targetTable", "field1", mockList, "20250101", "20250101120000")
  ValFunUtil.validation_granularity_input_NotNull(mockDF, "targetdb", "targetTable", "field1", mockList, "20250101", "20250101120000")
  ValFunUtil.validation_granularity_input_NotNullString(mockDF, "targetdb", "targetTable", "field1", mockList, "20250101", "20250101120000")
  ValFunUtil.validation_granularity_input_null(mockDF, "targetdb", "targetTable", "field1", mockList, "20250101", "20250101120000")
}


test("Validation - Functional, NotNull and Combined Granularity") {
  val mockList: List[String] = List("val1")
  val mockFields: List[String] = List("field1", "field2")

  ValFunUtil.validation_granInput_granOutput_NotNull(mockDF, "targetdb", "targetTable", mockFields, mockList, mockList, "20250101", "20250101120000")
  ValFunUtil.validation_granInput_granOutput_functional_range(mockDF, "targetdb", "targetTable", "field1", mockList, mockList, false, "20250101", "20250101120000")
  ValFunUtil.validation_granInput_granOutput_functional_range(mockDF, "targetdb", "targetTable", "field1", mockList, mockList, true, "20250101", "20250101120000")
  ValFunUtil.validation_functional_range(mockDF, "targetdb", "targetTable", "field1", mockList, true, "20250101", "20250101120000")
  ValFunUtil.validation_functional_range(mockDF, "targetdb", "targetTable", "field1", mockList, false, "20250101", "20250101120000")
  ValFunUtil.validation_granularity_input_functional_range(mockDF, "targetdb", "targetTable", "field1", mockFields, true, mockList, "20250101", "20250101120000")
  ValFunUtil.validation_granularity_input_functional_range(mockDF, "targetdb", "targetTable", "field1", mockFields, false, mockList, "20250101", "20250101120000")
  ValFunUtil.validation_NullString(mockDF, "targetdb", "targetTable", "field1", "20250101", "20250101120000")
  ValFunUtil.validation_NotNullAndNullString(mockDF, "targetdb", "targetTable", "field1", "20250101", "20250101120000")
  ValFunUtil.validation_fields_NotNull(mockDF, "targetdb", "targetTable", mockFields, true, "20250101", "20250101120000")
  ValFunUtil.validation_fields_NotNull(mockDF, "targetdb", "targetTable", mockFields, false, "20250101", "20250101120000")
  ValFunUtil.validation_granularity_input_fieldsNotNull(mockDF, "targetdb", "targetTable", mockFields, mockList, true, "20250101", "20250101120000")
  ValFunUtil.validation_granularity_input_fieldsNotNull(mockDF, "targetdb", "targetTable", mockFields, mockList, false, "20250101", "20250101120000")
}

