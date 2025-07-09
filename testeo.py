test("All validations should execute without error") {
  // Validaciones que no requieren lógica adicional ni verify
  ValFunUtil.validation_numeric_data_type(
    mockDF, "targetdb", "targetTable", List("col1"), "20250101", "20250101120000"
  )

  ValFunUtil.validation_greater_equal_values(
    mockDF, "targetdb", "targetTable", Map("col1" -> 3.0f), "20250101", "20250101120000"
  )

  ValFunUtil.validation_range_values(
    mockDF, "targetdb", "targetTable", Map("col1" -> (1.0f, 5.0f)), "20250101", "20250101120000"
  )

  ValFunUtil.validation_period_BaseYear(
    mockDF, "20250101", "20250101120000", "targetdb", "targetTable",
    new java.util.HashMap[String, java.util.List[String]] {{
      put("type", java.util.Arrays.asList("col1"))
    }},
    "type", List("val1"), baseYear = true
  )

  ValFunUtil.validation_granInput_granOutput_range_value(
    mockDF, "targetdb", "targetTable", List("col1"), List("A"), List("B"),
    min_range = 1.0, max_range = 5.0,
    baseYear = false, dateLoad = "20250101", timestamp = "20250101120000"
  )

  ValFunUtil.validate_granularity_input_type_contains(
    mockDF, "targetdb", "targetTable", "A", "20250101", "20250101120000"
  )

  ValFunUtil.validate_less_value(
    mockDF, "targetdb", "targetTable", List("col1"), value = 5.0,
    baseYear = false, fieldCond = "A", dateLoad = "20250101", timestamp = "20250101120000"
  )

  ValFunUtil.validate_equal_value(
    mockDF, "targetdb", "targetTable", List("col1"), value = 5.0,
    baseYear = false, fieldCond = "A", dateLoad = "20250101", timestamp = "20250101120000"
  )

  ValFunUtil.validate_greater_equal_value(
    mockDF, "targetdb", "targetTable", List("col1"), value = 5.0,
    baseYear = false, fieldCond = "A", dateLoad = "20250101", timestamp = "20250101120000"
  )

  ValFunUtil.validate_range_value(
    mockDF, "targetdb", "targetTable", List("col1"),
    min_range = 1.0, max_range = 5.0,
    baseYear = false, fieldCond = "A", dateLoad = "20250101", timestamp = "20250101120000"
  )
}
