test("validation_period_BaseYear with baseYear true") {
  val fieldsMap = new HashMap[String, List[String]]()
  fieldsMap.put("col1", List("subcol1"))

  ValFunUtil.validation_period_BaseYear(
    mockDF,
    "20250101",
    "20250101120000",
    "targetdb",
    "targetTable",
    fieldsMap,
    "NOT_UNIQUE_VALUE",
    List("col1"),
    true
  )
}
