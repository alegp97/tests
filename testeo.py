test("precoalesce con deep mocks") {
  val sourcedb = "sourcedb"
  val scope = "some_scope"
  val filterColumn = mock[Column]

  val sqlContext = mock[SQLContext]
  val scenarioDF = mock[DataFrame](withSettings().defaultAnswer(RETURNS_DEEP_STUBS))
  val scenarioDataDF = mock[DataFrame](withSettings().defaultAnswer(RETURNS_DEEP_STUBS))
  val scenarioOverrideDF = mock[DataFrame](withSettings().defaultAnswer(RETURNS_DEEP_STUBS))
  val baselineDF = mock[DataFrame](withSettings().defaultAnswer(RETURNS_DEEP_STUBS))
  val timePeriodDF = mock[DataFrame](withSettings().defaultAnswer(RETURNS_DEEP_STUBS))

  when(sqlContext.table(s"$sourcedb.escenario_vigente")).thenReturn(scenarioDF)
  when(sqlContext.table(s"$sourcedb.sae_scenario_data")).thenReturn(scenarioDataDF)
  when(sqlContext.table(s"$sourcedb.sae_scendata_override")).thenReturn(scenarioOverrideDF)
  when(sqlContext.table(s"$sourcedb.sae_baseline_data")).thenReturn(baselineDF)
  when(sqlContext.table(s"$sourcedb.sae_time_period")).thenReturn(timePeriodDF)

  when(scenarioDataDF.where(filterColumn)).thenReturn(scenarioDataDF)
  when(scenarioOverrideDF.where(filterColumn)).thenReturn(scenarioOverrideDF)

  // mocks del map+collect
  val camposFijos = mock[Dataset[Row]]
  val camposFijosStr = mock[Dataset[String]]
  when(camposFijos.map(
    isA(classOf[Function1[Row, String]]),
    any[Encoder[String]]
  )).thenReturn(camposFijosStr)
  when(camposFijosStr.collect()).thenReturn(Array("dummy_1", "dummy_2"))

  // mock del select final
  val joinedDF = mock[DataFrame](withSettings().defaultAnswer(RETURNS_DEEP_STUBS))
  when(joinedDF.select(any[Seq[Column]]: _*)).thenReturn(joinedDF)

  // mock para coalesceByColumns (usamos función externa)
  val coalesceByColumnsMock = mock[List[Column]]
  val coalesceByColumnsObj = mock[BoardDataUtilWrapper.type]
  when(coalesceByColumnsObj.coalesceByColumns(any[DataFrame], any[DataFrame], any[List[String]])).thenReturn(coalesceByColumnsMock)

  // llamada final
  val result = BoardDataUtil.precoalesce(sqlContext, scope, sourcedb, filterColumn)

  // Assertions opcionales si haces return de algo o invocas lógica importante
  assert(result != null)
}
