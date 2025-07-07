import org.apache.spark.sql.{DataFrame, SparkSession, Column}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.execution.datasources.InsertableRelation
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.mockito.scalatest.MockitoSugar
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.catalyst.plans.logical.Project

import scala.collection.mutable.HashMap

class ValFunUtilTest extends AnyFunSuite with BeforeAndAfterAll with MockitoSugar {

  // Mocks comunes y reusables
  val mockDf: DataFrame = mock(classOf[DataFrame], RETURNS_DEEP_STUBS)
  val mockDfWriter: DataFrameWriter[Row] = mock(classOf[DataFrameWriter[Row]], RETURNS_DEEP_STUBS)

  override def beforeAll(): Unit = {
    super.beforeAll()

    // Preparación general
    when(mockDf.select(anyVararg[Column])).thenReturn(mockDf)
    when(mockDf.where(any[Column])).thenReturn(mockDf)
    when(mockDf.filter(any[Column])).thenReturn(mockDf)
    when(mockDf.distinct()).thenReturn(mockDf)
    when(mockDf.count()).thenReturn(2L)
    when(mockDf.columns).thenReturn(Array("col1", "col2"))
    when(mockDf.write).thenReturn(mockDfWriter)
    when(mockDfWriter.insertInto(any[String])).thenReturn(())
  }

  // Test directo a writeDrilldown
  test("writeDrilldown llama correctamente a insertInto") {
    ValFunUtil.writeDrilldown(
      field_val = mockDf,
      field = "col1",
      type_period = "NOT_UNIQUE_VALUE",
      dateLoad = "20250101",
      timestamp = "20250101120000",
      targetdb = "targetdb",
      targetTable = "targetTable"
    )

    verify(mockDf.write).insertInto("targetdb.targetTable")
  }

  test("validation_unique_value llama a writeDrilldown si hay duplicados") {
    val valFunMock = mockStatic(classOf[ValFunUtil])
    try {
      when(ValFunUtil.selectPkValue(any[DataFrame], any[String]))
        .thenReturn(mockDf)
      doNothing().when(ValFunUtil)
        .writeDrilldown(any[DataFrame], any[String], any[String], any[String], any[String], any[String], any[String])

      when(mockDf.select(any[Column])).thenReturn(mockDf)
      when(mockDf.distinct()).thenReturn(mockDf)
      when(mockDf.count()).thenReturn(2L)

      ValFunUtil.validation_unique_value(
        mockDf, "20250101", "20250101120000", "targetdb", "targetTable", List("col1")
      )

      verifyStatic(ValFunUtil, times(1))
      ValFunUtil.writeDrilldown(
        any[DataFrame], eq("col1"), eq("NOT_UNIQUE_VALUE"),
        eq("20250101"), eq("20250101120000"), eq("targetdb"), eq("targetTable")
      )
    } finally valFunMock.close()
  }

  test("validation_unique_value NO llama a writeDrilldown si campo único") {
    val valFunMock = mockStatic(classOf[ValFunUtil])
    try {
      when(ValFunUtil.selectPkValue(any[DataFrame], any[String]))
        .thenReturn(mockDf)
      doNothing().when(ValFunUtil)
        .writeDrilldown(any[DataFrame], any[String], any[String], any[String], any[String], any[String], any[String])

      when(mockDf.select(any[Column])).thenReturn(mockDf)
      when(mockDf.distinct()).thenReturn(mockDf)
      when(mockDf.count()).thenReturn(1L)

      ValFunUtil.validation_unique_value(
        mockDf, "20250101", "20250101120000", "targetdb", "targetTable", List("col1")
      )

      verifyStatic(ValFunUtil, never())
      ValFunUtil.writeDrilldown(
        any[DataFrame], any[String], any[String], any[String], any[String], any[String], any[String]
      )
    } finally valFunMock.close()
  }

  test("validation_unique_period (outer) llama a la inner") {
    val valFunMock = mockStatic(classOf[ValFunUtil])
    try {
      doNothing().when(ValFunUtil)
        .validation_unique_period(any[DataFrame], any[String], any[String], any[String], any[String], any[List[String]], any[List[String]])

      val fields = HashMap("gran1" -> List("col1"))
      ValFunUtil.validation_unique_period(
        mockDf, "20250101", "20250101120000", "targetdb", "targetTable", fields, List("gran1")
      )

      verifyStatic(ValFunUtil, times(1))
      ValFunUtil.validation_unique_period(
        any[DataFrame], eq("20250101"), eq("20250101120000"),
        eq("targetdb"), eq("targetTable"), eq(List("col1")), eq(List("gran1"))
      )
    } finally valFunMock.close()
  }

  test("validation_unique_period_base_year_informed (outer) llama a la inner") {
    val valFunMock = mockStatic(classOf[ValFunUtil])
    try {
      doNothing().when(ValFunUtil)
        .validation_unique_period_base_year_informed(any[DataFrame], any[String], any[String], any[String], any[String], any[List[String]], any[List[String]])

      val fields = HashMap("gran2" -> List("col2"))
      ValFunUtil.validation_unique_period_base_year_informed(
        mockDf, "20250101", "20250101120000", "targetdb", "targetTable", fields, List("gran2")
      )

      verifyStatic(ValFunUtil,
