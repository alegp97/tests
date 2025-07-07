import org.apache.spark.sql.DataFrame
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql.functions._
import org.mockito.Mockito.{CALLS_REAL_METHODS, mockStatic, never, times, verify}
import org.mockito.ArgumentMatchers._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import org.mockito.scalatest.MockitoSugar

class ValFunUtilTest
  extends AnyFunSuite
     with BeforeAndAfterAll
     with SparkSessionTestWrapper
     with MockitoSugar {

  import spark.implicits._

  // ───────────────────────── helper gen. DF ─────────────────────────
  private def baseDF =
    Seq(
      ("A", "daily", "2025-01-01", "2025-01-02"),
      ("B", "monthly", "2025-01-01", "2025-01-01")
    ).toDF("value_col", "granularity_input_type", "end_date", "report_date")

  override protected def afterAll(): Unit = spark.stop()

  // 1 ───────── validation_unique_value ──────────────────────────────
  test("validation_unique_value detecta duplicados y llama a writeDrilldown") {

    val df = Seq("X", "Y").toDF("country")

    // Mock estático de las dependencias internas
    val utils = mockStatic(classOf[ValFunUtil], CALLS_REAL_METHODS)

    try {
      utils.when(() => ValFunUtil.selectPkValue(any[DataFrame](), any[String]()))
           .thenReturn(df)
      utils.when(() => ValFunUtil.writeDrilldown(
        any[DataFrame](), any[String](), any[String](),
        any[String](), any[String](), any[String]())
      ).thenReturn(())

      ValFunUtil.validation_unique_value(
        df, "20250101", "20250101120000",
        "db", "table", List("country")
      )

      // Se espera exactamente UNA llamada porque hay 2 valores distintos
      utils.verify(() => ValFunUtil.writeDrilldown(
        any[DataFrame](), eq("country"), eq("NOT_UNIQUE_VALUE"),
        eq("20250101"), eq("20250101120000"), eq("db"), eq("table")
      ), times(1))

    } finally utils.close()
  }

  test("validation_unique_value no llama a writeDrilldown si el campo es único") {

    val df = Seq("X", "X").toDF("country")
    val utils = mockStatic(classOf[ValFunUtil], CALLS_REAL_METHODS)

    try {
      utils.when(() => ValFunUtil.selectPkValue(any[DataFrame](), any[String]()))
           .thenReturn(df)
      utils.when(() => ValFunUtil.writeDrilldown(
        any[DataFrame](), any[String](), any[String](),
        any[String](), any[String](), any[String]())
      ).thenReturn(())

      ValFunUtil.validation_unique_value(
        df, "20250101", "20250101120000",
        "db", "table", List("country")
      )

      utils.verify(() => ValFunUtil.writeDrilldown(
        any[DataFrame](), any[String](), any[String](),
        any[String](), any[String](), any[String]()), never())

    } finally utils.close()
  }

  // 2 ───────── validation_unique_period  (INNER) ────────────────────
  test("validation_unique_period (inner) registra NOT_UNIQUE_PERIOD") {

    val df = baseDF
      .withColumn("country", $"value_col") // campo a validar

    val utils = mockStatic(classOf[ValFunUtil], CALLS_REAL_METHODS)

    try {
      utils.when(() => ValFunUtil.selectPkValue(any[DataFrame](), any[String]()))
           .thenAnswer(inv => inv.getArgument) // devuelve DF filtrado
      utils.when(() => ValFunUtil.writeDrilldown(
        any[DataFrame](), any[String](), any[String](),
        any[String](), any[String](), any[String]())
      ).thenReturn(())

      ValFunUtil.validation_unique_period(
        df, "20250101", "20250101120000", "db", "table",
        List("country"), List("daily")
      )

      utils.verify(() => ValFunUtil.writeDrilldown(
        any[DataFrame](), eq("country"), eq("NOT_UNIQUE_PERIOD"),
        any[String](), any[String](), any[String]()), times(1))

    } finally utils.close()
  }

  // 3 ───────── validation_unique_period  (OUTER) ────────────────────
  test("validation_unique_period (outer) delega en la inner por cada variable") {

    val vars   = List("daily", "monthly")
    val fields = HashMap("daily"   -> List("value_col"),
                         "monthly" -> List("value_col"))

    val spyStatic = mockStatic(classOf[ValFunUtil], CALLS_REAL_METHODS)

    try {
      // anulamos SOLO la inner variant para que no haga heavy-lifting,
      // pero mantenemos lógica del outer.
      spyStatic.when(() => ValFunUtil.validation_unique_period(
        any[DataFrame](), any[String](), any[String](),
        any[String](), any[String](), any[List[String]](), any[List[String]]()
      )).thenReturn(())

      ValFunUtil.validation_unique_period(
        baseDF, "20250101", "20250101120000",
        "db", "table", fields, vars
      )

      // se llama exactamente tantas veces como variables
      spyStatic.verify(() => ValFunUtil.validation_unique_period(
        any[DataFrame](), any[String](), any[String](),
        any[String](), any[String](), any[List[String]](), any[List[String]]()
      ), times(vars.size))

    } finally spyStatic.close()
  }

  // 4 ───────── validation_unique_period_base_year_informed (INNER) ─
  test("validation_unique_period_base_year_informed (inner) registra BASE_YEAR_NOT_REPORTED") {

    val df = baseDF
      .withColumn("country", $"value_col")

    val utils = mockStatic(classOf[ValFunUtil], CALLS_REAL_METHODS)

    try {
      utils.when(() => ValFunUtil.selectPkValue(any[DataFrame](), any[String]()))
           .thenAnswer(inv => inv.getArgument)
      utils.when(() => ValFunUtil.writeDrilldown(
        any[DataFrame](), any[String](), any[String](),
        any[String](), any[String](), any[String]())
      ).thenReturn(())

      ValFunUtil.validation_unique_period_base_year_informed(
        df, "20250101", "20250101120000", "db", "table",
        List("country"), List("daily")
      )

      utils.verify(() => ValFunUtil.writeDrilldown(
        any[DataFrame](), eq("country"), eq("BASE_YEAR_NOT_REPORTED"),
        any[String](), any[String](), any[String]()), times(1))

    } finally utils.close()
  }

  // 5 ───────── validation_unique_period_base_year_informed (OUTER) ─
  test("validation_unique_period_base_year_informed (outer) delega correctamente") {

    val vars   = List("daily")
    val fields = HashMap("daily" -> List("value_col"))

    val spyStatic = mockStatic(classOf[ValFunUtil], CALLS_REAL_METHODS)
    try {
      spyStatic.when(() => ValFunUtil.validation_unique_period_base_year_informed(
        any[DataFrame](), any[String](), any[String](),
        any[String](), any[String](), any[List[String]](), any[List[String]]()
      )).thenReturn(())

      ValFunUtil.validation_unique_period_base_year_informed(
        baseDF, "20250101", "20250101120000",
        "db", "table", fields, vars
      )

      spyStatic.verify(() => ValFunUtil.validation_unique_period_base_year_informed(
        any[DataFrame](), any[String](), any[String](),
        any[String](), any[String](), any[List[String]](), any[List[String]]()
      ), times(vars.size))

    } finally spyStatic.close()
  }
}
