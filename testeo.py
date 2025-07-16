import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.mockito.ArgumentMatchers._
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Answers.RETURNS_DEEP_STUBS

class BoardDataUtilTest extends AnyFunSuite with MockitoSugar {

  test("precoalesce con deep stubs y ejecución real de coalesceByColumns") {
    val sourcedb = "sourcedb"
    val scope = "K.FIXED"
    val filterColumn = mock[Column]

    val sqlContext = mock[SQLContext]

    // Mocks de todas las tablas
    val saeScenarioDF = mock[DataFrame](withSettings().defaultAnswer(RETURNS_DEEP_STUBS))
    val scenarioDataDF = mock[DataFrame](withSettings().defaultAnswer(RETURNS_DEEP_STUBS))
    val scenarioOverrideDF = mock[DataFrame](withSettings().defaultAnswer(RETURNS_DEEP_STUBS))
    val baselineDF = mock[DataFrame](withSettings().defaultAnswer(RETURNS_DEEP_STUBS))
    val timePeriodDF = mock[DataFrame](withSettings().defaultAnswer(RETURNS_DEEP_STUBS))

    // when SQLContext.table(...) devuelve los mocks anteriores
    when(sqlContext.table(s"$sourcedb.escenario_vigente")).thenReturn(saeScenarioDF)
    when(sqlContext.table(s"$sourcedb.sae_scenario_data")).thenReturn(scenarioDataDF)
    when(sqlContext.table(s"$sourcedb.sae_scendata_override")).thenReturn(scenarioOverrideDF)
    when(sqlContext.table(s"$sourcedb.sae_baseline_data")).thenReturn(baselineDF)
    when(sqlContext.table(s"$sourcedb.sae_time_period")).thenReturn(timePeriodDF)

    // Mocks del where para filtros incrementales
    when(scenarioDataDF.where(filterColumn)).thenReturn(scenarioDataDF)
    when(scenarioOverrideDF.where(filterColumn)).thenReturn(scenarioOverrideDF)

    // Campos para coalesceByColumns
    val dummyCol1 = mock[Column]
    val dummyCol2 = mock[Column]
    val coalesced = mock[Column]
    val finalCol = mock[Column]

    // Simulamos que hay una columna llamada "dummy"
    when(scenarioOverrideDF.col("dummy")).thenReturn(dummyCol1)
    when(scenarioDataDF.col("dummy")).thenReturn(dummyCol2)
    when(coalesce(dummyCol1, dummyCol2)).thenReturn(coalesced)
    when(coalesced.as("dummy")).thenReturn(finalCol)

    // Dataset[Row] y Dataset[String] para camposFijos
    val camposFijos = mock[Dataset[Row]]
    val camposFijosStr = mock[Dataset[String]]
    when(camposFijos.map(
      isA(classOf[Function1[Row, String]]),
      any[Encoder[String]]
    )).thenReturn(camposFijosStr)
    when(camposFijosStr.collect()).thenReturn(Array("dummy"))

    // Requiere implicit Encoder si hace falta — a veces evita errores tontos
    implicit val encoder: Encoder[String] = ExpressionEncoder()

    // Ejecutamos
    val result = BoardDataUtil.precoalesce(sqlContext, scope, sourcedb, filterColumn)

    // Validación
    assert(result != null)
  }
}
