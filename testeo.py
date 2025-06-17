package com.santander.puntospartidabd

import org.apache.spark.sql.{SparkSession, SQLContext, DataFrame, DataFrameWriter, Row}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers.{anyString, contains}
import org.scalatest.Assertions._

/**
  * Prueba unitaria para [[BDRAggregationJob]] **usando mocks al 100 %**.
  *
  * ‼️ No se arranca ninguna instancia real de Spark: mockeamos **SparkSession**, **SQLContext** y **DataFrame**
  * con *Mockito* (deep stubs).  El objetivo principal es **aumentar la cobertura en Sonar** ejecutando la
  * lógica del job y verificando que se realizan las lecturas/escrituras esperadas.
  */
class BDRAggregationJobSpec extends AnyFunSuite with MockitoSugar {

  // -------------------------------------------------------------------------
  // 🔧  Mocks de SparkSession y SQLContext  ---------------------------------
  // -------------------------------------------------------------------------
  implicit val sparkMock: SparkSession =
    mock[SparkSession](withSettings().defaultAnswer(RETURNS_DEEP_STUBS))

  implicit val sqlContext: SQLContext =
    mock[SQLContext](withSettings().defaultAnswer(RETURNS_DEEP_STUBS))

  // Vinculamos el SQLContext mockeado a la SparkSession mockeada
  when(sparkMock.sqlContext).thenReturn(sqlContext)


  val mockDFWithCols: DataFrame =
    mock[DataFrame](withSettings().defaultAnswer(RETURNS_DEEP_STUBS))
  when(mockDFWithCols.columns).thenReturn(dummyCols)
  when(mockDFWithCols.count()).thenReturn(42L)

  // -------------------------------------------------------------------------
  // 🔧  Mock genérico de DataFrame (deep stubs) ------------------------------
  // -------------------------------------------------------------------------
  private val mockDF: DataFrame =
    mock[DataFrame](withSettings().defaultAnswer(RETURNS_DEEP_STUBS))

  val dummyCols = Array("col1", "col2", "col3")
  when(mockDF.columns).thenReturn(dummyCols)
  when(mockDF.columns.mkString(",")).thenReturn(dummyCols.mkString(","))
  when(mockDF.count()).thenReturn(42L)

  // Encadenamiento de transformaciones comunes para evitar NPEs
  when(mockDF.sort(any[Column])).thenReturn(mockDF)
  when(mockDF.sort(any[Column], any[Column])).thenReturn(mockDF)
  when(mockDF.select(any[Column])).thenReturn(mockDF)
  when(mockDF.select(any[Column], any[Column])).thenReturn(mockDF)
  when(mockDF.withColumn(anyString(), any())).thenReturn(mockDF)
  when(mockDF.withColumnRenamed(anyString(), anyString())).thenReturn(mockDF)
  when(mockDF.join(any[DataFrame], any[Column])).thenReturn(mockDF)
  when(mockDF.join(any[DataFrame], any[Column], anyString())).thenReturn(mockDF)
  when(mockDF.filter(any[Column])).thenReturn(mockDF)

  // Mock de groupBy → agg
  val mockGrouped = mock[RelationalGroupedDataset](withSettings().defaultAnswer(RETURNS_DEEP_STUBS))
  when(mockDF.groupBy(anyVararg[String])).thenReturn(mockGrouped)
  when(mockGrouped.agg(any())).thenReturn(mockDF)

  // Todas las lecturas devuelven el mismo DataFrame mockeado
  when(sqlContext.table(anyString())).thenReturn(mockDF)
  when(sparkMock.table(anyString())).thenReturn(mockDF)

  // -------------------------------------------------------------------------
  // 🧪  Test -----------------------------------------------------------------
  // -------------------------------------------------------------------------
  test("BDRAggregationJob.run se ejecuta sin excepciones y realiza las interacciones mínimas esperadas") {
    // ---------- Act --------------------------------------------------------
    noException should be thrownBy {
      BDRAggregationJob.run(
        sourcedb = "sourcedb",
        targetdb = "targetdb",
        targetTableOptionalName = "agg_table",
        entities = List.empty,
        extra_filter = "",
        sourceTable = "starting_points_contract",
        process = "FULL",
        is_incremental = "false"
      )
    }

    // ---------- Verify (Mockito) ------------------------------------------
    verify(sqlContext, atLeastOnce()).table(contains("starting_points_contract"))
    verify(mockDF.write, atLeastOnce()).saveAsTable("targetdb.agg_table")
  }
}
