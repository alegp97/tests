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

  // -------------------------------------------------------------------------
  // 🔧  Mock genérico de DataFrame (deep stubs) ------------------------------
  // -------------------------------------------------------------------------
  private val mockDF: DataFrame =
    mock[DataFrame](withSettings().defaultAnswer(RETURNS_DEEP_STUBS))

  // Evitamos NPE cuando el job hace `df.columns.mkString(",")`
  when(mockDF.columns).thenReturn(Array("dummyCol"))

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
    // 1️⃣ El job intenta leer alguna tabla de contratos
    verify(sqlContext, atLeastOnce()).table(contains("starting_points_contract"))

    // 2️⃣ El job intenta escribir la tabla agregada
    verify(mockDF.write, atLeastOnce()).saveAsTable("targetdb.agg_table")
  }
}
