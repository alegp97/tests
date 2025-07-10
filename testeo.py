import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito._
import org.mockito.{ArgumentMatchers => AM, Answers}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.storage.StorageLevel







val rows = Array(RowFactory.create("dummy_id1"), RowFactory.create("dummy_id2"))

when(idLogit.collect()).thenReturn(rows)   // Para versión “collect → map”
when(idLogit.map(ArgumentMatchers.any()))  // Para versión “map → collect”
  .thenReturn(idLogit)                     // encadenado
when(idLogit.collect()).thenReturn(rows)





class CalculateDFModelsCreditTest extends AnyFunSuite with MockitoSugar {

  test("calculateDFModels – scope CREDIT") {

    // ─── Mocks base ──────────────────────────────────────────────────────────────
    val sqlContext = mock[SQLContext](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))

    // Todos los DataFrames que la función va creando
    val engModel       = mock[DataFrame](RETURNS_DEEP_STUBS)
    val modelVersions  = mock[DataFrame](RETURNS_DEEP_STUBS)
    val engine         = mock[DataFrame](RETURNS_DEEP_STUBS)
    val joinComum      = mock[DataFrame](RETURNS_DEEP_STUBS)
    val joinStage1     = mock[DataFrame](RETURNS_DEEP_STUBS)
    val joinStage2     = mock[DataFrame](RETURNS_DEEP_STUBS)
    val joinStage3     = mock[DataFrame](RETURNS_DEEP_STUBS)
    val joinStage4     = mock[DataFrame](RETURNS_DEEP_STUBS)
    val expectedResult = mock[DataFrame](RETURNS_DEEP_STUBS)

    // ─── sqlContext.table(...) ──────────────────────────────────────────────────
    when(sqlContext.table(AM.eq("sourcedb.sae_eng_model"))).thenReturn(engModel)
    when(sqlContext.table(AM.eq("sourcedb.sae_model_versions"))).thenReturn(modelVersions)
    when(sqlContext.table(AM.eq("sourcedb.sae_model"))).thenReturn(engModel)            // simplificación
    when(sqlContext.table(AM.eq("sourcedb.sae_engine"))).thenReturn(engine)             // para branch CREDIT

    // ─── joins/where/select encadenados ─────────────────────────────────────────
    when(engModel.join(AM.any[DataFrame], AM.any[Column])).thenReturn(joinComum)
    when(joinComum.join(AM.any[DataFrame], AM.any[Column])).thenReturn(joinComum)

    // where(isin...) stages
    when(joinComum.where(AM.any[Column])).thenReturn(joinStage1, joinStage2, joinStage3, joinStage4)

    // selects finales para ps1, ps2, etc.
    when(joinStage4.select(AM.any[Seq[Column]])).thenReturn(expectedResult)

    // ─── persist / unpersist (no hacen nada) ────────────────────────────────────
    when(joinComum.persist(AM.any[StorageLevel])).thenReturn(joinComum)
    when(joinComum.unpersist()).thenReturn(joinComum)

    // ─── Row arrays para .collect().toList (se ignora su contenido) ─────────────
    import org.apache.spark.sql.RowFactory
    val dummyRows = Array(RowFactory.create("id1"), RowFactory.create("id2"))
    when(engine.where(AM.any[Column]).select(AM.any[Column]).distinct.collect()).thenReturn(dummyRows)

    // ─── Ejecutar ───────────────────────────────────────────────────────────────
    val result = BoardDataUtil.calculateDFModels("sourcedb", K.CREDIT, sqlContext)

    // ─── Assert: la función devolvió el último DF mock que indicamos ────────────
    assert(result eq expectedResult)
  }


  test("calculateDFModels – scope distinto de CREDIT") {

    val sqlContext = mock[SQLContext](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))

    val engModel       = mock[DataFrame](RETURNS_DEEP_STUBS)
    val modelVersions  = mock[DataFrame](RETURNS_DEEP_STUBS)
    val engine         = mock[DataFrame](RETURNS_DEEP_STUBS)  // usará tabla sae_engine pero sin filtros CREDIT
    val joinComum      = mock[DataFrame](RETURNS_DEEP_STUBS)
    val expectedResult = mock[DataFrame](RETURNS_DEEP_STUBS)

    // Tablas base
    when(sqlContext.table(AM.eq("sourcedb.sae_eng_model"))).thenReturn(engModel)
    when(sqlContext.table(AM.eq("sourcedb.sae_model_versions"))).thenReturn(modelVersions)
    when(sqlContext.table(AM.eq("sourcedb.sae_model"))).thenReturn(engModel)
    when(sqlContext.table(AM.eq("sourcedb.sae_engine"))).thenReturn(engine)

    // Joins / persist
    when(engModel.join(AM.any[DataFrame], AM.any[Column])).thenReturn(joinComum)
    when(joinComum.persist(AM.any[StorageLevel])).thenReturn(joinComum)
    when(joinComum.unpersist()).thenReturn(joinComum)

    // .where(isIn(...)) para else-branch
    when(joinComum.where(AM.any[Column])).thenReturn(joinComum)

    // select final con lit(null) (ramas else)
    when(joinComum.select(AM.any[Seq[Column]])).thenReturn(expectedResult)

    // Ejecutar con scope K.FIXED (o el que uses para else)
    val result = BoardDataUtil.calculateDFModels("sourcedb", K.FIXED, sqlContext)

    assert(result eq expectedResult)
  }
}
