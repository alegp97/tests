import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.SaveMode
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito._
import org.mockito.Answers
import org.mockito.MockedStatic

class BDRFlowsJobTest extends AnyFunSuite with MockitoSugar {

  test("BDRFlowsJob.run ejecuta correctamente y cubre la lógica principal") {
    implicit val spark: SparkSession = mock[SparkSession](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))
    val sqlContext = mock[SQLContext]
    val dfMock = mock[DataFrame](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))

    // Spark y lectura parquet
    when(spark.sqlContext).thenReturn(sqlContext)
    val readerMock = mock[DataFrameReader](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))
    when(sqlContext.read).thenReturn(readerMock)
    when(readerMock.format("parquet")).thenReturn(readerMock)
    when(readerMock.load(any[String])).thenReturn(dfMock)

    // Mocks para DataFrame
    when(dfMock.withColumn(any[String], any[Column])).thenReturn(dfMock)
    when(dfMock.drop(any[String])).thenReturn(dfMock)
    when(dfMock.sort(any[Column], any[Column], any[Column])).thenReturn(dfMock)
    when(dfMock.select(any(), anyVararg())).thenReturn(dfMock)
    when(dfMock.where(any[Column])).thenReturn(dfMock)
    when(dfMock.unionAll(any[DataFrame])).thenReturn(dfMock)

    // Write
    val dfWriterMock = mock[DataFrameWriter[Row]](withSettings().defaultAnswer(Answers.RETURNS_DEEP_STUBS))
    when(dfMock.write).thenReturn(dfWriterMock)
    when(dfWriterMock.mode(SaveMode.Overwrite)).thenReturn(dfWriterMock)
    when(dfWriterMock.format("parquet")).thenReturn(dfWriterMock)
    when(dfWriterMock.saveAsTable(any[String])).thenReturn(())

    // HDFS mocks
    val fsMock = mock[FileSystem]
    val staticHdfsMock: MockedStatic[HDFSHandler] = mockStatic(classOf[HDFSHandler])
    staticHdfsMock.when(() => HDFSHandler.getFileSystem(any[String])).thenReturn(fsMock)
    when(fsMock.exists(any[Path])).thenReturn(true)
    when(fsMock.delete(any[Path], eq(true))).thenReturn(true)

    // Ejecutar el método a testear
    BDRFlowsJob.run(
      sourcecb = "sourcedb",
      targetdb = "targetdb",
      targetTableOptionalName = "agg_table",
      entities = List.empty,
      extra_filter = "",
      sourceTable = "starting_points_contract",
      process = "FULL",
      is_incremental = "false"
    )

    // Verificación mínima
    verify(dfMock.write, atLeastOnce()).saveAsTable(any[String])
    staticHdfsMock.close()
  }
}
