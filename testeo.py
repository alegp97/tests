import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import org.mockito.Mockito._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar
import com.santander.stresstest.parser.config.BoardsArgs
import com.santander.stresstest.boards.execution_def.GenerateExecutionDefViewJob

class GenerateExecutionDefViewJobTest extends AnyFunSuite with MockitoSugar {

  test("run should generate view when fields_dict is not empty") {
    // Mocks
    val sqlContext = mock[SQLContext]
    val settings = mock[BoardsArgs]
    val inputDF = mock[DataFrame]
    val outputDF = mock[DataFrame]
    val fieldsDictDF = mock[DataFrame]
    val fieldsColDF = mock[DataFrame]

    // Configuraciones mock
    when(settings.sourcedb).thenReturn("src_db")
    when(settings.targetdb).thenReturn("tgt_db")
    when(settings.sourceTable).thenReturn("mytable")

    val prefixTable = "mytable"
    val inputExecDef = prefixTable + "_input"
    val outputExecDef = prefixTable + "_output"

    // Simulación de input/output exec_def
    when(sqlContext.table(s"tgt_db.$inputExecDef")).thenReturn(inputDF)
    when(sqlContext.table(s"tgt_db.$outputExecDef")).thenReturn(outputDF)

    // Simulación de columnas
    when(inputDF.columns).thenReturn(Array("col_a", "col_b"))
    when(outputDF.columns).thenReturn(Array("col_a", "col_b"))

    // Simulación de .sql con partition
    val rowWithPartition = mock[Row]
    when(rowWithPartition.getString(0)).thenReturn("20240601=xxx")
    when(sqlContext.sql(contains("show partitions"))).thenReturn(mock[DataFrame])
    when(sqlContext.sql(contains("show partitions")).collect()).thenReturn(Array(rowWithPartition))

    // Simulación de fields_dict
    when(sqlContext.table(s"src_db.fields_dict")).thenReturn(fieldsDictDF)
    when(fieldsDictDF.where(any())).thenReturn(fieldsDictDF)
    when(fieldsDictDF.select(any())).thenReturn(fieldsColDF)
    when(fieldsColDF.distinct()).thenReturn(fieldsColDF)

    val rowField = Row("col_a")
    val schema = StructType(Seq(StructField("fld_name", StringType)))
    val fieldRDD = org.apache.spark.sql.SparkSession.builder.getOrCreate().sparkContext.parallelize(Seq(rowField))
    val fieldDF = org.apache.spark.sql.SparkSession.builder.getOrCreate().createDataFrame(fieldRDD, schema)
    when(fieldsColDF.collect()).thenReturn(fieldDF.collect())

    // Mock schema tipo columna
    val schemaMock = StructType(Seq(
      StructField("col_a", DecimalType(10, 2)),
      StructField("col_b", IntegerType)
    ))
    when(outputDF.schema).thenReturn(schemaMock)
    when(inputDF.schema).thenReturn(schemaMock)

    // Simulación de ejecución SQL final
    when(sqlContext.sql(org.mockito.ArgumentMatchers.startsWith("DROP VIEW"))).thenReturn(mock[DataFrame])
    when(sqlContext.sql(org.mockito.ArgumentMatchers.startsWith("CREATE VIEW"))).thenReturn(mock[DataFrame])

    // Ejecución
    GenerateExecutionDefViewJob.run(sqlContext, settings)
  }

  test("run should skip creation when fields_dict is empty") {
    val sqlContext = mock[SQLContext]
    val settings = mock[BoardsArgs]
    val emptyDF = mock[DataFrame]

    when(settings.sourcedb).thenReturn("src_db")
    when(settings.targetdb).thenReturn("tgt_db")
    when(settings.sourceTable).thenReturn("source")

    val row = mock[Row]
    when(row.getString(0)).thenReturn("20240601=abc")
    when(sqlContext.sql(contains("show partitions"))).thenReturn(emptyDF)
    when(emptyDF.collect()).thenReturn(Array(row))

    val fieldsDictDF = mock[DataFrame]
    when(sqlContext.table("src_db.fields_dict")).thenReturn(fieldsDictDF)
    when(fieldsDictDF.where(any())).thenReturn(fieldsDictDF)
    val filteredDF = mock[DataFrame]
    when(fieldsDictDF.select(any())).thenReturn(filteredDF)
    when(filteredDF.distinct()).thenReturn(filteredDF)
    when(filteredDF.collect()).thenReturn(Array.empty)

    GenerateExecutionDefViewJob.run(sqlContext, settings)
  }
}
