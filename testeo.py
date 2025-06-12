import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatestplus.mockito.MockitoSugar
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import com.santander.stresstest.parser.config.BoardsArgs
import com.santander.stresstest.boards.partitionkey.GeneratePartitionKeyJob

class GeneratePartitionKeyJobTest extends AnyFunSuite with MockitoSugar {

  test("run - caso completo con partition_key presente y columnas transformadas") {
    val sqlContext = mock[SQLContext]
    val settings = mock[BoardsArgs]

    val sourcedb = "src_db"
    val targetdb = "tgt_db"
    val sourceTable = "my_source"
    val timestamp = "20240601"
    val outputTable = s"st_metrics_input_pk_$timestamp"

    when(settings.sourcedb).thenReturn(sourcedb)
    when(settings.targetdb).thenReturn(targetdb)
    when(settings.sourceTable).thenReturn(sourceTable)
    when(settings.data_timestamp_part).thenReturn(timestamp)

    // Mock sourceDF y contextDF
    val sourceDF = mock[DataFrame]
    val contextDF = mock[DataFrame]
    val joinedDF = mock[DataFrame]
    val selectedDF = mock[DataFrame]
    val renamedDF = mock[DataFrame]
    val withPKDF = mock[DataFrame]
    val withRowCountDF = mock[DataFrame]
    val castedDF = mock[DataFrame]
    val finalDF = mock[DataFrame]

    // Encadenamiento inicial
    when(sqlContext.table(s"$sourcedb.$sourceTable")).thenReturn(sourceDF)
    when(sourceDF.where(any())).thenReturn(sourceDF)
    when(sourceDF.drop("supra_source")).thenReturn(sourceDF)
    when(sourceDF.drop("data_timestamp_part")).thenReturn(sourceDF)
    when(sourceDF.columns).thenReturn(Array("report_date", "workspace", "col_a", "num_col", "weight_inout"))

    when(sqlContext.table(s"$targetdb.contexts_st")).thenReturn(contextDF)
    when(contextDF.drop("supra_source")).thenReturn(contextDF)

    when(sourceDF.col(any[String])).thenAnswer(inv => col(inv.getArgument(0)))
    when(contextDF.col(any[String])).thenAnswer(inv => col(inv.getArgument(0)))

    when(sourceDF.join(any(), any(), any())).thenReturn(joinedDF)
    when(joinedDF.drop("workspace")).thenReturn(joinedDF)
    when(joinedDF.select(any[List[org.apache.spark.sql.Column]])).thenReturn(selectedDF)
    when(selectedDF.withColumnRenamed(any(), any())).thenReturn(renamedDF)

    // Simula que el partition_key ya está presente
    when(renamedDF.select(any())).thenReturn(withPKDF)
    val partitionRow = mock[Row]
    when(partitionRow.getString(0)).thenReturn("value")
    when(withPKDF.collect()).thenReturn(Array(partitionRow))

    // Simula transformaciones
    when(renamedDF.select(any[List[org.apache.spark.sql.Column]])).thenReturn(renamedDF)
    when(renamedDF.withColumn(any(), any())).thenReturn(renamedDF)
    when(renamedDF.drop("data_date_part")).thenReturn(renamedDF)
    when(renamedDF.columns).thenReturn(Array("num_col", "weight_inout"))

    when(renamedDF.withColumn("row_count", any())).thenReturn(withRowCountDF)
    when(withRowCountDF.withColumn(any(), any())).thenReturn(castedDF)
    when(castedDF.withColumn(any(), any())).thenReturn(finalDF)
    when(finalDF.columns).thenReturn(Array("x"))

    // Simula conteos
    when(renamedDF.count()).thenReturn(10L)
    when(finalDF.count()).thenReturn(10L)

    // Mocks para sql y saveAsTable
    when(sqlContext.sql(contains("drop table"))).thenReturn(mock[DataFrame])
    val writer = mock[DataFrame#DataFrameWriter[Row]]
    when(finalDF.write).thenReturn(writer)
    when(writer.saveAsTable(s"$targetdb.$outputTable")).thenReturn(())

    // Ejecutar
    GeneratePartitionKeyJob.run(sqlContext, settings)
  }
