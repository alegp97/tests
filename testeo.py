import org.apache.spark.sql.{SQLContext, SparkSession}
import org.apache.spark.sql.hive.test.TestHiveContext
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan
import org.mockito.Mockito._
import org.scalatest.BeforeAndAfterAll
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import com.santander.stresstest.compaction.CompactorProcess
import com.santander.stresstest.util.HiveUtil
import com.santander.stresstest.process.CompactionProcess
import com.santander.stresstest.entity.IngestEntity
import com.santander.stresstest.parser.config.CompactorArgs

class CompactorProcessSpec extends AnyFunSuite with Matchers with MockitoSugar with BeforeAndAfterAll {

  test("CompactorProcess.run should call CompactionProcess.run once per entity when data_date_part exists") {
    val mockSparkSession = mock[SparkSession]
    val mockSqlContext = mock[SQLContext]
    val mockHiveUtil = mock[HiveUtil.type]
    val mockCompaction = mock[CompactionProcess.type]

    val args = CompactorArgs(
      database = "test_db",
      table = "test_table",
      thread = 4,
      sizeFile = 128,
      ingestEntity = "[{\"data_date_part\":\"2024-01-01\"}]"
    )

    when(mockHiveUtil.getLocationTable("test_db", "test_table")).thenReturn("/tmp/test_path")
    when(mockHiveUtil.getPartitions("test_db", "test_table")).thenReturn(List("data_date_part"))

    val parsedEntity = IngestEntity("2024-01-01")
    val parsedList = List(parsedEntity)

    // Ejecutar lógica de CompactorProcess en un método extraíble y testeable
    CompactorProcess.runWithDependencies(
      args,
      mockSqlContext,
      hiveUtil = mockHiveUtil,
      compaction = mockCompaction,
      parsedEntities = Some(parsedList)
    )

    verify(mockCompaction).run(
      eqTo(mockSqlContext),
      eqTo("/tmp/test_path/data_date_part=2024-01-01"),
      eqTo(4),
      eqTo(128)
    )
  }

  test("CompactorProcess.run should call CompactionProcess.run once when data_date_part does not exist") {
    val mockSparkSession = mock[SparkSession]
    val mockSqlContext = mock[SQLContext]
    val mockHiveUtil = mock[HiveUtil.type]
    val mockCompaction = mock[CompactionProcess.type]

    val args = CompactorArgs(
      database = "test_db",
      table = "test_table",
      thread = 2,
      sizeFile = 64,
      ingestEntity = "[]"
    )

    when(mockHiveUtil.getLocationTable("test_db", "test_table")).thenReturn("/tmp/test_path")
    when(mockHiveUtil.getPartitions("test_db", "test_table")).thenReturn(List("other_partition"))

    CompactorProcess.runWithDependencies(
      args,
      mockSqlContext,
      hiveUtil = mockHiveUtil,
      compaction = mockCompaction
    )

    verify(mockCompaction).run(
      eqTo(mockSqlContext),
      eqTo("/tmp/test_path"),
      eqTo(2),
      eqTo(64)
    )
  }
}
