import org.apache.spark.sql.SQLContext
import org.mockito.Mockito._
import org.mockito.ArgumentMatchers._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

import com.santander.stresstest.process.CompactionProcess
import com.santander.stresstest.util.HdfsUtil

class CompactorProcessSpec extends AnyFunSuite with Matchers with MockitoSugar {

  test("getMapPath should split paths evenly across threads") {
    val allPaths = List("a", "b", "c", "d", "e")
    val numThreads = 3

    val result = CompactionProcess.getMapPath(allPaths, numThreads)

    result.keySet should contain allElementsOf (0 until numThreads)
    val allAssigned = result.values.flatten.toList
    allAssigned.sorted shouldEqual allPaths.sorted
  }

  test("run should execute reduceBySize on each assigned path") {
    val mockSqlContext = mock[SQLContext]
    val mockHdfsUtil = mock[HdfsUtil.type]

    val allPaths = List("p1", "p2", "p3")
    val path = "/tmp"
    val numThreads = 3
    val sizeFile = 100

    // Stub getAllPathsOfParent
    when(mockHdfsUtil.getAllPathsOfParent(eqTo(path), eqTo(mockSqlContext)))
      .thenReturn(allPaths)

    // Inject mocks via a local object
    object TestCompaction extends CompactionProcess.type {
      override def getMapPath(all: List[String], threads: Int) = super.getMapPath(all, threads)
    }

    // Replace the HdfsUtil inside the method via partial mocking
    val originalMethod = HdfsUtil.reduceBySize _
    try {
      var calls = scala.collection.mutable.ListBuffer.empty[String]
      HdfsUtil.reduceBySize = (p, s, ctx) => calls += p

      TestCompaction.run(mockSqlContext, path, numThreads, sizeFile)

      calls.sorted shouldEqual allPaths.sorted
    } finally {
      HdfsUtil.reduceBySize = originalMethod
    }
  }
}
