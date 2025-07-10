
import org.apache.spark.sql.{SparkSession, SQLContext, Column}

trait BoardDataUtilWrapper {
  def spark: SparkSession
  def columnNotIn(big: List[String], small: List[String]): List[String]
  def columnNotInColumn(big: List[Column], small: List[Column]): List[Column]
  def getMaxPartition(source: String, fieldToFilter: String, sqlContext: SQLContext): String
}
