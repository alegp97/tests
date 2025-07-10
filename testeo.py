
import org.apache.spark.sql.{SparkSession, SQLContext, Column}

trait BoardDataUtilWrapper {
  def spark: SparkSession
  def columnNotIn(big: List[String], small: List[String]): List[String]
  def columnNotInColumn(big: List[Column], small: List[Column]): List[Column]
  def getMaxPartition(source: String, fieldToFilter: String, sqlContext: SQLContext): String
}


object ProdBoardDataUtilWrapper extends BoardDataUtilWrapper {

  val log = LogManager.getLogger(getClass.getName)

  override lazy val spark: SparkSession = SparkSession.builder
    .appName("[SAS] BoardDataUtil")
    .config("hive.exec.dynamic.partition.mode", "nonstrict")
    .config("hive.metastore.try.direct.sql", "true")
    .config("spark.sql.hive.convertMetastoreParquet", "false")
    .enableHiveSupport()
    .getOrCreate()
