object BoardDataUtilWrapper {

  val DRIVER_APP_NAME = "[SAS] BoardDataUtil"

  private var _spark: Option[SparkSession] = None

  def setSpark(session: SparkSession): Unit = {
    _spark = Some(session)
  }

  lazy val spark: SparkSession = _spark.getOrElse {
    SparkSession.builder
      .appName(DRIVER_APP_NAME)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.metastore.try.direct.sql", "true")
      .config("spark.sql.hive.convertMetastoreParquet", "false")
      .enableHiveSupport()
      .getOrCreate()
  }
}
