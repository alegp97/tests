object DropRecordDriver extends HdfsFunctions {

  val DRIVER_APP_NAME = "[SAST] - Clean punctual elements - Spark Driver"
  val log = LogManager.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {
    log.info(s"[SAST] - Starting drop elements")
    val parsedArgs = parseArgsOrExit(args)
    val spark = createSparkSession()

    SparkContextUtil.withHiveContext(DRIVER_APP_NAME) { implicit spark =>
      configureSparkAndHadoop()
      processFile(parsedArgs.file)
    }
  }

  private def parseArgsOrExit(args: Array[String]): Args =
    ArgsParser.parse(args, Args()).fold(ifEmpty = sys.exit(1))

  private def createSparkSession(): SparkSession =
    SparkSession.builder
      .appName(DRIVER_APP_NAME)
      .config("hive.exec.dynamic.partition.mode", "nonstrict")
      .config("hive.metastore.try.direct.sql", "true")
      .config("spark.sql.hive.convertMetastoreParquet", "false")
      .enableHiveSupport()
      .getOrCreate()

  private def configureSparkAndHadoop()(implicit spark: SparkSession): Unit = {
    val sc = spark.sqlContext
    sc.setConf("spark.sql.tungsten.enabled", "false")
    sc.setConf("spark.sql.hive.convertMetastoreParquet", "false")
    sc.setConf("hive.exec.dynamic.partition.mode", "nonstrict")
    sc.hadoopConfiguration.set("mapreduce.fileoutputcommitter.marksuccessfuljobs", "false")
    sc.setConf("parquet.enable.summary-metadata", "false")
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    sc.sql("set parquet.compression=GZIP")
  }

  private def processFile(fileName: String)(implicit spark: SparkSession): Unit = {
    val fs = HDFSHandler.getFileSystem(fileName)
    val filePath = new Path(fileName)

    if (!fs.exists(filePath)) {
      log.error(s"[SAST] - File $fileName not found")
      throw new EresearchFileNotFoundException(s"The file : $fileName not exists")
    }

    val conf = Iterator.continually(HDFSHandler.getStream(fileName).readLine())
      .takeWhile(_ != null).mkString
    val entity = parse(conf, true).extract[CleanEntity]
    entity.inputs.foreach(input => processInput(input, fileName, fs))
    fs.rename(new Path(fileName), new Path(fileName + "." + System.currentTimeMillis()))
  }

  private def processInput(input: CleanInput, fileName: String, fs: FileSystem)
                          (implicit spark: SparkSession): Unit = {
    val table = s"${input.database}.${input.table}"
    if (input.elements_are_partitions.getOrElse(false)) {
      input.elements.foreach { element =>
        val path = HiveUtil.getLocationTable(input.database, input.table) + "/" +
                   element.toRemove.mkString("/").replace("\"", "")
        val pathObj = new Path(path)
        if (fs.exists(pathObj)) fs.delete(pathObj, true)
        spark.sqlContext.sql(s"ALTER TABLE $table DROP IF EXISTS PARTITION (${element.toRemove.mkString(",")})")
      }
    } else {
      val filter = input.partition_filter.get.mkString(" and ")
      val partitionFilter = spark.sqlContext.table(input.partition_table.get).where(filter)
      input.elements.foreach { element =>
        val filtro = element.toRemove.mkString(" and ")
        val target = spark.sqlContext.table(table).where(filtro)
        val diff = partitionFilter.except(target)
        val tmpPath = new Path(fileName).getParent + "/ruta_temp" + System.currentTimeMillis()
        diff.write.format("parquet").save(tmpPath)
        spark.sqlContext.read.parquet(tmpPath).write.mode("overwrite").format("parquet").insertInto(table)
        fs.delete(new Path(tmpPath), true)
      }
    }
  }
} 
