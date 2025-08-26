object LogFileProcess {

  val log = LogManager.getLogger(getClass.getName)

  // Constantes tal y como las usas
  private val SEQUENCE_SEPARATOR: String = ","
  private val FILE_CSV_DELIMITER: Char  = ';'
  private val WORKSPACE_COLUMN: String  = "workspace"
  private val SA_CONTEXT_ID_COLUMN      = "sa_context_id"
  private val DATAGET_TIMESTAMP_COLUMN  = "datagen_timestamp"

  /**
    * Sonarqube less cognitive complexity:
    * - Orquestación lineal de pasos
    * - Guard clauses en vez de if/else anidados
    * - Reutilización de helpers existentes (addIndex, getIds, isLogVFStopper, generateBody)
    * - Helpers privados pequeños para decisiones repetidas (sin cambiar lógica)
    */
  def run(
    validationdb: String,
    sourcedb: String,
    stagingdb: String,
    data_date_part: String,
    data_timestamp_part: String,
    mailServer: MailServerConfig,
    message: MailConfig,
    process: String,
    pathUser: String,
    source_table: String,
    log_table: String,
    tmpPath: String,
    env: String,
    emailSenderWrapper: EmailSenderWrapper = new AzureEmailSenderWrapper()
  )(implicit spark: SparkSession): Unit = {

    // ---------------- logs de entrada (se conservan) ----------------
    log.info("validationdb: " + validationdb)
    log.info("sourcedb: " + sourcedb)
    log.info("stagingdb: " + stagingdb)
    log.info("data_date_part: " + data_date_part)
    log.info("data_timestamp_part: " + data_timestamp_part)
    log.info("process: " + process)
    log.info("pathUser: " + pathUser)
    log.info("source_table: " + source_table)
    log.info("log_table: " + log_table)
    log.info("tmpPath: " + tmpPath)
    log.info("env: " + env)

    // ---------------- contexto HDFS ----------------
    val hadoopConf = spark.sqlContext.sparkContext.hadoopConfiguration
    val fs         = HDFSHandler.getFileSystem(tmpPath)

    // ---------------- carga base ----------------
    val baseLogDF = spark.sqlContext
      .table(s"$validationdb.$log_table")
      .where(col("data_date_part") === s"$data_date_part")
      .where(col("data_timestamp_part") === s"$data_timestamp_part")
      .distinct

    val windowSpec = Window.partitionBy(col("errormsg")).orderBy(col("errormsg"))

    val stMetricsInputTable = spark.sqlContext
      .table(s"$sourcedb.$source_table")
      .where(col("data_date_part") === s"$data_date_part")
      .where(col("data_timestamp_part") === s"$data_timestamp_part")
      .persist(StorageLevel.MEMORY_AND_DISK)

    // ---------------- normalización VT / VF ----------------
    val processKey = process.trim.toUpperCase
    val logTableDF = normalizeLog(baseLogDF, processKey, windowSpec)

    // ---------------- warnings VT si aplica ----------------
    val withWarningsDF =
      addVTWarningsIfNeeded(
        logTableDF,
        enabled             = !isLogVFStopper(log_table),
        validationdb        = validationdb,
        source_table        = source_table,
        data_date_part      = data_date_part,
        data_timestamp_part = data_timestamp_part,
        windowSpec          = windowSpec
      )

    log.info("logTableDF final count: " + withWarningsDF.count)

    // ---------------- traducimos nombre de campo con fields_dict ----------------
    val colsDictOpt = latestFieldsDictValue(stagingdb)
      .map(value => loadColumnsVariablesDF(stagingdb, source_table, value))

    val logJoinFieldsDict =
      colsDictOpt.map(df => joinFieldsDict(withWarningsDF, df)).getOrElse(withWarningsDF)

    log.info("logJoinFieldsDict.count: " + logJoinFieldsDict.count)

    // ---------------- selección PK + index (reutiliza addIndex) ----------------
    val stMetricsPk = stMetricsInputTable.select(
      col("report_date"), col(SA_CONTEXT_ID_COLUMN), col(DATAGET_TIMESTAMP_COLUMN),
      col("end_date"), col("dataset"),
      col("granularity_input_type"), col("granularity_output_type"),
      col("granularity_input"), col("granularity_output")
    )
    val stWithIndex = addIndex(stMetricsPk, spark.sqlContext)

    // ---------------- join final por PK (pkvalue separado por “|”) ----------------
    val pkValue  = split(logJoinFieldsDict.col("pkvalue"), "\\|")
    val joinCond = pkJoinCondition(pkValue, stWithIndex)

    val result = stWithIndex
      .join(logJoinFieldsDict, joinCond)
      .select(
        logJoinFieldsDict.col("tablename"),
        logJoinFieldsDict.col("datagen_timestamp"),
        logJoinFieldsDict.col("pkvalue"),
        logJoinFieldsDict.col("fieldname"),
        logJoinFieldsDict.col("translated_name"),
        logJoinFieldsDict.col("fieldvalue"),
        logJoinFieldsDict.col("errormsg"),
        logJoinFieldsDict.col("data_date_part"),
        logJoinFieldsDict.col("data_timestamp_part"),
        stWithIndex.col("index")
      )
      .distinct

    log.info("result.count: " + result.count)

    // ---------------- escritura csv en /logs ----------------
    writeLogsCsv(result, tmpPath)

    // ---------------- renombrado del part-*.csv ----------------
    renameLogsCsv(fs, tmpPath, s"log_${source_table}_${process.toLowerCase}.csv")

    // ---------------- variables para notificación ----------------
    val notifVars = stMetricsInputTable
      .select(col(WORKSPACE_COLUMN), col(SA_CONTEXT_ID_COLUMN), col(DATAGET_TIMESTAMP_COLUMN))
      .distinct

    if (hasAnyRow(notifVars)) {
      log.info("[SAST] Preparing send email")

      val notifArr     = notifVars.collect()
      val workspaces   = notifArr.map(_.getAs[String](WORKSPACE_COLUMN)).distinct
      val workspacesUp = workspaces.map(_.toUpperCase)

      val workspaceStr = workspaces.mkString(SEQUENCE_SEPARATOR)
      val ctxIds       = getIds(SA_CONTEXT_ID_COLUMN, notifArr)
      val tsIds        = getIds(DATAGET_TIMESTAMP_COLUMN, notifArr)

      // base del mensaje (se conserva el patrón original)
      val messageToSend = new MailConfig(
        message.getFrom(), message.getTo(), message.getCc(), message.getBcc(),
        message.getSubject(), message.getBody()
      )

      // consultamos tabla de usuarios y metemos BCC
      val emails = fetchUserEmails(stagingdb, workspacesUp)
      if (emails.nonEmpty) {
        messageToSend.addBc(emails.toList)
        // subject/body con generateBody + NotificationUtil (misma lógica)
        prepareSubjectAndBody(messageToSend, env, source_table, workspaceStr, ctxIds, tsIds)
        // envío adjuntando el csv generado
        emailSenderWrapper.sendEmail(
          messageToSend.getSubject(),
          messageToSend.getBody(),
          messageToSend.getTo(),
          messageToSend.getCc(),
          messageToSend.getBcc(),
          Array(s"$tmpPath/logs/log_${source_table}_${process.toLowerCase}.csv")
        )
      } else {
        log.info("[SAST] There is not adresses to send the email")
      }
    } else {
      log.info("[SAST] There is not table with workspaces to send")
    }
  }

  // ============================ helpers privados (sin cambiar la lógica) ============================

  // Sonarqube less cognitive complexity: normaliza VT/VF con match en vez de if/else anidados
  private def normalizeLog(base: DataFrame, processKey: String, window: Window): DataFrame =
    processKey match {
      case "VT" =>
        log.info("[SAST] validacion tecnica")
        base.where(col("errplevel") === "fatal")
          .select(
            col("tablename"), col("pkvalue"),
            col("fieldname"), col("fieldvalue"),
            col("errormsg"),
            col("data_date_part"), col("data_timestamp_part")
          )
          .withColumn("row_number", row_number().over(window))
      case _ =>
        log.info("[SAST] validacion funcional")
        base.select(
          col("tablename"),
          col("reg").as("pkvalue"),
          col("data_source_entity_code").as("fieldname"),
          col("data_source_entity").as("fieldvalue"),
          col("validatiocode").as("errormsg"),
          col("data_date_part"),
          col("data_timestamp_part")
        )
    }

  // Sonarqube less cognitive complexity: guard clause para warnings VT
  private def addVTWarningsIfNeeded(
    logDF: DataFrame,
    enabled: Boolean,
    validationdb: String,
    source_table: String,
    data_date_part: String,
    data_timestamp_part: String,
    windowSpec: Window
  )(implicit spark: SparkSession): DataFrame = {
    if (!enabled) return logDF

    log.info("[SAST] Load VT warnings")
    val vtWarn = spark.sqlContext
      .table(s"$validationdb.uv_statistics_detail_$source_table")
      .where(col("data_date_part") === s"$data_date_part")
      .where(col("data_timestamp_part") === s"$data_timestamp_part")
      .select(
        col("tablename"), col("pkvalue"), col("fieldname"), col("fieldvalue"),
        col("errormsg"), col("data_date_part"), col("data_timestamp_part")
      )

    log.info("[SAST] union logs VT (validaciones catalogadas como warning) + VF")
    logDF.union(vtWarn).withColumn("row_number", row_number().over(windowSpec))
  }

  // Último data_date_part de fields_dict
  private def latestFieldsDictValue(stagingdb: String)(implicit spark: SparkSession): Option[String] =
    spark.sqlContext
      .sql(s"show partitions $stagingdb.fields_dict")
      .orderBy(col("partition").desc)
      .limit(1)
      .collect()
      .headOption
      .map(_.getString(0).split("=", 2)(1))

  // Carga (fld_name, src_fld_header) filtrando por source y proceso=file
  private def loadColumnsVariablesDF(stagingdb: String, source_table: String, value: String)
                                    (implicit spark: SparkSession): DataFrame =
    spark.sqlContext.table(s"$stagingdb.fields_dict")
      .where(col("data_date_part") === value)
      .where(lower(col("src_name")) === source_table)
      .where(lower(col("process")) === "file")
      .select(
        trim(lower(col("fld_name"))).as("fld_name"),
        trim(lower(col("src_fld_header"))).as("src_fld_header")
      ).distinct

  // Join para obtener translated_name
  private def joinFieldsDict(logDF: DataFrame, colsDF: DataFrame): DataFrame =
    logDF.drop("row_number")
      .join(colsDF, lower(logDF.col("fieldname")) === lower(colsDF.col("src_fld_header")))
      .select(
        logDF.col("tablename"),
        logDF.col("datagen_timestamp"),
        logDF.col("pkvalue"),
        logDF.col("fieldname"),
        colsDF.col("fld_name").as("translated_name"),
        logDF.col("fieldvalue"),
        logDF.col("errormsg"),
        logDF.col("data_date_part"),
        logDF.col("data_timestamp_part")
      )

  // Condición de join por PK (pkvalue con 9 componentes separados por “|”)
  private def pkJoinCondition(pkValue: Column, idxDF: DataFrame): Column =
    trim(idxDF.col("report_date"))             === trim(pkValue.getItem(0)) &&
    trim(idxDF.col("sa_context_id"))           === trim(pkValue.getItem(1)) &&
    trim(idxDF.col("datagen_timestamp"))       === trim(pkValue.getItem(2)) &&
    trim(idxDF.col("end_date"))                === trim(pkValue.getItem(3)) &&
    trim(idxDF.col("dataset"))                 === trim(pkValue.getItem(4)) &&
    trim(idxDF.col("granularity_input_type"))  === trim(pkValue.getItem(5)) &&
    trim(idxDF.col("granularity_output_type")) === trim(pkValue.getItem(6)) &&
    trim(idxDF.col("granularity_input"))       === trim(pkValue.getItem(7)) &&
    trim(idxDF.col("granularity_output"))      === trim(pkValue.getItem(8))

  // Escritura CSV (mismas opciones que tu código)
  private def writeLogsCsv(df: DataFrame, tmpPath: String): Unit =
    df.coalesce(1)
      .write.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", FILE_CSV_DELIMITER.toString)
      .option("nullValue", "")
      .option("parserLib", "univocity")
      .option("escape", "\\")
      .mode("overwrite")
      .option("quoteMode", "NON_NUMERIC")
      .save(tmpPath + "/logs")

  // Renombrado del único part-*.csv al nombre final
  private def renameLogsCsv(fs: org.apache.hadoop.fs.FileSystem, tmpPath: String, finalName: String): Unit = {
    val part = fs.globStatus(new Path(s"$tmpPath/logs/part*"))(0).getPath.getName
    val src  = new Path(tmpPath + "/logs/" + part)
    val dst  = new Path(tmpPath + "/logs/" + finalName)
    log.info("[SAST] rutaFile: " + dst)
    log.info("[SAST] rutaCsv: " + src)
    fs.rename(src, dst)
  }

  // Pequeña utilidad para evitar count cuando solo queremos saber si hay filas
  private def hasAnyRow(df: DataFrame): Boolean = df.take(1).nonEmpty

  // Subject/Body usando generateBody + NotificationUtil (reutiliza tus helpers)
  private def prepareSubjectAndBody(m: MailConfig, env: String, sourceTable: String,
                                    workspaceStr: String, contextIds: String, timestamps: String): Unit = {
    val notif = new Notification(workspaceStr, env, sourceTable, "", contextIds, timestamps, "", "", "")
    if (m.getBody.isEmpty) m.setBody(generateBody(notif))
    else m.setBody(NotificationUtil.replaceStringWithInfoNotification(m.getBody, notif))
    if (m.getSubject.isEmpty) m.setSubject("Validaciones tecnicas o funcionales")
    else m.setSubject(NotificationUtil.replaceStringWithInfoNotification(m.getSubject, notif))
  }

  // Lectura de emails autorizados (usa HiveUtil.tableExists)
  private def fetchUserEmails(stagingdb: String, workspacesUpper: Array[String])
                            (implicit spark: SparkSession): Seq[String] = {
    if (!HiveUtil.tableExists(stagingdb, "users_stress_test")) return Seq.empty
    spark.sqlContext
      .table(s"$stagingdb.users_stress_test")
      .where(col("user_email").contains("@"))
      .where(trim(upper(col("validation_notification"))) === lit("Y"))
      .where(upper(col(WORKSPACE_COLUMN)).isin(workspacesUpper: _*) || upper(col(WORKSPACE_COLUMN)) === lit("ANY"))
      .select(col("user_email"))
      .distinct
      .collect()
      .map(r => r.getString(0).trim.toLowerCase)
  }
