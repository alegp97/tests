package com.santander.stresstest.notifications

import com.santander.stresstest.entity.{MailConfig, MailServerConfig}
import com.santander.stresstest.util.{AzureEmailSenderWrapper, EmailSenderWrapper, HiveUtil, NotificationUtil}
import com.santander.supra.core.datale.hdfs.HDFSHandler

import org.apache.hadoop.fs.Path
import org.apache.log4j.LogManager
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel

import scala.collection.JavaConversions._
import scala.language.implicitConversions

/**
  * Refactor orientado a **bajar la Cognitive Complexity** del método `run` (≈ 41 ➜ ≤ 15)
  * manteniendo la **lógica original** y los **logs** en español. La reducción se consigue:
  *  - Orquestación lineal en `run` y extracción de decisiones a pequeños helpers puros.
  *  - Guard clauses y uso de `Option` para evitar if/else anidados.
  *  - Reutilización de condiciones (por ejemplo, join por PK) en funciones auxiliares.
  */
object LogFileProcess {

  private val log = LogManager.getLogger(getClass.getName)

  // ======= Constantes originales (se mantienen los nombres y valores) ======================
  private val SEQUENCE_SEPARATOR: String = ","
  private val FILE_CSV_DELIMITER: Char   = ';'
  private val WORKSPACE_COLUMN: String   = "workspace"
  private val SA_CONTEXT_ID_COLUMN: String = "sa_context_id"
  private val DATAGET_TIMESTAMP_COLUMN: String = "datagen_timestamp"

  // ===================================== RUN ==============================================
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

    // ---- Logs de entrada (conservados) ----------------------------------------------------
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

    // ---- Contexto HDFS -------------------------------------------------------------------
    val hadoopConf = spark.sqlContext.sparkContext.hadoopConfiguration
    val fs         = HDFSHandler.getFileSystem(tmpPath)

    // ---- Carga base de logs y tabla de métricas ------------------------------------------
    val baseLogDF = spark.sqlContext
      .table(s"$validationdb.$log_table")
      .where(col("data_date_part") === s"$data_date_part")
      .where(col("data_timestamp_part") === s"$data_timestamp_part")
      .distinct()

    val windowSpec = Window.partitionBy(col("errormsg")).orderBy(col("errormsg"))

    val stMetricsInputTable = spark.sqlContext
      .table(s"$sourcedb.$source_table")
      .where(col("data_date_part") === s"$data_date_part")
      .where(col("data_timestamp_part") === s"$data_timestamp_part")
      .persist(StorageLevel.MEMORY_AND_DISK)

    // ---- Normalizamos según proceso (VT / VF) --------------------------------------------
    val processKey = process.trim.toUpperCase
    val logTableDF = normalizeLog(baseLogDF, processKey, windowSpec)

    // ---- Añadimos VT warnings si procede --------------------------------------------------
    val logTableWithWarningsDF = addVTWarningsIfNeeded(
      logDF = logTableDF,
      enabled = !isLogVFStopper(log_table),
      validationdb = validationdb,
      source_table = source_table,
      data_date_part = data_date_part,
      data_timestamp_part = data_timestamp_part,
      windowSpec = windowSpec
    )

    log.info("logTableDF final count: " + logTableWithWarningsDF.count)

    // ---- fields_dict: obtenemos mapeo y unimos para 'translated_name' ---------------------
    val fieldsDictValueOpt  = latestFieldsDictValue(stagingdb)
    val columnsVariablesOpt = fieldsDictValueOpt.map(v => loadColumnsVariablesDF(stagingdb, source_table, v))

    val logJoinFieldsDict = columnsVariablesOpt
      .map(df => joinFieldsDict(logTableWithWarningsDF, df))
      .getOrElse(logTableWithWarningsDF)

    log.info("logJoinFieldsDict.count: " + logJoinFieldsDict.count)

    // ---- Selección PK de métricas + index -------------------------------------------------
    val stMetricsInputSelectPK = stMetricsInputTable.select(
      col("report_date"), col(SA_CONTEXT_ID_COLUMN), col(DATAGET_TIMESTAMP_COLUMN),
      col("end_date"), col("dataset"),
      col("granularity_input_type"), col("granularity_output_type"),
      col("granularity_input"),     col("granularity_output")
    )

    val stMetricsInputWithIndex = addIndex(stMetricsInputSelectPK, spark.sqlContext)

    // ---- Join final contra PK (split por '|') --------------------------------------------
    val pkValue  = split(logJoinFieldsDict.col("pkvalue"), "\\|")
    val joinCond = pkJoinCondition(pkValue, stMetricsInputWithIndex)

    val result = stMetricsInputWithIndex
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
        stMetricsInputWithIndex.col("index")
      )
      .distinct()

    log.info("result.count: " + result.count)

    // ---- Escribimos CSV en tmp/logs ------------------------------------------------------
    writeLogsCsv(result, tmpPath)

    // ---- Renombramos el CSV al formato pedido --------------------------------------------
    renameLogsCsv(fs, tmpPath, s"log_${source_table}_${process.toLowerCase}.csv")

    // ---- Variables para notificación por email -------------------------------------------
    val stMetricsTableNotificationVars = stMetricsInputTable
      .select(col(WORKSPACE_COLUMN), col(SA_CONTEXT_ID_COLUMN), col(DATAGET_TIMESTAMP_COLUMN))
      .distinct()

    if (hasAnyRow(stMetricsTableNotificationVars)) {
      log.info("[SAST] Preparing send email")

      val stMetInpNotificationVarsArray = stMetricsTableNotificationVars.collect()
      val workspacesLower  = stMetInpNotificationVarsArray.map(_.getAs[String](WORKSPACE_COLUMN)).distinct
      val workspacesUpper  = workspacesLower.map(_.toUpperCase)

      val workspace  = workspacesLower.mkString(SEQUENCE_SEPARATOR)
      val contextIds = getIds(SA_CONTEXT_ID_COLUMN, stMetInpNotificationVarsArray)
      val timestamps = getIds(DATAGET_TIMESTAMP_COLUMN, stMetInpNotificationVarsArray)

      val messageToSend = new MailConfig(message.getFrom(), message.getTo(), message.getCc(), message.getBcc(), message.getSubject(), message.getBody())

      // usuarios destinatarios
      val emails = fetchUserEmails(stagingdb, workspacesUpper)
      if (emails.nonEmpty) {
        messageToSend.addBc(emails.toList)
        prepareSubjectAndBody(messageToSend, env, source_table, contextIds, timestamps)
        // adjuntamos el csv recién generado
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

  // ================================ HELPERS ===============================================

  /** VT/VF normalizados (mantiene logs). */
  private def normalizeLog(base: DataFrame, processKey: String, window: Window): DataFrame = {
    processKey match {
      case "VT" =>
        log.info("[SAST] validacion tecnica")
        base.where(col("errplevel") === "fatal")
          .select(
            col("tablename"), col("pkvalue"), col("fieldname"), col("fieldvalue"),
            col("errormsg"), col("data_date_part"), col("data_timestamp_part")
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
  }

  /** Añadir VT warnings si procede. */
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

  /** Último valor de partición en fields_dict (data_date_part). */
  private def latestFieldsDictValue(stagingdb: String)(implicit spark: SparkSession): Option[String] =
    spark.sqlContext
      .sql(s"show partitions $stagingdb.fields_dict")
      .orderBy(col("partition").desc)
      .limit(1)
      .collect()
      .headOption
      .map(_.getString(0).split("=", 2)(1))

  /** Carga y normaliza columnas de fields_dict para el source. */
  private def loadColumnsVariablesDF(stagingdb: String, source_table: String, value: String)
                                    (implicit spark: SparkSession): DataFrame =
    spark.sqlContext.table(s"$stagingdb.fields_dict")
      .where(col("data_date_part") === value)
      .where(lower(col("src_name")) === source_table)
      .where(lower(col("process")) === "file")
      .select(
        trim(lower(col("fld_name"))).as("fld_name"),
        trim(lower(col("src_fld_header"))).as("src_fld_header")
      ).distinct()

  /** Unión con fields_dict para obtener translated_name. */
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

  /** Condición de join por PK (split por '|'). */
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

  /** Escritura CSV (mismos parámetros). */
  private def writeLogsCsv(result: DataFrame, tmpPath: String): Unit =
    result.coalesce(1).write.format("com.databricks.spark.csv")
      .option("header", "true")
      .option("delimiter", FILE_CSV_DELIMITER.toString)
      .option("nullValue", "")
      .option("parserLib", "univocity")
      .option("escape", "\\")
      .mode("overwrite")
      .option("quoteMode", "NON_NUMERIC")
      .save(tmpPath + "/logs")

  /** Renombrado del único part-*.csv a nombre final. */
  private def renameLogsCsv(fs: org.apache.hadoop.fs.FileSystem, tmpPath: String, finalName: String): Unit = {
    val part    = fs.globStatus(new Path(s"$tmpPath/logs/part*"))(0).getPath.getName
    val rutaCsv = new Path(tmpPath + "/logs/" + part)
    val rutaFile= new Path(tmpPath + "/logs/" + finalName)
    log.info("[SAST] rutaFile: " + rutaFile)
    log.info("[SAST] rutaCsv: " + rutaCsv)
    fs.rename(rutaCsv, rutaFile)
  }

  /** ¿Hay filas sin usar count? */
  private def hasAnyRow(df: DataFrame): Boolean = df.take(1).nonEmpty

  /** Preparación de Subject y Body (manteniendo reemplazos). */
  private def prepareSubjectAndBody(m: MailConfig, env: String, source_table: String,
                                    contextIds: String, timestamps: String): Unit = {
    val notif = new Notification(workspace = "", env = env, sourceTable = source_table,
      stage = "", contextIds = contextIds, timestamps = timestamps, x1 = "", x2 = "", x3 = "")

    if (m.getBody().isEmpty)
      m.setBody(generateBody(notif))
    else
      m.setBody(NotificationUtil.replaceStringWithInfoNotification(m.getBody, notif))

    if (m.getSubject().isEmpty)
      m.setSubject("Validaciones tecnicas o funcionales")
    else
      m.setSubject(NotificationUtil.replaceStringWithInfoNotification(m.getSubject, notif))
  }

  /** Consulta tabla de usuarios notificados por workspace. */
  private def fetchUserEmails(stagingdb: String, workspacesUpper: Array[String])
                             (implicit spark: SparkSession): Seq[String] = {
    if (!HiveUtil.tableExists(stagingdb, "users_stress_test")) return Seq.empty

    spark.sqlContext
      .table(s"$stagingdb.users_stress_test")
      .where(col("user_email").contains("@"))
      .where(trim(upper(col("validation_notification"))) === lit("Y"))
      .where(upper(col(WORKSPACE_COLUMN)).isin(workspacesUpper: _*) || upper(col(WORKSPACE_COLUMN)) === lit("ANY"))
      .select(col("user_email"))
      .distinct()
      .collect()
      .map(r => r.getString(0).trim.toLowerCase)
  }

  // ================= Helpers ya existentes en tu fichero (se usan tal cual) ================
  // getFirstNotEmptyWorkspace, generateBody, addIndex, getIds, isLogVFStopper
  // (No se redefinen aquí; se asume que permanecen igual en el archivo.)

  // Stubs mínimos para que el código compile si no se importan tus clases de notificación.
  // Sustituye por tus implementaciones reales ya presentes en el proyecto.
  case class Notification(workspace: String, env: String, sourceTable: String,
                          stage: String, contextIds: String, timestamps: String,
                          x1: String, x2: String, x3: String)
}
