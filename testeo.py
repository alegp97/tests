object LogFileProcess {
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
