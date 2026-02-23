
package util

trait DbUtilsWrapper {
  def isAvailable: Boolean

  // añade aquí lo que realmente uses:
  def widgetGet(name: String): String
}



package util

import com.databricks.dbutils_v1.DBUtilsHolder

final class DatabricksDbUtils extends DbUtilsWrapper {

  // OJO: acceder aquí en local puede reventar si no estás en Databricks runtime
  private lazy val dbutils = DBUtilsHolder.dbutils

  override def isAvailable: Boolean =
    try {
      val _ = dbutils // fuerza acceso
      true
    } catch {
      case _: Throwable => false
    }

  override def widgetGet(name: String): String =
    dbutils.widgets.get(name)
}



package jobs

import util.DbUtilsWrapper

final class ProcessingJob(dbutils: DbUtilsWrapper) {

  def run(): Unit = {
    // En vez de assert(dbutils != null), usa:
    require(dbutils.isAvailable, "dbutils no está disponible (no estás en Databricks runtime)")

    val inputPath = dbutils.widgetGet("input_path")
    println(s"input_path = $inputPath")
  }
}
