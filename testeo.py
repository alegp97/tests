%scala
import org.apache.hadoop.fs.{FileSystem, FSDataInputStream, Path}
import java.io.BufferedInputStream
import org.apache.poi.ss.usermodel.{Workbook, WorkbookFactory}

def getFS(): FileSystem = FileSystem.get(spark.sparkContext.hadoopConfiguration)


%scala
def openWorkbookBuffered(fs: FileSystem, path: Path): Workbook = {
  val raw: FSDataInputStream = fs.open(path)
  // usamos try-with-resources “a mano”: cerramos el Workbook en quien lo consuma
  val bin = new BufferedInputStream(raw)
  // si quieres, “marca” amplio para poder hacer resets locales si lees algo antes
  bin.mark(8192)
  val wb = WorkbookFactory.create(bin) // POI puede “asomar” y volver sin problemas
  // OJO: no cierres aquí bin/raw; se cierran cuando cierres wb (POI no cierra los streams)
  wb
}

val fs = getFS()
val inPathStr = "dbfs:/FileStore/contactos.xlsx"   // ajusta tu ruta
val inPath    = new Path(inPathStr)

require(fs.exists(inPath), s"No existe: $inPathStr")

val wb = openWorkbookBuffered(fs, inPath)
println(s"Hojas detectadas: ${(0 until wb.getNumberOfSheets).map(i => wb.getSheetName(i)).mkString(", ")}")
wb.close()




%scala
import java.io.{BufferedInputStream, InputStream}
import org.apache.poi.ss.usermodel.{Workbook, WorkbookFactory}

case class MailEntry(key: String, value: String) // ejemplo; cambia por tu MailsInfo

def obtainJsonConfigDemo(wb: Workbook): Seq[MailEntry] = {
  // DEMO: lee la primera fila como cabecera y la segunda como valores
  val sheet = wb.getSheetAt(0)
  val header = sheet.getRow(0)
  val row1   = sheet.getRow(1)
  if (header == null || row1 == null) return Seq.empty

  val cells = header.getLastCellNum.toInt
  (0 until cells).flatMap { i =>
    val k = Option(header.getCell(i)).map(_.toString).getOrElse(s"col_$i")
    val v = Option(row1.getCell(i)).map(_.toString).getOrElse("")
    Some(MailEntry(k, v))
  }
}

def writeJsonConfigDemo(outputFile: String, entries: Seq[MailEntry]): Unit = {
  import spark.implicits._
  val df = entries.toDF
  df.coalesce(1).write.mode("overwrite").json(outputFile)  // escribe JSON en DBFS/HDFS
}

def convertExcelToJSONBuffered(inputFile: String, outputFile: String): Unit = {
  val fs   = getFS()
  val inP  = new Path(inputFile)
  require(fs.exists(inP), s"Input no existe: $inputFile")

  // abrir con buffer
  val raw = fs.open(inP)
  val bin = new BufferedInputStream(raw)
  bin.mark(1 << 14) // 16 KiB por si lees antes que POI

  // crear workbook
  val wb = WorkbookFactory.create(bin)
  try {
    // === aquí conectarías tus funciones reales ===
    val config = obtainJsonConfigDemo(wb)            // <- reemplaza por obtainJsonConfig(wb)
    writeJsonConfigDemo(outputFile, config)          // <- reemplaza por writeJsonConfig(...)
  } finally {
    wb.close()     // esto cierra internamente, y liberamos buffers
    bin.close()
    raw.close()
  }
}





%scala
val input  = "dbfs:/FileStore/contactos.xlsx"                 // tu Excel
val output = "dbfs:/FileStore/emailconf/contactos_json_out"   // carpeta de salida JSON

convertExcelToJSONBuffered(input, output)
display(spark.read.json(output))






