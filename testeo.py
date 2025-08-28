import org.scalatest.funsuite.AnyFunSuite
import java.io.File
import java.util.jar.JarFile

class ExcelUtilClasspathSpec extends AnyFunSuite {

  private def jarInfo(c: Class[_]): (String, String) = {
    val loc  = Option(c.getProtectionDomain.getCodeSource).map(_.getLocation).get
    val file = new File(loc.toURI)
    val ver =
      if (file.isFile && file.getName.endsWith(".jar")) {
        val jf = new JarFile(file)
        try {
          val attrs = jf.getManifest.getMainAttributes
          Option(attrs.getValue("Implementation-Version"))
            .orElse(Option(attrs.getValue("Bundle-Version")))
            .getOrElse("UNKNOWN")
        } finally jf.close()
      } else "CLASSES-DIR"
    (file.getName, ver)
  }

  test("ExcelUtil usa POI 5.2.0 y XMLBeans 5.0.2") {
    // Fuerza la carga de clases de POI que usa ExcelUtil
    new org.apache.poi.xssf.streaming.SXSSFWorkbook().dispose()

    val (poiJar, poiVer) =
      jarInfo(classOf[org.apache.poi.xssf.usermodel.XSSFWorkbook])
    val (xmlbeansJar, xmlbeansVer) =
      jarInfo(classOf[org.apache.xmlbeans.XmlOptions])

    println(s"[DIAG] POI   -> $poiJar (Manifest: $poiVer)")
    println(s"[DIAG] XBeans-> $xmlbeansJar (Manifest: $xmlbeansVer)")

    // Comprueba por nombre de JAR o por versión del manifest
    assert(poiJar.contains("poi-ooxml-5.2.0") || poiVer.startsWith("5.2.0"))
    assert(xmlbeansJar.contains("xmlbeans-5.0.2") || xmlbeansVer.startsWith("5.0.2"))

    // Extra: garantiza que es XMLBeans 5.x (método ausente en 2.6.0)
    val hasPut = classOf[org.apache.xmlbeans.XmlOptions]
      .getMethods.exists(m => m.getName == "put" && m.getParameterCount == 2)
    assert(hasPut, "XmlOptions debe tener put(Object,Object) ⇒ xmlbeans 5.x")
  }
}
