import org.scalatest.funsuite.AnyFunSuite
import java.util.Properties

class CommonsIoVersionSpec extends AnyFunSuite {

  private def implVersion(cls: Class[_]): Option[String] =
    Option(cls.getPackage).flatMap(p => Option(p.getImplementationVersion))

  // Para JARs publicados por Maven: lee META-INF/maven/.../pom.properties (si existe)
  private def mavenPomVersion(cls: Class[_],
                              groupId: String,
                              artifactId: String): Option[String] = {
    val path = s"META-INF/maven/$groupId/$artifactId/pom.properties"
    Option(cls.getClassLoader.getResource(path)).flatMap { url =>
      val props = new Properties()
      val in = url.openStream()
      try {
        props.load(in)
        Option(props.getProperty("version"))
      } finally in.close()
    }
  }

  private def codeSource(cls: Class[_]): String =
    Option(cls.getProtectionDomain).flatMap(pd => Option(pd.getCodeSource))
      .map(_.getLocation.toString).getOrElse("<unknown>")

  test("commons-io version en runtime (original o sombreada)") {
    val expected = sys.props.getOrElse("expected.commons.io", "2.12.0")

    val candidates = Seq(
      "org.apache.commons.io.IOUtils",        // sin shade
      "com.tuorg.shaded.commons.io.IOUtils"   // con shade + relocation (ajusta tu paquete)
    ).flatMap { fqcn =>
      try {
        val c = Class.forName(fqcn)
        Some((fqcn, c))
      } catch { case _: ClassNotFoundException => None }
    }

    assert(candidates.nonEmpty, "No se encontró IOUtils ni original ni sombreada en el classpath.")

    // Log útil para inspección
    candidates.foreach { case (name, c) =>
      val iv  = implVersion(c).getOrElse("<desconocida>")
      val pom = mavenPomVersion(c, "commons-io", "commons-io").getOrElse("<no pom.properties>")
      val src = codeSource(c)
      println(s"$name -> implVersion=$iv ; mavenPomVersion=$pom ; location=$src")
    }

    // Criterio de éxito:
    // - Si es la clase original: debe dar 2.12.0 (o el valor pasado por -Dexpected.commons.io=...)
    // - Si es la clase sombreada: no solemos tener implVersion/pom de commons-io; validamos que
    //   la clase reubicada proviene de TU JAR (por nombre o 'shaded' en la ruta).
    val ok = candidates.exists {
      case (name, c) if name.startsWith("org.apache.commons.io") =>
        val ver = implVersion(c)
          .orElse(mavenPomVersion(c, "commons-io", "commons-io"))
        ver.contains(expected)

      case (name, c) if name.startsWith("com.tuorg.shaded.commons.io") =>
        val src = codeSource(c).toLowerCase
        src.contains("shaded") || src.contains("tu-jar") || src.contains("your-artifact-id")
    }

    assert(ok,
      "La versión de commons-io en runtime no coincide con la esperada o no proviene del JAR sombreado.")
  }
}
