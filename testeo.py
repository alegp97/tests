# 1. Inspecciona el contenido
java -jar ~/.m2/repository/org/jacoco/org.jacoco.cli/0.8.11/*.jar \
     execinfo target/jacoco.exec

# 2. Genera el informe en consola, modo verbose
java -jar ~/.m2/repository/org/jacoco/org.jacoco.cli/0.8.11/*.jar \
     report target/jacoco.exec \
     --classfiles target/classes \
     --sourcefiles src/main/java \
     --html target/manual-report -v



java -cp "C:\Users\x068801\.m2\repository\org\jacoco\org.jacoco.cli\0.8.11\org.jacoco.cli-0.8.11.jar" org.jacoco.cli.internal.Main execinfo target/jacoco.exec

<execution>
  <id>jacoco-report</id>
  <phase>verify</phase>
  <goals><goal>report</goal></goals>

  <configuration>
    <!-- 1. carpeta de salida Java normal -->
    <classFiles>
      <param>${project.build.outputDirectory}</param>

      <!-- 2. carpetas Scala (todas las versiones) -->
      <param>${project.build.directory}/scala-*/classes</param>

      <!-- 3. el/los JARs generados por shade -->
      <param>${project.build.directory}/${project.build.finalName}-*.jar</param>
    </classFiles>

    <!-- Si quieres que enlace al código Scala -->
    <sourceFiles>
      <param>src/main/scala</param>
      <param>src/main/java</param>
    </sourceFiles>
  </configuration>
</execution>
