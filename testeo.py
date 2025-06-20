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

