1. **Notebook:** Ejecuta un cuaderno de Databricks. Ideal para pruebas y tareas sencillas; puedes pasarle parámetros y hasta instalar librerías dentro del propio notebook.

2. **Python script:** Corre un archivo `.py` tal cual. Útil para scripts simples sin necesidad de empaquetar nada.

3. **SQL query:** Lanza una consulta SQL guardada sobre un SQL Warehouse. Perfecto cuando todo se resuelve con SQL (sin clúster Spark).

4. **SQL file:** Ejecuta un archivo `.sql` completo (creación/modificación de tablas, cargas). Bueno para cambios versionados en Git.

5. **SQL alert:** Comprueba una alerta SQL y avisa si se cumple una condición. Sirve para vigilar SLAs o calidad de datos.

6. **Ingestion pipeline:** Asistente para traer datos desde apps y bases de datos a tablas en Databricks. Muy útil para cargas periódicas e incrementales (CDC).

7. **ETL Pipeline:** Desencadena un pipeline declarativo (p. ej., Delta Live Tables) que transforma datos y aplica reglas de calidad automáticamente.

8. **dbt:** Ejecuta un proyecto **dbt** (modelos y tests en SQL) sobre Databricks. Ideal si tu equipo ya trabaja con dbt.

9. **Run Job:** Lanza otro **Job** existente. Útil para encadenar procesos o reutilizar pipelines.

10. **If/else condition:** Toma decisiones en el flujo (rama “si” / “si no”) según una condición sencilla, como el entorno o la fecha.

11. **For each:** Repite una tarea para una lista de elementos (clientes, fechas, regiones) con control del paralelismo.

12. **Python wheel:** Ejecuta la entrada de un paquete Python empaquetado (`.whl`). Enfoque profesional y reproducible para producción.

13. **JAR:** Corre una clase `main` de un **JAR** (Scala/Java) en Spark. Robusto y muy usado en entornos productivos.

14. **Spark Submit:** Lanza Spark con parámetros avanzados como en `spark-submit`. Máxima flexibilidad cuando lo estándar no basta.

15. **Clean Room notebook:** Ejecuta un notebook dentro de un entorno de colaboración seguro (Clean Room), con restricciones de acceso y salida.

16. **Legacy dashboard:** Refresca un panel “clásico” de Databricks SQL y lo envía a los suscriptores. Útil si aún usas dashboards antiguos.

17. **Power BI:** Orquesta la actualización de datasets/modelos semánticos de Power BI desde Databricks de forma automática.

18. **Dashboard (moderno):** Refresca y distribuye un dashboard moderno de Databricks con programación y notificaciones integradas.
