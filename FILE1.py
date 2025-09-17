1) Notebook

Qué hace: Ejecuta un notebook de Databricks.

Cuándo: Prototipos, pipelines ligeros, orquestación con %run, widgets/params.

Entradas: notebook_path, base_parameters (widgets), cluster/warehouse.

Notas: Ideal para pip install in-notebook si no puedes instalar libs a nivel de cluster. Versiona con Repos.

2) Python script

Qué hace: Lanza un .py con Spark (spark_python_task).

Cuándo: Scripts simples empaquetados como archivo suelto.

Entradas: python_file (DBFS/Repo), parameters.

Notas: Menos “dev-ex” que wheel; perfecto si no necesitas packaging.

3) SQL query

Qué hace: Ejecuta una query almacenada en SQL (SQL Warehouse).

Cuándo: ETLs/analíticas puras en SQL, sin Spark cluster.

Entradas: query_id, warehouse_id, parámetros de la query.

Notas: Usa Serverless SQL cuando puedas; control de coste y arranque rápido.

4) SQL file

Qué hace: Ejecuta un archivo .sql (DDL/DML) contra un Warehouse.

Cuándo: Migraciones/seed, tareas largas de SQL versionadas en Git.

Entradas: path del fichero, warehouse_id.

Notas: Útil para infra de datos como código (IaC).

5) SQL alert

Qué hace: Evalúa un alert de Databricks SQL y notifica si se cumple la condición.

Cuándo: Monitorizar SLAs, umbrales, calidad de datos.

Entradas: alert_id, warehouse_id.

Notas: Encadena con tareas de remediación vía dependencias.

Ingestion & Transformation
6) Ingestion pipeline

Qué hace: Orquesta ingesta desde SaaS/DBs a tablas (conectores gestionados).

Cuándo: Captura incremental de orígenes externos y RDBMS.

Entradas: Conexiones/credenciales, mapping a destinos.

Notas: Ideal para CDC y cargas periódicas sin mucho código.

7) ETL Pipeline

Qué hace: Desencadena un pipeline declarativo de transformación (p.ej. DLT).

Cuándo: Curado/orquestación de broncé-silver-gold con calidad.

Entradas: pipeline_id, flags (full refresh, etc.).

Notas: Hereda reglas de calidad y auto-gestiona dependencias.

8) dbt

Qué hace: Ejecuta un proyecto dbt en Databricks.

Cuándo: Modelado SQL con tests, docs y macros dbt.

Entradas: project_directory, commands (ej. dbt run, dbt test), schema, profiles.

Notas: Usa un SQL Warehouse; perfecto para equipos con estándar dbt.

Advanced
9) Run Job

Qué hace: Lanza otro Job del workspace (fan-out/fan-in).

Cuándo: Reutilizar pipelines ya definidos; composiciones jerárquicas.

Entradas: job_id, job_parameters.

Notas: Controla concurrencia del hijo; ojo a bucles de llamadas.

10) If/else condition

Qué hace: Evalúa una condición y redirecciona el flujo.

Cuándo: Branching por entorno/fecha/resultado de tareas.

Entradas: Expresión/operador y ramas if / else.

Notas: Muy útil para cortes de seguridad y caminos alternativos.

11) For each

Qué hace: Repite una tarea anidada para cada elemento de una lista.

Cuándo: Procesar particiones, clientes, regiones, fechas.

Entradas: Lista (literal, param o salida de otra task), task hija parametrizada.

Notas: Controla paralelismo; limita fan-out para no saturar clúster/WH.

12) Python wheel

Qué hace: Ejecuta un entry point de un paquete Python (.whl).

Cuándo: Producción/CI con packaging, dependencias fijas.

Entradas: libraries.whl, package_name, entry_point, parameters.

Notas: Reproducible; perfecto para MLOps/ETL robusto.

13) JAR

Qué hace: Ejecuta una clase main de un JAR Spark.

Cuándo: Pipelines en Scala/Java.

Entradas: main_class_name, parameters, libraries.jar.

Notas: Arranque rápido; muy estable en producción.

14) Spark Submit

Qué hace: Lanza Spark con parámetros de spark-submit.

Cuándo: Flexibilidad máxima (conf flags, jars múltiple, pyfiles…).

Entradas: parameters (tal cual a spark-submit).

Notas: Menos “guardarraíles”; úsalo si JAR/Wheel se te queda corto.

15) Clean Room notebook

Qué hace: Ejecuta un notebook en un Clean Room (data collaboration segura).

Cuándo: Analítica compartida con terceros con controles estrictos.

Entradas: Notebook + políticas del Clean Room.

Notas: Restricciones de salida/logging/joins según políticas.

Dashboards
16) Legacy dashboard

Qué hace: Refresca un dashboard clásico de Databricks SQL y lo envía a suscriptores.

Cuándo: Sigues usando legacy dashboards.

Entradas: dashboard_id, warehouse_id, subscripciones.

Notas: Considera migrar a dashboards modernos.

17) Power BI

Qué hace: Mantiene datasets/semantic models de Power BI al día.

Cuándo: Orquestar refresh desde Databricks.

Entradas: Conexión/credenciales PBI, dataset/workspace.

Notas: Suele requerir Service Principal y permisos en PBI.

18) Dashboard (moderno)

Qué hace: Refresca un dashboard moderno de Databricks y lo distribuye.

Cuándo: Reporting operativo y distribución a stakeholders.

Entradas: dashboard_id, warehouse_id, schedule y notificaciones.

Notas: Integra bien con alerts y parameters.
