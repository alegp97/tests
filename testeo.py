Esta guía establece cómo trabajaremos en Databricks (SupraCloud Santander) para cualquier dominio de datos (ej.: riesgo, fraude, finanzas, clientes, canal, ESG, etc.). Su meta es que un nuevo ingeniero pueda entender, operar y evolucionar la plataforma con criterios homogéneos de seguridad, calidad y gobierno, independientemente del caso de uso.

Objetivos

Definir el modelo operativo en Databricks: Repos/Git, Jobs, Clusters/SQL Warehouses, DLT, Unity Catalog y CI/CD.

Estandarizar patrones de ingesta, validación, transformación y publicación (bronze–silver–gold).

Alinear naming, permisos, calidad, costes y observabilidad con normas internas.

Describir la integración con orquestación corporativa (p. ej., Control-M/Workflow Manager) y con herramientas de negocio/analítica.

Alcance

Pipelines batch/streaming en Databricks Jobs y/o Delta Live Tables con Unity Catalog.

Consumo analítico mediante SQL Warehouses y data products para distintos equipos.

Prácticas de seguridad, calidad, monitorización y operación 24x7.

Fuera de alcance (v1)

Herramientas de reporting finales (Power BI/Tableau) más allá del acceso a datasets.

Metodologías específicas de modelización estadística (se referencian pero no se detallan).

Procedimientos particulares de unidades fuera del SupraCloud.

Audiencia

Data/ML Engineers, Data Analysts/BI, Data Stewards, Operaciones/Explotación, Seguridad/Compliance, Product Owners.

Beneficios clave de Databricks

Elasticidad y coste bajo demanda (incl. Serverless SQL).

Delta Lake (ACID, time travel) y Unity Catalog (metastore único, permisos finos, lineage).

DLT para calidad declarativa y auto-healing de pipelines.

Observabilidad y versionado Git integrados; automatización vía CI/CD.
