¡Vamos! Desglose **sección a sección** de la configuración de un **Databricks Job** (como en tu captura). Incluyo qué es, por qué importa y trucos prácticos.



 En Databricks puedes poner thresholds sobre dos tipos de métricas: Run duration, que controla el tiempo total de cada ejecución (útil para detectar jobs que se alargan); y métricas de streaming backlog, que miden la cola pendiente de consumo desde distintos ángulos: bytes (volumen), duration (latencia entre llegada y consumo), files (nº de ficheros sin procesar) y records (nº de eventos pendientes). Define umbrales con histórico (p. ej., P95) y notifica solo a quien corresponda; combinar volumen (bytes/records) con latencia (duration) suele dar la alerta más útil.


# Job details

* **Job ID:** Identificador único e inmutable del job. Útil para logs, API y auditoría. Guárdalo cuando integres con CI/CD o tickets.
* **Creator:** Quién lo creó. No determina quién lo ejecuta (para eso está *Run as*).
* **Run as:** Identidad con la que **se ejecutan todas las tasks** del job. Afecta permisos de lectura/escritura (Unity Catalog, external locations, secrets, warehouses) y la **atribución de lineage**.

  * Recomendado: usar un **Service Principal** dedicado de producción para evitar roturas por bajas de usuarios y para tener permisos mínimos.
* **Description:** Texto libre. Aprovéchalo para: propósito, inputs/outputs, owner, SLO, enlace al repo/PR y a runbooks.

# Schedule

* **Add trigger:** Programa el job.

  * **Tipos comunes:** horario fijo (cada X min/h/h/d), **CRON** (flexible), *one-time* (única vez).
  * **Zona horaria:** fija la TZ del disparo; no dependas del huso del navegador.
  * **Pausa/Resume:** puedes desactivar el trigger sin borrar la regla.
  * **Buenas prácticas:** documenta ventana de ejecución y dependencias aguas arriba; evita solapamiento ajustando concurrencia (ver más abajo).

# Job parameters

* **Qué son:** Pares clave/valor que el job pasa a las tasks.
* **Cómo llegan a tu código:**

  * **Notebook:** via `base_parameters` → `dbutils.widgets.get("param")`.
  * **Python script:** como `argv` (`sys.argv`).
  * **JAR/Scala:** como argumentos del `main`.
  * **SQL (query/file):** variables/parameters del engine (y placeholders según tu plantilla).
* **Defaults y validación:** pon valores por defecto seguros (fecha de proceso, rutas “/bronze/yyyy-MM-dd”, flags). Valida al inicio y falla rápido con mensajes claros.
* **Secrets:** puedes referenciar secretos (p. ej., `{{secrets/scope/key}}`) para no exponer credenciales en claro.

# Tags

* **Para qué:** Costeo, trazabilidad y políticas. Se propagan al clúster/warehouse.
* **Sugeridos:** `env=prod|pre|dev`, `owner=equipo`, `system=dominio`, `cost_center=…`, `criticality=…`.
  Algunas **cluster policies** exigen tags: úsalas de forma consistente.

# Job notifications

* **Destinos:** normalmente email; si tu admin configuró **destinations** (webhooks/Teams/Slack), también puedes usarlos.
* **Eventos:** on start, on success, on failure, on retry, on timeout.
* **Consejo:** en fallos, incluye *run page link*, último error y *owner*; evita spam (no notifiques “success” de jobs muy frecuentes).

## Duration and streaming backlog thresholds

* **Duration threshold:** alerta si una ejecución supera X minutos. Útil para detectar cuellos de botella o colas por falta de recursos.
* **Streaming backlog threshold:** para tareas **streaming** (Auto Loader/Structured Streaming). Avisa si el backlog supera un número de archivos/mensajes/latencia. Evita acumulaciones silenciosas.
* **Tip:** define estos umbrales con datos históricos (p. ej., P95 de duración + 20–30%).

# Permissions

* **Roles típicos:**

  * **Owner:** control total (editar, borrar, permisos, *Run as*).
  * **Can Manage:** editar job/tareas, pero no siempre permisos del propio job.
  * **Can Run:** puede lanzar/reenlanzar, ver runs y logs, sin editar definición.
  * **Can View:** solo lectura (definición y runs).
* **Principio de mínimo privilegio:** da *Can Run* a operaciones, *Can Manage* al equipo de datos, *Owner* a la cuenta de servicio.
* **Dato fino:** los **permisos de datos** se evalúan con la identidad de **Run as**, no con quien pulsa “Run”.

# Advanced settings

* **Queue (ON/OFF):**

  * **ON:** si se dispara otra ejecución y ya alcanzaste el límite de concurrencia, **la nueva se encola** y correrá cuando haya hueco.
  * **OFF:** si llega un disparo y ya estás al máximo, la nueva **no se programa** (se descarta/“skipped”).
  * **Recomendación:** ON para cargas periódicas críticas; OFF para jobs ad-hoc donde no quieres colas.
* **Maximum concurrent runs:** cuántas ejecuciones de **este job** pueden correr **en paralelo**.

  * Sube el valor si tu pipeline es **idempotente** y tus recursos (clúster/WH) lo soportan.
  * Déjalo en **1** si hay riesgo de **carreras** (p. ej., sobreescritura de mismas rutas o particiones).
* **Cómo encaja con Schedule:** si programas cada 5 minutos, pero tu job tarda 15 y `concurrent_runs=1` con **Queue ON**, tendrás hasta 3 en cola; con **Queue OFF**, saltarás disparos intermedios.

---

## Mini-checklist recomendado

1. **Run as** = service principal de prod con permisos mínimos necesarios (catálogos, ubicaciones, secrets, warehouses).
2. **Schedule** con TZ explícita y ventana conocida; evita solapes.
3. **Parameters** con defaults seguros + validación temprana.
4. **Tags** para costeo/propósito/entorno.
5. **Notifications**: failure + duration threshold; evita notificar éxitos de alta frecuencia.
6. **Concurrency**: 1 si no es idempotente; >1 si lo es y necesitas throughput. **Queue ON** para no perder disparos.
7. **Permissions**: mínimo necesario por rol; documenta *owner* y runbook en **Description**.

Si quieres, hacemos un ejemplo concreto (nocturno ETL diario + umbrales + concurrencia) con textos exactos para cada campo y valores realistas.
