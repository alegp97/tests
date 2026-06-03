Actúa como experto senior en riesgo de crédito, stress testing EBA, reporting regulatorio bancario, arquitectura de datos y procesos end-to-end en entorno Santander.

Necesito que analices y expliques el proceso “Starting Points EBA Stress Test” comparando la situación AS IS 2025 frente al TO BE 2027.

El objetivo no es solo describir el flujo, sino construir una visión clara de:
1. Qué sabemos del proceso actual.
2. Qué no sabemos todavía.
3. Qué hipótesis tenemos.
4. Qué componentes habría que tocar para implementar el cambio 2027.
5. Qué dependencias, riesgos y preguntas abiertas existen.

Contexto funcional disponible:

AS IS 2025:
- El proceso parte principalmente de BDR.
- Desde BDR se generan campos BDR.
- Se ejecuta generación de métricas, TRs y tratamiento de información en fase Post CRM.
- Se incorpora información externa: sectores, CRE, overlays.
- Se usa COREP filtrado para alimentar parte del flujo del resto del grupo.
- Existen parámetros de proyección, incluyendo Moody’s y ficheros Excel.
- Se generan ficheros/intermedios como T0 y CR Projections.
- Se realiza consolidación.
- Se utiliza mapping de campos.
- Se generan salidas SCEN/SECTOR para ECB.
- Hay varios ajustes de usuario en T0, CR Projections y SCEN/SECTOR.

TO BE 2027:
- Se incorpora GRT / MASTER como fuente adicional.
- El input pasa de Campos BDR a Campos BDR + FINREP.
- El tratamiento de información pasa de Post CRM a Pre CRM.
- Se incorpora FINREP como input explícito.
- Se refuerza la integración con Resto Grupo.
- Se mantiene información externa: sectores, CRE, overlays.
- Se mantienen T0, CR Projections, consolidación, mapping de campos y outputs SCEN/SECTOR.
- Persisten ajustes de usuario, pero el flujo parece más integrado.

Detalle adicional del nuevo flujo 2027:
- Inputs: BDR, SDH FINREP MASTER, Stellantis, local data, COREP, FINREP.
- Local data incluye parámetros IFRS9, completitud y reclasificación, NACEs, overlays y flag CRE.
- FINREP incluye provisiones totales, CRE y sectores.
- COREP incluye información de grupo societario y grupo geográfico.
- Se introduce una conciliación con FINREP.
- El flujo incluye copia de tablas, actualización, creación de partición, creación de ventana, ajustes por reglas, marcado de contratos, agrupación y calibrado, agregaciones, ajuste de stocks.
- Se generan nuevos outputs: FINREP y ESG.
- Se mantienen outputs SCEN y SECTOR, con ajustes de forecasting e integridad.
- Aparecen motores o procesos plataformados: Contract Engines, Aggregation Engines, Stress Test DB.
- Aparecen nodos analíticos intermedios.
- Persisten componentes ofimáticos o manuales: Excel, VBA, PWB, ajustes manuales.

Tareas que quiero que realices:

1. Explica el proceso E2E en lenguaje funcional:
   - Desde la ingesta de datos hasta los outputs regulatorios.
   - Diferencia claramente inputs, ejecución y outputs.
   - Explica qué papel juegan BDR, FINREP, COREP, local data, Moody’s, T0, CR Projections, SCEN, SECTOR y ECB.

2. Compara AS IS 2025 vs TO BE 2027:
   - Identifica cambios funcionales.
   - Identifica cambios técnicos.
   - Identifica cambios de datos.
   - Identifica cambios operativos.
   - Identifica cambios de control y gobierno.

3. Construye una matriz “Sabemos / No sabemos / Hipótesis / Acción requerida”.
   Para cada bloque del proceso, indica:
   - Qué sabemos.
   - Qué no sabemos.
   - Qué hipótesis razonable podemos asumir.
   - Qué habría que preguntar o validar.
   - Qué componentes habría que modificar.

4. Identifica qué habría que tocar para implementar el TO BE 2027:
   - Fuentes de datos.
   - Interfaces de carga.
   - Modelo de datos.
   - Reglas de transformación.
   - Reglas de conciliación FINREP.
   - Reglas de marcado de contratos.
   - Ventanas temporales.
   - Particiones.
   - Motores de contrato.
   - Motores de agregación.
   - Cálculo o calibración ECL.
   - Parámetros IFRS9.
   - Parámetros Moody’s.
   - Ajustes de forecasting.
   - Ajustes de integridad.
   - Mapping de campos.
   - Outputs FINREP, ESG, SCEN y SECTOR.
   - Controles de calidad.
   - Trazabilidad y auditoría.
   - Dependencias con Resto Grupo y ECB.

5. Identifica riesgos principales:
   - Riesgo de conciliación BDR vs FINREP.
   - Riesgo de doble conteo o pérdida de contratos.
   - Riesgo de inconsistencia entre Post CRM y Pre CRM.
   - Riesgo de dependencia de Excel/VBA/PWB.
   - Riesgo de ajustes manuales no trazables.
   - Riesgo de mappings incompletos.
   - Riesgo de falta de gobierno sobre overlays, CRE, sectores y NACEs.
   - Riesgo de descuadres entre SCEN/SECTOR y FINREP.
   - Riesgo de impacto en calendario EBA Stress Test.

6. Genera un listado de preguntas abiertas para negocio, riesgo, finanzas, tecnología y reporting regulatorio.
   Organiza las preguntas por bloque:
   - Inputs.
   - Pre CRM.
   - FINREP/COREP.
   - Conciliación.
   - Motores.
   - Forecasting.
   - Outputs.
   - Controles.
   - Operación.
   - Gobierno del dato.

7. Propón una estructura de documentación funcional:
   - Visión ejecutiva.
   - Alcance.
   - Proceso AS IS.
   - Proceso TO BE.
   - Diferencias.
   - Diccionario de datos.
   - Reglas de negocio.
   - Reglas de conciliación.
   - Controles.
   - Outputs.
   - Dependencias.
   - Riesgos.
   - Preguntas abiertas.
   - Plan de implementación.
   - Plan de pruebas.

8. Entrega el resultado con formato claro y accionable:
   - Usa tablas cuando ayuden.
   - Separa lo confirmado de lo inferido.
   - Marca explícitamente los puntos donde falte información.
   - No inventes reglas técnicas concretas si no están descritas.
   - Cuando propongas hipótesis, márcalas como hipótesis.
   - El tono debe ser profesional, ejecutivo y orientado a decisión.

Resultado esperado:
Una explicación completa del proceso Starting Points EBA Stress Test 2027, orientada a entender qué sabemos, qué no sabemos y qué componentes habría que modificar para pasar del AS IS 2025 al TO BE 2027.
