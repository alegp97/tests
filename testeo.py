private static List<Detail> processDetailRows(Execution execution,
                                              SessionWrapper session,
                                              Connection con,
                                              String query) throws SQLException {

    List<Map<String, Object>> rsDetail =
        JDBCExecutorHandler.getInstance()
                           .launchQueriesAndGetRows(execution, session, con, query,
                                                    /*isCritical*/ true,
                                                    /*isTraceable*/ true,
                                                    JDBCHandler.HIVE);

    logger.info("launchQueriesAndGetRows.generateDetailedTable: rsDetail.isEmpty? {}", rsDetail.isEmpty());

    List<Detail> details = new ArrayList<>();
    for (Map<String, Object> mapRecord : rsDetail) {
        Detail detail = new Detail();
        detail.setColumnaOrigen((String) mapRecord.get(K.COLUMNA_ORIGEN));
        detail.setValorCampo   ((String) mapRecord.get(K.VALOR_CAMPO));
        detail.setMensajeError((String) mapRecord.get(K.MENSAJE_ERROR));
        detail.setSeveridadError((String) mapRecord.get(K.SEVERIDAD_ERROR));
        detail.setValidacion   ((String) mapRecord.get(K.VALIDACION));
        detail.setValores      ((String) mapRecord.get(K.VALORES));
        details.add(detail);
    }
    return details;
}

public static void generateDetailedTable(String sqlStatement,
                                         Table table,
                                         XSSFWorkbook wb,
                                         XSSFSheet sheet,
                                         Execution execution,
                                         SessionWrapper session,
                                         String typeValidation)
                                         throws EconomicResearchException {

    // estilos y anchuras de columna …
    CellStyle styleTitle   = WorkbookStyleManager.getTitleStyle(wb);
    CellStyle styleHeader  = WorkbookStyleManager.getHeaderStyle(wb);
    CellStyle styleRegular = WorkbookStyleManager.getRegularStyle(wb);

    String query = sqlStatement.replace(K.SOURCE_TABLE, table.getNombreTabla());
    logger.info("{} sqlStatement= {}", ERESEARCH_EXCEL_EXTRACTOR, query);

    try (Connection con = JDBCHandler.getConnection(JDBCHandler.HIVE)) {

        /* --- 1) Recuperar datos de detalle --- */
        List<Detail> details = processDetailRows(execution, session, con, query);

        /* --- 2) Escribir título --- */
        int lastRowNum  = sheet.getLastRowNum();
        int currentRow  = lastRowNum + 2;
        Row rowHead     = sheet.createRow(currentRow);
        Cell cellHead   = rowHead.createCell(0);
        cellHead.setCellValue(typeValidation);
        cellHead.setCellStyle(styleTitle);
        sheet.addMergedRegion(new CellRangeAddress(currentRow, currentRow, 0, 1));

        /* --- 3) Cabecera de tabla detalle --- */
        currentRow++;
        XSSFRow row = sheet.createRow(currentRow);
        Cell cell = row.createCell(0); cell.setCellValue(new XSSFRichTextString(K.C_ORIGEN)); cell.setCellStyle(styleHeader);
        cell = row.createCell(1);      cell.setCellValue(new XSSFRichTextString(K.V_CAMPO));  cell.setCellStyle(styleHeader);
        cell = row.createCell(2);      cell.setCellValue(new XSSFRichTextString(K.ID_VALIDACION)); cell.setCellStyle(styleHeader);
        cell = row.createCell(3);      cell.setCellValue(new XSSFRichTextString(K.TIPO_SEVERIDAD)); cell.setCellStyle(styleHeader);
        cell = row.createCell(4);      cell.setCellValue(new XSSFRichTextString(K.ERRORMSG)); cell.setCellStyle(styleHeader);
        cell = row.createCell(5);      cell.setCellValue(new XSSFRichTextString(K.VALORES));  cell.setCellStyle(styleHeader);

        /* --- 4) Rellenar filas --- */
        for (Detail det : details) {
            currentRow++;
            row  = sheet.createRow(currentRow);
            row.createCell(0).setCellValue(det.getColumnaOrigen()); row.getCell(0).setCellStyle(styleRegular);
            row.createCell(1).setCellValue(det.getValorCampo());    row.getCell(1).setCellStyle(styleRegular);
            row.createCell(2).setCellValue(det.getValidacion());    row.getCell(2).setCellStyle(styleRegular);
            row.createCell(3).setCellValue(det.getSeveridadError());row.getCell(3).setCellStyle(styleRegular);
            row.createCell(4).setCellValue(det.getMensajeError());  row.getCell(4).setCellStyle(styleRegular);
            row.createCell(5).setCellValue(det.getValores());       row.getCell(5).setCellStyle(styleRegular);
        }

    } catch (SQLException | RuntimeException e) {
        logger.info("SQLException/Runtime: {}", e.getMessage());
        throw new EconomicResearchException(e.getMessage(), e);
    }
}







                                                                                                          |
| ---------------------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------- |
| **Seguridad SQL** (`procesos`, `bloque`)                                           | Sustituimos concatenaciones vulnerables y uso de `Statement` por **`PreparedStatement`** (place-holders `?`) y obtuvimos la `Connection` vía `st.getConnection()`.                                                                     | · Elimina inyección SQL (S2077) · Cambian firmas internas pero se mantiene la llamada externa (sigue recibiendo `Statement`). |
| **Gestor de estilos Excel** (`WorkbookStyleManager`)                               | • Eliminamos los campos `styleTitle`, `styleHeader`, `styleRegular` y creamos los estilos **en cada llamada**.<br>• Sustituimos colores de `HSSFColor.*` por `IndexedColors.*`.                                                        | · Sin estado global → thread-safe · Se evita uso de APIs deprecadas.                                                          |
| **Excel – generación de tablas** (`generateSummaryTable`, `generateDetailedTable`) | • Extraímos la lógica de consulta+procesado a métodos auxiliares (`processSummaryRows`, `processDetailRows`).<br>• El `try` anidado desaparece; el método principal solo abre/­cierra conexión y delega la lógica.                     | · Jerarquía de `try/catch` simplificada (S1141) · Métodos más cortos y testeables.                                            |
| **Logging & utilidades**                                                           | • Añadimos `private static final Logger logger = LoggerFactory.getLogger(…)` y reemplazamos `System.out/printStackTrace()` por `logger.*`.<br>• Constructor privado en clases utilitarias (`SenderEmailUtil`, `WorkbookStyleManager`). | · Cumple reglas S106 y S1118 · Logging configurable a nivel de framework.                                                     |
| **Tipos genéricos** (`ActionParameters`, `Table`, etc.)                            | Todos los `Map`/`List` raw se parametrizaron con `<String, Object>` y se usó el operador **diamante `<>`**.<br>Se tiparon helpers `cloneMap`, `deepMerge`, `mapFromJson`.                                                              | · Fuera code-smell S3740 · Menos castings y mayor seguridad de tipos.                                                         |
| **Optional**                                                                       | Reemplazamos `com.google.common.base.Optional.fromNullable` por `java.util.Optional.ofNullable`.                                                                                                                                       | · Dependencia innecesaria a Guava eliminada · Cumple regla S3973.                                                             |



