private static void processSummaryRows(XSSFSheet sheet,
                                       Table table,
                                       Execution execution,
                                       SessionWrapper session,
                                       Connection con,
                                       String query) throws SQLException {

    List<Map<String, Object>> resultRows =
        JDBCExecutorHandler.getInstance()
                           .launchQueriesAndGetRows(execution, session, con, query,
                                                    /*isCritical*/ true,
                                                    /*isTraceable*/ true,
                                                    JDBCHandler.HIVE);

    if (resultRows.isEmpty()) {
        logger.info("LaunchQueriesAndGetRows: resultRows.isEmpty");
        return;
    }

    // --- título ---
    Row rowHead = sheet.createRow((short) 0);
    Cell cell = rowHead.createCell((short) 0);
    for (Map<String,Object> mapRecord : resultRows) {
        String fileName = (String) mapRecord.get(K.FILE_NAME);
        cell.setCellValue("Validación de " + table.getNombreTabla() + " - Fichero: " + fileName);
    }
    cell.setCellStyle(WorkbookStyleManager.getTitleStyle(con.unwrap(XSSFWorkbook.class)));
    sheet.addMergedRegion(new CellRangeAddress(0, 0, 0, 1));

    // --- cabecera ---
    XSSFRow row = sheet.createRow(1);
    cell = row.createCell(0);
    cell.setCellValue(new XSSFRichTextString(K.T_ERROR));
    cell.setCellStyle(WorkbookStyleManager.getHeaderStyle(con.unwrap(XSSFWorkbook.class)));
    cell = row.createCell(1);
    cell.setCellValue(new XSSFRichTextString(K.CAMPO_COMPROBADO));
    cell.setCellStyle(WorkbookStyleManager.getHeaderStyle(con.unwrap(XSSFWorkbook.class)));

    // --- filas ---
    List<Campo> columnas = table.getColumnas();
    int cont = 1;
    for (Campo entry : columnas) {
        cont++;
        row = sheet.createRow(cont);
        cell = row.createCell(0);
        cell.setCellValue(entry.getType());
        cell.setCellStyle(WorkbookStyleManager.getRegularStyle(con.unwrap(XSSFWorkbook.class)));

        cell = row.createCell(1);
        cell.setCellValue(entry.getField());
        cell.setCellStyle(WorkbookStyleManager.getRegularStyle(con.unwrap(XSSFWorkbook.class)));

        logger.info("columna {} : type-> {}, field-> {}", cont, entry.getType(), entry.getField());
    }
}



public static void generateSummaryTable(Table table,
                                        XSSFWorkbook wb,
                                        XSSFSheet sheet,
                                        Execution execution,
                                        SessionWrapper session,
                                        String sqlStatementPhysicalFileName)
                                        throws EconomicResearchException {

    // Ajustes de ancho/estilos…
    /* … */

    String query = sqlStatementPhysicalFileName.replace(K.SOURCE_TABLE, table.getNombreTabla());
    logger.info("{} sqlStatementPhysicalFileName= {}", ERESEARCH_EXCEL_EXTRACTOR, query);

    try (Connection con = JDBCHandler.getConnection(JDBCHandler.HIVE)) {
        processSummaryRows(sheet, table, execution, session, con, query);
    } catch (SQLException | RuntimeException e) {
        throw new EconomicResearchException(e.getMessage(), e);
    }
}


