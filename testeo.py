private Table buildErrorTable(String tableName,
                              Execution execution,
                              Session session,
                              Connection con,
                              String query) throws Exception {

    Table table_vf = new Table(tableName);

    List<Map<String, Object>> errorRows =
        JDBCExecutorHandler.getInstance()
                            .launchQueriesAndGetRows(execution, session, con, query,
                                                     /*isCritical*/ true, /*isTracing*/ false);

    if (errorRows.isEmpty()) {
        logger.info("LaunchQueriesAndGetRows: errorRows.isEmpty");
    }

    for (Map<String, Object> row : errorRows) {
        String type       = (String) row.get(K.TIPO_ERROR);
        String columnName = (String) row.get(K.COLUMNA_ORIGEN);
        table_vf.addCampo(type, columnName);
    }
    return table_vf;
}



try (Connection con = JDBCHandler.getConnection(JDBCHandler.HIVE)) {
    return buildErrorTable(tableName, execution, session, con, query);
} catch (Exception e) {                         // cubre SQLException + otras
    throw new EconomicResearchException(e.getMessage(), e);
}


