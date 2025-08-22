private void okDataDatePart(List<IngestEntity> dataDatePartToProcess) throws SQLException {
    logger.error(" - [SAST] - PROCESS " + processName + " OK ");

    final String sql =
        "UPDATE executions_stress_cloud " +
        "SET status=1, ts_end=now() " +
        "WHERE layer='business' AND process_name=? " +
        "  AND data_date_part=? AND data_timestamp_part=? " +
        "  AND feed=? AND source=? " +
        "  AND ts_start=(SELECT MAX(ts_start) FROM executions_stress_cloud " +
        "               WHERE data_date_part=? AND layer='business' AND process_name=?)";

    try (PreparedStatement ps = connection.prepareStatement(sql)) {
        for (IngestEntity info : dataDatePartToProcess) {
            int i = 1;
            ps.setString(i++, processName);
            ps.setString(i++, info.getDataDatePart());
            ps.setString(i++, info.getDataTimestampPart());
            ps.setString(i++, feedDest);
            ps.setString(i++, source);
            ps.setString(i++, info.getDataDatePart()); // subquery
            ps.setString(i++, processName);            // subquery
            ps.executeUpdate();
        }
    }
}





private void errorDataDatePart(List<IngestEntity> dataDatePartToProcess) throws SQLException {
    logger.error(" - [SAST] - PROCESS KO ");

    if (dataDatePartToProcess.isEmpty()) return;

    final String sql =
        "UPDATE executions_stress_cloud " +
        "SET status=2, ts_end=now() " +
        "WHERE layer='business' AND process_name=? " +
        "  AND data_date_part=? AND data_timestamp_part=? " +
        "  AND feed=? AND source=? " +
        "  AND ts_start=(SELECT MAX(ts_start) FROM executions_stress_cloud " +
        "               WHERE data_date_part=? AND layer='business' AND process_name=?)";

    try (PreparedStatement ps = connection.prepareStatement(sql)) {
        for (IngestEntity it : dataDatePartToProcess) {
            int i = 1;
            ps.setString(i++, processName);
            ps.setString(i++, it.getDataDatePart());
            ps.setString(i++, it.getDataTimestampPart());
            ps.setString(i++, feedDest);
            ps.setString(i++, source);
            ps.setString(i++, it.getDataDatePart()); // subquery
            ps.setString(i++, processName);          // subquery
            logger.error(" - [SAST] - Executing update (KO)");
            ps.executeUpdate();
        }
    }
}



private void runningDataDatePart(final Execution execution,
                                 final List<IngestEntity> dataDatePartToProcess) throws SQLException {
    if (dataDatePartToProcess.isEmpty()) return;

    final String sql =
        "INSERT INTO executions_stress_cloud " +
        "(id_execution_supra, ts_start, data_date_part, data_timestamp_part, layer, " +
        " process_name, \"scope\", sandbox, \"source\", feed, status, row_count) " +
        "VALUES (?, now(), ?, ?, 'business', ?, ?, ?, ?, ?, 0, ?)";

    try (PreparedStatement ps = connection.prepareStatement(sql)) {
        for (IngestEntity it : dataDatePartToProcess) {
            int i = 1;
            ps.setString(i++, String.valueOf(execution.getIdExecution()));
            ps.setString(i++, it.getDataDatePart());
            ps.setString(i++, it.getDataTimestampPart());
            ps.setString(i++, processName);
            ps.setString(i++, scope);
            ps.setString(i++, sandbox);
            ps.setString(i++, source);
            ps.setString(i++, feedDest);
            ps.setInt(i++, it.getRowCount());
            ps.addBatch();
        }
        logger.error(" - [SAST] - Executing insert batch");
        ps.executeBatch();
    }
}



