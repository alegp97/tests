private void procesos(final String data_date_part, final String data_timestamp_part,
                      final String process_table, final String state,
                      final List<String> tables, final Connection conn) throws SQLException {

    String query = "SELECT max(id) as id FROM process_sr_cloud " +
                   "WHERE status = ? AND data_date_part = ? AND data_timestamp_part = ? AND feed = ?";
    try (PreparedStatement ps = conn.prepareStatement(query)) {
        ps.setString(1, "0");
        ps.setString(2, data_date_part);
        ps.setString(3, data_timestamp_part);
        ps.setString(4, process_table);

        try (ResultSet rs = ps.executeQuery()) {
            logger.info("[ER] - Executed query to obtain id : " + query);

            int id = 0;
            if (rs.next()) {
                id = rs.getInt("id");
            }

            String updateStatusSql = "UPDATE process_sr_cloud SET status = ?, ts_end = now() WHERE id = ?";
            try (PreparedStatement updatePs = conn.prepareStatement(updateStatusSql)) {
                updatePs.setString(1, state);
                updatePs.setInt(2, id);
                updatePs.executeUpdate();
                logger.info("[ER] - Executed query to update status");
            }

            String updateIngestSql = "UPDATE ingestas_sr_cloud SET id_process_sr = ? " +
                                     "WHERE pid = (SELECT pid FROM ingestas_sr_cloud " +
                                     "WHERE id = (SELECT max(id) FROM ingestas_sr_cloud WHERE status != '3' AND feed = ?) " +
                                     "AND id_process_sr IS NULL)";

            for (String t : tables) {
                try (PreparedStatement updateIngestPs = conn.prepareStatement(updateIngestSql)) {
                    updateIngestPs.setInt(1, id);
                    updateIngestPs.setString(2, t);
                    updateIngestPs.executeUpdate();
                    logger.info("[ER] - Executed query to update ingest process id for table: " + t);
                }
            }
        }
    } catch (SQLException e) {
        logger.error(e.getMessage(), e);
        throw e;
    }
}












private void bloque(final String data_date_part, final String data_timestamp_part,
                    final String state, final List<String> tables, final Connection conn) throws SQLException {

    String query = "SELECT max(id) as id FROM bloques_sr_cloud " +
                   "WHERE status = ? AND data_date_part = ? AND data_timestamp_part = ?";
    try (PreparedStatement ps = conn.prepareStatement(query)) {
        ps.setString(1, "0");
        ps.setString(2, data_date_part);
        ps.setString(3, data_timestamp_part);

        try (ResultSet rs = ps.executeQuery()) {
            logger.info("[ER] - Executed query to obtain id : " + query);

            int id = 0;
            if (rs.next()) {
                id = rs.getInt("id");
            }

            String updateStatusSql = "UPDATE bloques_sr_cloud SET status = ?, ts_end = now() WHERE id = ?";
            try (PreparedStatement updatePs = conn.prepareStatement(updateStatusSql)) {
                updatePs.setString(1, state);
                updatePs.setInt(2, id);
                updatePs.executeUpdate();
                logger.info("[ER] - Executed query to update status");
            }

            String updateIngestSql = "UPDATE ingestas_sr_cloud SET id_bloque_sr = ? " +
                                     "WHERE pid = (SELECT pid FROM ingestas_sr_cloud " +
                                     "WHERE id = (SELECT max(id) FROM ingestas_sr_cloud WHERE status != '3' AND feed = ?) " +
                                     "AND id_bloque_sr IS NULL)";

            for (String t : tables) {
                try (PreparedStatement updateIngestPs = conn.prepareStatement(updateIngestSql)) {
                    updateIngestPs.setInt(1, id);
                    updateIngestPs.setString(2, t);
                    updateIngestPs.executeUpdate();
                    logger.info("[ER] - Executed query to update ingest process id for table: " + t);
                }
            }
        }
    } catch (SQLException e) {
        logger.error(e.getMessage(), e);
        throw e;
    }
}

