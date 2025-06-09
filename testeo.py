private void procesos(final String data_date_part,
                      final String data_timestamp_part,
                      final String process_table,
                      final String state,
                      final List<String> tables,
                      final Statement st) throws SQLException {

    /* ---------- Sanitización mínima (una línea por parámetro externo) ---------- */
    final String datePartSafe   = StringEscapeUtils.escapeSql(data_date_part);
    final String timePartSafe   = StringEscapeUtils.escapeSql(data_timestamp_part);
    final String feedSafe       = StringEscapeUtils.escapeSql(process_table);
    final String stateSafe      = StringEscapeUtils.escapeSql(state);

    /* ---------- (1) SELECT max(id)  ─ igual que antes pero con vars *_Safe ---------- */
    String queryId = "select max(id) as id "
                   + "from process_sr_cloud "
                   + "where status='0' "
                   + "and data_date_part='"   + datePartSafe + "' "
                   + "and data_timestamp_part='" + timePartSafe + "' "
                   + "and feed='"            + feedSafe + "'";

    ResultSet rs = st.executeQuery(queryId);
    logger.info("[ER] - Executed query to obtain id : {}", queryId);

    int id = 0;
    if (rs.next()) { id = rs.getInt("id"); }

    /* ---------- (2) UPDATE status  ─ solo sanitizar 'state' ---------- */
    String updateStatus = "update process_sr_cloud set status='" + stateSafe
                        + "', ts_end=now() where id='" + id + "'";
    st.execute(updateStatus);
    logger.info("[ER] - Executed query to update status : {}", updateStatus);

    /* ---------- (3) UPDATE por cada tabla  ─ sanitizar 't' ---------- */
    for (String t : tables) {
        String tableSafe = StringEscapeUtils.escapeSql(t);

        String updateQuery = "update ingestas_sr_cloud set id_process_sr='" + id + "' "
                           + "where pid=(select pid from ingestas_sr_cloud "
                           + "where id=(select max(id) from ingestas_sr_cloud "
                           + "where status!='3' and feed='" + tableSafe + "') "
                           + "and id_process_sr is null)";
        st.execute(updateQuery);
        logger.info("[ER] - Executed query to link ingest : {}", updateQuery);
    }
}




private void bloque(final String data_date_part,
                    final String data_timestamp_part,
                    final String state,
                    final List<String> tables,
                    final Statement st) throws SQLException {

    /* ---------- Sanitización mínima ---------- */
    final String datePartSafe  = StringEscapeUtils.escapeSql(data_date_part);
    final String timePartSafe  = StringEscapeUtils.escapeSql(data_timestamp_part);
    final String stateSafe     = StringEscapeUtils.escapeSql(state);

    /* ---------- (1) SELECT max(id) ---------- */
    String queryId = "select max(id) as id "
                   + "from bloques_sr_cloud "
                   + "where status='0' "
                   + "and data_date_part='"   + datePartSafe + "' "
                   + "and data_timestamp_part='" + timePartSafe + "'";

    ResultSet rs = st.executeQuery(queryId);
    logger.info("[ER] - Executed query to obtain id : {}", queryId);

    int id = 0;
    if (rs.next()) { id = rs.getInt("id"); }

    /* ---------- (2) UPDATE status ---------- */
    String updateStatus = "update bloques_sr_cloud set status='" + stateSafe
                        + "', ts_end=now() where id='" + id + "'";
    st.execute(updateStatus);
    logger.info("[ER] - Executed query to update status : {}", updateStatus);

    /* ---------- (3) UPDATE por cada tabla ---------- */
    for (String t : tables) {
        String tableSafe = StringEscapeUtils.escapeSql(t);

        String updateQuery = "update ingestas_sr_cloud set id_bloque_sr='" + id + "' "
                           + "where pid=(select pid from ingestas_sr_cloud "
                           + "where id=(select max(id) from ingestas_sr_cloud "
                           + "where status!='3' and feed='" + tableSafe + "') "
                           + "and id_bloque_sr is null)";
        st.execute(updateQuery);
        logger.info("[ER] - Executed query to link ingest : {}", updateQuery);
    }
}
