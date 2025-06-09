private void procesos(final String data_date_part,
                      final String data_timestamp_part,
                      final String process_table,
                      final String state,
                      final List<String> tables,
                      final Statement st) throws SQLException {

    Connection conn = st.getConnection();               // ← única línea nueva antes del primer SQL

    /* ---------- 1) SELECT max(id) ---------- */
    final String selSql =
        "SELECT max(id) AS id "
      + "FROM process_sr_cloud "
      + "WHERE status = ? "
      + "AND data_date_part = ? "
      + "AND data_timestamp_part = ? "
      + "AND feed = ?";

    int id = 0;
    try (PreparedStatement psSel = conn.prepareStatement(selSql)) {
        psSel.setString(1, "0");
        psSel.setString(2, data_date_part);
        psSel.setString(3, data_timestamp_part);
        psSel.setString(4, process_table);

        try (ResultSet rs = psSel.executeQuery()) {
            while (rs.next()) { id = rs.getInt("id"); }
        }
    }

    /* ---------- 2) UPDATE status ---------- */
    final String updStatusSql =
        "UPDATE process_sr_cloud "
      + "SET status = ?, ts_end = now() "
      + "WHERE id = ?";

    try (PreparedStatement psUpd = conn.prepareStatement(updStatusSql)) {
        psUpd.setString(1, state);
        psUpd.setInt(2, id);
        psUpd.executeUpdate();
    }

    /* ---------- 3) UPDATE ingestas_sr_cloud para cada tabla ---------- */
    final String updIngestSql =
        "UPDATE ingestas_sr_cloud SET id_process_sr = ? "
      + "WHERE pid = ( "
      +   "SELECT pid FROM ingestas_sr_cloud "
      +   "WHERE id = (SELECT max(id) "
      +                "FROM ingestas_sr_cloud "
      +                "WHERE status != '3' AND feed = ?) "
      +   "AND id_process_sr IS NULL)";

    try (PreparedStatement psIngest = conn.prepareStatement(updIngestSql)) {
        psIngest.setInt(1, id);
        for (String t : tables) {
            psIngest.setString(2, t);
            psIngest.executeUpdate();
        }
    }
}





private void bloque(final String data_date_part,
                    final String data_timestamp_part,
                    final String state,
                    final List<String> tables,
                    final Statement st) throws SQLException {

    Connection conn = st.getConnection();               // ← única línea nueva

    /* ---------- 1) SELECT max(id) ---------- */
    final String selSql =
        "SELECT max(id) AS id "
      + "FROM bloques_sr_cloud "
      + "WHERE status = ? "
      + "AND data_date_part = ? "
      + "AND data_timestamp_part = ?";

    int id = 0;
    try (PreparedStatement psSel = conn.prepareStatement(selSql)) {
        psSel.setString(1, "0");
        psSel.setString(2, data_date_part);
        psSel.setString(3, data_timestamp_part);

        try (ResultSet rs = psSel.executeQuery()) {
            while (rs.next()) { id = rs.getInt("id"); }
        }
    }

    /* ---------- 2) UPDATE status ---------- */
    final String updStatusSql =
        "UPDATE bloques_sr_cloud "
      + "SET status = ?, ts_end = now() "
      + "WHERE id = ?";

    try (PreparedStatement psUpd = conn.prepareStatement(updStatusSql)) {
        psUpd.setString(1, state);
        psUpd.setInt(2, id);
        psUpd.executeUpdate();
    }

    /* ---------- 3) UPDATE ingestas_sr_cloud para cada tabla ---------- */
    final String updIngestSql =
        "UPDATE ingestas_sr_cloud SET id_bloque_sr = ? "
      + "WHERE pid = ( "
      +   "SELECT pid FROM ingestas_sr_cloud "
      +   "WHERE id = (SELECT max(id) "
      +                "FROM ingestas_sr_cloud "
      +                "WHERE status != '3' AND feed = ?) "
      +   "AND id_bloque_sr IS NULL)";

    try (PreparedStatement psIngest = conn.prepareStatement(updIngestSql)) {
        psIngest.setInt(1, id);
        for (String t : tables) {
            psIngest.setString(2, t);
            psIngest.executeUpdate();
        }
    }
}

