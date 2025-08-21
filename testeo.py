private void executeQuery(String queryId, List<Object> params) throws SQLException {
    // Whitelist de consultas permitidas
    Map<String, String> queries = Map.of(
        "INSERT_EXEC", "INSERT INTO exec_log (user_id, started_at) VALUES (?, ?)",
        "UPDATE_STATE", "UPDATE exec_log SET state=? WHERE id=?",
        "SELECT_LAST", "SELECT id, state FROM exec_log WHERE user_id=? ORDER BY started_at DESC LIMIT 1"
    );

    String sql = queries.get(queryId);
    if (sql == null) {
        throw new SQLException("Query no permitida");
    }

    try (PreparedStatement ps = connection.prepareStatement(sql)) {
        // Bind de parámetros dinámicos
        for (int i = 0; i < params.size(); i++) {
            ps.setObject(i + 1, params.get(i));
        }

        // Diferenciar SELECT de UPDATE/INSERT
        if (sql.trim().toUpperCase().startsWith("SELECT")) {
            try (ResultSet rs = ps.executeQuery()) {
                while (rs.next()) {
                    // Manejo de resultados (ejemplo simple)
                    System.out.println("Row: " + rs.getObject(1));
                }
            }
        } else {
            ps.executeUpdate();
        }
    }
}
