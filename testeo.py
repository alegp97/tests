@SuppressWarnings("SqlSourceToSinkFlow") // Justificado: validación estricta + parámetros
private Object executeSqlSingle(
        Connection connection,
        String sql,
        List<Object> params,
        boolean allowWrite,
        int maxRows
) throws SQLException {

    if (connection == null) throw new SQLException("Connection is null");
    if (sql == null || sql.isBlank()) throw new SQLException("Empty SQL");

    final String trimmed = sql.trim();
    final String U = trimmed.toUpperCase(java.util.Locale.ROOT);

    // 1) Bloquear patrones peligrosos (sin whitelist de consultas, pero con “gates”)
    if (U.contains(";")) throw new SQLException("Multiple statements not allowed");
    if (U.contains("--") || U.contains("/*")) throw new SQLException("SQL comments not allowed");
    if (U.matches(".*\\b(CREATE|ALTER|DROP|TRUNCATE|GRANT|REVOKE|EXEC|EXECUTE|CALL)\\b.*"))
        throw new SQLException("DDL/DCL/EXEC not allowed");

    final boolean isSelect = U.startsWith("SELECT") || U.startsWith("WITH");
    if (!allowWrite && !isSelect) throw new SQLException("Write queries not allowed in read-only mode");

    // 2) Asegurar uso de parámetros: nº de '?' == nº de params
    final long placeholders = trimmed.chars().filter(ch -> ch == '?').count();
    if (placeholders != params.size())
        throw new SQLException("Parameter count mismatch: expected " + placeholders + " got " + params.size());

    // 3) Ejecutar con PreparedStatement y límites seguros
    try (PreparedStatement ps = connection.prepareStatement(trimmed)) {

        if (isSelect) {
            if (maxRows > 0) ps.setMaxRows(maxRows);
            if (maxRows > 0) ps.setFetchSize(Math.min(maxRows, 1000));
            connection.setReadOnly(true);
        } else {
            connection.setReadOnly(false);
        }

        for (int i = 0; i < params.size(); i++) {
            ps.setObject(i + 1, params.get(i));
        }

        if (isSelect) {
            try (ResultSet rs = ps.executeQuery()) {
                final java.util.List<java.util.Map<String, Object>> rows = new java.util.ArrayList<>();
                final int colCount = rs.getMetaData().getColumnCount();
                while (rs.next()) {
                    final java.util.Map<String, Object> row = new java.util.LinkedHashMap<>();
                    for (int c = 1; c <= colCount; c++) {
                        row.put(rs.getMetaData().getColumnLabel(c), rs.getObject(c));
                    }
                    rows.add(row);
                }
                return rows;                  // ← SELECT → lista de filas (mapa)
            }
        } else {
            final int updated = ps.executeUpdate();
            return Integer.valueOf(updated);  // ← DML → nº de filas afectadas
        }
    }
}
