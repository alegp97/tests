private void executeQuery(final String sql) throws SQLException {
    if (connection == null) throw new SQLException("Connection is null");
    if (sql == null || sql.isBlank()) throw new SQLException("Empty SQL");

    final String sanitized = sanitizeSql(sql);      // ← valida y normaliza, lanza excepción si huele mal
    final String U = sanitized.toUpperCase(java.util.Locale.ROOT);
    final boolean isSelect = U.startsWith("SELECT") || U.startsWith("WITH");

    // Por defecto, modo lectura (cámbialo sólo si realmente necesitas escribir)
    final boolean ALLOW_WRITE = false;
    if (!ALLOW_WRITE && !isSelect) {
        throw new SQLException("Write queries are not allowed in read-only mode");
    }

    try (PreparedStatement ps = connection.prepareStatement(sanitized)) {
        if (isSelect) {
            connection.setReadOnly(true);
            ps.setMaxRows(1000);
            ps.setFetchSize(1000);
            try (ResultSet rs = ps.executeQuery()) {
                // consume/usa el ResultSet según tu lógica
                while (rs.next()) {
                    // ...
                }
            }
        } else {
            connection.setReadOnly(false);
            ps.executeUpdate();
        }
    }
}

/**
 * Valida el SQL recibido (sin whitelist), bloqueando patrones de inyección comunes.
 * Si todo es correcto, devuelve la misma cadena (o una versión normalizada).
 */
private static String sanitizeSql(String sql) throws SQLException {
    final String s = sql.trim();

    // 1) Prohibir múltiples sentencias y comentarios
    if (s.indexOf(';') >= 0) throw new SQLException("Multiple statements not allowed");
    if (s.contains("--") || s.contains("/*") || s.contains("*/"))
        throw new SQLException("SQL comments not allowed");

    final String U = s.toUpperCase(java.util.Locale.ROOT);

    // 2) Bloquear DDL/DCL/EXEC y otras puertas de ataque
    if (U.matches(".*\\b(CREATE|ALTER|DROP|TRUNCATE|GRANT|REVOKE|EXEC|EXECUTE|CALL)\\b.*"))
        throw new SQLException("DDL/DCL/EXEC not allowed");
    if (U.matches(".*\\b(UNION\\s+ALL|UNION\\s+SELECT|INFORMATION_SCHEMA|PG_).*"))
        throw new SQLException("UNION/metadata not allowed");

    // 3) Evitar literales sospechosos: comillas sueltas o hex/bin inyectables
    //    (si necesitas literales legítimos, refina esta regla para tu caso)
    if (U.contains("'") || U.contains("\""))
        throw new SQLException("Literals are not allowed in ad-hoc SQL");
    if (U.matches(".*0x[0-9A-F]+.*") || U.matches(".*\\bX'[0-9A-F]+'\\b.*"))
        throw new SQLException("Hex literals not allowed");

    // 4) Limitar a verbos soportados
    if (!(U.startsWith("SELECT") || U.startsWith("WITH")
       || U.startsWith("UPDATE") || U.startsWith("INSERT") || U.startsWith("DELETE"))) {
        throw new SQLException("Only SELECT/WITH/UPDATE/INSERT/DELETE are allowed");
    }

    // 5) Opcional: forzar esquema/tablas permitidas por patrón (sin enumerar una whitelist exacta)
    // if (!U.matches(".*\\b(TU_ESQUEMA)\\.[A-Z0-9_]+\\b.*")) throw new SQLException("Schema not allowed");

    return s;
}
