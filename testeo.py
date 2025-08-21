// Sonarqube/Fortify fixed: usar PreparedStatement y try-with-resources (no Statement).
private void executeQuery(final String sql) throws SQLException {
    if (sql == null || sql.isBlank()) {
        throw new SQLException("Empty SQL");
    }
    try (PreparedStatement ps = connection.prepareStatement(sql)) {
        ps.execute();  // no loguear 'sql'
    }
}
