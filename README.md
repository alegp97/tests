# tests
tests santander

####################################
@Test
public void testExecute_shouldLaunchQueries() throws Exception {
    // Subclase del componente para sobrescribir comportamiento
    InvalidateMetadataEconomicResearch component = new InvalidateMetadataEconomicResearch() {
        @Override
        protected List<String> getTables() {
            return Arrays.asList("db.table1", "db.table2");
        }
    };

    SessionWrapper session = mock(SessionWrapper.class);
    Execution execution = mock(Execution.class);

    // Mock JDBC y otras dependencias internas si aplica
    Connection mockConnection = mock(Connection.class);
    when(JDBCHandler.getConnection(JDBCHandler.IMPALA)).thenReturn(mockConnection);

    JDBCExecutorHandler mockExecHandler = mock(JDBCExecutorHandler.class);
    mockStatic(JDBCExecutorHandler.class);
    when(JDBCExecutorHandler.getInstance()).thenReturn(mockExecHandler);

    // Ejecutamos y verificamos
    int result = component.execute(session, execution);
    assertEquals(DataProperties.RESULT_SUCCESS, result);

    // Verificamos llamadas
    verify(mockExecHandler, times(2)).launchQueries(eq(execution), eq(session), eq(mockConnection), anyString(),
        argThat(map -> map.get("isCritical").equals(true) && map.get("isTraceable").equals(true)));
}

