# tests
tests santander

####################################
@Test
public void testExecute_shouldLaunchInvalidateQueries() throws Exception {
    // Arrange
    InvalidateMetadataEconomicResearch component = new InvalidateMetadataEconomicResearch();

    // Mocks
    SessionWrapper session = mock(SessionWrapper.class);
    Execution execution = mock(Execution.class);

    VariableResolutionHandler mockHandler = mock(VariableResolutionHandler.class);
    when(mockHandler.translateQuery(any(), any(), any())).thenReturn("tables:db1,db2");

    VariableResolutionHandlerFactory factory = mockStatic(VariableResolutionHandlerFactory.class).getMock();
    when(VariableResolutionHandlerFactory.getVariableResolutionHandler()).thenReturn(mockHandler);

    Connection mockConn = mock(Connection.class);
    mockStatic(JDBCHandler.class);
    when(JDBCHandler.getConnection(JDBCHandler.IMPALA)).thenReturn(mockConn);

    JDBCExecutorHandler mockExecHandler = mock(JDBCExecutorHandler.class);
    mockStatic(JDBCExecutorHandler.class);
    when(JDBCExecutorHandler.getInstance()).thenReturn(mockExecHandler);

    doNothing().when(mockExecHandler).launchQueries(any(), any(), any(), any(), anyBoolean(), anyBoolean(), anyInt(), isNull());

    // Act
    int result = component.execute(session, execution);

    // Assert
    assertEquals(DataProperties.RESULT_SUCCESS, result);
    verify(mockExecHandler, times(2)).launchQueries(any(), any(), any(), any(), anyBoolean(), anyBoolean(), anyInt(), isNull());
}

