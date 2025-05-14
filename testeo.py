@Test
    public void testExecute_shouldLaunchQueries() throws Exception {
        // set env
        System.setProperty("HDFS_CONFIG", "mock-config");

        // Mocks de dependencias
        SessionWrapper session = mock(SessionWrapper.class);
        Execution execution = mock(Execution.class);
        VariableResolutionHandler mockResolver = mock(VariableResolutionHandler.class);
        JDBCExecutorHandler mockExecHandler = mock(JDBCExecutorHandler.class);

        // mock args + resolución
        String applicationArgs = "{\"tables\": \"tableA,tableB\"}";
        when(session.getApplicationArguments()).thenReturn(applicationArgs);
        when(execution.getName()).thenReturn("mockExecution");

        PowerMockito.mockStatic(VariableResolutionHandlerFactory.class);
        when(VariableResolutionHandlerFactory.getVariableResolutionHandler()).thenReturn(mockResolver);
        when(mockResolver.translateQuery(any(), any(), any())).thenReturn(applicationArgs);

        PowerMockito.mockStatic(JDBCExecutorHandler.class);
        when(JDBCExecutorHandler.getInstance()).thenReturn(mockExecHandler);

        // Espía del componente para exponer el método protegido
        InvalidateMetadataEconomicResearch component = new InvalidateMetadataEconomicResearch() {
            @Override
            protected void parseArguments(String args) {
                super.parseArguments(args);  // deja el comportamiento real
            }

            @Override
            protected String invalidateBuild(String table) {
                return "INVALIDATE METADATA " + table;
            }
        };

        int result = component.execute(session, execution);

        // Verifica que se ejecutaron dos queries
        verify(mockExecHandler, times(2)).launchQueries(
            eq(execution), eq(session), any(Connection.class), contains("INVALIDATE METADATA"), any());

        assertEquals(DataProperties.RESULT_SUCCESS, result);
    }
