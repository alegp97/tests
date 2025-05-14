# tests
tests santander

####################################

import com.santander.bigdata.core3.uv.actions.InvalidateMetadataEconomicResearch;
import com.santander.supra.core.execution.Execution;
import com.santander.supra.core.session.SessionWrapper;
import com.santander.supra.core.jdbc.JDBCExecutorHandler;
import com.santander.supra.core.jdbc.JDBCHandler;
import com.santander.supra.core.variables.VariableResolutionHandler;
import com.santander.supra.core.variables.VariableResolutionHandlerFactory;
import com.santander.utils.JSONUtils;
import org.junit.jupiter.api.*;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.Connection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PrepareForTest({VariableResolutionHandlerFactory.class,
                 JDBCExecutorHandler.class,
                 JSONUtils.class})
class InvalidateMetadataEconomicResearchTest {

    private InvalidateMetadataEconomicResearch action;

    @BeforeEach
    void setUp() {
        action = new InvalidateMetadataEconomicResearch();
    }

    // ---------- 1. parseArguments ----------
    @Test
    void parseArgumentsPopulatesTables() throws Exception {
        // Mock JSONUtils.argsFromJson(...) para que devuelva dos tablas separadas por coma
        PowerMockito.mockStatic(JSONUtils.class);
        when(JSONUtils.argsFromJson("{dummy}", "tables"))
                .thenReturn("db1.tableA,db2.tableB");

        action.parseArguments("{dummy}");

        List<String> tables = action.getTables();
        assertEquals(2, tables.size());
        assertTrue(tables.contains("db1.tableA"));
        assertTrue(tables.contains("db2.tableB"));
    }

    // ---------- 2. invalidateBuild ----------
    @Test
    void invalidateBuildReturnsCorrectSentence() {
        String sql = action.invalidateBuild("db1.tableA");
        assertEquals("INVALIDATE METADATA db1.tableA", sql);
    }

    // ---------- 3. getTables reflejado ----------
    @Test
    void getTablesAfterParseReturnsSameList() throws Exception {
        PowerMockito.mockStatic(JSONUtils.class);
        when(JSONUtils.argsFromJson(anyString(), eq("tables")))
                .thenReturn("t1");

        action.parseArguments("{}");
        assertEquals(List.of("t1"), action.getTables());
    }

    // ---------- 4. execute(...) lanza queries ----------
    @Test
    void executeLaunchesOneQueryPerTable() throws Exception {
        // ---- mocks estáticos ----
        PowerMockito.mockStatic(JSONUtils.class);
        when(JSONUtils.argsFromJson(anyString(), eq("tables"))).thenReturn("tbl1,tbl2");

        VariableResolutionHandler vrh = mock(VariableResolutionHandler.class);
        when(vrh.translateQuery(any(), any(), any(), anyString()))
                .thenReturn("{ \"tables\":\"tbl1,tbl2\" }");

        PowerMockito.mockStatic(VariableResolutionHandlerFactory.class);
        when(VariableResolutionHandlerFactory.getVariableResolutionHandler(any()))
                .thenReturn(vrh);

        JDBCExecutorHandler jdbcExec = mock(JDBCExecutorHandler.class);
        PowerMockito.mockStatic(JDBCExecutorHandler.class);
        when(JDBCExecutorHandler.getInstance()).thenReturn(jdbcExec);

        // Para evitar NPE con la conexión, mockeamos getConnection (si lo usas)
        PowerMockito.mockStatic(JDBCHandler.class);
        when(JDBCHandler.getConnection(JDBCHandler.IMPALA)).thenReturn(mock(Connection.class));

        // ---- objetos “dummy” ----
        SessionWrapper session = mock(SessionWrapper.class);
        Execution execution    = mock(Execution.class);

        // ---- llamada ----
        action.execute(session, execution);

        // ---- verificación ----
        ArgumentCaptor<String> sqlCap = ArgumentCaptor.forClass(String.class);
        verify(jdbcExec, times(2))
                .launchQueries(eq(execution), eq(session), any(), sqlCap.capture());

        List<String> sentencias = sqlCap.getAllValues();
        assertTrue(sentencias.contains("INVALIDATE METADATA tbl1"));
        assertTrue(sentencias.contains("INVALIDATE METADATA tbl2"));
    }
}

