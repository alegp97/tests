# tests
tests santander

####################################
package eresearch.actions;

import com.santander.supra.core.execution.Execution;
import com.santander.supra.core.jdbc.JDBCExecutorHandler;
import com.santander.supra.core.jdbc.JDBCHandler;
import com.santander.supra.core.session.SessionWrapper;
import com.santander.supra.core.utils.JSONUtils;
import com.santander.supra.core.variables.VariableResolutionHandler;
import com.santander.supra.core.variables.VariableResolutionHandlerFactory;
import com.santander.supra.economicresearch.actions.InvalidateMetadataEconomicResearch;
import com.santander.supra.exceptions.SparkPropertyNotFoundException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.sql.Connection;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")
@PrepareForTest({
        JSONUtils.class,
        VariableResolutionHandlerFactory.class,
        JDBCExecutorHandler.class,
        JDBCHandler.class,
        InvalidateMetadataEconomicResearch.class
})
public class InvalidateMetadataEconomicResearchTest {

    private static final class TestableInvalidate extends InvalidateMetadataEconomicResearch {
        @Override
        public void parseArguments(String args) throws SparkPropertyNotFoundException {
            super.parseArguments(args);
        }
    }

    private TestableInvalidate action;

    @BeforeEach
    void setUp() {
        action = new TestableInvalidate();
    }

    @Test
    void parseArgumentsPopulatesTables() throws Exception {
        PowerMockito.mockStatic(JSONUtils.class);
        when(JSONUtils.argsFromJson("{dummy}", "tables"))
                .thenReturn("db1.tableA,db2.tableB");

        action.parseArguments("{dummy}");

        assertEquals(
                List.of("db1.tableA", "db2.tableB"),
                action.getTables(),
                "La lista interna de tablas debe contener las dos tablas parseadas"
        );
    }

    @Test
    void invalidateBuildGeneratesCorrectSentence() {
        String sql = action.invalidateBuild("db1.tableA");
        assertEquals("INVALIDATE METADATA db1.tableA", sql);
    }

    @Test
    void executeLaunchesOneQueryPerTable() throws Exception {
        PowerMockito.mockStatic(JSONUtils.class);
        when(JSONUtils.argsFromJson(anyString(), eq("tables")))
                .thenReturn("tbl1,tbl2");

        VariableResolutionHandler vrh = mock(VariableResolutionHandler.class);
        when(vrh.translateQuery(any(), any(), any(), anyString()))
                .thenReturn("{\"tables\":\"tbl1,tbl2\"}");

        PowerMockito.mockStatic(VariableResolutionHandlerFactory.class);
        when(VariableResolutionHandlerFactory.getVariableResolutionHandler(any()))
                .thenReturn(vrh);

        JDBCExecutorHandler jdbcExec = mock(JDBCExecutorHandler.class);
        PowerMockito.mockStatic(JDBCExecutorHandler.class);
        when(JDBCExecutorHandler.getInstance()).thenReturn(jdbcExec);

        PowerMockito.mockStatic(JDBCHandler.class);
        when(JDBCHandler.getConnection(JDBCHandler.IMPALA))
                .thenReturn(mock(Connection.class));

        SessionWrapper session = mock(SessionWrapper.class);
        Execution execution = mock(Execution.class);

        int result = action.execute(session, execution);

        assertEquals(0, result);

        ArgumentCaptor<String> sqlCap = ArgumentCaptor.forClass(String.class);
        verify(jdbcExec, times(2))
                .launchQueries(eq(execution), eq(session), any(), sqlCap.capture());

        List<String> sentencias = sqlCap.getAllValues();
        assertTrue(sentencias.contains("INVALIDATE METADATA tbl1"));
        assertTrue(sentencias.contains("INVALIDATE METADATA tbl2"));
    }
}
