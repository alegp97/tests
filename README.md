# tests
tests santander

####################################
/*  ────────────────────────────────────────────────────────────────────────────
    ADAPTA el package a tu estructura de proyecto
   ────────────────────────────────────────────────────────────────────────── */
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

/**
 * Tests unitarios para {@link InvalidateMetadataEconomicResearch}.
 */
@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.management.*")       // evita warning con JDK 8+
@PrepareForTest({
        JSONUtils.class,                       // ⬅  métodos estáticos que mockeamos
        VariableResolutionHandlerFactory.class,
        JDBCExecutorHandler.class,
        JDBCHandler.class,                     // si haces getConnection en execute()
        InvalidateMetadataEconomicResearch.class
})
public class InvalidateMetadataEconomicResearchTest {

    /** Subclase que expone el método protected parseArguments. */
    private static final class TestableInvalidate
            extends InvalidateMetadataEconomicResearch {

        @Override public void parseArguments(String args)
                throws SparkPropertyNotFoundException {
            super.parseArguments(args);
        }
    }

    private TestableInvalidate action;

    @BeforeEach
    void setUp() { action = new TestableInvalidate(); }

    /* ───────────────────────── 1. parseArguments ─────────────────────────── */

    @Test
    void parseArgumentsPopulatesTables() throws Exception {
        // 1️⃣  Mock del estático JSONUtils.argsFromJson(...)
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

    /* ───────────────────────── 2. invalidateBuild ────────────────────────── */

    @Test
    void invalidateBuildGeneratesCorrectSentence() {
        String sql = action.invalidateBuild("db1.tableA");
        assertEquals("INVALIDATE METADATA db1.tableA", sql);
    }

    /* ───────────────────────── 3. execute(...) ───────────────────────────── */

    @Test
    void executeLaunchesOneQueryPerTable() throws Exception {

        /* 3-A  Mock de JSONUtils para devolver dos tablas */
        PowerMockito.mockStatic(JSONUtils.class);
        when(JSONUtils.argsFromJson(anyString(), eq("tables")))
                .thenReturn("tbl1,tbl2");

        /* 3-B  Mock del VariableResolutionHandler para devolver el mismo args */
        VariableResolutionHandler vrh = mock(VariableResolutionHandler.class);
        when(vrh.translateQuery(any(), any(), any(), anyString()))
                .thenReturn("{\"tables\":\"tbl1,tbl2\"}");

        PowerMockito.mockStatic(VariableResolutionHandlerFactory.class);
        when(VariableResolutionHandlerFactory.getVariableResolutionHandler(any()))
                .thenReturn(vrh);

        /* 3-C  Mock del JDBCExecutorHandler y (opcional) JDBCHandler.getConnection */
        JDBCExecutorHandler jdbcExec = mock(JDBCExecutorHandler.class);
        PowerMockito.mockStatic(JDBCExecutorHandler.class);
        when(JDBCExecutorHandler.getInstance()).thenReturn(jdbcExec);

        PowerMockito.mockStatic(JDBCHandler.class);
        when(JDBCHandler.getConnection(JDBCHandler.IMPALA))
                .thenReturn(mock(Connection.class));

        /* 3-D  Objetos de soporte */
        SessionWrapper session = mock(SessionWrapper.class);
        Execution      execution = mock(Execution.class);

        /* 3-E  Invocación */
        int result = action.execute(session, execution);

        /* 3-F  Verificaciones */
        assertEquals(0, result, "Action debería devolver RESULT_SUCCESS (0)");

        ArgumentCaptor<String> sqlCap = ArgumentCaptor.forClass(String.class);
        verify(jdbcExec, times(2))
                .launchQueries(eq(execution), eq(session), any(), sqlCap.capture());

        List<String> sentencias = sqlCap.getAllValues();
        assertTrue(sentencias.contains("INVALIDATE METADATA tbl1"));
        assertTrue(sentencias.contains("INVALIDATE METADATA tbl2"));
    }
}

