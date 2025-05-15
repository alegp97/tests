package com.santander.eresearch.metadata.ddl.invalidatemetadata;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

import java.util.Collections;
import java.util.List;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import com.santander.supra.core.actions.Feed;
import com.santander.supra.core.model.Execution;
import com.santander.supra.core.staging.PartitionsHandler;
import com.santander.supra.core.utils.sql.SQLConstants;
import com.santander.supra.core.datalake.ddbb.SessionWrapper;
import com.santander.supra.core.sqlbuilder.Field;

/**
 * Tests for {@link SelectCheckFatalVT}.
 */
public class SelectCheckFatalVTTest {

    /** Sub-clase que expone métodos protected y simplifica dependencias */
    private static class Testable extends SelectCheckFatalVT {

        // ---  Stub de métodos protegidos de la super-clase  ---
        @Override
        protected Feed getFeed(SessionWrapper s, Execution e) {
            Feed f = Mockito.mock(Feed.class);
            when(f.getName()).thenReturn("testFeed");
            return f;
        }

        @Override
        protected PartitionsHandler getPartitionsHandler() {
            PartitionsHandler ph = mock(PartitionsHandler.class);
            when(ph.filterByPartitionsRaw(any(), any())).thenReturn("part-filter");
            return ph;
        }

        /** Evitamos la lógica real de concatHints para simplificar el assert */
        @Override
        protected String concatHints(String sql) {
            return sql;
        }

        // ---  Exponemos los protected que queremos invocar desde el test  ---
        String callSelect(Feed f, List<Field> fs, String... a) throws Exception { return select(f, fs, a); }
        String callFrom (Feed f)                                   { return from(f); }
        String callWhere(Feed f, PartitionsHandler p)              { return where(f, p); }
    }

    private Testable sut;

    @Before
    public void setUp() {
        sut = new Testable();
    }

    // -------------------------------------------------------------------------
    //  TESTS
    // -------------------------------------------------------------------------

    @Test
    public void projectionList_shouldContainMSG() {
        assertEquals(Collections.singletonList("MSG"), sut.getProjectionList());
    }

    @Test
    public void select_from_where_returnEmptyString() throws Exception {
        Feed feed = mock(Feed.class);
        assertEquals("", sut.callSelect(feed, Collections.emptyList()));
        assertEquals("", sut.callFrom(feed));
        assertEquals("", sut.callWhere(feed, mock(PartitionsHandler.class)));
    }

    @Test
    public void build_shouldIncludeAllPlaceHolders() throws Exception {
        SessionWrapper session   = mock(SessionWrapper.class);
        Execution      execution = mock(Execution.class);

        String sql = sut.build(session, execution);

        /* Comprobamos los tokens / placeholders importantes */
        assertTrue(sql.contains("SELECT"));                       // cuerpo base del FATAL_ERRORS
        assertTrue(sql.contains(SQLConstants.VAL_DATABASE));      // base de datos
        assertTrue(sql.contains("${TABLE_ALERTS_VF}"));           // placeholder tabla
        assertTrue(sql.contains("${LOAD_DATE}"));                 // placeholder fecha load
        assertTrue(sql.contains("${EXECUTION_DATE}"));            // placeholder fecha exec
        assertTrue(sql.contains("testFeed"));                     // nombre del feed (mock)
        assertTrue(sql.contains("part-filter"));                  // filtro de particiones (mock)
    }
}

