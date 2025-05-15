package com.santander.eresearch.metadata.ddl.invalidatemetadata;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;

import com.santander.supra.core.actions.Feed;
import com.santander.supra.core.utils.sql.SQLConstants;

/**
 * Unit-tests for {@link AlterViewRawSanitedERVT}.
 */
public class AlterViewRawSanitedERVTTest {

    /** Sub-clase que expone y controla la dependencia getTableName(feed). */
    private static class Testable extends AlterViewRawSanitedERVT {
        @Override
        protected String getTableName(Feed feed) {   // forzamos un nombre estable
            return "vw_raw_sanitized_orders";
        }
        // Si necesitáramos exponer create(...) como público:
        String callCreate(Feed feed) { return super.create(feed); }
    }

    private Testable sut;   // System Under Test

    @Before
    public void setUp() {
        sut = new Testable();
    }

    // ---------------------------------------------------------------------
    //  TEST: isHiveForced()
    // ---------------------------------------------------------------------
    @Test
    public void isHiveForced_shouldReturnTrue() {
        assertEquals(true, sut.isHiveForced());
    }

    // ---------------------------------------------------------------------
    //  TEST: create(feed)
    // ---------------------------------------------------------------------
    @Test
    public void create_buildsExpectedAlterViewStatement() {
        // Arrange
        Feed feed = mock(Feed.class);          // no es necesario stubbear nada

        String expected =
            new StringBuilder()
                .append(SQLConstants.USE_DATABASE)
                .append(SQLConstants.RAW_SANITED_DATABASE)
                .append(SQLConstants.QUERY_SEPARATOR)
                .append(SQLConstants.ALTER_VIEW)
                .append("vw_raw_sanitized_orders")   // lo retorna la sub-clase Testable
                .append(SQLConstants.AS)
                .toString();

        // Act
        String actual = sut.callCreate(feed);

        // Assert
        assertEquals(expected, actual);
    }
}
