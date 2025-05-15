package com.santander.eresearch.metadata.ddl.invalidatemetadata;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

import org.junit.Before;
import org.junit.Test;

import com.santander.supra.core.actions.Feed;
import com.santander.supra.core.utils.sql.SQLConstants;

/**
 * Unit-test for {@link SelectCountStaging}.
 */
public class SelectCountStagingTest {

    /** Subclase interna que expone el método protected para el test. */
    private static class Testable extends SelectCountStaging {
        public String callFrom(Feed feed) {          // simple puente hacia el protected
            return super.from(feed);
        }
    }

    private Testable sut;            // System-Under-Test

    @Before
    public void setUp() {
        sut = new Testable();
    }

    @Test
    public void from_returnsExpectedStagingSelect() {
        // --- Arrange ------------------------------------------------------
        Feed feed = mock(Feed.class);
        when(feed.getFunctionalName()).thenReturn("orders_cur");

        String expected =
            SQLConstants.FROM + SQLConstants.STAGING_DATABASE + ".orders_cur";

        // --- Act ----------------------------------------------------------
        String actual = sut.callFrom(feed);

        // --- Assert -------------------------------------------------------
        assertEquals(expected, actual);
    }
}

