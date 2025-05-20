import com.santander.supra.core.model.Feed;
import com.santander.supra.core.staging.sql.SQLConstants;
import com.santander.supra.eresearch.staging.sql.ddl.create.SQLCreateStagingPartenon;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class SQLCreateStagingPartenonTest {

    private SQLCreateStagingPartenonWrapper component;

    // Wrapper to access protected method
    public static class SQLCreateStagingPartenonWrapper extends SQLCreateStagingPartenon {
        public String callGetTableName(Feed feed) {
            return super.getTableName(feed);
        }
    }

    @BeforeEach
    public void setUp() {
        component = new SQLCreateStagingPartenonWrapper();
    }

    @Test
    public void testGetTableName_returnsCorrectFormat() {
        Feed mockFeed = mock(Feed.class);
        when(mockFeed.getFunctionalName()).thenReturn("transactions");

        String expected = SQLConstants.STAGING_DATABASE + "." + "transactions";
        String actual = component.callGetTableName(mockFeed);

        assertEquals(expected, actual);
    }
}

