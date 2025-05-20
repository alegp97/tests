import com.santander.supra.core.model.Feed;
import com.santander.supra.core.staging.sql.SQLConstants;
import com.santander.supra.eresearch.staging.sql.ddl.create.SQLCreateStagingPartenon;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class SQLCreateStagingPartenonTest {

    private SQLCreateStagingPartenon component;

    @BeforeEach
    public void setUp() {
        component = new SQLCreateStagingPartenon();
    }

    @Test
    public void testGetTableName_returnsCorrectFormat() {
        Feed mockFeed = mock(Feed.class);
        when(mockFeed.getFunctionalName()).thenReturn("transactions");

        String expected = SQLConstants.STAGING_DATABASE + "." + "transactions";
        String actual = component.getTableName(mockFeed);

        assertEquals(expected, actual);
    }
}
