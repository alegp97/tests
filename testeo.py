import com.santander.supra.core.model.Feed;
import com.santander.supra.core.model.Field;
import com.santander.supra.core.staging.sql.SQLConstants;
import com.santander.supra.eresearch.staging.sql.ddl.create.SQLCreateStaging;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

public class SQLCreateStagingTest {

    private SQLCreateStagingWrapper component;

    // Wrapper para exponer métodos protegidos
    public static class SQLCreateStagingWrapper extends SQLCreateStaging {
        public String callGetTableName(Feed feed) {
            return super.getTableName(feed);
        }

        public String callCreate(Feed feed) {
            return super.create(feed);
        }

        public String callFields(Feed feed, List<Field> fields) {
            return super.fields(feed, fields);
        }
    }

    @BeforeEach
    public void setUp() {
        component = new SQLCreateStagingWrapper();
    }

    @Test
    public void testCreate_returnsExpectedCreateStatement() {
        Feed mockFeed = mock(Feed.class);
        when(mockFeed.getFunctionalName()).thenReturn("users");

        String expected = "CREATE EXTERNAL TABLE IF NOT EXISTS " + SQLConstants.STAGING_DATABASE + ".users";
        String actual = component.callCreate(mockFeed);

        assertTrue(actual.startsWith(expected));
    }

    @Test
    public void testFields_generatesCorrectFieldList() {
        Feed mockFeed = mock(Feed.class);

        Field f1 = mock(Field.class);
        when(f1.getFunctionalName()).thenReturn("col1");
        when(f1.getTypeConversion()).thenReturn("STRING");
        when(f1.getComment()).thenReturn("comment 1");

        Field f2 = mock(Field.class);
        when(f2.getFunctionalName()).thenReturn(null);
        when(f2.getName()).thenReturn("backup_col2");
        when(f2.getTypeConversion()).thenReturn("INT");
        when(f2.getComment()).thenReturn(null);

        List<Field> fields = Arrays.asList(f1, f2);

        String result = component.callFields(mockFeed, fields);

        assertTrue(result.contains("col1 STRING COMMENT 'comment 1'"));
        assertTrue(result.contains("backup_col2 INT COMMENT 'N/A'"));
    }
}
