import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

import com.santander.supra.eresearch.staging.sql.resolver.PartitionsHandlerTSImpl;
import com.santander.supra.core.model.Feed;
import com.santander.supra.core.model.Target;
import com.santander.supra.core.staging.sql.SQLConstants;
import com.santander.supra.eresearch.staging.sql.SQLConstantsER;

public class PartitionsHandlerTSImplTest {

    private PartitionsHandlerTSImpl handler;

    // Wrapper público dentro del test
    public static class PartitionsHandlerWrapper extends PartitionsHandlerTSImpl {
        public List<String> callGetListPartitionsRawInternal() {
            return super.getListPartitionsRawInternal();
        }
    
        public String callBuildStringFromList(List<String> partitions, boolean typed) {
            return super.buildStringFromList(partitions, typed);
        }
    }


    @BeforeEach
    public void setup() {
        handler = new PartitionsHandlerTSImpl();
    }

    @Test
    public void testGetListPartitionsRawInternal() {
        List<String> expected = Arrays.asList(
            SQLConstants.DATA_DATE_FIELD_NAME,
            SQLConstants.DATA_TIMESTAMP_FIELD_NAME
        );
        List<String> actual = handler.getListPartitionsRawInternal();
        assertEquals(expected, actual);
    }

    @Test
    public void testBuildStringFromList_TypedTrue() {
        List<String> partitions = Arrays.asList(SQLConstants.DATA_DATE_FIELD_NAME);
        String expected = SQLConstants.DATA_DATE_FIELD_NAME + " " + SQLConstants.STRING;
        String actual = handler.buildStringFromList(partitions, true);
        assertEquals(expected, actual);
    }

    @Test
    public void testBuildStringFromList_TypedFalse() {
        List<String> partitions = Arrays.asList("my_partition");
        String expected = "my_partition";
        String actual = handler.buildStringFromList(partitions, false);
        assertEquals(expected, actual);
    }

    @Test
    public void testBuildFiltersFromList_asStringTrue() {
        List<String> partitions = Arrays.asList(SQLConstants.DATA_DATE_FIELD_NAME);
        String expected = SQLConstants.DATA_DATE_FIELD_NAME + "='" + SQLConstants.DATA_DATE_REPLACE + "'";
        String actual = handler.buildFiltersFromList(partitions, "=", true);
        assertEquals(expected, actual);
    }

    @Test
    public void testBuildFiltersFromList_asStringFalse() {
        List<String> partitions = Arrays.asList(SQLConstants.DATA_TIMESTAMP_FIELD_NAME);
        String expected = SQLConstants.DATA_TIMESTAMP_FIELD_NAME + "=" + SQLConstants.EXECUTION_DATE_REPLACE;
        String actual = handler.buildFiltersFromList(partitions, "=", false);
        assertEquals(expected, actual);
    }

    @Test
    public void testGetListPartitionsInternal_forLoadTypeS() {
        String loadType = SQLConstantsER.LOAD_TYPE_S;
        List<String> expected = Arrays.asList(
            SQLConstants.DATA_DATE_FIELD_NAME,
            SQLConstants.DATA_TIMESTAMP_FIELD_NAME
        );
        List<String> actual = handler.getListPartitionsInternal(loadType);
        assertEquals(expected, actual);
    }

    @Test
    public void testGetListPartitionsTargetInternal() {
        Target target = new Target();
        target.setLoadType(SQLConstantsER.LOAD_TYPE_S);
        List<String> expected = Arrays.asList(
            SQLConstants.DATA_DATE_FIELD_NAME,
            SQLConstants.DATA_TIMESTAMP_FIELD_NAME
        );
        List<String> actual = handler.getListPartitionsTargetInternal(target);
        assertEquals(expected, actual);
    }

    @Test
    public void testGetListPartitionsRaw() {
        Feed dummyFeed = new Feed();
        String actual = handler.getListPartitionsRaw(dummyFeed);
        String expected = SQLConstants.DATA_DATE_FIELD_NAME + " " + SQLConstants.STRING +
                          SQLConstants.COLON + SQLConstants.DATA_TIMESTAMP_FIELD_NAME + " " + SQLConstants.STRING;
        assertEquals(expected, actual);
    }

    @Test
    public void testGetListPartitionsRawNotTyped() {
        Feed dummyFeed = new Feed();
        String actual = handler.getListPartitionsRawNotTyped(dummyFeed);
        String expected = SQLConstants.DATA_DATE_FIELD_NAME + SQLConstants.COLON + SQLConstants.DATA_TIMESTAMP_FIELD_NAME;
        assertEquals(expected, actual);
    }

    @Test
    public void testGetListPartitionsHist() {
        Feed feed = new Feed();
        feed.setLoadType(SQLConstantsER.LOAD_TYPE_S);
        String actual = handler.getListPartitionsHist(feed);
        String expected = SQLConstants.DATA_DATE_FIELD_NAME + " " + SQLConstants.STRING +
                          SQLConstants.COLON + SQLConstants.DATA_TIMESTAMP_FIELD_NAME + " " + SQLConstants.STRING;
        assertEquals(expected, actual);
    }

    @Test
    public void testGetListPartitionsHistNotTyped() {
        Feed feed = new Feed();
        feed.setLoadType(SQLConstantsER.LOAD_TYPE_S);
        String actual = handler.getListPartitionsHistNotTyped(feed);
        String expected = SQLConstants.DATA_DATE_FIELD_NAME + SQLConstants.COLON + SQLConstants.DATA_TIMESTAMP_FIELD_NAME;
        assertEquals(expected, actual);
    }

    @Test
    public void testGetListPartitionsPrep() {
        Feed feed = new Feed();
        feed.setLoadType(SQLConstants.LOAD_TYPE_M);
        String expected = SQLConstants.DATA_DATE_FIELD_NAME + " " + SQLConstants.STRING;
        String actual = handler.getListPartitionsPrep(feed);
        assertEquals(expected, actual);
    }

    @Test
    public void testFilterByPartitionsRaw() {
        Feed feed = new Feed();
        String expected = SQLConstants.DATA_DATE_FIELD_NAME + "='" + SQLConstants.DATA_DATE_REPLACE + "'" +
                          SQLConstants.COLON + SQLConstants.DATA_TIMESTAMP_FIELD_NAME + "=" + SQLConstants.EXECUTION_DATE_REPLACE;
        String actual = handler.filterByPartitionsRaw(feed, "=");
        assertEquals(expected, actual);
    }

    @Test
    public void testGetListPartitionsHistTarget() {
        String[] args = { SQLConstantsER.LOAD_TYPE_S, "opt" };
        String expected = SQLConstants.DATA_DATE_FIELD_NAME + " " + SQLConstants.STRING +
                          SQLConstants.COLON + SQLConstants.DATA_TIMESTAMP_FIELD_NAME + " " + SQLConstants.STRING;
        String actual = handler.getListPartitionsHistTarget(args);
        assertEquals(expected, actual);
    }

    @Test
    public void testFilterByPartitionsHistTarget() {
        String[] args = { SQLConstantsER.LOAD_TYPE_S, "opt" };
        String expected = SQLConstants.DATA_DATE_FIELD_NAME + "='" + SQLConstants.DATA_DATE_REPLACE + "'" +
                          SQLConstants.COLON + SQLConstants.DATA_TIMESTAMP_FIELD_NAME + "=" + SQLConstants.EXECUTION_DATE_REPLACE;
        String actual = handler.filterByPartitionsHistTarget("=", args);
        assertEquals(expected, actual);
    }
}

