# tests
tests santander

####################################
import org.junit.Test;
import static org.junit.Assert.*;

import java.util.Arrays;
import java.util.List;

import com.santander.eresearch.metadata.ddl.invalidatemetadata.InvalidateMetadataEconomicResearch;
import com.santander.supra.core.staging.exceptions.SparkPropertyNotFoundException;

public class InvalidateMetadataEconomicResearchTest {


    public static class InvalidateMetadataEconomicResearchTestable extends InvalidateMetadataEconomicResearch {
        @Override
        public void parseArguments(String applicationArgs) throws SparkPropertyNotFoundException {
            super.parseArguments(applicationArgs);
        }

        @Override
        public List<String> getTables() {
            return super.getTables();
        }

        public String callInvalidateBuild(String table) {
            return invalidateBuild(table);
        }
    }

    @Test
    public void testParseArgumentsAndInvalidateBuild() throws Exception {
        InvalidateMetadataEconomicResearchTestable testInstance = new InvalidateMetadataEconomicResearchTestable();

        String argsJson = "{\"tables\": \"table1,table2\"}";


        testInstance.parseArguments(argsJson);
        List<String> tables = testInstance.getTables();
        String query = testInstance.callInvalidateBuild("table1");

        assertEquals(Arrays.asList("table1", "table2"), tables);
        assertEquals("INVALIDATE METADATA table1", query);
    }
}
