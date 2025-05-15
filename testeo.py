@RunWith(MockitoJUnitRunner.class)
public class SelectCheckFatalVFTest {

    private SelectCheckFatalVF component;

    @Before
    public void setUp() {
        component = new SelectCheckFatalVF();
    }

    @Test
    public void testGetProjectionList() {
        List<String> expected = Collections.singletonList("MSG");
        List<String> actual = component.getProjectionList();
        assertEquals(expected, actual);
    }

    @Test
    public void testSelect() throws Exception {
        Feed mockFeed = mock(Feed.class);
        List<Field> mockFields = Collections.emptyList();
        String result = component.select(mockFeed, mockFields);
        assertEquals("", result);
    }

    @Test
    public void testFrom() {
        Feed mockFeed = mock(Feed.class);
        String result = component.from(mockFeed);
        assertEquals("", result);
    }

    @Test
    public void testWhere() {
        PartitionsHandler mockPartitions = mock(PartitionsHandler.class);
        Feed mockFeed = mock(Feed.class);
        String result = component.where(mockFeed, mockPartitions);
        assertEquals("", result);
    }

    @Test
    public void testBuild() throws Exception {
        SessionWrapper mockSession = mock(SessionWrapper.class);
        Execution mockExecution = mock(Execution.class);

        String result = component.build(mockSession, mockExecution);

        assertTrue(result.contains(SQLConstants.VAL_DATABASE));
        assertTrue(result.contains("${TABLE_ALERTS_VF}"));
        assertTrue(result.contains("${LOAD_DATE}"));
        assertTrue(result.contains("${EXECUTION_DATE}"));
        assertTrue(result.contains("${tableName}"));
    }
}
