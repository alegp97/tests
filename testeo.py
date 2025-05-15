@RunWith(MockitoJUnitRunner.class)
public class SelectCheckFatalVFTest {

    /** Subclase solo para exponer los métodos protected */
    private static class Testable extends SelectCheckFatalVF {
        String callSelect(Feed f, List<Field> fs, String... args) throws Exception { return select(f, fs, args); }
        String callFrom(Feed f)                                { return from(f); }
        String callWhere(Feed f, PartitionsHandler p)          { return where(f, p); }
    }

    private final Testable sut = new Testable();

    @Test
    public void projectionList_shouldContainMSG() {
        assertEquals(Collections.singletonList("MSG"), sut.getProjectionList());
    }

    @Test
    public void select_from_where_returnEmptyString() throws Exception {
        Feed feed = Mockito.mock(Feed.class);
        PartitionsHandler ph = Mockito.mock(PartitionsHandler.class);
        assertEquals("", sut.callSelect(feed, Collections.emptyList()));
        assertEquals("", sut.callFrom(feed));
        assertEquals("", sut.callWhere(feed, ph));
    }

    @Test
    public void build_mustContainPlaceholders() throws Exception {
        SessionWrapper session = Mockito.mock(SessionWrapper.class);
        Execution       exec   = Mockito.mock(Execution.class);

        String sql = sut.build(session, exec);

        assertTrue(sql.contains(SQLConstants.VAL_DATABASE));
        assertTrue(sql.contains("${TABLE_ALERTS_VF}"));
        assertTrue(sql.contains("${LOAD_DATE}"));
        assertTrue(sql.contains("${EXECUTION_DATE}"));
        assertTrue(sql.contains("${tableName}"));
    }
}
