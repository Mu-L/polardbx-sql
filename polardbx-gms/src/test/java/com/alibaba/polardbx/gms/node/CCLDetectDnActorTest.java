package com.alibaba.polardbx.gms.node;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.metadb.ccl.DnCclRecord;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.function.Supplier;

import static com.alibaba.polardbx.gms.node.CCLDetectUtils.DubiousItem;

/**
 * Unit test for CCLDetectDnActor
 *
 * @author liugaoji
 */
@RunWith(MockitoJUnitRunner.class)
public class CCLDetectDnActorTest {

    @InjectMocks
    private CCLDetectDnActor target;

    @Mock
    private Supplier<Connection> mockConnectionSupplier;

    @Mock
    private Connection mockConnection;

    @Mock
    private Statement mockStatement;

    @Mock
    private ResultSet mockResultSet;

    @Mock
    private DynamicConfig mockDynamicConfig;

    @Before
    public void setUp() throws SQLException {
        Mockito.when(mockConnectionSupplier.get()).thenReturn(mockConnection);
        Mockito.when(mockConnection.createStatement()).thenReturn(mockStatement);
        Mockito.when(mockStatement.getResultSet()).thenReturn(mockResultSet);
    }

    @Test
    public void testGetGenSqlWithNewVersion() {
        try (MockedStatic<DynamicConfig> mockedDynamicConfig = Mockito.mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);

            // Mock new version detection
            Mockito.when(mockResultSet.next()).thenReturn(true);

            String result = target.getGenSql(mockConnectionSupplier, 10L, "test_key");

            Assert.assertNotNull("Generated SQL should not be null", result);
            Assert.assertTrue("Should use new version SQL format",
                result.contains("add_ccl_rule_returning"));
            Assert.assertTrue("Should contain concurrency parameter", result.contains("10"));
            Assert.assertTrue("Should contain key parameter", result.contains("test_key"));
        } catch (SQLException e) {
            Assert.fail("Should not throw SQLException: " + e.getMessage());
        }
    }

    @Test
    public void testGetGenSqlWithOldVersion() {
        try (MockedStatic<DynamicConfig> mockedDynamicConfig = Mockito.mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);

            // Mock old version detection (no result returned)
            Mockito.when(mockResultSet.next()).thenReturn(false);

            String result = target.getGenSql(mockConnectionSupplier, 5L, "old_key");

            Assert.assertNotNull("Generated SQL should not be null", result);
            Assert.assertFalse("Should not use new version SQL format",
                result.contains("add_ccl_rule_returning"));
            Assert.assertTrue("Should use old version SQL format",
                result.contains("add_ccl_rule"));
            Assert.assertTrue("Should contain concurrency parameter", result.contains("5"));
            Assert.assertTrue("Should contain key parameter", result.contains("old_key"));
        } catch (SQLException e) {
            Assert.fail("Should not throw SQLException: " + e.getMessage());
        }
    }

    @Test
    public void testKillAndGenerateCclSuccess() throws SQLException {
        // Setup test data
        List<DubiousItem> toKill = new ArrayList<>();
        toKill.add(new DubiousItem(1L, "SELECT * FROM test", 1000L));
        toKill.add(new DubiousItem(2L, "SELECT * FROM test2", 2000L));

        String genSql = "call dbms_ccl.add_ccl_rule_returning('SELECT', '', '', 10, 'test_key')";

        // Mock successful execution
        Mockito.when(mockResultSet.next()).thenReturn(true);
        Mockito.when(mockResultSet.getLong("RULE_ID")).thenReturn(456L);

        Long result = target.killAndGenerateCcl(mockConnectionSupplier, toKill, true, genSql);

        Assert.assertEquals("Should return correct rule ID", Long.valueOf(456L), result);

        // Verify kill statements were executed
        Mockito.verify(mockStatement).execute("kill 1");
        Mockito.verify(mockStatement).execute("kill 2");
        Mockito.verify(mockStatement).execute(genSql);
    }

    @Test
    public void testKillAndGenerateCclWithoutGeneration() throws SQLException {
        // Setup test data
        List<DubiousItem> toKill = new ArrayList<>();
        toKill.add(new DubiousItem(3L, "SELECT * FROM test3", 3000L));

        String genSql = "call dbms_ccl.add_ccl_rule('SELECT', '', '', 5, 'test_key2')";

        Long result = target.killAndGenerateCcl(mockConnectionSupplier, toKill, false, genSql);

        Assert.assertEquals("Should return -1 when not generating", Long.valueOf(-1L), result);

        // Verify only kill statement was executed, not generation
        Mockito.verify(mockStatement).execute("kill 3");
        Mockito.verify(mockStatement, Mockito.never()).execute(genSql);
    }

    @Test
    public void testKillAndGenerateCclWithNullConnection() throws SQLException {
        // Mock null connection
        Mockito.when(mockConnectionSupplier.get()).thenReturn(null);

        List<DubiousItem> toKill = new ArrayList<>();
        toKill.add(new DubiousItem(4L, "SELECT * FROM test4", 4000L));

        Long result = target.killAndGenerateCcl(mockConnectionSupplier, toKill, true, "test_sql");

        Assert.assertEquals("Should return -1 when connection is null", Long.valueOf(-1L), result);
    }

    @Test
    public void testDeleteDnCCLWithNewVersion() throws SQLException {
        // Setup test data
        HashMap<String, Long> cclRuleKey = new HashMap<>();
        cclRuleKey.put("key1", 100L);
        cclRuleKey.put("key2", 200L);

        // Mock new version detection
        Mockito.when(mockResultSet.next()).thenReturn(true);

        target.deleteDnCCL(mockConnectionSupplier, cclRuleKey);

        // Verify batch delete was called
        Mockito.verify(mockStatement).execute("call dbms_ccl.del_ccl_rule_batch('100,200')");
    }

    @Test
    public void testDeleteDnCCLWithOldVersion() throws SQLException {
        // Setup test data
        HashMap<String, Long> cclRuleKey = new HashMap<>();
        cclRuleKey.put("test_key", 300L);

        // Mock old version detection
        Mockito.when(mockResultSet.next()).thenReturn(false).thenReturn(true).thenReturn(false);
        Mockito.when(mockResultSet.getString("KEYWORDS")).thenReturn("test_key");
        Mockito.when(mockResultSet.getLong("ID")).thenReturn(300L);

        target.deleteDnCCL(mockConnectionSupplier, cclRuleKey);

        // Verify show and individual delete were called
        Mockito.verify(mockStatement).execute("call dbms_ccl.show_ccl_rule()");
        Mockito.verify(mockStatement).execute("call dbms_ccl.del_ccl_rule(300)");
    }

    @Test
    public void testDeleteAllDnCCLWithOldVersion() throws SQLException {
        // Create a separate connection supplier for old version test
        Supplier<Connection> oldVersionConnectionSupplier = () -> {
            try {
                Connection conn = Mockito.mock(Connection.class);
                Statement stmt = Mockito.mock(Statement.class);
                ResultSet rs = Mockito.mock(ResultSet.class);

                Mockito.when(conn.createStatement()).thenReturn(stmt);
                Mockito.when(stmt.getResultSet()).thenReturn(rs);

                // Mock version check to return false (old version)
                Mockito.when(rs.next()).thenReturn(false).thenReturn(true).thenReturn(true).thenReturn(false);

                return conn;
            } catch (SQLException e) {
                throw new RuntimeException(e);
            }
        };

        boolean result = CCLDetectDnActor.deleteAllDnCCL(oldVersionConnectionSupplier);

        Assert.assertTrue("Should return true on successful deletion", result);
    }

    @Test
    public void testGetCclConcurrencyWithNewVersion() {
        try (MockedStatic<DynamicConfig> mockedDynamicConfig = Mockito.mockStatic(DynamicConfig.class)) {

//            // Mock new version detection
//            Mockito.when(mockResultSet.next()).thenReturn(true);

            // Math.max(0, 16 >> (2 + 1)) = Math.max(0, 16 >> 3) = Math.max(0, 2) = 2
            long result = target.getCclConcurrency(16, 2);

            Assert.assertEquals("Should return correct concurrency for new version", 2L, result);
        }
    }

    @Test
    public void testGetActiveSessionNumSuccess() throws SQLException {
        try (MockedStatic<DynamicConfig> mockedDynamicConfig = Mockito.mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);
            Mockito.when(mockDynamicConfig.getCclDetectLevel()).thenReturn("dql,dml,ddl");

            // Mock successful query
            Mockito.when(mockResultSet.next()).thenReturn(true);
            Mockito.when(mockResultSet.getLong(1)).thenReturn(15L);

            long result = target.getActiveSessionNum(mockConnectionSupplier, "inst1", "storage1");

            Assert.assertEquals("Should return correct active session count", 15L, result);
        }
    }

    @Test
    public void testGetActiveSessionNumWithHighLevel() throws SQLException {
        try (MockedStatic<DynamicConfig> mockedDynamicConfig = Mockito.mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);
            Mockito.when(mockDynamicConfig.getCclDetectLevel()).thenReturn("dql,dml,ddl");

            // Mock successful query with SELECT filter
            Mockito.when(mockResultSet.next()).thenReturn(true);
            Mockito.when(mockResultSet.getLong(1)).thenReturn(8L);

            long result = target.getActiveSessionNum(mockConnectionSupplier, "inst2", "storage2");

            Assert.assertEquals("Should return correct active session count with SELECT filter", 8L, result);
        }
    }

    @Test
    public void testGetActiveSessionNumWithNullConnection() {
        // Mock null connection
        Mockito.when(mockConnectionSupplier.get()).thenReturn(null);

        long result = target.getActiveSessionNum(mockConnectionSupplier, "inst3", "storage3");

        Assert.assertEquals("Should return -1 when connection is null", -1L, result);
    }

    @Test
    public void testGetActiveSessionSuccess() throws SQLException {
        // Mock successful query
        Mockito.when(mockResultSet.next()).thenReturn(true).thenReturn(true).thenReturn(false);
        Mockito.when(mockResultSet.getLong(1)).thenReturn(101L).thenReturn(102L);
        Mockito.when(mockResultSet.getString(2)).thenReturn("SELECT * FROM test1").thenReturn("SELECT * FROM test2");
        Mockito.when(mockResultSet.getLong(3)).thenReturn(1500L).thenReturn(2500L);

        List<DubiousItem> result = target.getActiveSession(mockConnectionSupplier, "storage4");

        Assert.assertNotNull("Result should not be null", result);
        Assert.assertEquals("Should return 1 items", 1, result.size());
    }

    @Test
    public void testGetActiveSessionWithNullConnection() {
        // Mock null connection
        Mockito.when(mockConnectionSupplier.get()).thenReturn(null);

        List<DubiousItem> result = target.getActiveSession(mockConnectionSupplier, "storage5");

        Assert.assertNull("Should return null when connection is null", result);
    }

    @Test
    public void testHasDdlCheckerWithDDLPresent() throws SQLException {
        try (MockedStatic<DynamicConfig> mockedDynamicConfig = Mockito.mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);
            Mockito.when(mockDynamicConfig.getCclDetectLevel()).thenReturn("ddl,dml,dql");

            // Mock DDL lock present
            Mockito.when(mockResultSet.next()).thenReturn(true);
            Mockito.when(mockResultSet.getString("object_schema")).thenReturn("test_schema");
            Mockito.when(mockResultSet.getString("object_name")).thenReturn("test_table");
            Mockito.when(mockResultSet.getString("object_type")).thenReturn("TABLE");
            Mockito.when(mockResultSet.getString("lock_type")).thenReturn("EXCLUSIVE");
            Mockito.when(mockResultSet.getString("lock_status")).thenReturn("GRANTED");
            Mockito.when(mockResultSet.getLong("owner_thread_id")).thenReturn(12345L);

            boolean result = target.hasDdlChecker(mockConnectionSupplier, "storage6");

            Assert.assertFalse("Should return false when DDL is present and level >= 2", result);
        }
    }

    @Test
    public void testHasDdlCheckerWithNoDDL() throws SQLException {
        try (MockedStatic<DynamicConfig> mockedDynamicConfig = Mockito.mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);
            Mockito.when(mockDynamicConfig.getCclDetectLevel()).thenReturn("ddl,dml,dql");

            // Mock no DDL lock
            Mockito.when(mockResultSet.next()).thenReturn(false);

            boolean result = target.hasDdlChecker(mockConnectionSupplier, "storage7");

            Assert.assertTrue("Should return true when no DDL is present", result);
        }
    }

    @Test
    public void testGetDnCclRulesWithNullConnection() {
        // Mock null connection
        Mockito.when(mockConnectionSupplier.get()).thenReturn(null);

        List<DnCclRecord> result = CCLDetectDnActor.getDnCclRules(mockConnectionSupplier, "inst9", "storage9");

        Assert.assertNull("Should return null when connection is null", result);
    }

    @Test
    public void testIsNewVersionTrue() throws SQLException {
        // Mock new version detection success
        Mockito.when(mockResultSet.next()).thenReturn(true);

        boolean result = CCLDetectDnActor.isNewVersion(mockConnectionSupplier);

        Assert.assertTrue("Should return true for new version", result);

        // Verify cleanup was called
        Mockito.verify(mockStatement).execute("select count(*) from information_schema.milli_processlist");
    }

    @Test
    public void testIsNewVersionFalse() throws SQLException {
        // Mock old version detection (no result)
        Mockito.when(mockResultSet.next()).thenReturn(false);

        boolean result = CCLDetectDnActor.isNewVersion(mockConnectionSupplier);

        Assert.assertFalse("Should return false for old version", result);
    }

    @Test
    public void testIsNewVersionWithException() throws SQLException {
        // Mock exception during version check
        Mockito.when(mockStatement.execute(Mockito.anyString())).thenThrow(new SQLException("Test exception"));

        boolean result = CCLDetectDnActor.isNewVersion(mockConnectionSupplier);

        Assert.assertFalse("Should return false when exception occurs", result);
    }

    // ==================== Connection leak regression tests (AONE-84879356) ====================
    // CCLDetectDnActor obtains the DN physical connection via connectionSupplier.get() but
    // never closes it. The following tests assert leaderConn.close() is invoked exactly once
    // for both the normal path and the exception path. Under current (unfixed) code these
    // assertions fail because the connection is leaked, which reproduces the DN connection
    // leak reported in the work item.

    @Test
    public void testKillAndGenerateCclClosesConnectionOnSuccess() throws SQLException {
        List<DubiousItem> toKill = new ArrayList<>();
        toKill.add(new DubiousItem(1L, "SELECT * FROM test", 1000L));

        String genSql = "call dbms_ccl.add_ccl_rule_returning('SELECT', '', '', 10, 'test_key')";
        Mockito.when(mockResultSet.next()).thenReturn(true);
        Mockito.when(mockResultSet.getLong("RULE_ID")).thenReturn(456L);

        target.killAndGenerateCcl(mockConnectionSupplier, toKill, true, genSql);

        Mockito.verify(mockConnection, Mockito.times(1)).close();
    }

    @Test
    public void testKillAndGenerateCclClosesConnectionOnException() throws SQLException {
        List<DubiousItem> toKill = new ArrayList<>();
        toKill.add(new DubiousItem(1L, "SELECT * FROM test", 1000L));

        Mockito.when(mockStatement.execute(Mockito.anyString())).thenThrow(new SQLException("boom"));

        target.killAndGenerateCcl(mockConnectionSupplier, toKill, true, "any sql");

        Mockito.verify(mockConnection, Mockito.times(1)).close();
    }

    @Test
    public void testGenerateCclClosesConnectionOnSuccess() throws SQLException {
        String genSql = "call dbms_ccl.add_ccl_rule_returning('SELECT', '', '', 10, 'test_key')";
        Mockito.when(mockResultSet.next()).thenReturn(true);
        Mockito.when(mockResultSet.getLong("RULE_ID")).thenReturn(456L);

        target.generateCcl(mockConnectionSupplier, genSql);

        Mockito.verify(mockConnection, Mockito.times(1)).close();
    }

    @Test
    public void testGenerateCclClosesConnectionOnException() throws SQLException {
        Mockito.when(mockStatement.execute(Mockito.anyString())).thenThrow(new SQLException("boom"));

        target.generateCcl(mockConnectionSupplier, "any sql");

        Mockito.verify(mockConnection, Mockito.times(1)).close();
    }

    @Test
    public void testKillAndGenerateCclClosesConnectionWhenStatementCloseFails() throws SQLException {
        List<DubiousItem> toKill = new ArrayList<>();
        toKill.add(new DubiousItem(1L, "SELECT * FROM test", 1000L));

        Mockito.when(mockResultSet.next()).thenReturn(true);
        Mockito.when(mockResultSet.getLong("RULE_ID")).thenReturn(456L);
        Mockito.doThrow(new SQLException("stmt close failed")).when(mockStatement).close();

        target.killAndGenerateCcl(mockConnectionSupplier, toKill, true, "any sql");

        Mockito.verify(mockConnection, Mockito.times(1)).close();
    }

    @Test
    public void testGenerateCclClosesConnectionWhenStatementCloseFails() throws SQLException {
        Mockito.when(mockResultSet.next()).thenReturn(true);
        Mockito.when(mockResultSet.getLong("RULE_ID")).thenReturn(456L);
        Mockito.doThrow(new SQLException("stmt close failed")).when(mockStatement).close();

        target.generateCcl(mockConnectionSupplier, "any sql");

        Mockito.verify(mockConnection, Mockito.times(1)).close();
    }

    @Test
    public void testDeleteDnCCLWithKillClosesConnection() throws SQLException {
        Mockito.when(mockResultSet.next()).thenReturn(false);

        target.deleteDnCCLWithKill(mockConnectionSupplier, Pair.of("test_key", 100L));

        Mockito.verify(mockConnection, Mockito.times(1)).close();
    }

    @Test
    public void testDeleteDnCCLClosesConnection() throws SQLException {
        HashMap<String, Long> cclRuleKey = new HashMap<>();
        cclRuleKey.put("key1", 100L);
        Mockito.when(mockResultSet.next()).thenReturn(true);

        target.deleteDnCCL(mockConnectionSupplier, cclRuleKey);

        Mockito.verify(mockConnection, Mockito.times(1)).close();
    }

    @Test
    public void testDeleteAllDnCCLClosesConnection() throws SQLException {
        Mockito.when(mockResultSet.next()).thenReturn(true);

        CCLDetectDnActor.deleteAllDnCCL(mockConnectionSupplier);

        Mockito.verify(mockConnection, Mockito.times(1)).close();
    }

    @Test
    public void testGetActiveSessionNumClosesConnection() throws SQLException {
        try (MockedStatic<DynamicConfig> mockedDynamicConfig = Mockito.mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);
            Mockito.when(mockDynamicConfig.getCclDetectLevel()).thenReturn("dql,dml,ddl");
            Mockito.when(mockResultSet.next()).thenReturn(true);
            Mockito.when(mockResultSet.getLong(1)).thenReturn(15L);

            target.getActiveSessionNum(mockConnectionSupplier, "inst1", "storage1");

            Mockito.verify(mockConnection, Mockito.times(1)).close();
        }
    }

    @Test
    public void testGetActiveSessionClosesConnection() throws SQLException {
        Mockito.when(mockResultSet.next()).thenReturn(false);

        target.getActiveSession(mockConnectionSupplier, "storage4");

        Mockito.verify(mockConnection, Mockito.times(1)).close();
    }

    @Test
    public void testHasDdlCheckerClosesConnection() throws SQLException {
        try (MockedStatic<DynamicConfig> mockedDynamicConfig = Mockito.mockStatic(DynamicConfig.class)) {
            mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);
            Mockito.when(mockDynamicConfig.getCclDetectLevel()).thenReturn("ddl,dml,dql");
            Mockito.when(mockResultSet.next()).thenReturn(false);

            target.hasDdlChecker(mockConnectionSupplier, "storage6");

            Mockito.verify(mockConnection, Mockito.times(1)).close();
        }
    }

    @Test
    public void testGetDnCclRulesClosesConnection() throws SQLException {
        Mockito.when(mockResultSet.next()).thenReturn(false);

        CCLDetectDnActor.getDnCclRules(mockConnectionSupplier, "inst9", "storage9");

        Mockito.verify(mockConnection, Mockito.times(1)).close();
    }

    @Test
    public void testIsNewVersionClosesConnection() throws SQLException {
        Mockito.when(mockResultSet.next()).thenReturn(true);

        CCLDetectDnActor.isNewVersion(mockConnectionSupplier);

        Mockito.verify(mockConnection, Mockito.times(1)).close();
    }
}