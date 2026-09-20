package com.alibaba.polardbx.qatest.dml.auto.externalized;

import com.alibaba.polardbx.qatest.CommonCaseRunner;
import com.alibaba.polardbx.qatest.ExternalizedColumnTestBase;

import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Comprehensive tests for externalized columns accessed via GSI paths.
 *
 * <p>Covers all 6 GSI/clustered index creation methods + non-covering GSI,
 * with full DML lifecycle (backfill → INSERT → UPDATE → DELETE) and
 * complex query scenarios (JOIN, aggregate, subquery, ORDER BY, etc.).
 *
 * <p>FETCH_BLOB injection: unified through
 * {@code ToDrdsRelVisitor.resolveExternalizedColumns()} —
 * applied above LogicalView (main / GSI), LogicalTableLookup, and OSSTableScan.
 */
@RunWith(CommonCaseRunner.class)
public class ExternalizedColumnGsiTest extends ExternalizedColumnTestBase {

    private static final String DB_SUFFIX = UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    private static final String AUTO_DB = "ext_gsi_auto_" + DB_SUFFIX;
    private static final String DRDS_DB = "ext_gsi_drds_" + DB_SUFFIX;

    public ExternalizedColumnGsiTest(DatabaseMode databaseMode) {
        super(databaseMode);
    }

    @Parameterized.Parameters(name = "{index}:mode={0}")
    public static List<Object[]> parameters() {
        return autoAndDrdsModes();
    }

    @BeforeClass
    public static void initClassDb() throws SQLException {
        createIsolatedDatabase(AUTO_DB, DatabaseMode.AUTO);
        createIsolatedDatabase(DRDS_DB, DatabaseMode.DRDS);
    }

    @AfterClass
    public static void dropClassDb() {
        dropIsolatedDatabase(AUTO_DB);
        dropIsolatedDatabase(DRDS_DB);
    }

    private static final String NO_DIRECT =
        "/*+TDDL:cmd_extra(ENABLE_DIRECT_PLAN=false)*/";

    private static final String T_INLINE_GSI = "t_ext_gsi_inline";
    private static final String T_STANDALONE_GSI = "t_ext_gsi_standalone";
    private static final String T_ALTER_GSI = "t_ext_gsi_alter";
    private static final String T_INLINE_CLUSTERED = "t_ext_gsi_inline_clust";
    private static final String T_STANDALONE_CLUSTERED = "t_ext_gsi_standalone_clust";
    private static final String T_ALTER_CLUSTERED = "t_ext_gsi_alter_clust";
    private static final String T_NONCOV_GSI = "t_ext_gsi_noncov";

    // DRDS exposes a GSI as a schema-level logical table, so index names must be unique in the database.
    private static final String I_INLINE_GSI = "idx_inline_name";
    private static final String I_STANDALONE_GSI = "idx_standalone_name";
    private static final String I_ALTER_GSI = "idx_alter_name";
    private static final String I_INLINE_CLUSTERED = "idx_inline_clust_name";
    private static final String I_STANDALONE_CLUSTERED = "idx_standalone_clust_name";
    private static final String I_ALTER_CLUSTERED = "idx_alter_clust_name";
    private static final String I_NONCOV_GSI = "idx_category";

    private static final Set<DatabaseMode> TABLES_CREATED =
        Collections.synchronizedSet(EnumSet.noneOf(DatabaseMode.class));

    private static final String[] ALL_TABLES = {
        T_INLINE_GSI, T_STANDALONE_GSI, T_ALTER_GSI,
        T_INLINE_CLUSTERED, T_STANDALONE_CLUSTERED, T_ALTER_CLUSTERED, T_NONCOV_GSI};

    private final String tInlineGsi = T_INLINE_GSI;
    private final String tStandaloneGsi = T_STANDALONE_GSI;
    private final String tAlterGsi = T_ALTER_GSI;
    private final String tInlineClustered = T_INLINE_CLUSTERED;
    private final String tStandaloneClustered = T_STANDALONE_CLUSTERED;
    private final String tAlterClustered = T_ALTER_CLUSTERED;
    private final String tNoncovGsi = T_NONCOV_GSI;

    @Before
    public void setUp() throws SQLException {
        // Bind tddlConnection to a URL-bound connection on our isolated DB.
        this.tddlConnection = ConnectionManager.getInstance().newPolarDBXConnection(classDatabase());

        JdbcUtil.executeSuccess(tddlConnection, "SET SESSION WORKLOAD_TYPE=TP");
        synchronized (TABLES_CREATED) {
            if (!TABLES_CREATED.contains(databaseMode)) {
                for (String t : ALL_TABLES) {
                    dropTestTable(tddlConnection, t);
                }

                // 1. Inline GSI COVERING
                JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                    "CREATE TABLE %s ("
                        + "  id BIGINT NOT NULL AUTO_INCREMENT,"
                        + "  name VARCHAR(64),"
                        + "  category VARCHAR(32),"
                        + "  content LONGTEXT EXTERNALIZE,"
                        + "  PRIMARY KEY (id),"
                        + "  GLOBAL INDEX " + I_INLINE_GSI + "(name) COVERING(content)%s"
                        + ") DEFAULT CHARSET=utf8mb4"
                        + "%s",
                    tInlineGsi, tableDistribution("name", 3), tableDistribution("id", 3)));

                // 2. Standalone CREATE GLOBAL INDEX COVERING
                createBaseTable(tStandaloneGsi);
                JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                    "CREATE GLOBAL INDEX " + I_STANDALONE_GSI + " ON %s(name) COVERING(content)%s",
                    tStandaloneGsi, tableDistribution("name", 3)));

                // 3. ALTER TABLE ADD GLOBAL INDEX COVERING
                createBaseTable(tAlterGsi);
                JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                    "ALTER TABLE %s ADD GLOBAL INDEX " + I_ALTER_GSI + "(name) COVERING(content)%s",
                    tAlterGsi, tableDistribution("name", 3)));

                // 4. Inline CLUSTERED INDEX
                JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                    "CREATE TABLE %s ("
                        + "  id BIGINT NOT NULL AUTO_INCREMENT,"
                        + "  name VARCHAR(64),"
                        + "  category VARCHAR(32),"
                        + "  content LONGTEXT EXTERNALIZE,"
                        + "  PRIMARY KEY (id),"
                        + "  CLUSTERED INDEX " + I_INLINE_CLUSTERED + "(name)%s"
                        + ") DEFAULT CHARSET=utf8mb4"
                        + "%s",
                    tInlineClustered, tableDistribution("name", 3), tableDistribution("id", 3)));

                // 5. Standalone CREATE CLUSTERED INDEX
                createBaseTable(tStandaloneClustered);
                JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                    "CREATE CLUSTERED INDEX " + I_STANDALONE_CLUSTERED + " ON %s(name)%s",
                    tStandaloneClustered, tableDistribution("name", 3)));

                // 6. ALTER TABLE ADD CLUSTERED INDEX
                createBaseTable(tAlterClustered);
                JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                    "ALTER TABLE %s ADD CLUSTERED INDEX " + I_ALTER_CLUSTERED + "(name)%s",
                    tAlterClustered, tableDistribution("name", 3)));

                // 7. Non-covering GSI (idx_category does NOT cover content)
                JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                    "CREATE TABLE %s ("
                        + "  id BIGINT NOT NULL AUTO_INCREMENT,"
                        + "  name VARCHAR(64),"
                        + "  category VARCHAR(32),"
                        + "  content LONGTEXT EXTERNALIZE,"
                        + "  PRIMARY KEY (id),"
                        + "  GLOBAL INDEX " + I_NONCOV_GSI + "(category)%s"
                        + ") DEFAULT CHARSET=utf8mb4"
                        + "%s",
                    tNoncovGsi, tableDistribution("category", 3), tableDistribution("id", 3)));

                TABLES_CREATED.add(databaseMode);
            }
        }

        // Reset data before each test
        for (String t : ALL_TABLES) {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("DELETE FROM %s WHERE 1=1", t));
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "INSERT INTO %s (id, name, category, content) VALUES "
                    + "(1, 'alice', 'tech', 'Alice writes about technology'), "
                    + "(2, 'bob', 'science', 'Bob explores science topics'), "
                    + "(3, 'charlie', 'tech', 'Charlie discusses tech trends')",
                t));
        }
    }

    @After
    public void tearDown() {
        // Tables are shared across tests; just close our isolated-DB connection.
        if (tddlConnection != null) {
            try {
                tddlConnection.close();
            } catch (SQLException ignore) {
                // best-effort
            }
            tddlConnection = null;
        }
    }

    // ==================== DML Lifecycle: Inline GSI COVERING ====================

    @Test
    public void testInlineGsiCoveringLifecycle() throws SQLException {
        dmlLifecycleCovering(tInlineGsi, I_INLINE_GSI);
    }

    // ==================== DML Lifecycle: Standalone CREATE GLOBAL INDEX COVERING ====================

    @Test
    public void testStandaloneGsiCoveringLifecycle() throws SQLException {
        dmlLifecycleCovering(tStandaloneGsi, I_STANDALONE_GSI);
    }

    // ==================== DML Lifecycle: ALTER TABLE ADD GLOBAL INDEX COVERING ====================

    @Test
    public void testAlterGsiCoveringLifecycle() throws SQLException {
        dmlLifecycleCovering(tAlterGsi, I_ALTER_GSI);
    }

    // ==================== DML Lifecycle: Inline CLUSTERED INDEX ====================

    @Test
    public void testInlineClusteredLifecycle() throws SQLException {
        dmlLifecycleCovering(tInlineClustered, I_INLINE_CLUSTERED);
    }

    // ==================== DML Lifecycle: Standalone CREATE CLUSTERED INDEX ====================

    @Test
    public void testStandaloneClusteredLifecycle() throws SQLException {
        dmlLifecycleCovering(tStandaloneClustered, I_STANDALONE_CLUSTERED);
    }

    // ==================== DML Lifecycle: ALTER TABLE ADD CLUSTERED INDEX ====================

    @Test
    public void testAlterClusteredLifecycle() throws SQLException {
        dmlLifecycleCovering(tAlterClustered, I_ALTER_CLUSTERED);
    }

    // ==================== DML Lifecycle: Non-covering GSI (BKAJoin path) ====================

    @Test
    public void testNonCoveringGsiLifecycle() throws SQLException {
        String table = tNoncovGsi;
        String index = I_NONCOV_GSI;

        // 1. Verify initial data via FORCE INDEX (non-covering → BKAJoin)
        ResultSet rs = query(String.format(
            "SELECT id, content FROM %s FORCE INDEX(%s) WHERE category = 'tech' ORDER BY id",
            table, index));
        assertRow(rs, 1, "Alice writes about technology");
        assertRow(rs, 3, "Charlie discusses tech trends");
        Assert.assertFalse(rs.next());
        rs.close();

        // 2. INSERT new row
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, category, content) VALUES (4, 'dave', 'tech', 'Dave codes all day')",
            table));
        rs = query(String.format(
            "SELECT id, content FROM %s FORCE INDEX(%s) WHERE category = 'tech' ORDER BY id", table, index));
        assertRow(rs, 1, "Alice writes about technology");
        assertRow(rs, 3, "Charlie discusses tech trends");
        assertRow(rs, 4, "Dave codes all day");
        Assert.assertFalse(rs.next());
        rs.close();

        // 3. UPDATE
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "UPDATE %s SET content = 'Alice updated her post' WHERE id = 1", table));
        rs = query(String.format(
            "SELECT content FROM %s FORCE INDEX(%s) WHERE category = 'tech' AND id = 1", table, index));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Alice updated her post", rs.getString("content"));
        rs.close();

        // 4. DELETE
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "DELETE FROM %s WHERE id = 3", table));
        rs = query(String.format(
            "SELECT id FROM %s FORCE INDEX(%s) WHERE category = 'tech' ORDER BY id", table, index));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getLong("id"));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(4, rs.getLong("id"));
        Assert.assertFalse(rs.next());
        rs.close();

        // 5. EXPLAIN shows BKAJoin (non-covering requires table lookup)
        String plan = explainPlan(String.format(
            "SELECT content FROM %s FORCE INDEX(%s) WHERE category = 'tech'", table, index));
        Assert.assertTrue("Non-covering GSI should use BKAJoin", plan.contains("BKAJoin"));
        Assert.assertTrue("Should have FETCH_BLOB", plan.contains("FETCH_BLOB"));
    }

    // ==================== Complex Queries ====================

    @Test
    public void testCoveringGsiComplexQueries() throws SQLException {
        String covTable = tInlineGsi;
        String covIdx = I_INLINE_GSI;
        String ncTable = tNoncovGsi;
        String ncIdx = I_NONCOV_GSI;

        // Expression: LENGTH on externalized column via covering GSI
        ResultSet rs = query(String.format(
            "SELECT LENGTH(content) as len FROM %s FORCE INDEX(%s) WHERE name = 'alice'",
            covTable, covIdx));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Alice writes about technology".length(), rs.getInt("len"));
        rs.close();

        // ORDER BY externalized column
        rs = query(String.format(
            "SELECT name, content FROM %s FORCE INDEX(%s) WHERE name IN ('alice', 'charlie') ORDER BY content",
            covTable, covIdx));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("alice", rs.getString("name")); // "Alice..." < "Charlie..."
        Assert.assertTrue(rs.next());
        Assert.assertEquals("charlie", rs.getString("name"));
        Assert.assertFalse(rs.next());
        rs.close();

        // LIMIT
        rs = query(String.format(
            "SELECT content FROM %s FORCE INDEX(%s) WHERE name IN ('alice','bob','charlie') ORDER BY name LIMIT 2",
            covTable, covIdx));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Alice writes about technology", rs.getString("content"));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Bob explores science topics", rs.getString("content"));
        Assert.assertFalse(rs.next());
        rs.close();

        // Aggregate: GROUP_CONCAT via non-covering GSI
        rs = query(String.format(
            "SELECT category, COUNT(*) as cnt FROM %s FORCE INDEX(%s) "
                + "WHERE category IN ('tech','science') GROUP BY category ORDER BY category",
            ncTable, ncIdx));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("science", rs.getString("category"));
        Assert.assertEquals(1, rs.getInt("cnt"));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("tech", rs.getString("category"));
        Assert.assertEquals(2, rs.getInt("cnt"));
        Assert.assertFalse(rs.next());
        rs.close();

        // Subquery
        rs = query(String.format(
            "SELECT content FROM %s FORCE INDEX(%s) WHERE name = (SELECT name FROM %s WHERE id = 2)",
            covTable, covIdx, covTable));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Bob explores science topics", rs.getString("content"));
        Assert.assertFalse(rs.next());
        rs.close();

        // Self-join via non-covering GSI
        rs = query(String.format(
            "SELECT a.id, a.content FROM %s a FORCE INDEX(%s) "
                + "JOIN %s b ON a.id = b.id WHERE a.category = 'tech' AND b.name = 'alice'",
            ncTable, ncIdx, ncTable));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getLong("id"));
        Assert.assertEquals("Alice writes about technology", rs.getString("content"));
        Assert.assertFalse(rs.next());
        rs.close();
    }

    @Test
    public void testCrossTableJoin() throws SQLException {
        // JOIN covering GSI table with non-covering GSI table
        ResultSet rs = query(String.format(
            "SELECT a.name, LENGTH(a.content) as cov_len, b.category, LENGTH(b.content) as nc_len "
                + "FROM %s a FORCE INDEX(" + I_INLINE_GSI + ") "
                + "JOIN %s b FORCE INDEX(" + I_NONCOV_GSI + ") ON a.name = b.name "
                + "WHERE a.name = 'alice' AND b.category = 'tech'",
            tInlineGsi, tNoncovGsi));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("alice", rs.getString("name"));
        Assert.assertEquals("Alice writes about technology".length(), rs.getInt("cov_len"));
        Assert.assertEquals("Alice writes about technology".length(), rs.getInt("nc_len"));
        Assert.assertFalse(rs.next());
        rs.close();
    }

    @Test
    public void testOptimizerAutoChoosePath() throws SQLException {
        // Without FORCE INDEX — optimizer should auto-pick covering GSI for name filter
        ResultSet rs = query(String.format(
            "SELECT name, content FROM %s WHERE name = 'bob'", tInlineGsi));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("bob", rs.getString("name"));
        Assert.assertEquals("Bob explores science topics", rs.getString("content"));
        Assert.assertFalse(rs.next());
        rs.close();
    }

    @Test
    public void testExplainCoveringGsiNoTableLookup() throws SQLException {
        // Covering GSI path should NOT have BKAJoin, just IndexScan + FETCH_BLOB
        String plan = explainPlan(String.format(
            "SELECT content FROM %s FORCE INDEX(" + I_INLINE_GSI + ") WHERE name = 'alice'", tInlineGsi));
        Assert.assertTrue("Covering GSI should show FETCH_BLOB", plan.contains("FETCH_BLOB"));
        Assert.assertFalse("Covering GSI should NOT have BKAJoin (no table lookup needed)",
            plan.contains("BKAJoin"));
        Assert.assertTrue("Covering GSI should use IndexScan", plan.contains("IndexScan"));
    }

    /**
     * Direct SELECT on the GSI index table itself (e.g. {@code SELECT * FROM `idx_name_$xxxx`}).
     * Without the GSI → primary-table resolution in {@code resolveExternalizedColumns},
     * the externalized column would return the raw hex blob address instead of restored content.
     */
    @Test
    public void testDirectSelectOnGsiIndexTable() throws SQLException {
        String gsiPhysicalName = getGsiPhysicalName(tInlineGsi, I_INLINE_GSI);
        Assert.assertNotNull("Failed to resolve GSI physical name", gsiPhysicalName);

        // Read content via main table as the source of truth
        ResultSet rsMain = query(String.format(
            "SELECT content FROM %s WHERE name = 'alice'", tInlineGsi));
        Assert.assertTrue(rsMain.next());
        String expectedContent = rsMain.getString("content");
        rsMain.close();

        // Direct SELECT on the GSI index table must return restored content, not hex blob_addr
        ResultSet rs = query(String.format(
            "SELECT name, content FROM `%s` WHERE name = 'alice'", gsiPhysicalName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("alice", rs.getString("name"));
        String content = rs.getString("content");
        Assert.assertEquals("Direct GSI SELECT must match main table content",
            expectedContent, content);
        Assert.assertFalse("content must not be a hex blob address",
            content.matches("^[0-9a-fA-F]{30,}$"));
        rs.close();

        // LENGTH(content) on GSI must equal main table length (catches non-restored hex case)
        rs = query(String.format(
            "SELECT LENGTH(content) AS len FROM `%s` WHERE name = 'alice'", gsiPhysicalName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(expectedContent.length(), rs.getInt("len"));
        rs.close();
    }

    @Test
    public void testInlineOrdinaryCoveringDoesNotGuessAddrColumn() throws SQLException {
        String table = "t_gsi_addr_collision_"
            + UUID.randomUUID().toString().substring(0, 8).replace("-", "");
        dropTestTable(tddlConnection, table);
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "CREATE TABLE %s ("
                    + "id BIGINT NOT NULL,"
                    + "name VARCHAR(64),"
                    + "content LONGTEXT,"
                    + "content_addr_ VARCHAR(128),"
                    + "PRIMARY KEY (id),"
                    + "GLOBAL INDEX idx_collision(name) COVERING(content)%s"
                    + ")%s",
                table, tableDistribution("name", 2), tableDistribution("id", 2)));

            String gsiPhysicalName = getGsiPhysicalName(table, "idx_collision");
            Assert.assertNotNull("Failed to resolve collision GSI physical name", gsiPhysicalName);
            try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
                "SELECT COUNT(*) FROM metadb.columns "
                    + "WHERE table_schema='%s' AND table_name='%s' AND column_name='content_addr_'",
                classDatabase(), gsiPhysicalName))) {
                Assert.assertTrue(rs.next());
                Assert.assertEquals("Ordinary addr-suffixed column must not be added as an implicit covering",
                    0, rs.getInt(1));
            }
        } finally {
            dropTestTable(tddlConnection, table);
        }
    }

    /**
     * Covering GSI on a table whose externalized column sits in the MIDDLE of the row (not the
     * tail). The GSI backfill INSERT addresses the column by its physical addr name, which the
     * logical index meta cannot resolve; the partition-column scan must skip that slot instead of
     * dereferencing it (regression for the mid-position NPE — every other fixture in this class
     * keeps the externalized column at the tail, where the partition column wins the scan first).
     */
    @Test
    public void testCoveringGsiWithMidPositionExternalizedColumn() throws SQLException {
        String table = "t_ext_gsi_mid_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");
        String index = "i_ext_gsi_mid";
        dropTestTable(tddlConnection, table);
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "CREATE TABLE %s ("
                    + "id BIGINT NOT NULL AUTO_INCREMENT,"
                    + "content LONGTEXT EXTERNALIZE,"
                    + "name VARCHAR(64),"
                    + "category VARCHAR(32),"
                    + "PRIMARY KEY (id)"
                    + ") DEFAULT CHARSET=utf8mb4%s",
                table, tableDistribution("id", 3)));
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "INSERT INTO %s (id, content, name, category) VALUES "
                    + "(1, 'mid alice content', 'alice', 'tech'), "
                    + "(2, 'mid bob content', 'bob', 'science')",
                table));

            // Standalone CREATE drives the GSI backfill — the path that used to NPE.
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "CREATE GLOBAL INDEX %s ON %s(name) COVERING(content)%s",
                index, table, tableDistribution("name", 3)));

            ResultSet rs = query(String.format(
                "SELECT id, content FROM %s FORCE INDEX(%s) WHERE name = 'alice'", table, index));
            assertRow(rs, 1, "mid alice content");
            Assert.assertFalse(rs.next());
            rs.close();

            // Incremental write after backfill keeps the covering GSI consistent.
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "INSERT INTO %s (id, content, name, category) VALUES (3, 'mid carol content', 'carol', 'art')",
                table));
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "UPDATE %s SET content = 'mid bob updated' WHERE id = 2", table));

            rs = query(String.format(
                "SELECT id, content FROM %s FORCE INDEX(%s) WHERE name = 'carol'", table, index));
            assertRow(rs, 3, "mid carol content");
            Assert.assertFalse(rs.next());
            rs.close();

            rs = query(String.format(
                "SELECT content FROM %s FORCE INDEX(%s) WHERE name = 'bob'", table, index));
            Assert.assertTrue(rs.next());
            Assert.assertEquals("mid bob updated", rs.getString("content"));
            Assert.assertFalse(rs.next());
            rs.close();
        } finally {
            dropTestTable(tddlConnection, table);
        }
    }

    @Test
    public void testMceGeneratedExternalizedColumnWithCoveringGsi() throws SQLException {
        String table = "t_ext_gsi_mce_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");
        dropTestTable(tddlConnection, table);
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "CREATE TABLE %s ("
                    + "id BIGINT NOT NULL AUTO_INCREMENT,"
                    + "name VARCHAR(64),"
                    + "category VARCHAR(32),"
                    + "content LONGTEXT,"
                    + "PRIMARY KEY (id)"
                    + ") DEFAULT CHARSET=utf8mb4%s",
                table, tableDistribution("id", 3)));
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "INSERT INTO %s (id, name, category, content) VALUES "
                    + "(1, 'alice', 'tech', 'mce alice'), "
                    + "(2, 'bob', 'science', 'mce bob')",
                table));
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("ALTER TABLE %s MODIFY COLUMN content LONGTEXT EXTERNALIZE", table));
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "ALTER TABLE %s ADD GLOBAL INDEX idx_mce_name(name) COVERING(content)%s",
                table, tableDistribution("name", 3)));

            ResultSet rs = query(String.format(
                "SELECT id, content FROM %s FORCE INDEX(idx_mce_name) WHERE name = 'alice'", table));
            assertRow(rs, 1, "mce alice");
            Assert.assertFalse(rs.next());
            rs.close();

            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("UPDATE %s SET content = 'mce alice updated' WHERE id = 1", table));
            rs = query(String.format(
                "SELECT content FROM %s FORCE INDEX(idx_mce_name) WHERE name = 'alice'", table));
            Assert.assertTrue(rs.next());
            Assert.assertEquals("mce alice updated", rs.getString("content"));
            Assert.assertFalse(rs.next());
            rs.close();

            String plan = explainPlan(String.format(
                "SELECT content FROM %s FORCE INDEX(idx_mce_name) WHERE name = 'alice'", table));
            Assert.assertTrue("MCE-generated covering GSI should restore externalized content",
                plan.contains("FETCH_BLOB"));
        } finally {
            dropTestTable(tddlConnection, table);
        }
    }

    // ==================== Helper Methods ====================

    private void createBaseTable(String table) {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT NOT NULL AUTO_INCREMENT,"
                + "  name VARCHAR(64),"
                + "  category VARCHAR(32),"
                + "  content LONGTEXT EXTERNALIZE,"
                + "  PRIMARY KEY (id)"
                + ") DEFAULT CHARSET=utf8mb4"
                + "%s",
            table, tableDistribution("id", 3)));
    }

    /**
     * Full DML lifecycle test for a covering GSI/clustered index.
     * Tests: initial data → INSERT → UPDATE → DELETE, each step verified via FORCE INDEX.
     */
    private void dmlLifecycleCovering(String table, String index) throws SQLException {
        // 1. Verify initial backfill/insert data via FORCE INDEX (covering scan)
        ResultSet rs = query(String.format(
            "SELECT id, content FROM %s FORCE INDEX(%s) WHERE name = 'alice'", table, index));
        assertRow(rs, 1, "Alice writes about technology");
        Assert.assertFalse(rs.next());
        rs.close();

        rs = query(String.format(
            "SELECT id, content FROM %s FORCE INDEX(%s) WHERE name = 'bob'", table, index));
        assertRow(rs, 2, "Bob explores science topics");
        Assert.assertFalse(rs.next());
        rs.close();

        // 2. INSERT new row → verify via GSI
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, category, content) VALUES (4, 'dave', 'art', 'Dave creates digital art')",
            table));
        rs = query(String.format(
            "SELECT id, content FROM %s FORCE INDEX(%s) WHERE name = 'dave'", table, index));
        assertRow(rs, 4, "Dave creates digital art");
        Assert.assertFalse(rs.next());
        rs.close();

        // 3. UPDATE → verify content changed via GSI
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "UPDATE %s SET content = 'Alice updated her blog' WHERE id = 1", table));
        rs = query(String.format(
            "SELECT content FROM %s FORCE INDEX(%s) WHERE name = 'alice'", table, index));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Alice updated her blog", rs.getString("content"));
        Assert.assertFalse(rs.next());
        rs.close();

        // 4. DELETE → verify row gone from GSI
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "DELETE FROM %s WHERE id = 2", table));
        rs = query(String.format(
            "SELECT id FROM %s FORCE INDEX(%s) WHERE name = 'bob'", table, index));
        Assert.assertFalse("Deleted row should not appear in GSI", rs.next());
        rs.close();

        // 5. EXPLAIN: covering path should show FETCH_BLOB, no BKAJoin
        String plan = explainPlan(String.format(
            "SELECT content FROM %s FORCE INDEX(%s) WHERE name = 'alice'", table, index));
        Assert.assertTrue("Should have FETCH_BLOB in covering scan", plan.contains("FETCH_BLOB"));
    }

    private ResultSet query(String sql) throws SQLException {
        return JdbcUtil.executeQuerySuccess(tddlConnection, NO_DIRECT + " " + sql);
    }

    private String explainPlan(String sql) throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, NO_DIRECT + " EXPLAIN " + sql);
        StringBuilder sb = new StringBuilder();
        while (rs.next()) {
            sb.append(rs.getString(1)).append("\n");
        }
        rs.close();
        return sb.toString();
    }

    private void assertRow(ResultSet rs, long expectedId, String expectedContent) throws SQLException {
        Assert.assertTrue("Expected row with id=" + expectedId, rs.next());
        Assert.assertEquals(expectedId, rs.getLong("id"));
        Assert.assertEquals(expectedContent, rs.getString("content"));
    }

    /**
     * Resolve the physical (hash-suffixed) GSI index table name via SHOW GLOBAL INDEX.
     */
    private String getGsiPhysicalName(String mainTable, String indexName) throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SHOW GLOBAL INDEX FROM " + mainTable);
        try {
            while (rs.next()) {
                String key = rs.getString("KEY_NAME");
                if (key != null && key.startsWith(indexName)) {
                    return key;
                }
            }
        } finally {
            rs.close();
        }
        return null;
    }

    private String classDatabase() {
        return databaseName(AUTO_DB, DRDS_DB);
    }
}
