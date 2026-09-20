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
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * Query pattern coverage for externalized columns with FETCH_BLOB.
 * Tests various SELECT plan shapes: UK lookup, range scan, aggregation,
 * LIMIT/OFFSET, CTE, ORDER BY, DISTINCT, UNION, subquery.
 *
 * <p>All queries exercise FETCH_BLOB on different RelNode topologies to ensure
 * the Project(FETCH_BLOB) node inserted by ToDrdsRelVisitor works
 * correctly regardless of plan shape.
 *
 * <p>Does not require a columnar node.
 */
@RunWith(CommonCaseRunner.class)
public class ExternalizedColumnQueryPatternTest extends ExternalizedColumnTestBase {

    private static final String DB_SUFFIX = UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    private static final String AUTO_DB = "ext_qp_auto_" + DB_SUFFIX;
    private static final String DRDS_DB = "ext_qp_drds_" + DB_SUFFIX;

    public ExternalizedColumnQueryPatternTest(DatabaseMode databaseMode) {
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

    private final String suffix = UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    private final String tableName = "ext_qp_" + suffix;
    private final String tableName2 = "ext_qp2_" + suffix;
    private final String groupTable = "ext_qpg_" + suffix;
    private final String normalTable = "ext_qpn_" + suffix;

    @Before
    public void setUp() throws SQLException {
        // Bind tddlConnection to a URL-bound connection on our isolated DB.
        this.tddlConnection = ConnectionManager.getInstance().newPolarDBXConnection(classDatabase());

        JdbcUtil.executeSuccess(tddlConnection, "SET SESSION WORKLOAD_TYPE=TP");
        dropTestTable(tddlConnection, tableName);
        dropTestTable(tddlConnection, tableName2);
        dropTestTable(tddlConnection, groupTable);
        dropTestTable(tddlConnection, normalTable);
        createTable();
        createNormalTable();
        insertTestData();
    }

    @After
    public void tearDown() {
        try {
            dropTestTable(tddlConnection, tableName);
            dropTestTable(tddlConnection, tableName2);
            dropTestTable(tddlConnection, groupTable);
            dropTestTable(tddlConnection, normalTable);
        } finally {
            if (tddlConnection != null) {
                try {
                    tddlConnection.close();
                } catch (SQLException ignore) {
                    // best-effort
                }
                tddlConnection = null;
            }
        }
    }

    // ========================= UK POINT QUERY =========================

    @Test
    public void testUniqueKeyPointQuery() throws SQLException {
        // Query by UNIQUE KEY (name), verify externalized column returned correctly
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, name, content FROM %s WHERE name = 'alice'", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getLong("id"));
        Assert.assertEquals("Content from Alice", rs.getString("content"));
        Assert.assertFalse(rs.next());
    }

    @Test
    public void testUniqueKeyPointQueryWithFunction() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT name, LENGTH(content) AS len FROM %s WHERE name = 'bob'", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("bob", rs.getString("name"));
        Assert.assertEquals("Content from Bob".length(), rs.getLong("len"));
        Assert.assertFalse(rs.next());
    }

    // ========================= RANGE QUERY =========================

    @Test
    public void testRangeQueryBetween() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, content FROM %s WHERE id BETWEEN 2 AND 4 ORDER BY id", tableName));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(2, rs.getLong("id"));
        Assert.assertEquals("Content from Bob", rs.getString("content"));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(3, rs.getLong("id"));
        Assert.assertEquals("Content from Charlie", rs.getString("content"));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(4, rs.getLong("id"));
        Assert.assertEquals("Content from Dave", rs.getString("content"));

        Assert.assertFalse(rs.next());
    }

    @Test
    public void testRangeQueryGreaterThan() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, LEFT(content, 12) AS preview FROM %s WHERE id > 3 ORDER BY id",
                tableName));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(4, rs.getLong("id"));
        Assert.assertEquals("Content from", rs.getString("preview"));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(5, rs.getLong("id"));
        Assert.assertEquals("Content from", rs.getString("preview"));

        Assert.assertFalse(rs.next());
    }

    @Test
    public void testRangeQueryIn() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, content FROM %s WHERE id IN (1, 3, 5) ORDER BY id", tableName));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getLong("id"));
        Assert.assertEquals("Content from Alice", rs.getString("content"));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(3, rs.getLong("id"));
        Assert.assertEquals("Content from Charlie", rs.getString("content"));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(5, rs.getLong("id"));
        Assert.assertEquals("Content from Eve", rs.getString("content"));

        Assert.assertFalse(rs.next());
    }

    // ========================= AGGREGATE =========================

    @Test
    public void testAggregateCount() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT COUNT(*) AS cnt FROM %s", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(5, rs.getLong("cnt"));
    }

    @Test
    public void testAggregateGroupByWithExtFunction() throws SQLException {
        // Uses groupTable (no UK) to allow duplicate names for grouping
        createGroupTable();
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES "
                + "(10, 'team_a', 'short'), "
                + "(11, 'team_a', 'a bit longer content'), "
                + "(12, 'team_b', 'medium text here'), "
                + "(13, 'team_b', 'very very very long content here')",
            groupTable));

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT name, COUNT(*) AS cnt, MAX(LENGTH(content)) AS max_len "
                + "FROM %s GROUP BY name ORDER BY name", groupTable));

        Assert.assertTrue(rs.next());
        Assert.assertEquals("team_a", rs.getString("name"));
        Assert.assertEquals(2, rs.getLong("cnt"));
        Assert.assertEquals("a bit longer content".length(), rs.getLong("max_len"));

        Assert.assertTrue(rs.next());
        Assert.assertEquals("team_b", rs.getString("name"));
        Assert.assertEquals(2, rs.getLong("cnt"));
        Assert.assertEquals("very very very long content here".length(), rs.getLong("max_len"));

        Assert.assertFalse(rs.next());
    }

    @Test
    public void testAggregateSumLength() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT SUM(LENGTH(content)) AS total_len FROM %s WHERE id <= 5", tableName));
        Assert.assertTrue(rs.next());
        long totalLen = rs.getLong("total_len");
        long expected = "Content from Alice".length()
            + "Content from Bob".length()
            + "Content from Charlie".length()
            + "Content from Dave".length()
            + "Content from Eve".length();
        Assert.assertEquals(expected, totalLen);
    }

    // ========================= LIMIT / OFFSET =========================

    @Test
    public void testLimitOnly() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, content FROM %s ORDER BY id LIMIT 3", tableName));

        List<Long> ids = new ArrayList<>();
        while (rs.next()) {
            ids.add(rs.getLong("id"));
            Assert.assertNotNull(rs.getString("content"));
        }
        Assert.assertEquals(3, ids.size());
        Assert.assertEquals(Long.valueOf(1), ids.get(0));
        Assert.assertEquals(Long.valueOf(2), ids.get(1));
        Assert.assertEquals(Long.valueOf(3), ids.get(2));
    }

    @Test
    public void testLimitOffset() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, content FROM %s ORDER BY id LIMIT 2 OFFSET 2", tableName));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(3, rs.getLong("id"));
        Assert.assertEquals("Content from Charlie", rs.getString("content"));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(4, rs.getLong("id"));
        Assert.assertEquals("Content from Dave", rs.getString("content"));

        Assert.assertFalse(rs.next());
    }

    // ========================= CTE =========================

    @Test
    public void testCteBasic() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format(
                "WITH cte AS (SELECT id, name, content FROM %s WHERE id <= 3) "
                    + "SELECT id, content FROM cte ORDER BY id",
                tableName));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getLong("id"));
        Assert.assertEquals("Content from Alice", rs.getString("content"));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(2, rs.getLong("id"));
        Assert.assertEquals("Content from Bob", rs.getString("content"));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(3, rs.getLong("id"));
        Assert.assertEquals("Content from Charlie", rs.getString("content"));

        Assert.assertFalse(rs.next());
    }

    @Test
    public void testCteWithFunction() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format(
                "WITH cte AS (SELECT id, name, LENGTH(content) AS len FROM %s) "
                    + "SELECT id, len FROM cte WHERE len > 16 ORDER BY id",
                tableName));

        // "Content from Alice" = 18, "Content from Bob" = 16, "Content from Charlie" = 20,
        // "Content from Dave" = 17, "Content from Eve" = 16
        List<Long> ids = new ArrayList<>();
        while (rs.next()) {
            ids.add(rs.getLong("id"));
            Assert.assertTrue(rs.getLong("len") > 16);
        }
        Assert.assertTrue("Should have rows with length > 16", ids.size() > 0);
        Assert.assertTrue(ids.contains(1L)); // Alice = 18
        Assert.assertTrue(ids.contains(3L)); // Charlie = 20
        Assert.assertTrue(ids.contains(4L)); // Dave = 17
    }

    // ========================= ORDER BY =========================

    @Test
    public void testOrderByDesc() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, content FROM %s ORDER BY id DESC LIMIT 3", tableName));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(5, rs.getLong("id"));
        Assert.assertEquals("Content from Eve", rs.getString("content"));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(4, rs.getLong("id"));
        Assert.assertEquals("Content from Dave", rs.getString("content"));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(3, rs.getLong("id"));
        Assert.assertEquals("Content from Charlie", rs.getString("content"));

        Assert.assertFalse(rs.next());
    }

    @Test
    public void testOrderByName() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT name, LEFT(content, 18) AS preview FROM %s ORDER BY name LIMIT 3",
                tableName));

        Assert.assertTrue(rs.next());
        Assert.assertEquals("alice", rs.getString("name"));
        Assert.assertEquals("Content from Alice", rs.getString("preview"));

        Assert.assertTrue(rs.next());
        Assert.assertEquals("bob", rs.getString("name"));
        Assert.assertEquals("Content from Bob", rs.getString("preview"));

        Assert.assertTrue(rs.next());
        Assert.assertEquals("charlie", rs.getString("name"));
        Assert.assertEquals("Content from Charl", rs.getString("preview"));

        Assert.assertFalse(rs.next());
    }

    // ========================= DISTINCT =========================

    @Test
    public void testDistinctOnNonExtCol() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT DISTINCT name FROM %s WHERE id <= 5 ORDER BY name", tableName));

        List<String> names = new ArrayList<>();
        while (rs.next()) {
            names.add(rs.getString("name"));
        }
        Assert.assertEquals(5, names.size());
        Assert.assertEquals("alice", names.get(0));
        Assert.assertEquals("bob", names.get(1));
        Assert.assertEquals("charlie", names.get(2));
        Assert.assertEquals("dave", names.get(3));
        Assert.assertEquals("eve", names.get(4));
    }

    @Test
    public void testDistinctWithExtFunction() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES "
                + "(20, 'dup1', 'same_content'), "
                + "(21, 'dup2', 'same_content'), "
                + "(22, 'dup3', 'different_content')",
            tableName));

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT DISTINCT LEFT(content, 12) AS prefix FROM %s WHERE id >= 20 ORDER BY prefix",
                tableName));

        Set<String> prefixes = new HashSet<>();
        while (rs.next()) {
            prefixes.add(rs.getString("prefix"));
        }
        Assert.assertEquals(2, prefixes.size());
        Assert.assertTrue(prefixes.contains("same_content"));
        Assert.assertTrue(prefixes.contains("different_co"));
    }

    // ========================= UNION =========================

    @Test
    public void testUnionAll() throws SQLException {
        createTable2();
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES "
                + "(1, 'x1', 'Table2 Content X'), "
                + "(2, 'x2', 'Table2 Content Y')",
            tableName2));

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format(
                "SELECT id, content FROM %s WHERE id = 1 "
                    + "UNION ALL "
                    + "SELECT id, content FROM %s WHERE id = 1",
                tableName, tableName2));

        List<String> contents = new ArrayList<>();
        while (rs.next()) {
            contents.add(rs.getString("content"));
        }
        Assert.assertEquals(2, contents.size());
        Assert.assertTrue(contents.contains("Content from Alice"));
        Assert.assertTrue(contents.contains("Table2 Content X"));
    }

    @Test
    public void testUnionDistinct() throws SQLException {
        createTable2();
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES (1, 'y1', 'Content from Alice')",
            tableName2));

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format(
                "SELECT content FROM %s WHERE id = 1 "
                    + "UNION "
                    + "SELECT content FROM %s WHERE id = 1",
                tableName, tableName2));

        List<String> contents = new ArrayList<>();
        while (rs.next()) {
            contents.add(rs.getString("content"));
        }
        Assert.assertEquals(1, contents.size());
        Assert.assertEquals("Content from Alice", contents.get(0));
    }

    // ========================= SUBQUERY =========================

    @Test
    public void testSubqueryInWhere() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format(
                "SELECT id, content FROM %s WHERE id IN "
                    + "(SELECT id FROM %s WHERE name IN ('alice', 'charlie')) ORDER BY id",
                tableName, tableName));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getLong("id"));
        Assert.assertEquals("Content from Alice", rs.getString("content"));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(3, rs.getLong("id"));
        Assert.assertEquals("Content from Charlie", rs.getString("content"));

        Assert.assertFalse(rs.next());
    }

    @Test
    public void testSubqueryInFrom() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format(
                "SELECT t.id, t.content FROM "
                    + "(SELECT id, content FROM %s WHERE id <= 3) t ORDER BY t.id",
                tableName));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getLong("id"));
        Assert.assertEquals("Content from Alice", rs.getString("content"));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(2, rs.getLong("id"));
        Assert.assertEquals("Content from Bob", rs.getString("content"));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(3, rs.getLong("id"));
        Assert.assertEquals("Content from Charlie", rs.getString("content"));

        Assert.assertFalse(rs.next());
    }

    @Test
    public void testCorrelatedSubquery() throws SQLException {
        createTable2();
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES "
                + "(1, 'alice', 'Table2 Alice'), "
                + "(3, 'charlie', 'Table2 Charlie')",
            tableName2));

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format(
                "SELECT a.id, a.content FROM %s a "
                    + "WHERE EXISTS (SELECT 1 FROM %s b WHERE b.name = a.name) ORDER BY a.id",
                tableName, tableName2));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getLong("id"));
        Assert.assertEquals("Content from Alice", rs.getString("content"));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(3, rs.getLong("id"));
        Assert.assertEquals("Content from Charlie", rs.getString("content"));

        Assert.assertFalse(rs.next());
    }

    // ========================= JOIN =========================

    @Test
    public void testJoinTwoExtTables() throws SQLException {
        createTable2();
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES "
                + "(1, 'alice', 'T2 Alice content'), "
                + "(2, 'bob', 'T2 Bob content')",
            tableName2));

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format(
                "SELECT a.id, a.content AS c1, b.content AS c2 "
                    + "FROM %s a JOIN %s b ON a.name = b.name "
                    + "WHERE a.id <= 2 ORDER BY a.id",
                tableName, tableName2));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getLong("id"));
        Assert.assertEquals("Content from Alice", rs.getString("c1"));
        Assert.assertEquals("T2 Alice content", rs.getString("c2"));

        Assert.assertTrue(rs.next());
        Assert.assertEquals(2, rs.getLong("id"));
        Assert.assertEquals("Content from Bob", rs.getString("c1"));
        Assert.assertEquals("T2 Bob content", rs.getString("c2"));

        Assert.assertFalse(rs.next());
    }

    @Test
    public void testColocatedJoinByPartitionKey() throws SQLException {
        createTable2();
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES "
                + "(1, 'alice', 'T2 Alice content'), "
                + "(2, 'bob', 'T2 Bob content')",
            tableName2));

        String sql = String.format(
            "SELECT a.id, a.content AS c1, b.content AS c2 "
                + "FROM %s a JOIN %s b ON a.id = b.id "
                + "WHERE a.id = 1",
            tableName, tableName2);

        // Verify JOIN is pushed down to DN (co-located join on partition key)
        ResultSet explainRs = JdbcUtil.executeQuerySuccess(tddlConnection, "EXPLAIN " + sql);
        StringBuilder plan = new StringBuilder();
        while (explainRs.next()) {
            plan.append(explainRs.getString(1)).append("\n");
        }
        String planStr = plan.toString().toLowerCase();
        Assert.assertTrue("Expected co-located join pushdown (LogicalView with JOIN), got:\n" + plan,
            planStr.contains("logicalview") && planStr.contains("inner join"));

        // Verify data correctness
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getLong("id"));
        Assert.assertEquals("Content from Alice", rs.getString("c1"));
        Assert.assertEquals("T2 Alice content", rs.getString("c2"));
        Assert.assertFalse(rs.next());
    }

    // ========================= EXT + NORMAL TABLE JOIN (same-named column) =========================

    @Test
    public void testJoinExtAndNormalTableSameColumnName() throws SQLException {
        String normalTable = "ext_qp_normal_" + suffix;
        dropTestTable(tddlConnection, normalTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT NOT NULL AUTO_INCREMENT,"
                + "  name VARCHAR(64),"
                + "  content LONGTEXT,"
                + "  PRIMARY KEY (id)"
                + ") DEFAULT CHARSET=utf8mb4"
                + "%s",
            normalTable, tableDistribution("id", 4)));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES "
                + "(1, 'alice', 'Normal Alice'), "
                + "(2, 'bob', 'Normal Bob'), "
                + "(3, 'charlie', 'Normal Charlie')",
            normalTable));

        try {
            // Bug 1 fix: ext_table.content uses FETCH_BLOB, normal_table.content must NOT be rewritten
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                String.format(
                    "SELECT a.id, a.content AS ext_content, b.content AS normal_content "
                        + "FROM %s a JOIN %s b ON a.id = b.id "
                        + "WHERE a.id <= 3 ORDER BY a.id",
                    tableName, normalTable));

            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getLong("id"));
            Assert.assertEquals("Content from Alice", rs.getString("ext_content"));
            Assert.assertEquals("Normal Alice", rs.getString("normal_content"));

            Assert.assertTrue(rs.next());
            Assert.assertEquals(2, rs.getLong("id"));
            Assert.assertEquals("Content from Bob", rs.getString("ext_content"));
            Assert.assertEquals("Normal Bob", rs.getString("normal_content"));

            Assert.assertTrue(rs.next());
            Assert.assertEquals(3, rs.getLong("id"));
            Assert.assertEquals("Content from Charlie", rs.getString("ext_content"));
            Assert.assertEquals("Normal Charlie", rs.getString("normal_content"));

            Assert.assertFalse(rs.next());
        } finally {
            dropTestTable(tddlConnection, normalTable);
        }
    }

    @Test
    public void testJoinExtAndNormalTableWhereExtColumn() throws SQLException {
        String normalTable = "ext_qp_normal_" + suffix;
        dropTestTable(tddlConnection, normalTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT NOT NULL AUTO_INCREMENT,"
                + "  name VARCHAR(64),"
                + "  content LONGTEXT,"
                + "  PRIMARY KEY (id)"
                + ") DEFAULT CHARSET=utf8mb4"
                + "%s",
            normalTable, tableDistribution("id", 4)));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES "
                + "(1, 'alice', 'Normal Alice'), "
                + "(2, 'bob', 'Normal Bob different length'), "
                + "(3, 'charlie', 'Normal Charlie')",
            normalTable));

        try {
            // Bug 2 fix: FETCH_BLOB in WHERE prevents join pushdown; query must still return correct results
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                String.format(
                    "SELECT COUNT(*) AS cnt FROM %s a JOIN %s b ON a.id = b.id "
                        + "WHERE LENGTH(a.content) != LENGTH(b.content)",
                    tableName, normalTable));

            Assert.assertTrue(rs.next());
            long cnt = rs.getLong("cnt");
            Assert.assertTrue("Expected some rows with different content length, got " + cnt, cnt > 0);
            Assert.assertFalse(rs.next());

            // Verify EXPLAIN shows join NOT pushed to single LogicalView
            ResultSet explainRs = JdbcUtil.executeQuerySuccess(tddlConnection,
                String.format(
                    "EXPLAIN SELECT COUNT(*) FROM %s a JOIN %s b ON a.id = b.id "
                        + "WHERE LENGTH(a.content) != LENGTH(b.content)",
                    tableName, normalTable));
            StringBuilder plan = new StringBuilder();
            while (explainRs.next()) {
                plan.append(explainRs.getString(1)).append("\n");
            }
            String planStr = plan.toString().toLowerCase();
            Assert.assertFalse(
                "FETCH_BLOB in WHERE should prevent join pushdown, but got single LogicalView:\n" + plan,
                planStr.contains("fetch_blob") && planStr.contains("logicalview(tables=\"")
                    && !planStr.contains("hashjoin") && !planStr.contains("bkajoin"));
        } finally {
            dropTestTable(tddlConnection, normalTable);
        }
    }

    @Test
    public void testCrossSchemaJoinSameTableAndColumnName() throws SQLException {
        String schemaB = "ext_qp_cross_" + suffix;
        String sameTableName = tableName; // same logical name as the ext table in current schema

        try {
            // Create a second schema with a same-named table but NO externalized column
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "CREATE DATABASE IF NOT EXISTS " + schemaB + " MODE='" + databaseMode.getDdlMode() + "'");
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "CREATE TABLE %s.%s ("
                    + "  id BIGINT NOT NULL AUTO_INCREMENT,"
                    + "  name VARCHAR(64),"
                    + "  content LONGTEXT,"
                    + "  PRIMARY KEY (id)"
                    + ") DEFAULT CHARSET=utf8mb4"
                    + "%s",
                schemaB, sameTableName, tableDistribution("id", 4)));
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "INSERT INTO %s.%s (id, name, content) VALUES "
                    + "(1, 'alice', 'Cross-schema Alice'), "
                    + "(2, 'bob', 'Cross-schema Bob')",
                schemaB, sameTableName));

            // Cross-schema JOIN: same table name, same column name, one ext one normal
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                String.format(
                    "SELECT a.id, a.content AS ext_c, b.content AS normal_c "
                        + "FROM %s a JOIN %s.%s b ON a.id = b.id "
                        + "ORDER BY a.id",
                    tableName, schemaB, sameTableName));

            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getLong("id"));
            Assert.assertEquals("Content from Alice", rs.getString("ext_c"));
            Assert.assertEquals("Cross-schema Alice", rs.getString("normal_c"));

            Assert.assertTrue(rs.next());
            Assert.assertEquals(2, rs.getLong("id"));
            Assert.assertEquals("Content from Bob", rs.getString("ext_c"));
            Assert.assertEquals("Cross-schema Bob", rs.getString("normal_c"));

            Assert.assertFalse(rs.next());
        } finally {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "DROP DATABASE IF EXISTS " + schemaB);
        }
    }

    // ========================= MIXED PATTERNS =========================

    @Test
    public void testGroupByHavingWithExtFunction() throws SQLException {
        // Uses groupTable (no UK) to allow duplicate names for grouping
        createGroupTable();
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES "
                + "(30, 'grp_a', 'short'), "
                + "(31, 'grp_a', 'longer content'), "
                + "(32, 'grp_b', 'x'), "
                + "(33, 'grp_b', 'y')",
            groupTable));

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format(
                "SELECT name, SUM(LENGTH(content)) AS total "
                    + "FROM %s GROUP BY name "
                    + "HAVING SUM(LENGTH(content)) > 5 ORDER BY name",
                groupTable));

        Assert.assertTrue(rs.next());
        Assert.assertEquals("grp_a", rs.getString("name"));
        Assert.assertEquals("short".length() + "longer content".length(), rs.getLong("total"));

        Assert.assertFalse("grp_b total=2 should be filtered by HAVING", rs.next());
    }

    @Test
    public void testOrderByLimitWithFunction() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format(
                "SELECT id, LENGTH(content) AS len FROM %s "
                    + "WHERE id <= 5 ORDER BY LENGTH(content) DESC LIMIT 2",
                tableName));

        Assert.assertTrue(rs.next());
        // "Content from Charlie" = 20 is longest
        Assert.assertEquals(3, rs.getLong("id"));
        Assert.assertEquals(20, rs.getLong("len"));

        Assert.assertTrue(rs.next());
        // "Content from Alice" = 18 is second
        Assert.assertEquals(1, rs.getLong("id"));
        Assert.assertEquals(18, rs.getLong("len"));

        Assert.assertFalse(rs.next());
    }

    @Test
    public void testPaginationWithWhereAndExtColumn() throws SQLException {
        String sql = String.format(
            "SELECT id, name, LEFT(content, 20) AS preview FROM %s "
                + "ORDER BY id LIMIT 3 OFFSET 1",
            tableName);

        // Verify ORDER BY + LIMIT + OFFSET pushes to DN
        ResultSet explainRs = JdbcUtil.executeQuerySuccess(tddlConnection, "EXPLAIN " + sql);
        StringBuilder plan = new StringBuilder();
        while (explainRs.next()) {
            plan.append(explainRs.getString(1)).append("\n");
        }
        String planStr = plan.toString().toLowerCase();
        Assert.assertTrue(
            "Expected MergeSort/LogicalView with LIMIT pushed to DN, got:\n" + plan,
            planStr.contains("logicalview") && planStr.contains("limit"));

        // Verify data: 5 rows total, offset 1 limit 3 = rows 2,3,4
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.next());
        Assert.assertFalse(rs.next());
    }

    @Test
    public void testCteLimitUnion() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format(
                "WITH top2 AS (SELECT id, content FROM %s ORDER BY id LIMIT 2), "
                    + "     bot2 AS (SELECT id, content FROM %s ORDER BY id DESC LIMIT 2) "
                    + "SELECT id, content FROM top2 "
                    + "UNION ALL "
                    + "SELECT id, content FROM bot2 "
                    + "ORDER BY id",
                tableName, tableName));

        List<Long> ids = new ArrayList<>();
        List<String> contents = new ArrayList<>();
        while (rs.next()) {
            ids.add(rs.getLong("id"));
            contents.add(rs.getString("content"));
        }
        Assert.assertEquals(4, ids.size());
        Assert.assertTrue(ids.contains(1L));
        Assert.assertTrue(ids.contains(2L));
        Assert.assertTrue(ids.contains(4L));
        Assert.assertTrue(ids.contains(5L));
        for (String c : contents) {
            Assert.assertTrue("Content should start with 'Content from', got: " + c,
                c.startsWith("Content from"));
        }
    }

    // ========================= ALIAS TESTS =========================

    @Test
    public void testTableAlias() throws SQLException {
        // Table alias: SELECT t.content FROM ext AS t
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT t.id, t.content FROM %s AS t WHERE t.id = 1", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getLong("id"));
        Assert.assertEquals("Content from Alice", rs.getString("content"));
        Assert.assertFalse(rs.next());
    }

    @Test
    public void testColumnAlias() throws SQLException {
        // Column alias: give externalized column an alias
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT content AS data FROM %s WHERE id = 1", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Content from Alice", rs.getString("data"));
        Assert.assertFalse(rs.next());
    }

    @Test
    public void testColumnAliasCollidesWithExternalizedName() throws SQLException {
        // Critical: non-ext column aliased to the same name as the externalized column.
        // Previously this caused the alias label to be rewritten to "content_addr_".
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format(
                "SELECT a.id AS content FROM %s a JOIN %s b ON a.id = b.id WHERE a.id = 1",
                normalTable, tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getLong("content"));
        Assert.assertFalse(rs.next());
    }

    @Test
    public void testJoinWithExplicitAlias() throws SQLException {
        // JOIN with aliases: normal table + ext table
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format(
                "SELECT a.id, LENGTH(b.content) AS clen FROM %s a JOIN %s b ON a.id = b.id WHERE a.id <= 3 ORDER BY a.id",
                normalTable, tableName));
        int count = 0;
        while (rs.next()) {
            count++;
            Assert.assertTrue(rs.getInt("clen") > 0);
        }
        Assert.assertEquals(3, count);
    }

    @Test
    public void testSelfJoinAlias() throws SQLException {
        // Self-join of externalized table with aliases
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format(
                "SELECT a.id, LENGTH(a.content) AS alen, LENGTH(b.content) AS blen "
                    + "FROM %s a JOIN %s b ON a.id = b.id WHERE a.id = 1",
                tableName, tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(rs.getInt("alen"), rs.getInt("blen"));
        Assert.assertFalse(rs.next());
    }

    @Test
    public void testSubqueryAlias() throws SQLException {
        // Subquery alias: SELECT t.content FROM (SELECT ...) t
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format(
                "SELECT t.id, t.content FROM (SELECT id, content FROM %s WHERE id <= 2) t ORDER BY t.id",
                tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Content from Alice", rs.getString("content"));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Content from Bob", rs.getString("content"));
        Assert.assertFalse(rs.next());
    }

    @Test
    public void testColumnAliasInOrderBy() throws SQLException {
        // ORDER BY a column alias that happens to be the externalized column name
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format(
                "SELECT id, LENGTH(content) AS content FROM %s ORDER BY content DESC LIMIT 3",
                tableName));
        int prevLen = Integer.MAX_VALUE;
        int count = 0;
        while (rs.next()) {
            int len = rs.getInt("content");
            Assert.assertTrue("Should be descending", len <= prevLen);
            prevLen = len;
            count++;
        }
        Assert.assertEquals(3, count);
    }

    @Test
    public void testConstAliasedToExternalizedName() throws SQLException {
        // A constant aliased to the externalized column name should not be corrupted
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format(
                "SELECT 'literal' AS content, id FROM %s WHERE id = 1", tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("literal", rs.getString("content"));
        Assert.assertEquals(1, rs.getLong("id"));
        Assert.assertFalse(rs.next());
    }

    @Test
    public void testMceGeneratedExternalizedColumnQueryPatterns() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("ALTER TABLE %s MODIFY COLUMN content LONGTEXT EXTERNALIZE", normalTable));

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, content FROM %s WHERE name = 'alice'", normalTable));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(1, rs.getLong("id"));
        Assert.assertEquals("Normal Alice", rs.getString("content"));
        Assert.assertFalse(rs.next());

        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT id, content FROM %s WHERE id BETWEEN 2 AND 4 ORDER BY id", normalTable));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(2, rs.getLong("id"));
        Assert.assertEquals("Normal Bob", rs.getString("content"));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(3, rs.getLong("id"));
        Assert.assertEquals("Normal Charlie", rs.getString("content"));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(4, rs.getLong("id"));
        Assert.assertEquals("Normal Dave", rs.getString("content"));
        Assert.assertFalse(rs.next());

        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT COUNT(*) AS cnt, SUM(LENGTH(content)) AS total_len FROM %s", normalTable));
        Assert.assertTrue(rs.next());
        Assert.assertEquals(5, rs.getLong("cnt"));
        Assert.assertTrue(rs.getLong("total_len") > 0);

        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT content FROM (SELECT id, content FROM %s WHERE id = 1 UNION ALL "
                    + "SELECT id, content FROM %s WHERE id = 2) u ORDER BY id",
                normalTable, normalTable));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Normal Alice", rs.getString("content"));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Normal Bob", rs.getString("content"));
        Assert.assertFalse(rs.next());

        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format(
                "SELECT n.content AS mce_content, e.content AS create_content FROM %s n JOIN %s e ON n.id = e.id "
                    + "WHERE n.id = 1",
                normalTable, tableName));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("Normal Alice", rs.getString("mce_content"));
        Assert.assertEquals("Content from Alice", rs.getString("create_content"));
        Assert.assertFalse(rs.next());

        rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT * FROM %s ORDER BY id LIMIT 1", normalTable));
        ResultSetMetaData metaData = rs.getMetaData();
        boolean foundContent = false;
        for (int i = 1; i <= metaData.getColumnCount(); i++) {
            String columnName = metaData.getColumnLabel(i);
            Assert.assertFalse("SELECT * must not expose content_addr_", "content_addr_".equalsIgnoreCase(columnName));
            if ("content".equalsIgnoreCase(columnName)) {
                foundContent = true;
            }
        }
        Assert.assertTrue("SELECT * should expose logical content", foundContent);
    }

    // ========================= HELPERS =========================

    private void createNormalTable() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT NOT NULL AUTO_INCREMENT,"
                + "  name VARCHAR(64),"
                + "  content LONGTEXT,"  // same column name but NOT externalized
                + "  PRIMARY KEY (id),"
                + "  UNIQUE KEY uk_name (name)"
                + ") DEFAULT CHARSET=utf8mb4"
                + "%s",
            normalTable, tableDistribution("id", 4)));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES "
                + "(1, 'alice', 'Normal Alice'), "
                + "(2, 'bob', 'Normal Bob'), "
                + "(3, 'charlie', 'Normal Charlie'), "
                + "(4, 'dave', 'Normal Dave'), "
                + "(5, 'eve', 'Normal Eve')",
            normalTable));
    }

    private void createTable() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT NOT NULL AUTO_INCREMENT,"
                + "  name VARCHAR(64),"
                + "  content LONGTEXT EXTERNALIZE,"
                + "  PRIMARY KEY (id),"
                + "  UNIQUE KEY uk_name (name)"
                + ") DEFAULT CHARSET=utf8mb4"
                + "%s",
            tableName, tableDistribution("id", 4)));
    }

    private void createGroupTable() {
        dropTestTable(tddlConnection, groupTable);
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT NOT NULL AUTO_INCREMENT,"
                + "  name VARCHAR(64),"
                + "  content LONGTEXT EXTERNALIZE,"
                + "  PRIMARY KEY (id)"
                + ") DEFAULT CHARSET=utf8mb4"
                + "%s",
            groupTable, tableDistribution("id", 4)));
    }

    private void createTable2() {
        dropTestTable(tddlConnection, tableName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "  id BIGINT NOT NULL AUTO_INCREMENT,"
                + "  name VARCHAR(64),"
                + "  content LONGTEXT EXTERNALIZE,"
                + "  PRIMARY KEY (id),"
                + "  UNIQUE KEY uk_name (name)"
                + ") DEFAULT CHARSET=utf8mb4"
                + "%s",
            tableName2, tableDistribution("id", 4)));
    }

    private String classDatabase() {
        return databaseName(AUTO_DB, DRDS_DB);
    }

    private void insertTestData() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES "
                + "(1, 'alice', 'Content from Alice'), "
                + "(2, 'bob', 'Content from Bob'), "
                + "(3, 'charlie', 'Content from Charlie'), "
                + "(4, 'dave', 'Content from Dave'), "
                + "(5, 'eve', 'Content from Eve')",
            tableName));
    }
}
