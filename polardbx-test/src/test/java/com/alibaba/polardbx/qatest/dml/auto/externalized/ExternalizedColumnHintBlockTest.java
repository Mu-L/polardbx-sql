package com.alibaba.polardbx.qatest.dml.auto.externalized;

import com.alibaba.polardbx.qatest.ExternalizedColumnTestBase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.UUID;

/**
 * Tests that the legacy SCAN hint guard remains isolated to tables with externalized columns.
 */
public class ExternalizedColumnHintBlockTest extends ExternalizedColumnTestBase {

    private static final String CLASS_DB =
        "ext_hint_" + UUID.randomUUID().toString().substring(0, 8).replace("-", "");

    @BeforeClass
    public static void initClassDb() throws SQLException {
        createIsolatedDatabase(CLASS_DB);
    }

    @AfterClass
    public static void dropClassDb() {
        dropIsolatedDatabase(CLASS_DB);
    }

    private final String suffix = UUID.randomUUID().toString().substring(0, 8).replace("-", "");
    private final String extTable = "ext_hint_blk_" + suffix;
    private final String normalTable = "normal_hint_blk_" + suffix;

    @Before
    public void setUp() throws SQLException {
        // Bind tddlConnection to a URL-bound connection on our isolated DB.
        this.tddlConnection = ConnectionManager.getInstance().newPolarDBXConnection(CLASS_DB);

        JdbcUtil.executeSuccess(tddlConnection, "SET SESSION WORKLOAD_TYPE=TP");
        long t0 = System.currentTimeMillis();
        System.out.println("[HintBlockTest] setUp start: " + t0);

        JdbcUtil.dropTable(tddlConnection, extTable);
        JdbcUtil.dropTable(tddlConnection, normalTable);

        long t1 = System.currentTimeMillis();
        System.out.println("[HintBlockTest] drop done +" + (t1 - t0) + "ms, creating extTable...");

        // Create table with externalized column
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "id bigint NOT NULL AUTO_INCREMENT, "
                + "name varchar(64), "
                + "content LONGTEXT EXTERNALIZE, "
                + "PRIMARY KEY (id)"
                + ") PARTITION BY KEY(id) PARTITIONS 2",
            extTable));

        long t2 = System.currentTimeMillis();
        System.out.println("[HintBlockTest] extTable created +" + (t2 - t1) + "ms, creating normalTable...");

        // Create normal table (no externalized columns)
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "CREATE TABLE %s ("
                + "id bigint NOT NULL AUTO_INCREMENT, "
                + "name varchar(64), "
                + "PRIMARY KEY (id)"
                + ") PARTITION BY KEY(id) PARTITIONS 2",
            normalTable));

        long t3 = System.currentTimeMillis();
        System.out.println("[HintBlockTest] normalTable created +" + (t3 - t2) + "ms, inserting...");

        // Insert test data
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name, content) VALUES (1, 'alice', 'hello')", extTable));
        JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
            "INSERT INTO %s (id, name) VALUES (1, 'alice')", normalTable));

        System.out.println("[HintBlockTest] setUp done, total=" + (System.currentTimeMillis() - t0) + "ms");
    }

    @After
    public void tearDown() {
        System.out.println("[HintBlockTest] tearDown start");
        try {
            JdbcUtil.dropTable(tddlConnection, extTable);
            JdbcUtil.dropTable(tddlConnection, normalTable);
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
        System.out.println("[HintBlockTest] tearDown done");
    }

    // ========================= SCAN hint =========================

    @Test
    public void testScanHintBlockedOnExternalizedTable() {
        String sql = String.format("/*+TDDL:SCAN()*/ SELECT * FROM %s", extTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "not support");
    }

    @Test
    public void testScanHintWithSpaceBlockedOnExternalizedTable() {
        String sql = String.format("/*+TDDL:SCAN ()*/ SELECT * FROM %s", extTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "not support");
    }

    @Test
    public void testScanHintWorksOnNormalTable() throws SQLException {
        String sql = String.format("/*+TDDL:SCAN()*/ SELECT * FROM %s", normalTable);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        rs.close();
    }

    @Test
    public void testScanDmlWorksOnNormalTable() throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            String.format("/*+TDDL:SCAN()*/ UPDATE %s SET name = 'normal-updated' WHERE id = 1", normalTable));
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT name FROM %s WHERE id = 1", normalTable))) {
            org.junit.Assert.assertTrue(rs.next());
            org.junit.Assert.assertEquals("normal-updated", rs.getString(1));
        }
    }

    @Test
    public void testScanInStringLiteralNotBlocked() throws SQLException {
        // SCAN( appearing in a string literal should NOT trigger the block
        String sql = String.format("SELECT * FROM %s WHERE name = 'SCAN('", extTable);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        rs.close();
    }

    // ========================= Combined / edge cases =========================

    @Test
    public void testScanHintBlockedRepeatedExecution() {
        // Verify plan cache doesn't cause the check to be bypassed on second execution
        String sql = String.format("/*+TDDL:SCAN()*/ SELECT * FROM %s", extTable);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "not support");
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "not support");
    }

    @Test
    public void testScanBlockedOnMceGeneratedExternalizedColumn() {
        String mceTable = extTable + "_mce";
        JdbcUtil.dropTable(tddlConnection, mceTable);
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, String.format(
                "CREATE TABLE %s ("
                    + "id bigint NOT NULL AUTO_INCREMENT, "
                    + "name varchar(64), "
                    + "content LONGTEXT, "
                    + "PRIMARY KEY (id)"
                    + ") PARTITION BY KEY(id) PARTITIONS 2",
                mceTable));
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("INSERT INTO %s (id, name, content) VALUES (1, 'alice', 'hello')", mceTable));
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                String.format("ALTER TABLE %s MODIFY COLUMN content LONGTEXT EXTERNALIZE", mceTable));

            JdbcUtil.executeUpdateFailed(tddlConnection,
                String.format("/*+TDDL:SCAN()*/ SELECT * FROM %s", mceTable), "not support");
        } finally {
            JdbcUtil.dropTable(tddlConnection, mceTable);
        }
    }
}
