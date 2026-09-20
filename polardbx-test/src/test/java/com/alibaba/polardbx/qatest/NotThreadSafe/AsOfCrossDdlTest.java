package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Integration tests for ENABLE_AS_OF_CROSS_DDL.
 * <p>
 * Each cross-DDL case follows the unified pattern:
 * tx1 (insert 1..8) -> snapshot (tso + ts) -> tx2 (insert 9..16) -> DDL ->
 * AS OF {snapshot} expects tx1's 8 rows when flag=ON, cross-DDL error when flag=OFF.
 * <p>
 * All writes use explicit TSO transactions (BEGIN/COMMIT) so the snapshot
 * captured between them is well-ordered against both tx1 and tx2.
 */
public class AsOfCrossDdlTest extends CrudBasedLockTestCase {

    private static final String TABLE_PREFIX = "AsOfCrossDdlTest_";
    private static final String CROSS_DDL_ERROR_KEYWORD = "definition";
    private static final int TX1_ROWS = 8;

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void setUp() {
        if (!isMySQL80()) {
            return;
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set global opt_flashback_area = true");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set global innodb_txn_retention = 259200");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "reload datasources");
    }

    @After
    public void tearDown() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "rollback");
    }

    @Test
    public void testCrossDdlAsOfTsoOn() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection()) {
            String table = TABLE_PREFIX + "tso_on";
            try {
                Snapshot snap = setupForCrossDdl(conn, table);
                JdbcUtil.executeUpdateSuccess(conn, "SET ENABLE_AS_OF_CROSS_DDL = TRUE");
                assertAsOfCount(conn, "select count(*) from " + table + " as of tso " + snap.tso, TX1_ROWS);
            } finally {
                JdbcUtil.executeUpdateSuccess(conn, "drop table if exists " + table);
            }
        }
    }

    @Test
    public void testCrossDdlAsOfTsoOff() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection()) {
            String table = TABLE_PREFIX + "tso_off";
            try {
                Snapshot snap = setupForCrossDdl(conn, table);
                JdbcUtil.executeUpdateSuccess(conn, "SET ENABLE_AS_OF_CROSS_DDL = FALSE");
                assertCrossDdlError(conn,
                    "select * from " + table + " as of tso " + snap.tso + " order by id");
            } finally {
                JdbcUtil.executeUpdateSuccess(conn, "drop table if exists " + table);
            }
        }
    }

    @Test
    public void testCrossDdlAsOfTimestampOn() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection()) {
            String table = TABLE_PREFIX + "ts_on";
            try {
                Snapshot snap = setupForCrossDdl(conn, table);
                JdbcUtil.executeUpdateSuccess(conn, "SET ENABLE_AS_OF_CROSS_DDL = TRUE");
                assertAsOfCount(conn,
                    "select count(*) from " + table + " as of timestamp '" + snap.ts + "'", TX1_ROWS);
            } finally {
                JdbcUtil.executeUpdateSuccess(conn, "drop table if exists " + table);
            }
        }
    }

    @Test
    public void testCrossDdlAsOfTimestampOff() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection()) {
            String table = TABLE_PREFIX + "ts_off";
            try {
                Snapshot snap = setupForCrossDdl(conn, table);
                JdbcUtil.executeUpdateSuccess(conn, "SET ENABLE_AS_OF_CROSS_DDL = FALSE");
                assertCrossDdlError(conn,
                    "select * from " + table + " as of timestamp '" + snap.ts + "' order by id");
            } finally {
                JdbcUtil.executeUpdateSuccess(conn, "drop table if exists " + table);
            }
        }
    }

    /**
     * Same session: flip ON -> OFF -> ON, exercising the per-statement
     * clearAsOfCrossDdl cleanup path. Run for both AS OF TSO and AS OF TIMESTAMP.
     */
    @Test
    public void testSwitchWithinSession() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection()) {
            // ---------- AS OF TSO arm ----------
            String tableTso = TABLE_PREFIX + "switch_tso";
            try {
                Snapshot snapTso = setupForCrossDdl(conn, tableTso);
                String tsoSql = "select count(*) from " + tableTso + " as of tso " + snapTso.tso;
                String tsoFullSql = "select * from " + tableTso + " as of tso " + snapTso.tso + " order by id";

                JdbcUtil.executeUpdateSuccess(conn, "SET ENABLE_AS_OF_CROSS_DDL = TRUE");
                assertAsOfCount(conn, tsoSql, TX1_ROWS);

                JdbcUtil.executeUpdateSuccess(conn, "SET ENABLE_AS_OF_CROSS_DDL = FALSE");
                assertCrossDdlError(conn, tsoFullSql);

                JdbcUtil.executeUpdateSuccess(conn, "SET ENABLE_AS_OF_CROSS_DDL = TRUE");
                assertAsOfCount(conn, tsoSql, TX1_ROWS);
            } finally {
                JdbcUtil.executeUpdateSuccess(conn, "drop table if exists " + tableTso);
            }

            // ---------- AS OF TIMESTAMP arm ----------
            String tableTs = TABLE_PREFIX + "switch_ts";
            try {
                Snapshot snapTs = setupForCrossDdl(conn, tableTs);
                String tsCountSql = "select count(*) from " + tableTs + " as of timestamp '" + snapTs.ts + "'";
                String tsFullSql = "select * from " + tableTs + " as of timestamp '" + snapTs.ts + "' order by id";

                JdbcUtil.executeUpdateSuccess(conn, "SET ENABLE_AS_OF_CROSS_DDL = TRUE");
                assertAsOfCount(conn, tsCountSql, TX1_ROWS);

                JdbcUtil.executeUpdateSuccess(conn, "SET ENABLE_AS_OF_CROSS_DDL = FALSE");
                assertCrossDdlError(conn, tsFullSql);

                JdbcUtil.executeUpdateSuccess(conn, "SET ENABLE_AS_OF_CROSS_DDL = TRUE");
                assertAsOfCount(conn, tsCountSql, TX1_ROWS);
            } finally {
                JdbcUtil.executeUpdateSuccess(conn, "drop table if exists " + tableTs);
            }
        }
    }

    /**
     * Verify ENABLE_AS_OF_CROSS_DDL is independent from ENABLE_FLASHBACK_AREA:
     * with ENABLE_AS_OF_CROSS_DDL=false and NO cross-DDL in between, AS OF must
     * still return the historical value via the flashback_area path.
     */
    @Test
    public void testIndependentFromFlashbackArea() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection()) {
            String table = TABLE_PREFIX + "indep";
            try {
                JdbcUtil.executeUpdateSuccess(conn, "drop table if exists " + table);
                JdbcUtil.executeUpdateSuccess(conn,
                    "create table " + table + "(id int primary key, v int) partition by key(id) partitions 4");

                // tx1: insert v=100
                beginTsoTrx(conn);
                JdbcUtil.executeUpdateSuccess(conn,
                    "insert into " + table
                        + " values (1,100),(2,100),(3,100),(4,100),(5,100),(6,100),(7,100),(8,100)");
                JdbcUtil.executeUpdateSuccess(conn, "commit");

                sleep(2000);
                long tsoBetween = getTso(conn);
                String tsBetween = nowStr(conn);
                sleep(2000);

                // tx2: update v=200 (no DDL in this test)
                beginTsoTrx(conn);
                JdbcUtil.executeUpdateSuccess(conn,
                    "update " + table + " set v=200 where id in (1,2,3,4,5,6,7,8)");
                JdbcUtil.executeUpdateSuccess(conn, "commit");

                JdbcUtil.executeUpdateSuccess(conn, "SET ENABLE_AS_OF_CROSS_DDL = FALSE");

                // AS OF TSO between tx1 and tx2: should see the pre-update value via flashback_area.
                ResultSet rs = JdbcUtil.executeQuerySuccess(conn,
                    "select v from " + table + " as of tso " + tsoBetween + " where id=1");
                Assert.assertTrue(rs.next());
                Assert.assertEquals(100, rs.getInt("v"));
                rs.close();

                // AS OF TIMESTAMP between tx1 and tx2: same expectation.
                rs = JdbcUtil.executeQuerySuccess(conn,
                    "select v from " + table + " as of timestamp '" + tsBetween + "' where id=1");
                Assert.assertTrue(rs.next());
                Assert.assertEquals(100, rs.getInt("v"));
                rs.close();
            } finally {
                JdbcUtil.executeUpdateSuccess(conn, "drop table if exists " + table);
            }
        }
    }

    /**
     * Verify the AbstractTransaction.clearAsOfCrossDdl cleanup path is exercised
     * when the AS OF query runs inside an explicit TSO transaction (which routes
     * to an AbstractTransaction subclass holding distributed connections, instead
     * of the autocommit ReadOnlyTsoTransaction that goes through the no-op
     * default cleanup).
     */
    @Test
    public void testCleanupInExplicitTrx() throws SQLException {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn = getPolardbxConnection()) {
            String table = TABLE_PREFIX + "explicit_trx";
            try {
                Snapshot snap = setupForCrossDdl(conn, table);
                JdbcUtil.executeUpdateSuccess(conn, "SET ENABLE_AS_OF_CROSS_DDL = TRUE");

                // Run AS OF inside an explicit distributed TSO trx so cleanup
                // walks AbstractTransaction.clearAsOfCrossDdl on COMMIT.
                JdbcUtil.executeUpdateSuccess(conn, "set transaction_policy = TSO");
                JdbcUtil.executeUpdateSuccess(conn, "begin");

                ResultSet rs = JdbcUtil.executeQuerySuccess(conn,
                    "select count(*) from " + table + " as of tso " + snap.tso);
                Assert.assertTrue(rs.next());
                Assert.assertEquals(TX1_ROWS, rs.getInt(1));
                rs.close();

                rs = JdbcUtil.executeQuerySuccess(conn,
                    "select count(*) from " + table + " as of timestamp '" + snap.ts + "'");
                Assert.assertTrue(rs.next());
                Assert.assertEquals(TX1_ROWS, rs.getInt(1));
                rs.close();

                JdbcUtil.executeUpdateSuccess(conn, "commit");
            } finally {
                JdbcUtil.executeUpdateSuccess(conn, "drop table if exists " + table);
            }
        }
    }

    // ------------------------------------------------------------------
    // helpers
    // ------------------------------------------------------------------

    private static class Snapshot {
        final long tso;
        final String ts;

        Snapshot(long tso, String ts) {
            this.tso = tso;
            this.ts = ts;
        }
    }

    /**
     * The unified cross-DDL setup:
     * tx1 (insert id 1..8) -> snapshot (tso + ts) -> tx2 (insert id 9..16) -> DDL (create index).
     * The returned snapshot lies strictly between tx1.commit and tx2.commit, and
     * before the DDL. AS OF the snapshot:
     * - sees only tx1 rows (8 rows)
     * - crosses the DDL, so DN must replay across the index creation.
     */
    private Snapshot setupForCrossDdl(Connection conn, String table) throws SQLException {
        JdbcUtil.executeUpdateSuccess(conn, "drop table if exists " + table);
        JdbcUtil.executeUpdateSuccess(conn,
            "create table " + table + "(id int primary key, v int) partition by key(id) partitions 4");

        // tx1
        beginTsoTrx(conn);
        JdbcUtil.executeUpdateSuccess(conn,
            "insert into " + table + " values (1,1),(2,1),(3,1),(4,1),(5,1),(6,1),(7,1),(8,1)");
        JdbcUtil.executeUpdateSuccess(conn, "commit");

        // Sleep so tx1 commit TSO settles before we record the snapshot.
        sleep(2000);
        long tso = getTso(conn);
        String ts = nowStr(conn);
        // Sleep so tx2 commits strictly after the recorded snapshot.
        sleep(2000);

        // tx2
        beginTsoTrx(conn);
        JdbcUtil.executeUpdateSuccess(conn,
            "insert into " + table + " values (9,2),(10,2),(11,2),(12,2),(13,2),(14,2),(15,2),(16,2)");
        JdbcUtil.executeUpdateSuccess(conn, "commit");

        // Sleep so the DDL commit TSO is strictly after tx2.
        sleep(2000);
        JdbcUtil.executeUpdateSuccess(conn,
            "alter table " + table + " add column extra varchar(32) default 'x'");

        return new Snapshot(tso, ts);
    }

    private void assertAsOfCount(Connection conn, String countSql, int expected) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn, countSql)) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(expected, rs.getInt(1));
        }
    }

    private void assertCrossDdlError(Connection conn, String sql) {
        JdbcUtil.executeQueryFaied(conn, sql, CROSS_DDL_ERROR_KEYWORD);
    }

    private void beginTsoTrx(Connection conn) {
        JdbcUtil.executeUpdateSuccess(conn, "set transaction_policy = TSO");
        JdbcUtil.executeUpdateSuccess(conn, "begin");
    }

    private long getTso(Connection conn) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn, "select tso_timestamp()")) {
            Assert.assertTrue(rs.next());
            return rs.getLong(1);
        }
    }

    private String nowStr(Connection conn) throws SQLException {
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(conn,
            "select date_format(now(3), '%Y-%m-%d %H:%i:%s.%f')")) {
            Assert.assertTrue(rs.next());
            return rs.getString(1);
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
