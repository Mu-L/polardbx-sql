package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class XaStatementTraceJDBCTest extends CrudBasedLockTestCase {
    private final static String DB_NAME = "XaStatementTraceJDBCTest_db";
    private final static String DB2_NAME = "XaStatementTraceJDBCTest_db2";
    private final static String CREATE_DB_SQL = "create database " + DB_NAME + " mode=auto LOCALITY='dn=%s'";
    private final static String DROP_DB_SQL = "drop database if exists " + DB_NAME;
    private final static String CREATE_DB2_SQL = "create database " + DB2_NAME + " mode=auto";
    private final static String DROP_DB2_SQL = "drop database if exists " + DB2_NAME;
    private final static String TABLE_NAME = "XaStatementTraceTest";
    private final static String CREATE_TABLE_SQL = "create table XaStatementTraceTest("
        + "id int primary key auto_increment)"
        + "partition by key(id) partitions 16";
    private final static String DROP_TABLE_SQL = "drop table if exists XaStatementTraceTest";
    private static String GROUP_NAME;
    private static boolean JDBC = false;

    @BeforeClass
    public static void before() throws SQLException {
        // db1
        try (Connection connection = getPolardbxConnection0()) {
            JdbcUtil.executeUpdateSuccess(connection, "SET GLOBAL ENABLE_DRDS_TRACE_FOR_XA = true");
            // get a master dn
            ResultSet rs = JdbcUtil.executeQuerySuccess(connection, "show storage");
            String storageInstId = null;
            while (rs.next()) {
                if (rs.getString("INST_KIND").equalsIgnoreCase("MASTER")) {
                    storageInstId = rs.getString("STORAGE_INST_ID");
                    break;
                }
            }
            JdbcUtil.executeUpdateSuccess(connection, String.format(CREATE_DB_SQL, storageInstId));
            JdbcUtil.executeUpdateSuccess(connection, "use " + DB_NAME);
            rs = JdbcUtil.executeQuerySuccess(connection, "show datasources");
            while (rs.next()) {
                if (rs.getString("URL").contains("jdbc:mysql://")) {
                    JDBC = true;
                    break;
                }
            }
            if (!JDBC) {
                JdbcUtil.executeUpdateSuccess(connection, "set global CONN_POOL_XPROTO_STORAGE_DB_PORT = -1");
            }
            JdbcUtil.executeUpdateSuccess(connection, DROP_TABLE_SQL);
            JdbcUtil.executeUpdateSuccess(connection, CREATE_TABLE_SQL);
            JdbcUtil.executeUpdateSuccess(connection, "set global log_output = 'TABLE'");
            JdbcUtil.executeUpdateSuccess(connection, "set global general_log = ON");

            rs = JdbcUtil.executeQuerySuccess(connection, "show topology from " + TABLE_NAME);
            if (rs.next()) {
                GROUP_NAME = rs.getString("GROUP_NAME");
            }
        }
        // db2
        try (Connection connection = getPolardbxConnection0()) {
            JdbcUtil.executeUpdateSuccess(connection, CREATE_DB2_SQL);
            JdbcUtil.executeUpdateSuccess(connection, "use " + DB2_NAME);
            JdbcUtil.executeUpdateSuccess(connection, DROP_TABLE_SQL);
            JdbcUtil.executeUpdateSuccess(connection, CREATE_TABLE_SQL);
        }
    }

    @AfterClass
    public static void after() throws SQLException {
        try (Connection connection = getPolardbxConnection0()) {
            if (!JDBC) {
                JdbcUtil.executeUpdateSuccess(connection, "set global CONN_POOL_XPROTO_STORAGE_DB_PORT = 0");
            }
            JdbcUtil.executeUpdateSuccess(connection, DROP_DB_SQL);
            JdbcUtil.executeUpdateSuccess(connection, DROP_DB2_SQL);
            JdbcUtil.executeUpdateSuccess(connection, "set global general_log = OFF");
        }
    }

    private static String getTraceId(Connection connection) throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(connection, "select current_trans_id()");
        Assert.assertTrue(rs.next());
        return rs.getString(1);
    }

    private static void assertContains(String notLike, String... likes) throws SQLException {
        try (Connection connection = getPolardbxConnection0(DB_NAME)) {
            String sql = "/*+TDDL:node(" + GROUP_NAME + ")*/ "
                + "select count(0) from mysql.general_log where argument not like '%general_log%'";
            for (String key : likes) {
                sql += " and argument like '%" + key + "%'";
            }
            if (notLike != null) {
                sql += " and argument not like '%" + notLike + "%'";
            }
            System.out.println(sql);
            ResultSet rs = JdbcUtil.executeQuerySuccess(connection, sql);
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getLong(1) > 0);
        }
    }

    private static void assertNotContains(String... likes) throws SQLException {
        try (Connection connection = getPolardbxConnection0(DB_NAME)) {
            String sql = "/*+TDDL:node(" + GROUP_NAME + ")*/ "
                + "select count(0) from mysql.general_log where argument not like '%general_log%' ";
            for (String key : likes) {
                sql += " and argument like '%" + key + "%'";
            }
            ResultSet rs = JdbcUtil.executeQuerySuccess(connection, sql);
            Assert.assertTrue(rs.next());
            Assert.assertEquals(0, rs.getLong(1));
        }
    }

    private static void printTrace(String traceId) throws SQLException {
        try (Connection connection = getPolardbxConnection0(DB_NAME)) {
            String sql = "/*+TDDL:node(" + GROUP_NAME + ")*/ "
                + "select CONVERT(argument USING utf8) from mysql.general_log where argument not like '%general_log%' "
                + "and argument like '%" + traceId + "%' ";
            ResultSet rs = JdbcUtil.executeQuerySuccess(connection, sql);
            while (rs.next()) {
                System.out.println(rs.getString(1));
            }
        }
    }

    /**
     * SRW: share read view
     * WP: write parallelism
     * XA + RR + SRW + WP:
     * XA START -> XA END -> XA PREPARE -> XA COMMIT
     * XA START -> XA END -> XA COMMIT ONE PHASE
     */
    @Test
    public void test1() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = XA");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = false");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_GROUP_PARALLELISM = true");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "XA START", "/*DRDS /");
            assertNotContains(traceId, "XA END", "/*DRDS /");
            assertNotContains(traceId, "XA PREPARE", "/*DRDS /");
            assertNotContains(traceId, "XA COMMIT", "/*DRDS /");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = ON");
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + TABLE_NAME + " values (null), (null), (null), (null), (null)");
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            assertContains(null, traceId, "XA START", "/*DRDS /");
            assertContains(null, traceId, "XA END", "/*DRDS /");
            assertContains(null, traceId, "XA PREPARE", "/*DRDS /");
            assertContains(null, traceId, "ONE PHASE", "/*DRDS /");
            assertContains("ONE PHASE", traceId, "XA COMMIT", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        }
    }

    /**
     * XA + RR + SRW + WP:
     * BEGIN -> ROLLBACK -> XA START -> XA END -> XA COMMIT ONE PHASE
     * BEGIN -> ROLLBACK -> XA START -> XA END -> XA ROLLBACK
     */
    @Test
    public void test2() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "SET @fp_clear = true");
            JdbcUtil.executeUpdateSuccess(connection, "SET @FP_FORBID_REUSE_WRITE_CONNECTION = 'true'");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = XA");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = false");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_GROUP_PARALLELISM = true");
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 4");
            JdbcUtil.executeUpdateSuccess(connection, "set MERGE_UNION = false");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "BEGIN");
            assertNotContains(traceId, "ROLLBACK");
            assertNotContains(traceId, "XA START", "/*DRDS /");
            assertNotContains(traceId, "XA END", "/*DRDS /");
            assertNotContains(traceId, "XA COMMIT", "/*DRDS /");
            assertNotContains(traceId, "XA ROLLBACK", "/*DRDS /");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = ON");
            // read first
            JdbcUtil.executeQuerySuccess(connection, "select * from " + TABLE_NAME);
            // read -> write one partition
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + TABLE_NAME + " values (null)");
            // read -> share read view read
            JdbcUtil.executeQuerySuccess(connection, "select * from " + TABLE_NAME);
            // commit
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            assertContains(null, traceId, "BEGIN", "/*DRDS /");
            assertContains("XA ROLLBACK", traceId, "ROLLBACK", "/*DRDS /");
            assertContains(null, traceId, "XA START", "/*DRDS /");
            assertContains(null, traceId, "XA END", "/*DRDS /");
            assertContains(null, traceId, "ONE PHASE", "/*DRDS /");
            assertContains(null, traceId, "XA ROLLBACK", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        } finally {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET @fp_clear = true");
        }
    }

    /**
     * XA + RR + SRW + WP:
     * BEGIN -> ROLLBACK -> XA START -> XA END -> XA COMMIT ONE PHASE
     * BEGIN -> ROLLBACK -> XA START -> XA END -> XA PREPARE -> XA COMMIT
     */
    @Test
    public void test3() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = XA");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = false");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_GROUP_PARALLELISM = true");
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 4");
            JdbcUtil.executeUpdateSuccess(connection, "set MERGE_UNION = false");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "BEGIN", "/*DRDS /");
            assertNotContains(traceId, "ROLLBACK", "/*DRDS /");
            assertNotContains(traceId, "XA START", "/*DRDS /");
            assertNotContains(traceId, "XA END", "/*DRDS /");
            assertNotContains(traceId, "XA PREPARE", "/*DRDS /");
            assertNotContains(traceId, "ONE PHASE", "/*DRDS /");
            assertNotContains(traceId, "XA COMMIT", "/*DRDS /");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = ON");
            // read first
            JdbcUtil.executeQuerySuccess(connection, "select * from " + TABLE_NAME);
            // read -> write one partition
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + TABLE_NAME + " values (null), (null), (null), (null)");
            // read -> share read view read
            JdbcUtil.executeQuerySuccess(connection, "select * from " + TABLE_NAME);
            // commit
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            assertContains(null, traceId, "BEGIN", "/*DRDS /");
            assertContains("XA ROLLBACK", traceId, "ROLLBACK", "/*DRDS /");
            assertContains(null, traceId, "XA START", "/*DRDS /");
            assertContains(null, traceId, "XA END", "/*DRDS /");
            assertContains(null, traceId, "XA PREPARE", "/*DRDS /");
            assertContains(null, traceId, "ONE PHASE", "/*DRDS /");
            assertContains("ONE PHASE", traceId, "XA COMMIT", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        }
    }

    /**
     * XA + RR + SRW + WP:
     * XA START -> XA ROLLBACK -> XA END -> XA ROLLBACK
     */
    @Test
    public void test4() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = XA");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = false");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_GROUP_PARALLELISM = true");
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 4");
            JdbcUtil.executeUpdateSuccess(connection, "set MERGE_UNION = false");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "XA START", "/*DRDS /");
            assertNotContains(traceId, "XA END", "/*DRDS /");
            assertNotContains(traceId, "XA ROLLBACK", "/*DRDS /");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = ON");
            // read -> write one partition
            JdbcUtil.executeUpdateSuccess(connection,
                "/* +TDDL:cmd_extra(FAILURE_INJECTION='FAIL_BEFORE_PRIMARY_COMMIT') */"
                    + "insert into " + TABLE_NAME + " values (null), (null), (null), (null)");
            // commit
            JdbcUtil.executeFailed(connection, "commit", "FAIL_BEFORE_PRIMARY_COMMIT");

            assertContains(null, traceId, "XA START", "/*DRDS /");
            assertContains(null, traceId, "XA END", "/*DRDS /");
            assertContains(null, traceId, "XA ROLLBACK", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        }
    }

    /**
     * XA + RR + SRW + WP:
     * XA START -> XA END -> XA ROLLBACK
     */
    @Test
    public void test5() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = XA");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = false");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_GROUP_PARALLELISM = true");
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 4");
            JdbcUtil.executeUpdateSuccess(connection, "set MERGE_UNION = false");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "XA START", "/*DRDS /");
            assertNotContains(traceId, "XA END", "/*DRDS /");
            assertNotContains(traceId, "XA ROLLBACK", "/*DRDS /");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = ON");
            // read -> write one partition
            JdbcUtil.executeUpdateSuccess(connection,
                    "insert into " + TABLE_NAME + " values (null), (null), (null), (null)");
            // commit
            JdbcUtil.executeUpdateSuccess(connection, "rollback");

            assertContains(null, traceId, "XA START", "/*DRDS /");
            assertContains(null, traceId, "XA END", "/*DRDS /");
            assertContains(null, traceId, "XA ROLLBACK", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        }
    }

    /**
     * XA + RR + no-SRW:
     * BEGIN -> COMMIT
     */
    @Test
    public void test6() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = XA");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = false");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "BEGIN");
            assertNotContains(traceId, "COMMIT");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = OFF");
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + TABLE_NAME + " values (null), (null), (null), (null), (null)");
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            assertContains(null, traceId, "BEGIN", "/*DRDS /");
            assertContains(null, traceId, "COMMIT", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        }
    }

    /**
     * XA + RR + no-SRW:
     * BEGIN -> COMMIT
     * XA START -> XA END -> XA PREPARE -> XA COMMIT
     */
    @Test
    public void test7() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = XA");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = false");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_GROUP_PARALLELISM = false");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "BEGIN", "/*DRDS /");
            assertNotContains(traceId, "COMMIT", "/*DRDS /");
            assertNotContains(traceId, "XA START", "/*DRDS /");
            assertNotContains(traceId, "XA END", "/*DRDS /");
            assertNotContains(traceId, "XA PREPARE", "/*DRDS /");
            assertNotContains(traceId, "XA COMMIT", "/*DRDS /");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = OFF");
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + TABLE_NAME + " values (null), (null), (null), (null), (null)");
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + DB2_NAME + "." + TABLE_NAME + " values (null), (null), (null), (null), (null)");
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            assertContains(null,traceId, "BEGIN", "/*DRDS /");
            assertContains("XA COMMIT",traceId, "COMMIT", "/*DRDS /");
            assertContains(null, traceId, "XA START", "/*DRDS /");
            assertContains(null, traceId, "XA END", "/*DRDS /");
            assertContains(null, traceId, "XA PREPARE", "/*DRDS /");
            assertContains("ONE PHASE", traceId, "XA COMMIT", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        }
    }

    /**
     * XA + RR + no-SRW:
     * BEGIN -> COMMIT
     * BEGIN -> ROLLBACK
     */
    @Test
    public void test8() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = XA");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = false");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_GROUP_PARALLELISM = false");
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 4");
            JdbcUtil.executeUpdateSuccess(connection, "set MERGE_UNION = false");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "BEGIN");
            assertNotContains(traceId, "ROLLBACK");
            assertNotContains(traceId, "COMMIT", "/*DRDS /");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = false");
            // read first
            JdbcUtil.executeQuerySuccess(connection, "select * from " + TABLE_NAME);
            JdbcUtil.executeQuerySuccess(connection, "select * from " + DB2_NAME + "." + TABLE_NAME);
            // read -> write one partition
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + TABLE_NAME + " values (null)");
            JdbcUtil.executeQuerySuccess(connection, "select * from " + TABLE_NAME);
            JdbcUtil.executeQuerySuccess(connection, "select * from " + DB2_NAME + "." + TABLE_NAME);
            // commit
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            assertContains(null, traceId, "BEGIN", "/*DRDS /");
            assertContains("XA ROLLBACK", traceId, "ROLLBACK", "/*DRDS /");
            assertContains("XA COMMIT", traceId, "COMMIT", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        }
    }

    /**
     * XA + RR + no-SRW + no-WP:
     * BEGIN -> COMMIT
     * BEGIN -> ROLLBACK -> XA START -> XA END -> XA PREPARE -> XA COMMIT
     */
    @Test
    public void test9() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = XA");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = false");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_GROUP_PARALLELISM = false");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "BEGIN", "/*DRDS /");
            assertNotContains(traceId, "COMMIT", "/*DRDS /");
            assertNotContains(traceId, "ROLLBACK", "/*DRDS /");
            assertNotContains(traceId, "XA START", "/*DRDS /");
            assertNotContains(traceId, "XA END", "/*DRDS /");
            assertNotContains(traceId, "XA PREPARE", "/*DRDS /");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = false");
            // read first
            JdbcUtil.executeQuerySuccess(connection, "select * from " + TABLE_NAME);
            JdbcUtil.executeQuerySuccess(connection, "select * from " + DB2_NAME + "." + TABLE_NAME);
            // read -> write one partition
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + TABLE_NAME + " values (null), (null), (null), (null)");
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + DB2_NAME + "." + TABLE_NAME + " values (null), (null), (null), (null)");
            JdbcUtil.executeQuerySuccess(connection, "select * from " + DB2_NAME + "." + TABLE_NAME);
            JdbcUtil.executeQuerySuccess(connection, "select * from " + TABLE_NAME);
            // commit
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            assertContains(null, traceId, "BEGIN", "/*DRDS /");
            assertContains("XA COMMIT", traceId, "COMMIT", "/*DRDS /");
            assertContains("XA ROLLBACK", traceId, "ROLLBACK", "/*DRDS /");
            assertContains(null, traceId, "XA START", "/*DRDS /");
            assertContains(null, traceId, "XA END", "/*DRDS /");
            assertContains(null, traceId, "XA PREPARE", "/*DRDS /");
            assertContains("ONE PHASE", traceId, "XA COMMIT", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        }
    }

    /**
     * XA + RR + no-SRW:
     * XA START -> XA ROLLBACK -> XA END -> XA ROLLBACK
     */
    @Test
    public void test10() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = XA");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = false");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_GROUP_PARALLELISM = false");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "XA START", "/*DRDS /");
            assertNotContains(traceId, "XA END", "/*DRDS /");
            assertNotContains(traceId, "XA ROLLBACK", "/*DRDS /");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = OFF");
            JdbcUtil.executeUpdateSuccess(connection,
                "/* +TDDL:cmd_extra(FAILURE_INJECTION='FAIL_BEFORE_PRIMARY_COMMIT') */"
                    + "insert into " + TABLE_NAME + " values (null), (null), (null), (null)");
            JdbcUtil.executeUpdateSuccess(connection,
                "/* +TDDL:cmd_extra(FAILURE_INJECTION='FAIL_BEFORE_PRIMARY_COMMIT') */"
                    + "insert into " + DB2_NAME + "." + TABLE_NAME + " values (null), (null), (null), (null)");
            // commit
            JdbcUtil.executeFailed(connection, "commit", "FAIL_BEFORE_PRIMARY_COMMIT");

            assertContains(null, traceId, "XA START", "/*DRDS /");
            assertContains(null, traceId, "XA END", "/*DRDS /");
            assertContains(null, traceId, "XA ROLLBACK", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        }
    }

    /**
     * XA + RR + no-SRW:
     * BEGIN -> ROLLBACK
     * XA START -> XA END -> XA ROLLBACK
     */
    @Test
    public void test11() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = XA");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = false");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_GROUP_PARALLELISM = false");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "XA START", "/*DRDS /");
            assertNotContains(traceId, "XA END", "/*DRDS /");
            assertNotContains(traceId, "ROLLBACK", "/*DRDS /");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = OFF");
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + TABLE_NAME + " values (null), (null), (null), (null)");
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + DB2_NAME + "." + TABLE_NAME + " values (null), (null), (null), (null)");
            JdbcUtil.executeUpdateSuccess(connection, "rollback");

            assertContains(null, traceId, "BEGIN", "/*DRDS /");
            assertContains("XA ROLLBACK", traceId, "ROLLBACK", "/*DRDS /");
            assertContains(null, traceId, "XA START", "/*DRDS /");
            assertContains(null, traceId, "XA END", "/*DRDS /");
            assertContains(null, traceId, "XA ROLLBACK", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        }
    }

    /**
     * TSO + RR + SRW + WP:
     * XA START -> XA END -> XA PREPARE -> XA COMMIT
     */
    @Test
    public void test21() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = TSO");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = false");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_GROUP_PARALLELISM = true");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "XA START", "/*DRDS /");
            assertNotContains(traceId, "XA END", "/*DRDS /");
            assertNotContains(traceId, "XA PREPARE", "/*DRDS /");
            assertNotContains(traceId, "XA COMMIT", "/*DRDS /");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = ON");
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + TABLE_NAME + " values (null), (null), (null), (null), (null)");
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            assertContains(null, traceId, "XA START", "/*DRDS /");
            assertContains(null, traceId, "XA END", "/*DRDS /");
            assertContains(null, traceId, "XA PREPARE", "/*DRDS /");
            assertContains("ONE PHASE", traceId, "XA COMMIT", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        }
    }

    /**
     * TSO + RR + SRW + WP:
     * BEGIN -> ROLLBACK -> XA START -> XA END -> XA COMMIT ONE PHASE
     * BEGIN -> ROLLBACK -> XA START -> XA END -> XA ROLLBACK
     */
    @Test
    public void test22() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "SET @fp_clear = true");
            JdbcUtil.executeUpdateSuccess(connection, "SET @FP_FORBID_REUSE_WRITE_CONNECTION = 'true'");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = TSO");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = false");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_GROUP_PARALLELISM = true");
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 4");
            JdbcUtil.executeUpdateSuccess(connection, "set MERGE_UNION = false");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "BEGIN");
            assertNotContains(traceId, "ROLLBACK");
            assertNotContains(traceId, "XA START", "/*DRDS /");
            assertNotContains(traceId, "XA END", "/*DRDS /");
            assertNotContains(traceId, "XA COMMIT", "/*DRDS /");
            assertNotContains(traceId, "XA ROLLBACK", "/*DRDS /");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = ON");
            // read first
            JdbcUtil.executeQuerySuccess(connection, "select * from " + TABLE_NAME);
            // read -> write one partition
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + TABLE_NAME + " values (null)");
            // read -> share read view read
            JdbcUtil.executeQuerySuccess(connection, "select * from " + TABLE_NAME);
            // commit
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            assertContains(null, traceId, "BEGIN", "/*DRDS /");
            assertContains("XA ROLLBACK", traceId, "ROLLBACK", "/*DRDS /");
            assertContains(null, traceId, "XA START", "/*DRDS /");
            assertContains(null, traceId, "XA END", "/*DRDS /");
            assertContains(null, traceId, "ONE PHASE", "/*DRDS /");
            assertContains(null, traceId, "XA ROLLBACK", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        } finally {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET @fp_clear = true");
        }
    }

    /**
     * TSO + RR + SRW + WP:
     * BEGIN -> ROLLBACK -> XA START -> XA END -> XA PREPARE -> XA COMMIT
     */
    @Test
    public void test23() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = TSO");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = false");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_GROUP_PARALLELISM = true");
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 4");
            JdbcUtil.executeUpdateSuccess(connection, "set MERGE_UNION = false");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "BEGIN", "/*DRDS /");
            assertNotContains(traceId, "ROLLBACK", "/*DRDS /");
            assertNotContains(traceId, "XA START", "/*DRDS /");
            assertNotContains(traceId, "XA END", "/*DRDS /");
            assertNotContains(traceId, "XA PREPARE", "/*DRDS /");
            assertNotContains(traceId, "XA COMMIT", "/*DRDS /");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = ON");
            // read first
            JdbcUtil.executeQuerySuccess(connection, "select * from " + TABLE_NAME);
            // read -> write one partition
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + TABLE_NAME + " values (null), (null), (null), (null)");
            // read -> share read view read
            JdbcUtil.executeQuerySuccess(connection, "select * from " + TABLE_NAME);
            // commit
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            assertContains(null, traceId, "BEGIN", "/*DRDS /");
            assertContains("XA ROLLBACK", traceId, "ROLLBACK", "/*DRDS /");
            assertContains(null, traceId, "XA START", "/*DRDS /");
            assertContains(null, traceId, "XA END", "/*DRDS /");
            assertContains(null, traceId, "XA PREPARE", "/*DRDS /");
            assertContains("ONE PHASE", traceId, "XA COMMIT", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        }
    }

    /**
     * TSO + RR + SRW + WP:
     * XA START -> XA ROLLBACK -> XA END -> XA ROLLBACK
     */
    @Test
    public void test24() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = TSO");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = false");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_GROUP_PARALLELISM = true");
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 4");
            JdbcUtil.executeUpdateSuccess(connection, "set MERGE_UNION = false");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "XA START", "/*DRDS /");
            assertNotContains(traceId, "XA END", "/*DRDS /");
            assertNotContains(traceId, "XA ROLLBACK", "/*DRDS /");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = ON");
            // read -> write one partition
            JdbcUtil.executeUpdateSuccess(connection,
                "/* +TDDL:cmd_extra(FAILURE_INJECTION='FAIL_BEFORE_PRIMARY_COMMIT') */"
                    + "insert into " + TABLE_NAME + " values (null), (null), (null), (null)");
            // commit
            JdbcUtil.executeFailed(connection, "commit", "Failed to write commit state on group");

            assertContains(null, traceId, "XA START", "/*DRDS /");
            assertContains(null, traceId, "XA END", "/*DRDS /");
            assertContains(null, traceId, "XA ROLLBACK", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        }
    }

    /**
     * TSO + RR + SRW + WP:
     * XA START -> XA END -> XA ROLLBACK
     */
    @Test
    public void test25() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = TSO");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = false");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_GROUP_PARALLELISM = true");
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 4");
            JdbcUtil.executeUpdateSuccess(connection, "set MERGE_UNION = false");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "XA START", "/*DRDS /");
            assertNotContains(traceId, "XA END", "/*DRDS /");
            assertNotContains(traceId, "XA ROLLBACK", "/*DRDS /");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = ON");
            // read -> write one partition
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + TABLE_NAME + " values (null), (null), (null), (null)");
            // commit
            JdbcUtil.executeUpdateSuccess(connection, "rollback");

            assertContains(null, traceId, "XA START", "/*DRDS /");
            assertContains(null, traceId, "XA END", "/*DRDS /");
            assertContains(null, traceId, "XA ROLLBACK", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        }
    }

    /**
     * TSO + RR + no-SRW:
     * XA START -> XA COMMIT ONE PHASE
     */
    @Test
    public void test26() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = TSO");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = false");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "XA START");
            assertNotContains(traceId, "COMMIT");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = OFF");
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + TABLE_NAME + " values (null), (null), (null), (null), (null)");
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            assertContains(null, traceId, "XA START", "/*DRDS /");
            assertContains(null, traceId, "ONE PHASE", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        }
    }

    /**
     * TSO + RR + no-SRW:
     * XA START -> XA END -> XA PREPARE -> XA COMMIT
     */
    @Test
    public void test27() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = TSO");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = false");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_GROUP_PARALLELISM = false");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "XA START", "/*DRDS /");
            assertNotContains(traceId, "XA END", "/*DRDS /");
            assertNotContains(traceId, "XA PREPARE", "/*DRDS /");
            assertNotContains(traceId, "XA COMMIT", "/*DRDS /");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = OFF");
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + TABLE_NAME + " values (null), (null), (null), (null), (null)");
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + DB2_NAME + "." + TABLE_NAME + " values (null), (null), (null), (null), (null)");
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            assertContains(null, traceId, "XA START", "/*DRDS /");
            assertContains(null, traceId, "XA END", "/*DRDS /");
            assertContains(null, traceId, "XA PREPARE", "/*DRDS /");
            assertContains("ONE PHASE", traceId, "XA COMMIT", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        }
    }

    /**
     * TSO + RR + no-SRW:
     * XA START -> XA END -> XA COMMIT ONE PHASE
     * BEGIN -> ROLLBACK
     */
    @Test
    public void test28() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = TSO");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = false");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_GROUP_PARALLELISM = false");
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 4");
            JdbcUtil.executeUpdateSuccess(connection, "set MERGE_UNION = false");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "BEGIN");
            assertNotContains(traceId, "XA START");
            assertNotContains(traceId, "XA END");
            assertNotContains(traceId, "COMMIT");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = false");
            // read first
            JdbcUtil.executeQuerySuccess(connection, "select * from " + TABLE_NAME);
            JdbcUtil.executeQuerySuccess(connection, "select * from " + DB2_NAME + "." + TABLE_NAME);
            // read -> write one partition
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + TABLE_NAME + " values (null)");
            JdbcUtil.executeQuerySuccess(connection, "select * from " + TABLE_NAME);
            JdbcUtil.executeQuerySuccess(connection, "select * from " + DB2_NAME + "." + TABLE_NAME);
            // commit
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            assertContains(null, traceId, "BEGIN", "/*DRDS /");
            assertContains(null, traceId, "XA START", "/*DRDS /");
            assertContains(null, traceId, "XA END", "/*DRDS /");
            assertContains(null, traceId, "ONE PHASE", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        }
    }

    /**
     * TSO + RR + no-SRW:
     * BEGIN -> ROLLBACK -> XA START -> XA END -> XA PREPARE -> XA COMMIT
     */
    @Test
    public void test29() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = TSO");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = false");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_GROUP_PARALLELISM = false");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "BEGIN", "/*DRDS /");
            assertNotContains(traceId, "COMMIT", "/*DRDS /");
            assertNotContains(traceId, "ROLLBACK", "/*DRDS /");
            assertNotContains(traceId, "XA START", "/*DRDS /");
            assertNotContains(traceId, "XA END", "/*DRDS /");
            assertNotContains(traceId, "XA PREPARE", "/*DRDS /");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = false");
            // read first
            JdbcUtil.executeQuerySuccess(connection, "select * from " + TABLE_NAME);
            JdbcUtil.executeQuerySuccess(connection, "select * from " + DB2_NAME + "." + TABLE_NAME);
            // read -> write one partition
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + TABLE_NAME + " values (null), (null), (null), (null)");
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + DB2_NAME + "." + TABLE_NAME + " values (null), (null), (null), (null)");
            JdbcUtil.executeQuerySuccess(connection, "select * from " + DB2_NAME + "." + TABLE_NAME);
            JdbcUtil.executeQuerySuccess(connection, "select * from " + TABLE_NAME);
            // commit
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            assertContains(null, traceId, "BEGIN", "/*DRDS /");
            assertContains("XA ROLLBACK", traceId, "ROLLBACK", "/*DRDS /");
            assertContains(null, traceId, "XA START", "/*DRDS /");
            assertContains(null, traceId, "XA END", "/*DRDS /");
            assertContains(null, traceId, "XA PREPARE", "/*DRDS /");
            assertContains("ONE PHASE", traceId, "XA COMMIT", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        }
    }

    /**
     * TSO + RR + no-SRW:
     * XA START -> XA ROLLBACK -> XA END -> XA ROLLBACK
     */
    @Test
    public void test30() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = TSO");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = false");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_GROUP_PARALLELISM = false");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "XA START", "/*DRDS /");
            assertNotContains(traceId, "XA END", "/*DRDS /");
            assertNotContains(traceId, "XA ROLLBACK", "/*DRDS /");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = OFF");
            JdbcUtil.executeUpdateSuccess(connection,
                "/* +TDDL:cmd_extra(FAILURE_INJECTION='FAIL_BEFORE_PRIMARY_COMMIT') */"
                    + "insert into " + TABLE_NAME + " values (null), (null), (null), (null)");
            JdbcUtil.executeUpdateSuccess(connection,
                "/* +TDDL:cmd_extra(FAILURE_INJECTION='FAIL_BEFORE_PRIMARY_COMMIT') */"
                    + "insert into " + DB2_NAME + "." + TABLE_NAME + " values (null), (null), (null), (null)");
            // commit
            JdbcUtil.executeFailed(connection, "commit", "Failed to write commit state on group");

            assertContains(null, traceId, "XA START", "/*DRDS /");
            assertContains(null, traceId, "XA END", "/*DRDS /");
            assertContains(null, traceId, "XA ROLLBACK", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        }
    }

    /**
     * TSO + RR + no-SRW:
     * XA START -> XA END -> XA ROLLBACK
     */
    @Test
    public void test31() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = TSO");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = false");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_GROUP_PARALLELISM = false");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "XA START", "/*DRDS /");
            assertNotContains(traceId, "XA END", "/*DRDS /");
            assertNotContains(traceId, "ROLLBACK", "/*DRDS /");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = OFF");
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + TABLE_NAME + " values (null), (null), (null), (null)");
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + DB2_NAME + "." + TABLE_NAME + " values (null), (null), (null), (null)");
            JdbcUtil.executeUpdateSuccess(connection, "rollback");

            assertContains(null, traceId, "XA START", "/*DRDS /");
            assertContains(null, traceId, "XA END", "/*DRDS /");
            assertContains(null, traceId, "XA ROLLBACK", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        }
    }

    /**
     * XA_TSO + RR + SRW + WP:
     * XA START -> XA END -> XA PREPARE -> XA COMMIT
     */
    @Test
    public void test41() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = XA");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = true");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_GROUP_PARALLELISM = true");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "XA START", "/*DRDS /");
            assertNotContains(traceId, "XA END", "/*DRDS /");
            assertNotContains(traceId, "XA PREPARE", "/*DRDS /");
            assertNotContains(traceId, "XA COMMIT", "/*DRDS /");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = ON");
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + TABLE_NAME + " values (null), (null), (null), (null), (null)");
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            assertContains(null, traceId, "XA START", "/*DRDS /");
            assertContains(null, traceId, "XA END", "/*DRDS /");
            assertContains(null, traceId, "XA PREPARE", "/*DRDS /");
            assertContains("ONE PHASE", traceId, "XA COMMIT", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        }
    }

    /**
     * XA_TSO + RR + SRW + WP:
     * BEGIN -> ROLLBACK -> XA START -> XA END -> XA COMMIT ONE PHASE
     * BEGIN -> ROLLBACK -> XA START -> XA END -> XA ROLLBACK
     */
    @Test
    public void test42() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "SET @fp_clear = true");
            JdbcUtil.executeUpdateSuccess(connection, "SET @FP_FORBID_REUSE_WRITE_CONNECTION = 'true'");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = XA");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = true");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_GROUP_PARALLELISM = true");
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 4");
            JdbcUtil.executeUpdateSuccess(connection, "set MERGE_UNION = false");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "BEGIN");
            assertNotContains(traceId, "ROLLBACK");
            assertNotContains(traceId, "XA START", "/*DRDS /");
            assertNotContains(traceId, "XA END", "/*DRDS /");
            assertNotContains(traceId, "XA COMMIT", "/*DRDS /");
            assertNotContains(traceId, "XA ROLLBACK", "/*DRDS /");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = ON");
            // read first
            JdbcUtil.executeQuerySuccess(connection, "select * from " + TABLE_NAME);
            // read -> write one partition
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + TABLE_NAME + " values (null)");
            // read -> share read view read
            JdbcUtil.executeQuerySuccess(connection, "select * from " + TABLE_NAME);
            // commit
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            assertContains(null, traceId, "BEGIN", "/*DRDS /");
            assertContains("XA ROLLBACK", traceId, "ROLLBACK", "/*DRDS /");
            assertContains(null, traceId, "XA START", "/*DRDS /");
            assertContains(null, traceId, "XA END", "/*DRDS /");
            assertContains(null, traceId, "ONE PHASE", "/*DRDS /");
            assertContains(null, traceId, "XA ROLLBACK", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        } finally {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "SET @fp_clear = true");
        }
    }

    /**
     * XA_TSO + RR + SRW + WP:
     * BEGIN -> ROLLBACK -> XA START -> XA END -> XA PREPARE -> XA COMMIT
     */
    @Test
    public void test43() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = XA");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = true");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_GROUP_PARALLELISM = true");
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 4");
            JdbcUtil.executeUpdateSuccess(connection, "set MERGE_UNION = false");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "BEGIN", "/*DRDS /");
            assertNotContains(traceId, "ROLLBACK", "/*DRDS /");
            assertNotContains(traceId, "XA START", "/*DRDS /");
            assertNotContains(traceId, "XA END", "/*DRDS /");
            assertNotContains(traceId, "XA PREPARE", "/*DRDS /");
            assertNotContains(traceId, "XA COMMIT", "/*DRDS /");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = ON");
            // read first
            JdbcUtil.executeQuerySuccess(connection, "select * from " + TABLE_NAME);
            // read -> write one partition
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + TABLE_NAME + " values (null), (null), (null), (null)");
            // read -> share read view read
            JdbcUtil.executeQuerySuccess(connection, "select * from " + TABLE_NAME);
            // commit
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            assertContains(null, traceId, "BEGIN", "/*DRDS /");
            assertContains("XA ROLLBACK", traceId, "ROLLBACK", "/*DRDS /");
            assertContains(null, traceId, "XA START", "/*DRDS /");
            assertContains(null, traceId, "XA END", "/*DRDS /");
            assertContains(null, traceId, "XA PREPARE", "/*DRDS /");
            assertContains("ONE PHASE", traceId, "XA COMMIT", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        }
    }

    /**
     * XA_TSO + RR + SRW + WP:
     * XA START -> XA ROLLBACK -> XA END -> XA ROLLBACK
     */
    @Test
    public void test44() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = XA");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = true");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_GROUP_PARALLELISM = true");
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 4");
            JdbcUtil.executeUpdateSuccess(connection, "set MERGE_UNION = false");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "XA START", "/*DRDS /");
            assertNotContains(traceId, "XA END", "/*DRDS /");
            assertNotContains(traceId, "XA ROLLBACK", "/*DRDS /");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = ON");
            // read -> write one partition
            JdbcUtil.executeUpdateSuccess(connection,
                "/* +TDDL:cmd_extra(FAILURE_INJECTION='FAIL_BEFORE_PRIMARY_COMMIT') */"
                    + "insert into " + TABLE_NAME + " values (null), (null), (null), (null)");
            // commit
            JdbcUtil.executeFailed(connection, "commit", "Failed to write commit state on group");

            assertContains(null, traceId, "XA START", "/*DRDS /");
            assertContains(null, traceId, "XA END", "/*DRDS /");
            assertContains(null, traceId, "XA ROLLBACK", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        }
    }

    /**
     * XA_TSO + RR + SRW + WP:
     * XA START -> XA END -> XA ROLLBACK
     */
    @Test
    public void test45() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = XA");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = true");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_GROUP_PARALLELISM = true");
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 4");
            JdbcUtil.executeUpdateSuccess(connection, "set MERGE_UNION = false");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "XA START", "/*DRDS /");
            assertNotContains(traceId, "XA END", "/*DRDS /");
            assertNotContains(traceId, "XA ROLLBACK", "/*DRDS /");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = ON");
            // read -> write one partition
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + TABLE_NAME + " values (null), (null), (null), (null)");
            // commit
            JdbcUtil.executeUpdateSuccess(connection, "rollback");

            assertContains(null, traceId, "XA START", "/*DRDS /");
            assertContains(null, traceId, "XA END", "/*DRDS /");
            assertContains(null, traceId, "XA ROLLBACK", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        }
    }

    /**
     * XA_TSO + RR + no-SRW:
     * XA START -> XA COMMIT ONE PHASE
     */
    @Test
    public void test46() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = XA");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = true");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "XA START");
            assertNotContains(traceId, "COMMIT");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = OFF");
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + TABLE_NAME + " values (null), (null), (null), (null), (null)");
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            assertContains(null, traceId, "XA START", "/*DRDS /");
            assertContains(null, traceId, "ONE PHASE", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        }
    }

    /**
     * XA_TSO + RR + no-SRW:
     * XA START -> XA END -> XA PREPARE -> XA COMMIT
     */
    @Test
    public void test47() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = XA");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = true");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_GROUP_PARALLELISM = false");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "XA START", "/*DRDS /");
            assertNotContains(traceId, "XA END", "/*DRDS /");
            assertNotContains(traceId, "XA PREPARE", "/*DRDS /");
            assertNotContains(traceId, "XA COMMIT", "/*DRDS /");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = OFF");
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + TABLE_NAME + " values (null), (null), (null), (null), (null)");
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + DB2_NAME + "." + TABLE_NAME + " values (null), (null), (null), (null), (null)");
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            assertContains(null, traceId, "XA START", "/*DRDS /");
            assertContains(null, traceId, "XA END", "/*DRDS /");
            assertContains(null, traceId, "XA PREPARE", "/*DRDS /");
            assertContains("ONE PHASE", traceId, "XA COMMIT", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        }
    }

    /**
     * XA_TSO + RR + no-SRW:
     * XA START -> XA END -> XA COMMIT ONE PHASE
     * BEGIN -> ROLLBACK
     */
    @Test
    public void test48() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = XA");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = true");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_GROUP_PARALLELISM = false");
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 4");
            JdbcUtil.executeUpdateSuccess(connection, "set MERGE_UNION = false");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "BEGIN");
            assertNotContains(traceId, "XA START");
            assertNotContains(traceId, "XA END");
            assertNotContains(traceId, "COMMIT");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = false");
            // read first
            JdbcUtil.executeQuerySuccess(connection, "select * from " + TABLE_NAME);
            JdbcUtil.executeQuerySuccess(connection, "select * from " + DB2_NAME + "." + TABLE_NAME);
            // read -> write one partition
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + TABLE_NAME + " values (null)");
            JdbcUtil.executeQuerySuccess(connection, "select * from " + TABLE_NAME);
            JdbcUtil.executeQuerySuccess(connection, "select * from " + DB2_NAME + "." + TABLE_NAME);
            // commit
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            assertContains(null, traceId, "BEGIN", "/*DRDS /");
            assertContains(null, traceId, "XA START", "/*DRDS /");
            assertContains(null, traceId, "XA END", "/*DRDS /");
            assertContains(null, traceId, "ONE PHASE", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        }
    }

    /**
     * XA_TSO + RR + no-SRW:
     * BEGIN -> ROLLBACK -> XA START -> XA END -> XA PREPARE -> XA COMMIT
     */
    @Test
    public void test49() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = XA");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = true");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_GROUP_PARALLELISM = false");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "BEGIN", "/*DRDS /");
            assertNotContains(traceId, "COMMIT", "/*DRDS /");
            assertNotContains(traceId, "ROLLBACK", "/*DRDS /");
            assertNotContains(traceId, "XA START", "/*DRDS /");
            assertNotContains(traceId, "XA END", "/*DRDS /");
            assertNotContains(traceId, "XA PREPARE", "/*DRDS /");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = false");
            // read first
            JdbcUtil.executeQuerySuccess(connection, "select * from " + TABLE_NAME);
            JdbcUtil.executeQuerySuccess(connection, "select * from " + DB2_NAME + "." + TABLE_NAME);
            // read -> write one partition
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + TABLE_NAME + " values (null), (null), (null), (null)");
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + DB2_NAME + "." + TABLE_NAME + " values (null), (null), (null), (null)");
            JdbcUtil.executeQuerySuccess(connection, "select * from " + DB2_NAME + "." + TABLE_NAME);
            JdbcUtil.executeQuerySuccess(connection, "select * from " + TABLE_NAME);
            // commit
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            assertContains(null, traceId, "BEGIN", "/*DRDS /");
            assertContains("XA ROLLBACK", traceId, "ROLLBACK", "/*DRDS /");
            assertContains(null, traceId, "XA START", "/*DRDS /");
            assertContains(null, traceId, "XA END", "/*DRDS /");
            assertContains(null, traceId, "XA PREPARE", "/*DRDS /");
            assertContains("ONE PHASE", traceId, "XA COMMIT", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        }
    }

    /**
     * XA_TSO + RR + no-SRW:
     * XA START -> XA ROLLBACK -> XA END -> XA ROLLBACK
     */
    @Test
    public void test50() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = XA");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = true");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_GROUP_PARALLELISM = false");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "XA START", "/*DRDS /");
            assertNotContains(traceId, "XA END", "/*DRDS /");
            assertNotContains(traceId, "XA ROLLBACK", "/*DRDS /");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = OFF");
            JdbcUtil.executeUpdateSuccess(connection,
                "/* +TDDL:cmd_extra(FAILURE_INJECTION='FAIL_BEFORE_PRIMARY_COMMIT') */"
                    + "insert into " + TABLE_NAME + " values (null), (null), (null), (null)");
            JdbcUtil.executeUpdateSuccess(connection,
                "/* +TDDL:cmd_extra(FAILURE_INJECTION='FAIL_BEFORE_PRIMARY_COMMIT') */"
                    + "insert into " + DB2_NAME + "." + TABLE_NAME + " values (null), (null), (null), (null)");
            // commit
            JdbcUtil.executeFailed(connection, "commit", "Failed to write commit state on group");

            assertContains(null, traceId, "XA START", "/*DRDS /");
            assertContains(null, traceId, "XA END", "/*DRDS /");
            assertContains(null, traceId, "XA ROLLBACK", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        }
    }

    /**
     * XA_TSO + RR + no-SRW:
     * XA START -> XA END -> XA ROLLBACK
     */
    @Test
    public void test51() throws SQLException {
        String traceId = null;
        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set GROUP_PARALLELISM = 8");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = XA");
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = true");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_GROUP_PARALLELISM = false");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            traceId = getTraceId(connection);

            assertNotContains(traceId, "XA START", "/*DRDS /");
            assertNotContains(traceId, "XA END", "/*DRDS /");
            assertNotContains(traceId, "ROLLBACK", "/*DRDS /");

            JdbcUtil.executeUpdateSuccess(connection, "set share_read_view = OFF");
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + TABLE_NAME + " values (null), (null), (null), (null)");
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into " + DB2_NAME + "." + TABLE_NAME + " values (null), (null), (null), (null)");
            JdbcUtil.executeUpdateSuccess(connection, "rollback");

            assertContains(null, traceId, "XA START", "/*DRDS /");
            assertContains(null, traceId, "XA END", "/*DRDS /");
            assertContains(null, traceId, "XA ROLLBACK", "/*DRDS /");
        } catch (Throwable t) {
            if (null != traceId) {
                printTrace(traceId);
            }
            throw t;
        }
    }

}
