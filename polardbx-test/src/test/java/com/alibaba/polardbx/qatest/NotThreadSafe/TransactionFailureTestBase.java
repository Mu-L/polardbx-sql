package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeOnMysqlAndTddl;

public class TransactionFailureTestBase extends CrudBasedLockTestCase {
    final protected int emptyBranchType;

    final protected static String DB_NAME = "test_async_commit";
    final protected static String TABLE_NAME = "test_async_commit";
    final protected static String CREATE_TABLE =
        "create table if not exists " + TABLE_NAME + " (id int primary key, a int) "
            + "partition by range(id) ("
            + "partition p1 values less than (1000), "
            + "partition p2 values less than (2000), "
            + "partition p3 values less than (3000), "
            + "partition p4 values less than (4000), "
            + "partition p5 values less than (5000), "
            + "partition p6 values less than (6000), "
            + "partition p7 values less than (7000), "
            + "partition p8 values less than (8000)"
            + ")";
    protected String INSERT_DATA = "insert into " + TABLE_NAME + " (id, a) values "
        + "(100, 0), (1100, 0), (2100, 0), (3100, 0), (4100, 0), (5100, 0), (6100, 0), (7100, 0)";
    final protected String DELETE_DATA = "delete from " + TABLE_NAME + " where 1=1";
    protected String UPDATE_DATA = "update " + TABLE_NAME + " set a = 100 where 1=1";
    final protected static String SELECT_DATA = "select sum(a) from " + TABLE_NAME;
    final protected static String SELECT_DATA_FOR_UPDATE = "select sum(a) from " + TABLE_NAME + " for update";
    final protected static Pattern ERROR_MSG = Pattern.compile("Total (\\d+) branches, (\\d+) prepared.");
    protected Connection polarxConn;
    protected Connection mysqlConn;

    @Parameterized.Parameters(name = "{index}:empty_branch={0}")
    public static List<Object[]> prepare() throws SQLException {
        List<Object[]> ret = new ArrayList<>();
        ret.add(new Object[] {0});
        ret.add(new Object[] {1});
        ret.add(new Object[] {2});
        return ret;
    }

    public TransactionFailureTestBase(int emptyBranchType) {
        this.emptyBranchType = emptyBranchType;
        if (1 == emptyBranchType) {
            INSERT_DATA = "insert into " + TABLE_NAME + " (id, a) values (100, 0)";
        }
        if (2 == emptyBranchType) {
            UPDATE_DATA = "update " + TABLE_NAME + " set a = 100 where id = 100";
        }
    }

    @BeforeClass
    public static void init() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn1 = getPolardbxConnection0();
            Connection conn2 = getMysqlConnection0()) {
            String sql = "create database if not exists " + DB_NAME;
            JdbcUtil.executeUpdateSuccess(conn1, sql + " mode=auto");
            JdbcUtil.executeUpdateSuccess(conn2, sql);
            executeOnMysqlAndTddl(conn1, conn2, "use " + DB_NAME, null);
            executeOnMysqlAndTddl(conn1, conn2, CREATE_TABLE, null);
        }
    }

    @AfterClass
    public static void destroy() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        try (Connection conn1 = getPolardbxConnection0();
            Connection conn2 = getMysqlConnection0()) {
            String sql = "drop database if exists " + DB_NAME;
            executeOnMysqlAndTddl(conn1, conn2, sql, null);
        }
    }

    @Before
    public void before() throws Exception {
        if (!isMySQL80()) {
            return;
        }
        polarxConn = getPolardbxConnection0(DB_NAME);
        mysqlConn = getMysqlConnection0(DB_NAME);
        JdbcUtil.executeUpdateSuccess(polarxConn, "set global ENABLE_TRX_DEBUG_MODE = true");
        executeOnMysqlAndTddl(mysqlConn, polarxConn, DELETE_DATA, null);
        executeOnMysqlAndTddl(mysqlConn, polarxConn, INSERT_DATA, null);
        JdbcUtil.executeUpdateSuccess(polarxConn, "set TRANSACTION_POLICY = TSO");
        JdbcUtil.executeUpdateSuccess(polarxConn, "set ENABLE_ASYNC_COMMIT_80 = true");
        JdbcUtil.executeUpdateSuccess(polarxConn, "set ENABLE_AUTO_SAVEPOINT = true");
    }

    @After
    public void after() {
        if (!isMySQL80()) {
            return;
        }
        JdbcUtil.executeUpdateSuccess(polarxConn, "set global ENABLE_TRX_DEBUG_MODE = false");
        try {
            polarxConn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        try {
            mysqlConn.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    protected void waitUntilTransFinished(String transId) throws SQLException, InterruptedException {
        String sql = "select count(0) from information_schema.PREPARED_TRX_BRANCH where trans_id = '" + transId + "'";
        int retry = 0;
        while (retry++ < 10) {
            ResultSet rs = JdbcUtil.executeQuerySuccess(polarxConn, sql);
            if (rs.next() && rs.getLong(1) == 0) {
                return;
            }
            Thread.sleep(2000);
        }
        throw new SQLException("Max retry exceed.");
    }
}
