package com.alibaba.polardbx.qatest.dql.auto.select;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.gms.util.JdbcUtil;
import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * @author fangwu
 */
public class ErrorCodeTest extends BaseTestCase {
    private static final String tblName = "tbl_not_exists";
    private static final String DB_NAME = "test_errorcode";

    public void testTableNotExists() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("drop table if exists " + tblName);
            c.createStatement().executeQuery("select * from " + tblName);
        } catch (Exception e) {
            e.printStackTrace();
            String msg = e.getMessage();
            System.out.println(msg);
            Assert.assertTrue(
                msg.contains("ERR-CODE: [PXC-4006][ERR_TABLE_NOT_EXIST] Table 'tbl_not_exists' doesn't exist"));
            Assert.assertTrue(msg.indexOf("ERR-CODE") == msg.lastIndexOf("ERR-CODE"));
        }
    }

    public void testCantChangeIsolation() throws SQLException {
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().execute("begin");
            c.createStatement().executeQuery("SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("PXC-25001"));
        }
    }

    @Test
    public void testErrorCode()
        throws SQLException, ClassNotFoundException, InterruptedException, InstantiationException,
        IllegalAccessException {
        // Without ConsistentErrorCode
        testSqlParserError();
        testCantChangeIsolation();
        testTableNotExists();

        // With ConsistentErrorCode
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().executeQuery("set global ENABLE_CONSISTENT_ERRORCODE=true");
            testConsistentErrorCode();
        } finally {
            try (Connection c = getPolardbxConnection()) {
                c.createStatement().execute("set global ENABLE_CONSISTENT_ERRORCODE=false");
            }
        }
    }

    /**
     * 测试数据库错误码的一致性
     *
     * @throws SQLException 数据库操作失败时抛出
     * @throws ClassNotFoundException 类加载器找不到指定的类时抛出
     * @throws InterruptedException 线程被中断时抛出
     */
    public void testConsistentErrorCode()
        throws SQLException, ClassNotFoundException, InterruptedException, InstantiationException,
        IllegalAccessException {
        // 错误码：1064（ER_PARSE_ERROR）
        checkSqlError(1064, "42000", "seect * frm table1");

        // 错误码：1146（表不存在）
        checkSqlError(1146, "42S02", "select * from table1");

        // 创建数据库和表用于测试
        setupDatabaseAndTables(DB_NAME);

        try (Connection connection = getPolardbxConnection(DB_NAME)) {
            // 测试列不存在的情况
            checkColumnNotFoundError(connection, "aaa", "test_errorcode_tbl");

            // 测试插入重复条目
            checkDuplicateEntryError(connection, "test_errorcode_tbl");

            // 测试字符串值错误
            checkIncorrectStringError(connection);

            // 测试锁等待超时
            checkLockWaitTimeoutError();

            // 测试死锁检测
            checkDeadlockDetectionError();
        } finally {
            dropDatabase(DB_NAME);
        }

        String addr = ConnectionManager.getInstance().getPolardbxAddress();
        if ("127.0.0.1".equalsIgnoreCase(addr) || "localhost".equalsIgnoreCase(addr)) {
            return;
        }

        // 测试用户不存在
        checkUserNotFoundError();

        // 测试密码错误
        checkPasswordError();

        // 测试密码为空
        checkEmptyPasswordError();
    }

    private void checkSqlError(int expectedCode, String expectedState, String query) {
        try (Connection connection = getPolardbxConnection()) {
            connection.createStatement().executeQuery(query);
            Assert.fail(); // 应该抛出异常
        } catch (SQLException e) {
            assertExpectedError(e, expectedCode, expectedState);
        }
    }

    private void checkColumnNotFoundError(Connection connection, String columnName, String tableName)
        throws SQLException {
        try {
            connection.createStatement().execute("select " + columnName + " from " + tableName + " where id = 1");
            Assert.fail(); // 应该抛出异常
        } catch (SQLException e) {
            assertExpectedError(e, 1054, "42S22");
        }
    }

    private void checkDuplicateEntryError(Connection connection, String tableName) {
        try {
            connection.createStatement().execute("insert into " + tableName + "(id) values(1)");
            connection.createStatement().execute("insert into " + tableName + "(id) values(1)");
            Assert.fail(); // 应该抛出异常
        } catch (SQLException e) {
            assertExpectedError(e, 1062, "23000");
        }
    }

    private void checkIncorrectStringError(Connection connection) {
        try {
            connection.createStatement().execute("CREATE TABLE if not exists test_table (" +
                "id INT," +
                "content VARCHAR(100)," +
                "PRIMARY KEY (id)" +
                ") CHARACTER SET latin1");

            connection.createStatement().execute("SET SESSION sql_mode = 'STRICT_TRANS_TABLES'");
            connection.createStatement().execute("INSERT INTO test_table (id, content) VALUES (1, '你好');");
            Assert.fail(); // 应该抛出异常
        } catch (SQLException e) {
            assertExpectedError(e, 1366, "HY000");
        }
    }

    private void checkLockWaitTimeoutError() {
        try (Connection conn1 = getPolardbxConnection(DB_NAME); Connection conn2 = getPolardbxConnection(DB_NAME)) {
            conn1.createStatement().execute("CREATE TABLE if not exists test_lock (" +
                "id INT," +
                "data VARCHAR(100)," +
                "PRIMARY KEY (id)" +
                ");" +
                "INSERT INTO test_lock VALUES (1, 'initial');");

            conn1.createStatement().execute("BEGIN ");
            conn1.createStatement().execute("UPDATE test_lock SET data = 'locked' WHERE id = 1");

            conn2.createStatement().execute("SET SESSION innodb_lock_wait_timeout = 1");
            conn2.createStatement().execute("BEGIN");
            conn2.createStatement().execute("UPDATE test_lock SET data = 'blocked' WHERE id = 1");
            Assert.fail(); // 应该抛出异常
        } catch (SQLException e) {
            assertExpectedError(e, 1205);
        }
    }

    private void checkDeadlockDetectionError() throws InterruptedException {
        try (Connection conn1 = getPolardbxConnection(DB_NAME); Connection conn2 = getPolardbxConnection(DB_NAME)) {
            conn1.createStatement().execute("CREATE TABLE if not exists test_deadlock (" +
                "id INT PRIMARY KEY," +
                "data VARCHAR(100)" +
                ") single");
            conn1.createStatement().execute("INSERT INTO test_deadlock VALUES (1, 'A'), (2, 'B')");

            conn1.createStatement().execute("BEGIN ;UPDATE test_deadlock SET data = 'X' WHERE id = 1;");

            conn2.createStatement().execute("BEGIN;UPDATE test_deadlock SET data = 'Y' WHERE id = 2;");

            Thread thread = new Thread(() -> {
                try {
                    conn1.createStatement().execute("SET SESSION innodb_lock_wait_timeout = 10");
                    conn1.createStatement().execute("UPDATE test_deadlock SET data = 'Z' WHERE id = 2;");
                } catch (SQLException ex) {
                    ex.printStackTrace();
                }
            });
            thread.start();

            Thread.sleep(1000);

            conn2.createStatement().execute("UPDATE test_deadlock SET data = 'W' WHERE id = 1");
            Assert.fail(); // 应该抛出异常
        } catch (SQLException e) {
            assertExpectedError(e, 1213, "40001");
        }
    }

    private void dropDatabase(String dbName) throws SQLException {
        try (Connection connection = getPolardbxConnection()) {
            connection.createStatement().execute("drop database if exists " + dbName);
        }
    }

    private void checkUserNotFoundError()
        throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        String url = ConnectionManager.getInstance().getPolarDBxUrl();
        Class.forName("com.mysql.jdbc.Driver").newInstance();

        boolean failedWithCorrectException = false;
        try {
            DriverManager.getConnection(url, "error_code_not_exists_user", "pass");
        } catch (SQLException e) {
            failedWithCorrectException = true;
            assertExpectedError(e, 1045, "28000");
        }

        Assert.assertTrue(failedWithCorrectException);
    }

    private void checkPasswordError() {
        String url = ConnectionManager.getInstance().getPolarDBxUrl();
        String user = ConnectionManager.getInstance().getPolardbxUser();

        boolean failedWithCorrectException = false;
        try {
            DriverManager.getConnection(url, user, "pass");
        } catch (SQLException e) {
            failedWithCorrectException = true;
            assertExpectedError(e, 1045, "28000");
        }

        Assert.assertTrue(failedWithCorrectException);
    }

    private void checkEmptyPasswordError() {
        String url = ConnectionManager.getInstance().getPolarDBxUrl();
        String user = ConnectionManager.getInstance().getPolardbxUser();

        boolean failedWithCorrectException = false;
        try {
            DriverManager.getConnection(url, user, "");
        } catch (SQLException e) {
            failedWithCorrectException = true;
            assertExpectedError(e, 1045, "28000");
        }

        Assert.assertTrue(failedWithCorrectException);
    }

    public void testSqlParserError() {
        try (Connection c = getPolardbxConnection()) {
            c.createStatement().executeQuery("select * from t1 xxx xxx");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("[PXC-4500][ERR_PARSER]"));
        }
    }

    private void setupDatabaseAndTables(String dbName) throws SQLException {
        try (Connection connection = getPolardbxConnection()) {
            connection.createStatement().execute("create database if not exists " + dbName + " mode=auto");
            connection.createStatement().execute("use " + dbName);
            connection.createStatement()
                .execute("create table if not exists test_errorcode_tbl(id int, primary key(id))");
        }
    }

    private void assertExpectedError(SQLException exception, int errorCode, String sqlState) {
        Assert.assertEqual(errorCode, exception.getErrorCode());
        Assert.assertEqual(sqlState, exception.getSQLState());
    }

    private void assertExpectedError(SQLException exception, int errorCode) {
        Assert.assertEqual(errorCode, exception.getErrorCode());
    }
}
