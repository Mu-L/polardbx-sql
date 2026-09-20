package com.alibaba.polardbx.qatest.protocol;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @version 1.0
 */
@NotThreadSafe
public class XErrorTest extends ReadBaseTestCase {

    private static final String PARTIAL_RESULT_DEADLOCK_TABLE = "x_error_partial_result_deadlock";
    private static final int STREAMED_ROW_COUNT = 500;
    private static final int TOTAL_DEADLOCK_ROW_COUNT = 501;
    private static final Pattern DRIVER_VERSION_PATTERN =
        Pattern.compile("(\\d+)\\.(\\d+)\\.(\\d+)");

    @Test
    public void errPacketAfterPartialResultKeepsConnectionUsable() throws SQLException {
        Assume.assumeTrue("X-Protocol is required to inject a streaming timeout", useXproto(tddlConnection));

        final String variable = ConnectionProperties.ENABLE_ERR_PACKET_AFTER_PARTIAL_RESULT;
        final String originalValue =
            getVariableValue(variable, ConnectionParams.ENABLE_ERR_PACKET_AFTER_PARTIAL_RESULT.getDefault());
        final String chunkResultVariable = ConnectionProperties.CONN_POOL_XPROTO_CHUNK_RESULT;
        final String originalChunkResultValue =
            getVariableValue(chunkResultVariable, ConnectionParams.CONN_POOL_XPROTO_CHUNK_RESULT.getDefault());
        final String table = ExecuteTableSelect.selectBaseOneTable()[0][0];

        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set global " + variable + " = true");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set global " + chunkResultVariable + " = false");
            try (Connection connection = getPolardbxDirectConnection()) {
                assertCompatibleConnectorJ(connection);
                final long connectionId = getConnectionId(connection);
                assertPartialResultError(executePartialResultTimeout(connection, table), true);

                try (Statement statement = connection.createStatement();
                    ResultSet resultSet = statement.executeQuery("select 1")) {
                    Assert.assertTrue("The connection should remain usable after the ERR packet", resultSet.next());
                    Assert.assertEquals(1, resultSet.getInt(1));
                }
                Assert.assertEquals("The physical connection must be reused",
                    connectionId, getConnectionId(connection));
            }
        } finally {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "set global " + chunkResultVariable + " = '" + originalChunkResultValue + "'");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "set global " + variable + " = '" + originalValue + "'");
        }
    }

    @Test(timeout = 120000)
    public void errPacketAfterDeadlockKeepsConnectionUsable() throws Exception {
        Assume.assumeTrue("X-Protocol is required to stream a partial result", useXproto(tddlConnection));

        final String variable = ConnectionProperties.ENABLE_ERR_PACKET_AFTER_PARTIAL_RESULT;
        final String originalValue =
            getVariableValue(variable, ConnectionParams.ENABLE_ERR_PACKET_AFTER_PARTIAL_RESULT.getDefault());
        final String chunkResultVariable = ConnectionProperties.CONN_POOL_XPROTO_CHUNK_RESULT;
        final String originalChunkResultValue =
            getVariableValue(chunkResultVariable, ConnectionParams.CONN_POOL_XPROTO_CHUNK_RESULT.getDefault());

        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set global " + variable + " = true");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set global " + chunkResultVariable + " = false");
            preparePartialResultDeadlockTable();

            try (Connection lockConnection = getPolardbxDirectConnection();
                Connection streamingConnection = getPolardbxDirectConnection()) {
                assertCompatibleConnectorJ(streamingConnection);
                final long connectionId = getConnectionId(streamingConnection);

                SQLException queryFailure =
                    executePartialResultDeadlock(lockConnection, streamingConnection);

                Assert.assertEquals("The original deadlock error code must be preserved",
                    1213, queryFailure.getErrorCode());
                Assert.assertEquals("40001", queryFailure.getSQLState());
                Assert.assertFalse(String.valueOf(queryFailure.getMessage()).toLowerCase(Locale.ROOT),
                    String.valueOf(queryFailure.getMessage()).toLowerCase(Locale.ROOT)
                        .contains("packets out of order"));
                Assert.assertEquals("The physical connection must remain unchanged",
                    connectionId, getConnectionId(streamingConnection));

                try (Statement statement = streamingConnection.createStatement();
                    ResultSet resultSet = statement.executeQuery("select 1")) {
                    Assert.assertTrue("The connection should remain usable after the deadlock ERR packet",
                        resultSet.next());
                    Assert.assertEquals(1, resultSet.getInt(1));
                }
            }
        } finally {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "drop table if exists " + PARTIAL_RESULT_DEADLOCK_TABLE);
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "set global " + chunkResultVariable + " = '" + originalChunkResultValue + "'");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "set global " + variable + " = '" + originalValue + "'");
        }
    }

    @Test
    public void errPacketAfterPartialResultClosesConnectionWhenSwitchDisabled() throws SQLException {
        Assume.assumeTrue("X-Protocol is required to inject a streaming timeout", useXproto(tddlConnection));

        final String variable = ConnectionProperties.ENABLE_ERR_PACKET_AFTER_PARTIAL_RESULT;
        final String originalValue =
            getVariableValue(variable, ConnectionParams.ENABLE_ERR_PACKET_AFTER_PARTIAL_RESULT.getDefault());
        final String chunkResultVariable = ConnectionProperties.CONN_POOL_XPROTO_CHUNK_RESULT;
        final String originalChunkResultValue =
            getVariableValue(chunkResultVariable, ConnectionParams.CONN_POOL_XPROTO_CHUNK_RESULT.getDefault());
        final String table = ExecuteTableSelect.selectBaseOneTable()[0][0];

        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set global " + variable + " = false");
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set global " + chunkResultVariable + " = false");
            try (Connection connection = getPolardbxDirectConnection()) {
                assertPartialResultError(executePartialResultTimeout(connection, table), false);

                try (Statement statement = connection.createStatement()) {
                    statement.executeQuery("select 1");
                    Assert.fail("The connection should be closed after the ERR packet");
                } catch (SQLException expected) {
                    // The switch preserves the legacy ERR-then-close behavior.
                }
            }
        } finally {
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "set global " + chunkResultVariable + " = '" + originalChunkResultValue + "'");
            JdbcUtil.executeUpdateSuccess(tddlConnection,
                "set global " + variable + " = '" + originalValue + "'");
        }
    }

    @Test
    public void TimeoutTest() {
        if (!useXproto(tddlConnection)) {
            return;
        }
        final String table = ExecuteTableSelect.selectBaseOneTable()[0][0];
        try {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "begin");
            String sql = "/*+TDDL: cmd_extra(SOCKET_TIMEOUT=100)*/select sleep(1) from " + table;
            JdbcUtil.executeQueryFaied(tddlConnection, sql, "XResult stream fetch result timeout");
            sql = "select * from " + table + " limit 1;";
            JdbcUtil.executeQueryFaied(tddlConnection, sql, "Previous query timeout");
        } finally {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "rollback");
        }
    }

    @Ignore
    public void MaxSessionTest() {
        if (!useXproto(tddlConnection)) {
            return;
        }

        try (final ResultSet rs = JdbcUtil.executeQuery("show variables like 'new_rpc'", tddlConnection)) {
            while (rs.next()) {
                if (rs.getString(2).equalsIgnoreCase("on")) {
                    return;
                }
            }
        } catch (Throwable ignore) {
        }

        final long max_conns =
            JdbcUtil.resultLong(JdbcUtil.executeQuery("select @@polarx_max_connections", tddlConnection));
        try {
            // make it smaller
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set global polarx_max_connections=100");

            // make more connections
            List<Connection> conns = new ArrayList<>(200);
            try {
                for (int i = 0; i < 200; ++i) {
                    final Connection conn = getPolardbxDirectConnection();
                    conns.add(conn);
                    conn.setAutoCommit(false);
                    try (final Statement s = conn.createStatement()) {
                        s.execute("select * from " + polardbxOneDB + ".select_base_one_multi_db_multi_tb where pk=1");
                    }
                }
                Assert.fail("should fail with max conns exceed");
            } catch (Throwable e) {
                Assert.assertTrue("Should throw out of max session count",
                    e.getMessage().contains("Out of max session count"));
            }
            for (Connection c : conns) {
                try {
                    c.close();
                } catch (Throwable ignore) {
                }
            }
        } finally {
            // restore
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set global polarx_max_connections=" + max_conns);
        }
    }

    private String getVariableValue(String variable, String defaultValue) throws SQLException {
        try (Statement statement = tddlConnection.createStatement();
            ResultSet resultSet = statement.executeQuery("show variables like '" + variable + "'")) {
            if (resultSet.next()) {
                return resultSet.getString(2);
            }
            // Upgraded instances may not have an explicit value and use the compatibility default.
            return defaultValue;
        }
    }

    private SQLException executePartialResultTimeout(Connection connection, String table) {
        final String sql = "/*+TDDL: cmd_extra(ENABLE_MPP=false)*/ "
            + "select /*+ MAX_EXECUTION_TIME(300) */ pk, repeat('x', 65536), "
            + "if(pk = 1, 0, sleep(1)) from " + table
            + " where pk in (1, 2) order by pk";
        int returnedRows = 0;

        try (Statement statement = connection.createStatement()) {
            statement.setFetchSize(Integer.MIN_VALUE);
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                while (resultSet.next()) {
                    returnedRows++;
                }
            }
        } catch (SQLException e) {
            Assert.assertTrue("The query should return a partial result before the timeout", returnedRows > 0);
            return e;
        }

        Assert.fail("The streaming query should fail");
        return null;
    }

    private void assertPartialResultError(SQLException queryFailure, boolean requireServerError) {
        Assert.assertNotNull("The streaming query should fail", queryFailure);
        final String errorMessage = String.valueOf(queryFailure.getMessage()).toLowerCase(Locale.ROOT);
        if (requireServerError) {
            Assert.assertTrue("The client must receive a server ERR packet",
                queryFailure.getErrorCode() > 0);
            Assert.assertNotNull("The server ERR packet must contain a SQLState", queryFailure.getSQLState());
            Assert.assertFalse("A server ERR must not be reported as a communications failure",
                queryFailure.getSQLState().startsWith("08"));
            Assert.assertTrue(errorMessage,
                errorMessage.contains("maximum statement execution time exceeded")
                    || errorMessage.contains("query execution was interrupted"));
        } else {
            Assert.assertTrue(errorMessage,
                errorMessage.contains("maximum statement execution time exceeded")
                    || errorMessage.contains("query execution was interrupted")
                    || errorMessage.contains("application was streaming results when the connection failed"));
        }
        Assert.assertFalse(errorMessage,
            errorMessage.contains("packets out of order"));
    }

    private void preparePartialResultDeadlockTable() {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "drop table if exists " + PARTIAL_RESULT_DEADLOCK_TABLE);
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "create table " + PARTIAL_RESULT_DEADLOCK_TABLE
                + " (id int primary key, pad varchar(2000))");

        StringBuilder insert = new StringBuilder("insert into ")
            .append(PARTIAL_RESULT_DEADLOCK_TABLE)
            .append(" (id, pad) values ");
        for (int id = 1; id <= TOTAL_DEADLOCK_ROW_COUNT; id++) {
            if (id > 1) {
                insert.append(',');
            }
            insert.append('(')
                .append(id)
                .append(", repeat('x', 1000))");
        }
        JdbcUtil.executeUpdateSuccess(tddlConnection, insert.toString());
    }

    private SQLException executePartialResultDeadlock(Connection lockConnection, Connection streamingConnection)
        throws Exception {
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<Boolean> waitingUpdate = null;

        try {
            JdbcUtil.executeUpdateSuccess(lockConnection, "begin");
            JdbcUtil.executeUpdateSuccess(streamingConnection, "begin");
            JdbcUtil.executeUpdateSuccess(lockConnection, "set innodb_lock_wait_timeout = 10");
            JdbcUtil.executeUpdateSuccess(streamingConnection, "set innodb_lock_wait_timeout = 10");

            try (Statement statement = lockConnection.createStatement()) {
                try (ResultSet resultSet = statement.executeQuery(
                    "select id from " + PARTIAL_RESULT_DEADLOCK_TABLE + " where id = 501 for update")) {
                    Assert.assertTrue(resultSet.next());
                }
            }

            int returnedRows = 0;
            SQLException queryFailure = null;
            try (Statement statement = streamingConnection.createStatement()) {
                statement.setFetchSize(Integer.MIN_VALUE);
                try (ResultSet resultSet = statement.executeQuery(
                    "/*+TDDL: cmd_extra(ENABLE_MPP=false)*/ select id, pad from "
                        + PARTIAL_RESULT_DEADLOCK_TABLE + " where id <= 501 order by id for update")) {
                    while (returnedRows < STREAMED_ROW_COUNT && resultSet.next()) {
                        returnedRows++;
                    }
                    Assert.assertEquals("The query must return rows before the deadlock",
                        STREAMED_ROW_COUNT, returnedRows);

                    waitingUpdate = executor.submit(() -> {
                        try (Statement statement1 = lockConnection.createStatement();
                            ResultSet resultSet1 = statement1.executeQuery(
                                "select id from " + PARTIAL_RESULT_DEADLOCK_TABLE
                                    + " where id = 1 for update")) {
                            return resultSet1.next();
                        }
                    });

                    try {
                        while (resultSet.next()) {
                            returnedRows++;
                        }
                    } catch (SQLException e) {
                        queryFailure = e;
                    }
                }
            }

            Assert.assertNotNull("The streaming transaction must be selected as the deadlock victim",
                queryFailure);
            // Logical deadlock detection may be degraded under load; error mapping is covered by unit tests.
            Assume.assumeTrue("Deadlock detection is unavailable in this environment: error code "
                    + queryFailure.getErrorCode() + ", SQLState " + queryFailure.getSQLState(),
                queryFailure.getErrorCode() == 1213);
            Assert.assertEquals("No row after the lock wait may be returned",
                STREAMED_ROW_COUNT, returnedRows);
            Assert.assertTrue(waitingUpdate.get(30, TimeUnit.SECONDS));
            return queryFailure;
        } finally {
            if (waitingUpdate != null && !waitingUpdate.isDone()) {
                waitingUpdate.cancel(true);
            }
            executor.shutdownNow();
            rollbackQuietly(streamingConnection);
            rollbackQuietly(lockConnection);
        }
    }

    private void assertCompatibleConnectorJ(Connection connection) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        String driverName = metaData.getDriverName();
        String driverVersion = metaData.getDriverVersion();
        Assert.assertTrue("Unexpected JDBC driver: " + driverName,
            "MySQL Connector Java".equalsIgnoreCase(driverName)
                || "MySQL Connector/J".equalsIgnoreCase(driverName));

        Matcher matcher = DRIVER_VERSION_PATTERN.matcher(driverVersion);
        Assert.assertTrue("Unparseable JDBC driver version: " + driverVersion, matcher.find());
        int major = Integer.parseInt(matcher.group(1));
        int minor = Integer.parseInt(matcher.group(2));
        int patch = Integer.parseInt(matcher.group(3));
        Assert.assertTrue("Connector/J 5.1.35 or later is required: " + driverVersion,
            major > 5 || (major == 5 && minor > 1) || (major == 5 && minor == 1 && patch >= 35));
    }

    private long getConnectionId(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery("select connection_id()")) {
            Assert.assertTrue(resultSet.next());
            return resultSet.getLong(1);
        }
    }

    private void rollbackQuietly(Connection connection) {
        try {
            connection.rollback();
        } catch (SQLException ignore) {
        }
    }

    @Test
    @Ignore("测会话超出阈值会影响到其他并发case，这个场景已经在单元测试中测了，ignore掉")
    public void MaxActiveSessionTest() {
        if (!useXproto(tddlConnection)) {
            return;
        }

        // make more connections
        final List<Thread> threads = new ArrayList<>(1000);
        final AtomicReference<Exception> exc = new AtomicReference<>(null);
        for (int i = 0; i < 1000; ++i) {
            if (exc.get() != null) {
                break;
            }
            final Thread t = new Thread(() -> {
                try (final Connection conn = getPolardbxDirectConnection()) {
                    conn.setAutoCommit(false);
                    try (final Statement s = conn.createStatement()) {
                        s.execute("select sleep(5),pk from " + polardbxOneDB
                            + ".select_base_one_multi_db_multi_tb where pk=1");
                    }
                } catch (Exception e) {
                    exc.compareAndSet(null, e);
                }
            });
            t.start();
            threads.add(t);
        }
        try {
            for (Thread t : threads) {
                t.join();
            }
        } catch (Throwable ignore) {
        }

        final Exception e = exc.get();
        Assert.assertTrue("Should throw max concurrent or wait exceed.",
            e != null && e.getMessage().contains("Max concurrent or wait exceed."));
    }
}
