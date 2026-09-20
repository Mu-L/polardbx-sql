package com.alibaba.polardbx.qatest.dml.auto.basecrud;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.qatest.AutoCrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

public class InsertSplitterSwitchoverRescheduleTest extends AutoCrudBasedLockTestCase {

    private static final String TABLE_NAME = "insert_splitter_switchover_reschedule_tb";
    private static final int VALUE_COUNT = 5;

    @Before
    public void prepareTable() {
        JdbcUtil.executeUpdate(tddlConnection, "drop table if exists " + TABLE_NAME);
        JdbcUtil.executeUpdateSuccess(tddlConnection, "create table " + TABLE_NAME + " ("
            + "id bigint not null auto_increment,"
            + "store_id bigint,"
            + "store_code varchar(64) not null default '',"
            + "val varchar(200),"
            + "primary key(id)"
            + ") partition by key(store_id) partitions 4");
    }

    @After
    public void cleanup() {
        JdbcUtil.executeUpdate(tddlConnection, "drop table if exists " + TABLE_NAME);
    }

    @Test
    public void testRescheduleBeforeAnySplitBatch() throws SQLException {
        executeAndAssertSplitInsertWithSimulatedSwitchover(0, true, VALUE_COUNT);
    }

    @Test
    public void testNoRescheduleAfterPartialSplitBatch() throws SQLException {
        executeAndAssertSplitInsertWithSimulatedSwitchover(2, true, VALUE_COUNT);
    }

    @Test
    public void testDuplicateRowsWhenInternalSubExecutionGuardDisabled() throws SQLException {
        final long targetPhySqlId = 2;
        executeAndAssertSplitInsertWithSimulatedSwitchover(targetPhySqlId, false,
            VALUE_COUNT + (int) targetPhySqlId);
        assertDuplicatedRowsBeforeReschedule(targetPhySqlId);
    }

    private void executeAndAssertSplitInsertWithSimulatedSwitchover(long targetPhySqlId,
                                                                    boolean enableInternalSubExecutionGuard,
                                                                    int expectedRowCount) throws SQLException {
        try {
            setSessionParam(ConnectionProperties.BATCH_INSERT_POLICY, "'SPLIT'");
            setSessionParam(ConnectionProperties.MAX_BATCH_INSERT_SQL_LENGTH, "0");
            setSessionParam(ConnectionProperties.BATCH_INSERT_CHUNK_SIZE, "1");
            setSessionParam(ConnectionProperties.SIMULATE_SWITCHOVER_RESCHEDULE_PHY_SQL_ID_FOR_TEST,
                String.valueOf(targetPhySqlId));
            setSessionParam(ConnectionProperties.ENABLE_SWITCHOVER_RESCHEDULE_INTERNAL_SUB_EXECUTION_GUARD_FOR_TEST,
                String.valueOf(enableInternalSubExecutionGuard));

            JdbcUtil.executeUpdateSuccess(tddlConnection, "trace " + buildInsertSql());
            assertSplitPhysicalInsertCount();
            assertRowCount(expectedRowCount);
        } finally {
            resetSessionParams();
        }
    }

    private void setSessionParam(String paramName, String value) {
        JdbcUtil.executeSuccess(tddlConnection, "set " + paramName + "=" + value);
    }

    private void resetSessionParams() {
        setSessionParam(ConnectionProperties.BATCH_INSERT_POLICY, "'NONE'");
        setSessionParam(ConnectionProperties.MAX_BATCH_INSERT_SQL_LENGTH, "256");
        setSessionParam(ConnectionProperties.BATCH_INSERT_CHUNK_SIZE, "200");
        setSessionParam(ConnectionProperties.SIMULATE_SWITCHOVER_RESCHEDULE_PHY_SQL_ID_FOR_TEST, "-1");
        setSessionParam(ConnectionProperties.ENABLE_SWITCHOVER_RESCHEDULE_INTERNAL_SUB_EXECUTION_GUARD_FOR_TEST,
            "true");
    }

    private String buildInsertSql() {
        final StringBuilder sql = new StringBuilder("insert into ").append(TABLE_NAME)
            .append(" (store_id, store_code, val) values");
        for (int i = 0; i < VALUE_COUNT; i++) {
            if (i > 0) {
                sql.append(",");
            }
            sql.append("(")
                .append(i)
                .append(",'store_")
                .append(i)
                .append("','value_")
                .append(i)
                .append("')");
        }
        return sql.toString();
    }

    private void assertRowCount(int expected) {
        final String count = JdbcUtil.executeQueryAndGetFirstStringResult(
            "select count(*) from " + TABLE_NAME, tddlConnection);
        Assert.assertEquals(expected, Integer.parseInt(count));
    }

    private void assertDuplicatedRowsBeforeReschedule(long targetPhySqlId) {
        final String duplicatedCount = JdbcUtil.executeQueryAndGetFirstStringResult(
            "select count(*) from " + TABLE_NAME + " where store_id < " + targetPhySqlId, tddlConnection);
        Assert.assertEquals(targetPhySqlId * 2, Long.parseLong(duplicatedCount));

        final String remainingCount = JdbcUtil.executeQueryAndGetFirstStringResult(
            "select count(*) from " + TABLE_NAME + " where store_id >= " + targetPhySqlId, tddlConnection);
        Assert.assertEquals(VALUE_COUNT - targetPhySqlId, Long.parseLong(remainingCount));
    }

    private void assertSplitPhysicalInsertCount() throws SQLException {
        int insertCount = 0;
        try (ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show trace")) {
            while (rs.next()) {
                final String statement = rs.getString("STATEMENT");
                if (statement != null && statement.toLowerCase().contains("insert")) {
                    insertCount++;
                }
            }
        }
        Assert.assertTrue("InsertSplitter should split the logical insert into physical insert batches",
            insertCount >= VALUE_COUNT);
    }
}
