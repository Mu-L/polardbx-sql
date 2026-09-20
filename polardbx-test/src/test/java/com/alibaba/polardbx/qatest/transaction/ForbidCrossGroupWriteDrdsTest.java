package com.alibaba.polardbx.qatest.transaction;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

@CdcIgnore(ignoreReason = "XA事务CDC不支持排序")
public class ForbidCrossGroupWriteDrdsTest extends BaseTestCase {
    private static final String DB_NAME = "forbid_cross_group_write_test_drds_db";
    private static final String TABLE_NAME = "forbid_cross_group_write_test_tb";
    private static final String CREATE_TABLE_SQL = "create table if not exists " + TABLE_NAME
        + " ( id int primary key, a int )";
    private static final String DROP_TABLE_SQL = "drop table if exists " + TABLE_NAME;
    private static final String FORBID_CROSS_WRITE = "set FORBID_CROSS_GROUP_WRITE_FOR_EXPLICIT_TRX = true";
    private static final String PERMIT_CROSS_WRITE = "set FORBID_CROSS_GROUP_WRITE_FOR_EXPLICIT_TRX = false";
    private static final String FORBID_AUTO_COMMIT_TRX = "set FORBID_AUTO_COMMIT_TRX = true";
    private static final String PERMIT_AUTO_COMMIT_TRX = "set FORBID_AUTO_COMMIT_TRX = false";

    @BeforeClass
    public static void init() throws SQLException {
        try (Connection connection = getPolardbxConnection0()) {
            JdbcUtil.executeUpdateSuccess(connection, "DROP DATABASE IF EXISTS " + DB_NAME);
            JdbcUtil.executeUpdateSuccess(connection, "CREATE DATABASE IF NOT EXISTS " + DB_NAME + " mode = drds");
        }
    }

    @AfterClass
    public static void destroy() throws SQLException {
        try (Connection connection = getPolardbxConnection0()) {
            JdbcUtil.executeUpdateSuccess(connection, "DROP DATABASE IF EXISTS " + DB_NAME);
        }
    }

    private void truncateTable(Connection connection) throws SQLException {
        JdbcUtil.executeUpdateSuccess(connection, "TRUNCATE TABLE " + TABLE_NAME);
    }

    @Test
    public void test() throws SQLException {
        try (Connection connection = getPolardbxConnection0(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = TSO");
            testSharding(connection);
            testBroadcast(connection);
            testSingle(connection);
            testMixBroadcastAndSharding(connection);
        }

        try (Connection connection = getPolardbxConnection0(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = XA");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_XA_TSO = true");
            testSharding(connection);
            testBroadcast(connection);
            testSingle(connection);
            testMixBroadcastAndSharding(connection);
        }

        try (Connection connection = getPolardbxConnection0(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_policy = XA");
            JdbcUtil.executeUpdateSuccess(connection, "set ENABLE_XA_TSO = false");
            testSharding(connection);
            testBroadcast(connection);
            testSingle(connection);
            testMixBroadcastAndSharding(connection);
        }
    }

    private void testSharding(Connection connection) throws SQLException {
        JdbcUtil.executeUpdateSuccess(connection, DROP_TABLE_SQL);
        JdbcUtil.executeUpdateSuccess(connection, CREATE_TABLE_SQL + " dbpartition by hash (id)");
        // permit cross group write in trx
        JdbcUtil.executeUpdateSuccess(connection, PERMIT_CROSS_WRITE);
        permitCrossWriteInTrx(connection);

        // forbid cross group write in trx
        JdbcUtil.executeUpdateSuccess(connection, FORBID_CROSS_WRITE);
        forbidCrossWriteInTrx(connection);

        // single statement trx + forbid_auto_commit_trx can not cross group
        JdbcUtil.executeUpdateSuccess(connection, FORBID_CROSS_WRITE);
        JdbcUtil.executeUpdateSuccess(connection, FORBID_AUTO_COMMIT_TRX);
        forbidAutoCommitTrxCrossGroup(connection);
    }

    private void testBroadcast(Connection connection) throws SQLException {
        JdbcUtil.executeUpdateSuccess(connection, DROP_TABLE_SQL);
        JdbcUtil.executeUpdateSuccess(connection, CREATE_TABLE_SQL + " broadcast");
        // permit cross group write in trx
        JdbcUtil.executeUpdateSuccess(connection, PERMIT_CROSS_WRITE);
        permitCrossWriteInTrx(connection);

        // forbid cross group write in trx, but broadcast can always cross group.
        JdbcUtil.executeUpdateSuccess(connection, FORBID_CROSS_WRITE);
        permitCrossWriteInTrx(connection);

        // single statement trx can always cross group for broadcast table
        JdbcUtil.executeUpdateSuccess(connection, FORBID_CROSS_WRITE);
        JdbcUtil.executeUpdateSuccess(connection, PERMIT_AUTO_COMMIT_TRX);
        permitAutoCommitTrxCrossWrite(connection);

        JdbcUtil.executeUpdateSuccess(connection, FORBID_CROSS_WRITE);
        JdbcUtil.executeUpdateSuccess(connection, FORBID_AUTO_COMMIT_TRX);
        permitAutoCommitTrxCrossWrite(connection);
    }

    private void testSingle(Connection connection) throws SQLException {
        JdbcUtil.executeUpdateSuccess(connection, DROP_TABLE_SQL);
        JdbcUtil.executeUpdateSuccess(connection, CREATE_TABLE_SQL + " single");
        // permit cross group write in trx
        JdbcUtil.executeUpdateSuccess(connection, PERMIT_CROSS_WRITE);
        permitCrossWriteInTrx(connection);

        // forbid cross group write in trx, but single table not cross group
        JdbcUtil.executeUpdateSuccess(connection, FORBID_CROSS_WRITE);
        permitCrossWriteInTrx(connection);

        // single statement trx can always cross group for single table
        JdbcUtil.executeUpdateSuccess(connection, FORBID_CROSS_WRITE);
        JdbcUtil.executeUpdateSuccess(connection, PERMIT_AUTO_COMMIT_TRX);
        permitAutoCommitTrxCrossWrite(connection);

        JdbcUtil.executeUpdateSuccess(connection, FORBID_CROSS_WRITE);
        JdbcUtil.executeUpdateSuccess(connection, FORBID_AUTO_COMMIT_TRX);
        permitAutoCommitTrxCrossWrite(connection);
    }

    private void testMixBroadcastAndSharding(Connection connection) throws SQLException {
        JdbcUtil.executeUpdateSuccess(connection, DROP_TABLE_SQL);
        JdbcUtil.executeUpdateSuccess(connection, CREATE_TABLE_SQL + " broadcast");
        // After writing broadcast table, write group 0 is allowed but others are forbidden.
        String shardingTable = TABLE_NAME + "_sharding";
        try {
            JdbcUtil.executeUpdateSuccess(connection, "drop table if exists " + shardingTable);
            String createTable =
                "create table if not exists " + shardingTable + "(id int primary key, a int) dbpartition by hash (id)";
            JdbcUtil.executeUpdateSuccess(connection, createTable);
            JdbcUtil.executeUpdateSuccess(connection, FORBID_CROSS_WRITE);
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getInsertSql(TABLE_NAME, 0, 1, 2, 3, 4, 5, 6, 7, 8));
            // 0 is OK
            JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getInsertSql(shardingTable, 0));
            // 1 is not OK
            JdbcUtil.executeUpdateFailed(connection, JdbcUtil.getInsertSql(shardingTable, 1),
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(connection, "rollback");

            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getInsertSql(TABLE_NAME, 0, 1, 2, 3, 4, 5, 6, 7, 8));
            // 0 is OK
            JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getUpdateSql(shardingTable, 0));
            // 1 is not OK
            JdbcUtil.executeUpdateFailed(connection, JdbcUtil.getUpdateSql(shardingTable, 1),
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(connection, "rollback");

            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getInsertSql(TABLE_NAME, 0, 1, 2, 3, 4, 5, 6, 7, 8));
            // 0 is OK
            JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getDeleteSql(shardingTable, 0));
            // 1 is not OK
            JdbcUtil.executeUpdateFailed(connection, JdbcUtil.getDeleteSql(shardingTable, 1),
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(connection, "rollback");
        } finally {
            JdbcUtil.executeUpdateSuccess(connection, "drop table if exists " + shardingTable);
        }
    }

    private void permitCrossWriteInTrx(Connection connection) throws SQLException {
        truncateTable(connection);
        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeQuerySuccess(connection, JdbcUtil.getSelectCountSql(TABLE_NAME, 0, 1, 2, 3, 4, 5, 6, 7, 8));
        JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getInsertSql(TABLE_NAME, 0, 1, 2, 3, 4, 5, 6, 7, 8));
        JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getUpdateSql(TABLE_NAME, 0, 1, 2, 3, 4, 5, 6, 7, 8));
        JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getDeleteSql(TABLE_NAME, 0, 1, 2, 3, 4, 5, 6, 7, 8));
        JdbcUtil.executeUpdateSuccess(connection, "commit");
    }

    private void forbidCrossWriteInTrx(Connection connection) throws SQLException {
        truncateTable(connection);
        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeQuerySuccess(connection, JdbcUtil.getSelectCountSql(TABLE_NAME, 0, 1, 2, 3, 4, 5, 6, 7, 8));
        JdbcUtil.executeUpdateFailed(connection, JdbcUtil.getInsertSql(TABLE_NAME, 0, 1, 2, 3, 4, 5, 6, 7, 8),
            "ERR_CROSS_GROUP_TRANSACTION");
        JdbcUtil.executeUpdateSuccess(connection, "rollback");

        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeQuerySuccess(connection, JdbcUtil.getSelectCountSql(TABLE_NAME, 0, 1, 2, 3, 4, 5, 6, 7, 8));
        JdbcUtil.executeUpdateFailed(connection, JdbcUtil.getUpdateSql(TABLE_NAME, 0, 1, 2, 3, 4, 5, 6, 7, 8),
            "ERR_CROSS_GROUP_TRANSACTION");
        JdbcUtil.executeUpdateSuccess(connection, "rollback");

        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeQuerySuccess(connection, JdbcUtil.getSelectCountSql(TABLE_NAME, 0, 1, 2, 3, 4, 5, 6, 7, 8));
        JdbcUtil.executeUpdateFailed(connection, JdbcUtil.getDeleteSql(TABLE_NAME, 0, 1, 2, 3, 4, 5, 6, 7, 8),
            "ERR_CROSS_GROUP_TRANSACTION");
        JdbcUtil.executeUpdateSuccess(connection, "rollback");
    }

    private void permitAutoCommitTrxCrossWrite(Connection connection) throws SQLException {
        truncateTable(connection);
        JdbcUtil.executeQuerySuccess(connection, JdbcUtil.getSelectCountSql(TABLE_NAME, 0, 1, 2, 3, 4, 5, 6, 7, 8));
        JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getInsertSql(TABLE_NAME, 0, 1, 2, 3, 4, 5, 6, 7, 8));
        JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getUpdateSql(TABLE_NAME, 0, 1, 2, 3, 4, 5, 6, 7, 8));
        JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getDeleteSql(TABLE_NAME, 0, 1, 2, 3, 4, 5, 6, 7, 8));
    }

    private void forbidAutoCommitTrxCrossGroup(Connection connection) throws SQLException {
        truncateTable(connection);
        JdbcUtil.executeQuerySuccess(connection, JdbcUtil.getSelectCountSql(TABLE_NAME, 0, 1, 2, 3, 4, 5, 6, 7, 8));
        JdbcUtil.executeUpdateFailed(connection, JdbcUtil.getInsertSql(TABLE_NAME, 0, 1, 2, 3, 4, 5, 6, 7, 8),
            "ERR_CROSS_GROUP_TRANSACTION");
        JdbcUtil.executeUpdateFailed(connection, JdbcUtil.getUpdateSql(TABLE_NAME, 0, 1, 2, 3, 4, 5, 6, 7, 8),
            "ERR_CROSS_GROUP_TRANSACTION");
        JdbcUtil.executeUpdateFailed(connection, JdbcUtil.getDeleteSql(TABLE_NAME, 0, 1, 2, 3, 4, 5, 6, 7, 8),
            "ERR_CROSS_GROUP_TRANSACTION");
    }
}
