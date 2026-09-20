package com.alibaba.polardbx.qatest.transaction;

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.ddl.datamigration.locality.LocalityTestCaseUtils.LocalityTestUtils;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

@CdcIgnore(ignoreReason = "XA事务CDC不支持排序")
public class ForbidCrossGroupWriteAutoTest extends BaseTestCase {
    private static final String DB_NAME = "forbid_cross_group_write_test_auto_db";
    private static final String TABLE_NAME = "forbid_cross_group_write_test_tb";
    private static final String CREATE_TABLE_SQL = "create table if not exists " + TABLE_NAME
        + " ( id int primary key, a int default 0 )";
    private static final String DROP_TABLE_SQL = "drop table if exists " + TABLE_NAME;
    private static final String FORBID_CROSS_WRITE = "set FORBID_CROSS_GROUP_WRITE_FOR_EXPLICIT_TRX = true";
    private static final String PERMIT_CROSS_WRITE = "set FORBID_CROSS_GROUP_WRITE_FOR_EXPLICIT_TRX = false";
    private static final String FORBID_AUTO_COMMIT_TRX = "set FORBID_AUTO_COMMIT_TRX = true";
    private static final String PERMIT_AUTO_COMMIT_TRX = "set FORBID_AUTO_COMMIT_TRX = false";

    @BeforeClass
    public static void init() throws SQLException {
        try (Connection connection = getPolardbxConnection0()) {
            JdbcUtil.executeUpdateSuccess(connection, "DROP DATABASE IF EXISTS " + DB_NAME);
            JdbcUtil.executeUpdateSuccess(connection, "CREATE DATABASE IF NOT EXISTS " + DB_NAME + " mode = auto");
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
        JdbcUtil.executeUpdateSuccess(connection, CREATE_TABLE_SQL
            + " partition by range (id) (PARTITION `p0` VALUES LESS THAN (5),PARTITION `p1` VALUES LESS THAN (10))");
        // permit cross group write in trx
        JdbcUtil.executeUpdateSuccess(connection, PERMIT_CROSS_WRITE);
        permitCrossWriteInTrx(connection);

        // forbid cross group write in trx
        JdbcUtil.executeUpdateSuccess(connection, FORBID_CROSS_WRITE);
        forbidCrossWriteInTrx(connection);

        // single statement trx can not cross group
        JdbcUtil.executeUpdateSuccess(connection, FORBID_CROSS_WRITE);
        forbidAutoCommitTrxCrossGroup(connection);

        // update sharding key
        testUpdateShardingKey(connection);

        // GSI
        testGsi(connection);
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

    private void testMixBroadcastAndSharding(Connection connection) {
        JdbcUtil.executeUpdateSuccess(connection, DROP_TABLE_SQL);
        JdbcUtil.executeUpdateSuccess(connection, CREATE_TABLE_SQL + " broadcast");
        // After writing broadcast table, write group 0 is allowed but others are forbidden.
        String shardingTable = TABLE_NAME + "_sharding";
        List<String> dataNodes = LocalityTestUtils.getDatanodes(connection);
        if (dataNodes.size() < 2) {
            return;
        }
        String group0 = dataNodes.get(0);
        String group1 = dataNodes.get(1);
        try {
            JdbcUtil.executeUpdateSuccess(connection, "drop table if exists " + shardingTable);
            String createTable =
                "create table if not exists " + shardingTable + "(id int primary key, a int) "
                    + "partition by list (id) "
                    + "( partition p1 values in (0) locality='dn=" + group0 + "',"
                    + " partition p2 values in (1) locality='dn=" + group1 + "')";
            JdbcUtil.executeUpdateSuccess(connection, createTable);
            JdbcUtil.executeUpdateSuccess(connection, FORBID_CROSS_WRITE);
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getInsertSql(TABLE_NAME, 0, 1, 2, 3, 4, 5, 6, 7, 8));
            // 0 is OK
            JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getInsertSql(shardingTable, 0));
            // 1 is not OK
            JdbcUtil.executeUpdateFailed(connection, JdbcUtil.getInsertSql(shardingTable, 1),
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getUpdateSql(TABLE_NAME, 0));
            JdbcUtil.executeUpdateSuccess(connection, "rollback");

            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getInsertSql(TABLE_NAME, 0, 1, 2, 3, 4, 5, 6, 7, 8));
            // 0 is OK
            JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getUpdateSql(shardingTable, 0));
            // 1 is not OK
            JdbcUtil.executeUpdateFailed(connection, JdbcUtil.getUpdateSql(shardingTable, 1),
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getUpdateSql(TABLE_NAME, 0));
            JdbcUtil.executeUpdateSuccess(connection, "rollback");

            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getInsertSql(TABLE_NAME, 0, 1, 2, 3, 4, 5, 6, 7, 8));
            // 0 is OK
            JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getDeleteSql(shardingTable, 0));
            // 1 is not OK
            JdbcUtil.executeUpdateFailed(connection, JdbcUtil.getDeleteSql(shardingTable, 1),
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getUpdateSql(TABLE_NAME, 0));
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
        JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getInsertSql(TABLE_NAME, 0, 1));
        JdbcUtil.executeUpdateSuccess(connection, "commit");

        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeQuerySuccess(connection, JdbcUtil.getSelectCountSql(TABLE_NAME, 0, 1, 2, 3, 4, 5, 6, 7, 8));
        JdbcUtil.executeUpdateFailed(connection, JdbcUtil.getUpdateSql(TABLE_NAME, 0, 1, 2, 3, 4, 5, 6, 7, 8),
            "ERR_CROSS_GROUP_TRANSACTION");
        JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getUpdateSql(TABLE_NAME, 0, 1));
        JdbcUtil.executeUpdateSuccess(connection, "commit");

        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeQuerySuccess(connection, JdbcUtil.getSelectCountSql(TABLE_NAME, 0, 1, 2, 3, 4, 5, 6, 7, 8));
        JdbcUtil.executeUpdateFailed(connection, JdbcUtil.getDeleteSql(TABLE_NAME, 0, 1, 2, 3, 4, 5, 6, 7, 8),
            "ERR_CROSS_GROUP_TRANSACTION");
        JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getDeleteSql(TABLE_NAME, 0, 1));
        JdbcUtil.executeUpdateSuccess(connection, "commit");
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
        ResultSet rs =
            JdbcUtil.executeQuerySuccess(connection, JdbcUtil.getSelectCountSql(TABLE_NAME, 0, 1, 2, 3, 4, 5, 6, 7, 8));
        if (rs.next()) {
            Assert.assertEquals(0, rs.getInt(1));
        }
        JdbcUtil.executeUpdateFailed(connection, JdbcUtil.getUpdateSql(TABLE_NAME, 0, 1, 2, 3, 4, 5, 6, 7, 8),
            "ERR_CROSS_GROUP_TRANSACTION");
        JdbcUtil.executeUpdateFailed(connection, JdbcUtil.getDeleteSql(TABLE_NAME, 0, 1, 2, 3, 4, 5, 6, 7, 8),
            "ERR_CROSS_GROUP_TRANSACTION");
    }

    private void testUpdateShardingKey(Connection connection) throws SQLException {
        JdbcUtil.executeUpdateSuccess(connection, FORBID_CROSS_WRITE);
        truncateTable(connection);
        JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getInsertSql(TABLE_NAME, 0, 0));
        JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getInsertSql(TABLE_NAME, 8, 0));
        // update sharding key is allowed if key not exists
        JdbcUtil.executeUpdateSuccess(connection, "update " + TABLE_NAME + " set id = 7 where id = 1");
        // update sharding key is not allowed if key exists
        JdbcUtil.executeUpdateFailed(connection, "update " + TABLE_NAME + " set id = 1 where id = 8",
            "ERR_CROSS_GROUP_TRANSACTION");
        // update sharding key is allowed if shard not changes
        JdbcUtil.executeUpdateSuccess(connection, "update " + TABLE_NAME + " set id = 1 where id = 0");
        // update with no sharding key specified is not allowed
        JdbcUtil.executeUpdateFailed(connection, "update " + TABLE_NAME + " set a = 1 where a = 2",
            "ERR_CROSS_GROUP_TRANSACTION");
        JdbcUtil.executeUpdateFailed(connection, "update " + TABLE_NAME + " set a = 1 where a = 0",
            "ERR_CROSS_GROUP_TRANSACTION");
        // delete with no sharding key specified is not allowed
        JdbcUtil.executeUpdateFailed(connection, "delete from " + TABLE_NAME + " where a = 2",
            "ERR_CROSS_GROUP_TRANSACTION");
        JdbcUtil.executeUpdateFailed(connection, "delete from " + TABLE_NAME + " where a = 0",
            "ERR_CROSS_GROUP_TRANSACTION");
        // in trx
        truncateTable(connection);
        JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getInsertSql(TABLE_NAME, 0, 0));
        JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getInsertSql(TABLE_NAME, 8, 0));
        // update sharding key is allowed if key not exists
        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeUpdateSuccess(connection, "update " + TABLE_NAME + " set id = 7 where id = 1");
        JdbcUtil.executeUpdateSuccess(connection, "commit");
        // update sharding key is not allowed if key exists
        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeUpdateFailed(connection, "update " + TABLE_NAME + " set id = 1 where id = 8",
            "ERR_CROSS_GROUP_TRANSACTION");
        JdbcUtil.executeUpdateSuccess(connection, "commit");
        // update sharding key is allowed if shard not changes
        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeUpdateSuccess(connection, "update " + TABLE_NAME + " set id = 1 where id = 0");
        JdbcUtil.executeUpdateSuccess(connection, "commit");
        // update with no sharding key specified is not allowed
        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeUpdateFailed(connection, "update " + TABLE_NAME + " set a = 1 where a = 2",
            "ERR_CROSS_GROUP_TRANSACTION");
        JdbcUtil.executeUpdateSuccess(connection, "rollback");
        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeUpdateFailed(connection, "update " + TABLE_NAME + " set a = 1 where a = 0",
            "ERR_CROSS_GROUP_TRANSACTION");
        JdbcUtil.executeUpdateSuccess(connection, "rollback");
        // delete with no sharding key specified is not allowed
        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeUpdateFailed(connection, "delete from " + TABLE_NAME + " where a = 2",
            "ERR_CROSS_GROUP_TRANSACTION");
        JdbcUtil.executeUpdateSuccess(connection, "rollback");
        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeUpdateFailed(connection, "delete from " + TABLE_NAME + " where a = 0",
            "ERR_CROSS_GROUP_TRANSACTION");
        JdbcUtil.executeUpdateSuccess(connection, "rollback");

        JdbcUtil.executeUpdateSuccess(connection, PERMIT_CROSS_WRITE);
        truncateTable(connection);
        JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getInsertSql(TABLE_NAME, 0, 0));
        JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getInsertSql(TABLE_NAME, 8, 0));
        JdbcUtil.executeUpdateSuccess(connection, "update " + TABLE_NAME + " set id = 1 where id = 8");
        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeUpdateSuccess(connection, "update " + TABLE_NAME + " set id = 7 where id = 0");
        JdbcUtil.executeUpdateSuccess(connection, "commit");
        JdbcUtil.executeUpdateSuccess(connection, "update " + TABLE_NAME + " set a = 1 where a = 2");
        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeUpdateSuccess(connection, "update " + TABLE_NAME + " set a = 1 where a = 2");
        JdbcUtil.executeUpdateSuccess(connection, "commit");
        JdbcUtil.executeUpdateSuccess(connection, "update " + TABLE_NAME + " set a = 1 where a = 0");
        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeUpdateSuccess(connection, "update " + TABLE_NAME + " set a = 2 where a = 1");
        JdbcUtil.executeUpdateSuccess(connection, "commit");
        JdbcUtil.executeUpdateSuccess(connection, "delete from " + TABLE_NAME + " where a = 2");
        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeUpdateSuccess(connection, "delete from " + TABLE_NAME + " where a = 2");
        JdbcUtil.executeUpdateSuccess(connection, "commit");
        JdbcUtil.executeUpdateSuccess(connection, "delete from " + TABLE_NAME + " where a = 0");
        JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getInsertSql(TABLE_NAME, 0, 0));
        JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getInsertSql(TABLE_NAME, 8, 0));
        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeUpdateSuccess(connection, "delete from " + TABLE_NAME + " where a = 0");
        JdbcUtil.executeUpdateSuccess(connection, "commit");
    }

    private void testGsi(Connection connection) throws SQLException {
        truncateTable(connection);
        JdbcUtil.executeUpdateSuccess(connection, PERMIT_CROSS_WRITE);
        JdbcUtil.executeUpdateSuccess(connection, "alter table " + TABLE_NAME + " add global index gsi(a)"
            + " partition by range (a) (PARTITION `p0` VALUES LESS THAN (5),PARTITION `p1` VALUES LESS THAN (10)) ");
        JdbcUtil.executeUpdateSuccess(connection, FORBID_CROSS_WRITE);
        // (0, 0) in the same partition, ok
        JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getInsertSql(TABLE_NAME, 0, 0));
        // (8, 0) in different partitions, not ok
        JdbcUtil.executeUpdateFailed(connection, JdbcUtil.getInsertSql(TABLE_NAME, 8, 0),
            "ERR_CROSS_GROUP_TRANSACTION");
        // (8, 8) in the same partition, ok
        JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getInsertSql(TABLE_NAME, 8, 8));
        // update sharding key is allowed if key not exists
        JdbcUtil.executeUpdateSuccess(connection,
            "update " + TABLE_NAME + " force index(gsi) set a = 7 where a = 1");
        JdbcUtil.executeUpdateSuccess(connection,
            "update " + TABLE_NAME + " force index(primary) set a = 7 where id = 1");
        // not allowed if wrong index is used
        JdbcUtil.executeUpdateFailed(connection, "update " + TABLE_NAME + " force index(primary) set a = 7 where a = 1",
            "ERR_CROSS_GROUP_TRANSACTION");
        JdbcUtil.executeUpdateFailed(connection, "update " + TABLE_NAME + " force index(gsi) set a = 7 where id = 1",
            "ERR_CROSS_GROUP_TRANSACTION");
        // update sharding key is not allowed if key exists
        JdbcUtil.executeUpdateFailed(connection, "update " + TABLE_NAME + " force index(primary) set a = 7 where a = 0",
            "ERR_CROSS_GROUP_TRANSACTION");
        JdbcUtil.executeUpdateFailed(connection, "update " + TABLE_NAME + " force index(gsi) set a = 7 where id = 1",
            "ERR_CROSS_GROUP_TRANSACTION");
        // delete is ok
        JdbcUtil.executeUpdateSuccess(connection, "delete from " + TABLE_NAME + " force index(gsi) where a = 1");
        JdbcUtil.executeUpdateSuccess(connection, "delete from " + TABLE_NAME + " force index(primary) where id = 1");
        JdbcUtil.executeUpdateSuccess(connection, "delete from " + TABLE_NAME + " force index(gsi) where a = 0");
        JdbcUtil.executeUpdateSuccess(connection, "delete from " + TABLE_NAME + " force index(primary) where id = 8");

        // in trx
        JdbcUtil.executeUpdateSuccess(connection, PERMIT_CROSS_WRITE);
        truncateTable(connection);
        JdbcUtil.executeUpdateSuccess(connection, FORBID_CROSS_WRITE);
        // (0, 0) in the same partition, ok
        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getInsertSql(TABLE_NAME, 0, 0));
        JdbcUtil.executeUpdateSuccess(connection, "commit");
        // (8, 0) in different partitions, not ok
        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeUpdateFailed(connection, JdbcUtil.getInsertSql(TABLE_NAME, 8, 0),
            "ERR_CROSS_GROUP_TRANSACTION");
        JdbcUtil.executeUpdateSuccess(connection, "rollback");
        // (8, 8) in the same partition, ok
        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeUpdateSuccess(connection, JdbcUtil.getInsertSql(TABLE_NAME, 8, 8));
        JdbcUtil.executeUpdateSuccess(connection, "commit");
        // update sharding key is allowed if key not exists
        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeUpdateSuccess(connection,
            "update " + TABLE_NAME + " force index(gsi) set a = 7 where a = 1");
        JdbcUtil.executeUpdateSuccess(connection,
            "update " + TABLE_NAME + " force index(primary) set a = 7 where id = 1");
        JdbcUtil.executeUpdateSuccess(connection, "commit");
        // update sharding key is not allowed if key exists
        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeUpdateFailed(connection, "update " + TABLE_NAME + " force index(gsi) set a = 7 where a = 0",
            "ERR_CROSS_GROUP_TRANSACTION");
        JdbcUtil.executeUpdateSuccess(connection, "rollback");
        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeUpdateFailed(connection,
            "update " + TABLE_NAME + " force index(primary) set a = 7 where id = 0",
            "ERR_CROSS_GROUP_TRANSACTION");
        JdbcUtil.executeUpdateSuccess(connection, "rollback");
        // delete is ok
        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeUpdateSuccess(connection, "delete from " + TABLE_NAME + " force index(gsi) where a = 1");
        JdbcUtil.executeUpdateSuccess(connection, "commit");
        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeUpdateSuccess(connection, "delete from " + TABLE_NAME + " force index(primary) where id = 1");
        JdbcUtil.executeUpdateSuccess(connection, "commit");
        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeUpdateSuccess(connection, "delete from " + TABLE_NAME + " force index(gsi) where a = 0");
        JdbcUtil.executeUpdateSuccess(connection, "commit");
        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeUpdateSuccess(connection, "delete from " + TABLE_NAME + " force index(primary) where id = 8");
        JdbcUtil.executeUpdateSuccess(connection, "commit");
    }

    @Test
    public void moreTest() throws SQLException {
        try (Connection connection = getPolardbxConnection0(DB_NAME)) {
            // insert
            JdbcUtil.executeUpdateSuccess(connection, "DROP TABLE IF EXISTS `tb1`");
            JdbcUtil.executeUpdateSuccess(connection, "CREATE TABLE `tb1` ("
                + "  `id` bigint NOT NULL, "
                + "  `a` int DEFAULT '0', "
                + "  PRIMARY KEY (`id`) "
                + ") ENGINE = InnoDB DEFAULT CHARSET = utf8mb4 DEFAULT COLLATE = utf8mb4_general_ci "
                + "PARTITION BY RANGE(`id`) "
                + "(PARTITION `p0` VALUES LESS THAN (100) ENGINE = InnoDB, "
                + " PARTITION `p1` VALUES LESS THAN (200) ENGINE = InnoDB)");
            JdbcUtil.executeUpdateSuccess(connection, FORBID_CROSS_WRITE);

            JdbcUtil.executeUpdateFailed(connection, "insert into tb1 values (1, 1), (101, 101)",
                "ERR_CROSS_GROUP_TRANSACTION");

            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateFailed(connection, "insert into tb1 values (2, 2), (102, 102)",
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(connection, "rollback");

            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateSuccess(connection, "insert into tb1 values (3, 3), (4, 4)");
            JdbcUtil.executeUpdateFailed(connection, "insert into tb1 values (103, 103), (104, 104)",
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(connection, "rollback");

            JdbcUtil.executeUpdateSuccess(connection, "insert into tb1 values (5, 5)");
            JdbcUtil.executeUpdateSuccess(connection, "insert into tb1 values (106, 106), (107, 107)");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateSuccess(connection, "insert into tb1 values (8, 8)");
            JdbcUtil.executeUpdateSuccess(connection, "insert into tb1 values (9, 9), (10, 10)");
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            JdbcUtil.executeUpdateFailed(connection, "replace into tb1 values (11, 11), (111, 111)",
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateFailed(connection, "replace into tb1 values (5, 5), (106, 106)",
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateFailed(connection,
                "insert into tb1 values (12, 12), (112, 112) on duplicate key update a = values(a)",
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(connection, "rollback");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateFailed(connection, "insert ignore into tb1 values (5, 5), (106, 106)",
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(connection, "rollback");

            JdbcUtil.executeUpdateSuccess(connection, "truncate table tb1");
            JdbcUtil.executeUpdateSuccess(connection, "insert into tb1 values (1, 1), (2, 2)");
            JdbcUtil.executeUpdateSuccess(connection, "insert into tb1 values (101, 101), (102, 102)");
            JdbcUtil.executeUpdateFailed(connection, "insert into tb1 select id + 10, a from tb1",
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateFailed(connection, "replace into tb1 select id + 10, a from tb1",
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(connection, "rollback");

            JdbcUtil.executeUpdateSuccess(connection, "insert into tb1 select id + 10, a from tb1 where id in (1, 2)");
            JdbcUtil.executeUpdateSuccess(connection,
                "insert ignore into tb1 select id + 10, a from tb1 where id in (101, 102)");
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into tb1 select id + 10, a from tb1 where id in (101, 102) on duplicate key update a = values(a)");

            // update
            JdbcUtil.executeUpdateSuccess(connection, "truncate table tb1");
            JdbcUtil.executeUpdateSuccess(connection, "insert into tb1 values (1, 1), (2, 2)");
            JdbcUtil.executeUpdateSuccess(connection, "insert into tb1 values (101, 101), (102, 102)");

            JdbcUtil.executeUpdateFailed(connection, "update tb1 set a = 0 where 1=1",
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateFailed(connection, "update tb1 set a = 0 where id in (1, 101)",
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateFailed(connection, "update tb1 set a = 0 where 1=1",
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(connection, "rollback");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateFailed(connection, "update tb1 set a = 0 where id in (1, 101)",
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(connection, "rollback");

            JdbcUtil.executeUpdateFailed(connection, "update tb1 a inner join tb1 b on a.id = b.id set a.a = b.a",
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateSuccess(connection,
                "update tb1 a inner join tb1 b on a.id = b.id set a.a = b.a where b.id in (101, 102)");
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            // delete
            JdbcUtil.executeUpdateSuccess(connection, "truncate table tb1");
            JdbcUtil.executeUpdateSuccess(connection, "insert into tb1 values (1, 1), (2, 2)");
            JdbcUtil.executeUpdateSuccess(connection, "insert into tb1 values (101, 101), (102, 102)");
            JdbcUtil.executeUpdateFailed(connection, "delete from tb1 where 1=1",
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateFailed(connection, "delete from tb1 where id in (1, 101)",
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateFailed(connection, "delete from tb1 where 1=1",
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(connection, "rollback");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateFailed(connection, "delete from tb1 where id in (1, 101)",
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(connection, "rollback");

            JdbcUtil.executeUpdateSuccess(connection, "delete from tb1 partition(p0) where id in (1, 101)");

            JdbcUtil.executeUpdateFailed(connection, "delete a from tb1 a join tb1 b on a.id = b.id",
                "ERR_CROSS_GROUP_TRANSACTION");

            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateSuccess(connection,
                "delete a from tb1 a inner join tb1 b on a.id = b.id where b.id in (101, 102)");
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            // broadcast
            JdbcUtil.executeUpdateSuccess(connection, "CREATE TABLE `broadcast_tb` ( "
                + "  `id` bigint NOT NULL, "
                + "  `a` int DEFAULT '0', "
                + "  PRIMARY KEY (`id`) "
                + ") BROADCAST");

            JdbcUtil.executeUpdateSuccess(connection, "insert into broadcast_tb values (0, 0), (1, 1)");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateSuccess(connection, "insert into broadcast_tb values (2, 2), (3, 3)");
            JdbcUtil.executeUpdateSuccess(connection, "commit");
            JdbcUtil.executeUpdateSuccess(connection, "insert into broadcast_tb select id + 10, a from broadcast_tb");
            JdbcUtil.executeUpdateSuccess(connection, "delete from broadcast_tb where 1 = 1");
            JdbcUtil.executeUpdateSuccess(connection, "replace into broadcast_tb values (0, 0), (1, 1)");
            JdbcUtil.executeUpdateSuccess(connection,
                "insert ignore into broadcast_tb values (0, 0), (1, 1), (2, 2), (3, 3)");
            JdbcUtil.executeUpdateSuccess(connection,
                "insert into broadcast_tb values (0, 0), (1, 1), (4, 4), (5, 5) on duplicate key update a = values(a)");
            JdbcUtil.executeUpdateSuccess(connection,
                "update broadcast_tb a inner join broadcast_tb b on a.id = b.a set a.a = b.a");
            JdbcUtil.executeUpdateSuccess(connection, "delete a from broadcast_tb a join broadcast_tb b on a.id = b.a");

            // broadcast in trx
            JdbcUtil.executeUpdateSuccess(connection, "truncate table tb1");
            JdbcUtil.executeUpdateSuccess(connection, "insert into tb1 values (1, 1), (2, 2)");
            JdbcUtil.executeUpdateSuccess(connection, "insert into tb1 values (101, 101), (102, 102)");

            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateSuccess(connection, "update tb1 set a = 10 where id = 101");
            JdbcUtil.executeUpdateSuccess(connection, "insert into broadcast_tb values (10, 10), (11, 11)");
            JdbcUtil.executeUpdateSuccess(connection, "update tb1 set a = 10 where id = 102");
            JdbcUtil.executeUpdateFailed(connection, "update tb1 set a = 10 where id = 1",
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateSuccess(connection, "insert into broadcast_tb values (12, 12), (13, 13)");
            JdbcUtil.executeUpdateFailed(connection, "update tb1 set a = 10 where id = 102",
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateSuccess(connection, "insert into broadcast_tb values (14, 14), (15, 15)");
            JdbcUtil.executeUpdateSuccess(connection, "update tb1 set a = 10 where id = 1");
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            // lock for select
            JdbcUtil.executeUpdateSuccess(connection, "truncate table tb1");
            JdbcUtil.executeUpdateSuccess(connection, "insert into tb1 values (1, 1), (2, 2)");
            JdbcUtil.executeUpdateSuccess(connection, "insert into tb1 values (101, 101), (102, 102)");

            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateFailed(connection, "select * from tb1 for update",
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateFailed(connection, "select * from tb1 lock in share mode",
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            JdbcUtil.executeUpdateSuccess(connection, "select * from tb1 for update");
            JdbcUtil.executeUpdateSuccess(connection, "select * from tb1 lock in share mode");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateSuccess(connection, "select * from tb1 where id in (1, 2) for update");
            JdbcUtil.executeUpdateSuccess(connection, "commit");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateSuccess(connection, "select * from tb1 where id in (101, 102) lock in share mode");
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            // update sharding key
            JdbcUtil.executeUpdateSuccess(connection, "truncate table tb1");
            JdbcUtil.executeUpdateSuccess(connection, "update tb1 set id = 101 where id = 1");

            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateSuccess(connection, "update tb1 set id = 101 where id = 1");
            JdbcUtil.executeUpdateSuccess(connection, "insert into tb1 values (2, 2)");
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateSuccess(connection, "update tb1 set id = 101 where id = 1");
            JdbcUtil.executeUpdateFailed(connection, "insert into tb1 values (102, 102)",
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            // insert select one shard
            JdbcUtil.executeUpdateSuccess(connection, "truncate table tb1");
            JdbcUtil.executeUpdateSuccess(connection, "insert into tb1 values (1, 1), (2, 2)");

            JdbcUtil.executeUpdateSuccess(connection, "insert into tb1 select id + 100, a from tb1");

            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateSuccess(connection, "insert into tb1 select id + 110, a from tb1 partition(p0)");
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            // complex update/delete
            List<String> dataNodes = LocalityTestUtils.getDatanodes(connection);
            if (dataNodes.size() < 2) {
                return;
            }
            String group0 = dataNodes.get(0);
            String group1 = dataNodes.get(1);
            JdbcUtil.executeUpdateSuccess(connection, "DROP TABLE IF EXISTS `tb1`");
            JdbcUtil.executeUpdateSuccess(connection, "CREATE TABLE `tb1` ( "
                + "  `id` bigint NOT NULL, "
                + "  `a` int DEFAULT '0', "
                + "  PRIMARY KEY (`id`) "
                + ") SINGLE LOCALITY='dn=" + group0 + "'");
            JdbcUtil.executeUpdateSuccess(connection, "CREATE TABLE `tb2` ( "
                + "  `id` bigint NOT NULL, "
                + "  `a` int DEFAULT '0', "
                + "  PRIMARY KEY (`id`) "
                + ") SINGLE LOCALITY='dn=" + group1 + "'");

            JdbcUtil.executeUpdateSuccess(connection, "insert into tb1 values (1, 1), (2, 2)");
            JdbcUtil.executeUpdateSuccess(connection, "insert into tb2 values (1, 1), (2, 2)");

            JdbcUtil.executeUpdateFailed(connection, "update tb1 inner join tb2 on tb1.id = tb2.id set tb1.a = tb2.a",
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateFailed(connection, "delete tb1 from tb1 join tb2 on tb1.id = tb2.id",
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateFailed(connection, "update tb1 inner join tb2 on tb1.id = tb2.id set tb1.a = tb2.a",
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(connection, "commit");

            JdbcUtil.executeUpdateSuccess(connection, "begin");
            JdbcUtil.executeUpdateFailed(connection, "delete tb1 from tb1 join tb2 on tb1.id = tb2.id",
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(connection, "commit");

        }
    }
}

