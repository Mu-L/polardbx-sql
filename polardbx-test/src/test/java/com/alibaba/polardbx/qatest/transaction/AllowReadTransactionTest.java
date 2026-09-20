package com.alibaba.polardbx.qatest.transaction;

import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

public class AllowReadTransactionTest extends CrudBasedLockTestCase {
    private static String DB_NAME = "allow_read_transaction_test_db";

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

    private String tableName;

    @Test
    public void testSingleTable() throws SQLException {
        tableName = "allow_read_transaction_single_table";
        String createTable = "create table if not exists " + tableName + " (id int primary key, a int) single";
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(conn, createTable);
            JdbcUtil.executeUpdateSuccess(conn, "set transaction_policy = allow_read");
            JdbcUtil.executeUpdateSuccess(conn, "set broadcast_dml = false");
            // All good.
            JdbcUtil.executeUpdateSuccess(conn, "begin");
            JdbcUtil.executeUpdateSuccess(conn, JdbcUtil.getSelectCountSql(tableName, 2, 3, 4));
            String trxId = JdbcUtil.getTrxId(conn);
            String trxType = JdbcUtil.getTrxType(conn, trxId);
            Assert.assertEquals("AllowReadTransaction", trxType);
            JdbcUtil.executeUpdateSuccess(conn, JdbcUtil.getInsertSql(tableName, 0));
            JdbcUtil.executeUpdateSuccess(conn, JdbcUtil.getInsertSql(tableName, 1, 2, 3, 4, 5, 6, 7, 8, 9));
            JdbcUtil.executeUpdateSuccess(conn, JdbcUtil.getUpdateSql(tableName, 0));
            JdbcUtil.executeUpdateSuccess(conn, JdbcUtil.getUpdateSql(tableName, 1, 2, 3, 4, 5, 6, 7, 8, 9));
            JdbcUtil.executeUpdateSuccess(conn, JdbcUtil.getDeleteSql(tableName, 0));
            JdbcUtil.executeUpdateSuccess(conn, JdbcUtil.getDeleteSql(tableName, 1, 2, 3, 4, 5, 6, 7, 8, 9));
            JdbcUtil.executeUpdateSuccess(conn, "commit");
        }
    }

    @Test
    public void testShadingTable() throws SQLException {
        tableName = "allow_read_transaction_sharding_table";
        String createTable = "create table if not exists " + tableName
            + " (id int primary key, a int) partition by key(id)";
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(conn, createTable);
            JdbcUtil.executeUpdateSuccess(conn, "set transaction_policy = allow_read");
            JdbcUtil.executeUpdateSuccess(conn, "set broadcast_dml = false");

            JdbcUtil.executeUpdateSuccess(conn, "begin");
            JdbcUtil.executeUpdateSuccess(conn, JdbcUtil.getSelectCountSql(tableName, 2, 3, 4));
            String trxId = JdbcUtil.getTrxId(conn);
            String trxType = JdbcUtil.getTrxType(conn, trxId);
            Assert.assertEquals("AllowReadTransaction", trxType);
            JdbcUtil.executeUpdateSuccess(conn, JdbcUtil.getInsertSql(tableName, 0));
            JdbcUtil.executeUpdateFailed(conn, JdbcUtil.getInsertSql(tableName, 1, 2, 3, 4, 5, 6, 7, 8, 9),
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(conn, "rollback");

            JdbcUtil.executeUpdateSuccess(conn, "begin");
            JdbcUtil.executeUpdateSuccess(conn, JdbcUtil.getInsertSql(tableName, 0));
            JdbcUtil.executeUpdateSuccess(conn, JdbcUtil.getUpdateSql(tableName, 0));
            JdbcUtil.executeUpdateFailed(conn, JdbcUtil.getUpdateSql(tableName, 1, 2, 3, 4, 5, 6, 7, 8, 9),
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(conn, "rollback");

            JdbcUtil.executeUpdateSuccess(conn, "begin");
            JdbcUtil.executeUpdateSuccess(conn, JdbcUtil.getInsertSql(tableName, 0));
            JdbcUtil.executeUpdateSuccess(conn, JdbcUtil.getDeleteSql(tableName, 0));
            JdbcUtil.executeUpdateFailed(conn, JdbcUtil.getDeleteSql(tableName, 1, 2, 3, 4, 5, 6, 7, 8, 9),
                "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(conn, "rollback");
        }
    }

    @Test
    public void testBroadcastTable() throws SQLException {
        tableName = "allow_read_transaction_broadcast_table";
        String createTable = "create table if not exists " + tableName
            + " (id int primary key, a int) broadcast";
        try (Connection conn = getPolardbxConnection(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(conn, createTable);
            JdbcUtil.executeUpdateSuccess(conn, "set transaction_policy = allow_read");
            JdbcUtil.executeUpdateSuccess(conn, "set broadcast_dml = false");

            JdbcUtil.executeUpdateSuccess(conn, "begin");
            JdbcUtil.executeUpdateFailed(conn, JdbcUtil.getInsertSql(tableName, 0), "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(conn, "rollback");

            JdbcUtil.executeUpdateSuccess(conn, "begin");
            JdbcUtil.executeUpdateFailed(conn, JdbcUtil.getUpdateSql(tableName, 0), "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(conn, "rollback");

            JdbcUtil.executeUpdateSuccess(conn, "begin");
            JdbcUtil.executeUpdateFailed(conn, JdbcUtil.getDeleteSql(tableName, 0), "ERR_CROSS_GROUP_TRANSACTION");
            JdbcUtil.executeUpdateSuccess(conn, "rollback");

            JdbcUtil.executeUpdateSuccess(conn, "set broadcast_dml = true");
            JdbcUtil.executeUpdateSuccess(conn, "begin");
            JdbcUtil.executeUpdateSuccess(conn, JdbcUtil.getInsertSql(tableName, 0));
            JdbcUtil.executeUpdateSuccess(conn, JdbcUtil.getInsertSql(tableName, 1, 2, 3, 4, 5, 6, 7, 8, 9));
            JdbcUtil.executeUpdateSuccess(conn, JdbcUtil.getUpdateSql(tableName, 0));
            JdbcUtil.executeUpdateSuccess(conn, JdbcUtil.getUpdateSql(tableName, 1, 2, 3, 4, 5, 6, 7, 8, 9));
            JdbcUtil.executeUpdateSuccess(conn, JdbcUtil.getDeleteSql(tableName, 0));
            JdbcUtil.executeUpdateSuccess(conn, JdbcUtil.getDeleteSql(tableName, 1, 2, 3, 4, 5, 6, 7, 8, 9));
            JdbcUtil.executeUpdateSuccess(conn, "commit");
        }
    }
}
