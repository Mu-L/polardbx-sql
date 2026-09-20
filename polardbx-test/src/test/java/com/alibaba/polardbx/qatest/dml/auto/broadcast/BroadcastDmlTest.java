package com.alibaba.polardbx.qatest.dml.auto.broadcast;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class BroadcastDmlTest extends ReadBaseTestCase {

    private final String TABLE_NAME = "broadcast_dml_test_tb";
    private final String CREATE_TABLE = "create table if not exists " + TABLE_NAME + "("
        + "pk int primary key, "
        + "integer_test int not null, "
        + "bigint_test bigint not null"
        + ") broadcast";
    private final String DROP_TABLE = "drop table if exists " + TABLE_NAME;

    @Before
    public void initData() throws Exception {
        JdbcUtil.executeUpdateSuccess(tddlConnection, CREATE_TABLE);
    }

    @After
    public void destroyData() throws Exception {
        JdbcUtil.executeUpdateSuccess(tddlConnection, DROP_TABLE);
    }

    private long getRowCount() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "select count(*) from " + TABLE_NAME);
        Assert.assertTrue(rs.next());
        return rs.getLong(1);
    }

    @Test
    public void testTransactionPolicy() throws Exception {
        // Allow read + no broadcast dml = fail
        try (Connection conn = getPolardbxConnection()) {
            conn.setAutoCommit(false);
            try {
                JdbcUtil.executeUpdateSuccess(conn, "set drds_transaction_policy = ALLOW_READ");
                JdbcUtil.executeUpdateSuccess(conn, "set broadcast_dml = false");
                String sql = "insert " + TABLE_NAME + "(pk, integer_test, bigint_test) value(0, 0, 0)";
                JdbcUtil.executeUpdateFailed(conn, sql, "ERR_CROSS_GROUP_TRANSACTION");
                // Can't executeSuccess any further sql
                sql = "commit";
                JdbcUtil.executeUpdateFailed(conn, sql, "");
            } finally {
                Assert.assertEquals(0, getRowCount());
            }
        }
    }

    @Test
    public void testTransactionPolicy2() throws Exception {
        // Allow read + broadcast dml = succeed
        try (Connection conn = getPolardbxConnection()) {
            conn.setAutoCommit(false);
            try {
                JdbcUtil.executeUpdateSuccess(conn, "set drds_transaction_policy = ALLOW_READ");
                JdbcUtil.executeUpdateSuccess(conn, "set broadcast_dml = true");
                String sql = "insert " + TABLE_NAME + "(pk, integer_test, bigint_test) value(0, 0, 0)";
                JdbcUtil.executeUpdateSuccess(conn, sql);
                JdbcUtil.executeUpdateSuccess(conn, "commit");
            } finally {
                conn.rollback();
                conn.setAutoCommit(true);
                Assert.assertEquals(1, getRowCount());
            }
        }
    }

    @Test
    public void testTransactionPolicy3() throws Exception {
        // XA + broadcast dml + USE_READ_CONN_FOR_XA_BROADCAST_DML = wrong
        try (Connection conn = getPolardbxConnection()) {
            conn.setAutoCommit(false);
            try {
                JdbcUtil.executeUpdateSuccess(conn, "set drds_transaction_policy = XA");
                JdbcUtil.executeUpdateSuccess(conn, "set broadcast_dml = true");
                JdbcUtil.executeUpdateSuccess(conn, "set USE_READ_CONN_FOR_XA_BROADCAST_DML = true");
                String sql = "insert " + TABLE_NAME + "(pk, integer_test, bigint_test) value(0, 0, 0)";
                JdbcUtil.executeUpdateSuccess(conn, sql);
                JdbcUtil.executeUpdateSuccess(conn, "commit");
            } finally {
                conn.rollback();
                conn.setAutoCommit(true);
                // wrong result
                Assert.assertEquals(0, getRowCount());
            }
        }
    }

    @Test
    public void testTransactionPolicy4() throws Exception {
        // XA + broadcast dml + not USE_READ_CONN_FOR_XA_BROADCAST_DML = right
        try (Connection conn = getPolardbxConnection()) {
            conn.setAutoCommit(false);
            try {
                JdbcUtil.executeUpdateSuccess(conn, "set drds_transaction_policy = XA");
                JdbcUtil.executeUpdateSuccess(conn, "set broadcast_dml = true");
                JdbcUtil.executeUpdateSuccess(conn, "set USE_READ_CONN_FOR_XA_BROADCAST_DML = false");
                String sql = "insert " + TABLE_NAME + "(pk, integer_test, bigint_test) value(0, 0, 0)";
                JdbcUtil.executeUpdateSuccess(conn, sql);
                JdbcUtil.executeUpdateSuccess(conn, "commit");
            } finally {
                conn.rollback();
                conn.setAutoCommit(true);
                // right result
                Assert.assertEquals(1, getRowCount());
            }
        }
    }

    @Test
    public void testTransactionPolicy5() throws Exception {
        // XA + not broadcast dml = right
        try (Connection conn = getPolardbxConnection()) {
            conn.setAutoCommit(false);
            try {
                JdbcUtil.executeUpdateSuccess(conn, "set drds_transaction_policy = XA");
                JdbcUtil.executeUpdateSuccess(conn, "set broadcast_dml = false");
                JdbcUtil.executeUpdateSuccess(conn, "set USE_READ_CONN_FOR_XA_BROADCAST_DML = true");
                String sql = "insert " + TABLE_NAME + "(pk, integer_test, bigint_test) value(0, 0, 0)";
                JdbcUtil.executeUpdateSuccess(conn, sql);
                JdbcUtil.executeUpdateSuccess(conn, "commit");
            } finally {
                conn.rollback();
                conn.setAutoCommit(true);
                // right result
                Assert.assertEquals(1, getRowCount());
            }
        }
    }

    @Test
    public void testTransactionPolicy6() throws Exception {
        // TSO + broadcast dml + USE_READ_CONN_FOR_XA_BROADCAST_DML = wrong
        try (Connection conn = getPolardbxConnection()) {
            conn.setAutoCommit(false);
            try {
                JdbcUtil.executeUpdateSuccess(conn, "set drds_transaction_policy = TSO");
                JdbcUtil.executeUpdateSuccess(conn, "set broadcast_dml = true");
                JdbcUtil.executeUpdateSuccess(conn, "set USE_READ_CONN_FOR_XA_BROADCAST_DML = true");
                String sql = "insert " + TABLE_NAME + "(pk, integer_test, bigint_test) value(0, 0, 0)";
                JdbcUtil.executeUpdateSuccess(conn, sql);
                JdbcUtil.executeUpdateSuccess(conn, "commit");
            } finally {
                conn.rollback();
                conn.setAutoCommit(true);
                // wrong result
                Assert.assertEquals(0, getRowCount());
            }
        }
    }

    @Test
    public void testTransactionPolicy7() throws Exception {
        // TSO + broadcast dml + not USE_READ_CONN_FOR_XA_BROADCAST_DML = right
        try (Connection conn = getPolardbxConnection()) {
            conn.setAutoCommit(false);
            try {
                JdbcUtil.executeUpdateSuccess(conn, "set drds_transaction_policy = TSO");
                JdbcUtil.executeUpdateSuccess(conn, "set broadcast_dml = true");
                JdbcUtil.executeUpdateSuccess(conn, "set USE_READ_CONN_FOR_XA_BROADCAST_DML = false");
                String sql = "insert " + TABLE_NAME + "(pk, integer_test, bigint_test) value(0, 0, 0)";
                JdbcUtil.executeUpdateSuccess(conn, sql);
                JdbcUtil.executeUpdateSuccess(conn, "commit");
            } finally {
                conn.rollback();
                conn.setAutoCommit(true);
                // right result
                Assert.assertEquals(1, getRowCount());
            }
        }
    }

    @Test
    public void testTransactionPolicy8() throws Exception {
        // TSO + not broadcast dml + USE_READ_CONN_FOR_XA_BROADCAST_DML = right
        try (Connection conn = getPolardbxConnection()) {
            conn.setAutoCommit(false);
            try {
                JdbcUtil.executeUpdateSuccess(conn, "set drds_transaction_policy = TSO");
                JdbcUtil.executeUpdateSuccess(conn, "set broadcast_dml = false");
                JdbcUtil.executeUpdateSuccess(conn, "set USE_READ_CONN_FOR_XA_BROADCAST_DML = true");
                String sql = "insert " + TABLE_NAME + "(pk, integer_test, bigint_test) value(0, 0, 0)";
                JdbcUtil.executeUpdateSuccess(conn, sql);
                JdbcUtil.executeUpdateSuccess(conn, "commit");
            } finally {
                conn.rollback();
                conn.setAutoCommit(true);
                // right result
                Assert.assertEquals(1, getRowCount());
            }
        }
    }
}
