package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;

public class GdnRelatedTrxTest extends ReadBaseTestCase {
    private final static String DB_NAME = "gdn_related_trx_db";
    private final static String TABLE_NAME = "gdn_related_trx_table";
    private final static String DROP_DATABASE = "drop database if exists " + DB_NAME;
    private final static String CREATE_DATABASE = "create database if not exists " + DB_NAME + " mode=auto";
    private final static String CREATE_TABLE = "create table if not exists " + TABLE_NAME
        + "( id int primary key, a int) partition by key(id)";
    private final static String INSERT = "insert into " + TABLE_NAME + " values(1,1),(2,2),(3,3),(4,4)";
    private final static String UPDATE = "update " + TABLE_NAME + " set a = 100 where 1=1";
    private final static String hint = "/* +TDDL:cmd_extra(FAILURE_INJECTION='FAIL_AFTER_PRIMARY_COMMIT') */";

    @BeforeClass
    public static void init() throws SQLException {
        try (Connection connection = getPolardbxConnection0()) {
            JdbcUtil.executeUpdateSuccess(connection, DROP_DATABASE);
            JdbcUtil.executeUpdateSuccess(connection, CREATE_DATABASE);
            JdbcUtil.executeUpdateSuccess(connection, "use " + DB_NAME);
            JdbcUtil.executeUpdateSuccess(connection, CREATE_TABLE);
            JdbcUtil.executeUpdateSuccess(connection, INSERT);
        }
    }

    @AfterClass
    public static void destroy() throws SQLException {
        try (Connection connection = getPolardbxConnection0()) {
            JdbcUtil.executeUpdateSuccess(connection, "set global transaction_policy = TSO");
            JdbcUtil.executeUpdateSuccess(connection, DROP_DATABASE);
        }
    }

    @Test
    public void testDrainHangingTrx() throws Exception {
        try (Connection connection = getPolardbxConnection0(DB_NAME)) {
            ResultSet rs = JdbcUtil.executeQuerySuccess(connection, "call polardbx.drain_hanging_trx()");
            Assert.assertTrue(rs.next());
            System.out.println(rs.getString(1));
            System.out.println(rs.getString(2));
            Assert.assertTrue("OK".equalsIgnoreCase(rs.getString(1)));

            boolean failed = false;
            connection.setAutoCommit(false);
            try {
                JdbcUtil.executeUpdateSuccess(connection, hint + UPDATE);
                try {
                    connection.commit();
                } catch (Throwable t) {
                    failed = true;
                }
            } finally {
                connection.rollback();
                connection.setAutoCommit(true);
            }
            Assert.assertTrue(failed);

            rs = JdbcUtil.executeQuerySuccess(connection, "call polardbx.drain_hanging_trx(1000)");
            Assert.assertTrue(rs.next());
            System.out.println(rs.getString(1));
            System.out.println(rs.getString(2));
            Assert.assertTrue("FAIL".equalsIgnoreCase(rs.getString(1)));
            Assert.assertTrue(rs.getString(2).contains("Timeout"));
        }
    }

    @Test
    public void testFixGdnTrxPolicy() throws Exception {
        JdbcUtil.executeUpdateSuccess(tddlConnection,
            "delete from metadb.inst_config where param_key = 'GDN_TRX_POLICY_STATUS'");
        // case 1: no transaction policy
        try (Connection connection = getPolardbxConnection0(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set global transaction_policy = ''");
            System.out.println("set global transaction_policy = ''");
            JdbcUtil.executeUpdateSuccess(connection, "set transaction_isolation = 'repeatable-read'");
            System.out.println("set transaction_isolation = 'repeatable-read'");
            // should be TSO by default
            JdbcUtil.waitUntilVariableChangedMetaDb(connection, "transaction_policy", "", 5);
            JdbcUtil.waitUntilVariableChanged(connection, "transaction_policy", "TSO", 5);

            ResultSet rs =
                JdbcUtil.executeQuerySuccess(tddlConnection, "call polardbx.fix_gdn_trx_policy(become_slave)");
            System.out.println("call polardbx.fix_gdn_trx_policy(become_slave)");
            Assert.assertTrue(rs.next());
            System.out.println(rs.getString(1));
            System.out.println(rs.getString(2));
            Assert.assertTrue("OK".equalsIgnoreCase(rs.getString(1)));
            JdbcUtil.waitUntilVariableChangedMetaDb(connection, "transaction_policy", "XA", 5);
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = true");
            JdbcUtil.waitUntilVariableChanged(connection, "transaction_policy", "XA_TSO", 5);

            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = false");
            JdbcUtil.waitUntilVariableChanged(connection, "transaction_policy", "XA", 5);

            rs = JdbcUtil.executeQuerySuccess(tddlConnection, "call polardbx.fix_gdn_trx_policy(become_master)");
            System.out.println("call polardbx.fix_gdn_trx_policy(become_master)");
            Assert.assertTrue(rs.next());
            System.out.println(rs.getString(1));
            System.out.println(rs.getString(2));
            Assert.assertTrue("OK".equalsIgnoreCase(rs.getString(1)));
            JdbcUtil.waitUntilVariableChangedMetaDb(connection, "transaction_policy", "", 5);
            JdbcUtil.waitUntilVariableChanged(connection, "transaction_policy", "TSO", 5);
        }

        // case 2: TSO
        try (Connection connection = getPolardbxConnection0(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set global transaction_policy = TSO");
            System.out.println("set global transaction_policy = TSO");
            // should be TSO by default
            JdbcUtil.waitUntilVariableChangedMetaDb(connection, "transaction_policy", "TSO", 5);
            JdbcUtil.waitUntilVariableChanged(connection, "transaction_policy", "TSO", 5);

            ResultSet rs =
                JdbcUtil.executeQuerySuccess(tddlConnection, "call polardbx.fix_gdn_trx_policy(become_slave)");
            System.out.println("call polardbx.fix_gdn_trx_policy(become_slave)");
            Assert.assertTrue(rs.next());
            System.out.println(rs.getString(1));
            System.out.println(rs.getString(2));
            Assert.assertTrue("OK".equalsIgnoreCase(rs.getString(1)));
            JdbcUtil.waitUntilVariableChangedMetaDb(connection, "transaction_policy", "XA", 5);
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = true");
            JdbcUtil.waitUntilVariableChanged(connection, "transaction_policy", "XA_TSO", 5);

            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = false");
            JdbcUtil.waitUntilVariableChanged(connection, "transaction_policy", "XA", 5);

            rs = JdbcUtil.executeQuerySuccess(tddlConnection, "call polardbx.fix_gdn_trx_policy(become_master)");
            System.out.println("call polardbx.fix_gdn_trx_policy(become_master)");
            Assert.assertTrue(rs.next());
            System.out.println(rs.getString(1));
            System.out.println(rs.getString(2));
            Assert.assertTrue("OK".equalsIgnoreCase(rs.getString(1)));
            JdbcUtil.waitUntilVariableChangedMetaDb(connection, "transaction_policy", "TSO", 5);
            JdbcUtil.waitUntilVariableChanged(connection, "transaction_policy", "TSO", 5);
        }

        // case 3: XA , should do nothing
        try (Connection connection = getPolardbxConnection0(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set global transaction_policy = XA");
            System.out.println("set global transaction_policy = XA");
            // should be TSO by default
            JdbcUtil.waitUntilVariableChangedMetaDb(connection, "transaction_policy", "XA", 5);
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = true");
            JdbcUtil.waitUntilVariableChanged(connection, "transaction_policy", "XA_TSO", 5);

            ResultSet rs =
                JdbcUtil.executeQuerySuccess(tddlConnection, "call polardbx.fix_gdn_trx_policy(become_slave)");
            System.out.println("call polardbx.fix_gdn_trx_policy(become_slave)");
            Assert.assertTrue(rs.next());
            System.out.println(rs.getString(1));
            System.out.println(rs.getString(2));
            Assert.assertTrue("OK".equalsIgnoreCase(rs.getString(1)));
            JdbcUtil.waitUntilVariableChangedMetaDb(connection, "transaction_policy", "XA", 5);
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = true");
            JdbcUtil.waitUntilVariableChanged(connection, "transaction_policy", "XA_TSO", 5);

            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = false");
            JdbcUtil.waitUntilVariableChanged(connection, "transaction_policy", "XA", 5);

            rs = JdbcUtil.executeQuerySuccess(tddlConnection, "call polardbx.fix_gdn_trx_policy(become_master)");
            System.out.println("call polardbx.fix_gdn_trx_policy(become_master)");
            Assert.assertTrue(rs.next());
            System.out.println(rs.getString(1));
            System.out.println(rs.getString(2));
            Assert.assertTrue("OK".equalsIgnoreCase(rs.getString(1)));
            JdbcUtil.waitUntilVariableChangedMetaDb(connection, "transaction_policy", "XA", 5);
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = true");
            JdbcUtil.waitUntilVariableChanged(connection, "transaction_policy", "XA_TSO", 5);

            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = false");
            JdbcUtil.waitUntilVariableChanged(connection, "transaction_policy", "XA", 5);
        }

        // case 3: TSO , but change under slave mode
        try (Connection connection = getPolardbxConnection0(DB_NAME)) {
            JdbcUtil.executeUpdateSuccess(tddlConnection, "set global transaction_policy = TSO");
            // should be TSO by default
            JdbcUtil.waitUntilVariableChangedMetaDb(connection, "transaction_policy", "TSO", 5);
            JdbcUtil.waitUntilVariableChanged(connection, "transaction_policy", "TSO", 5);

            ResultSet rs =
                JdbcUtil.executeQuerySuccess(tddlConnection, "call polardbx.fix_gdn_trx_policy(become_slave)");
            Assert.assertTrue(rs.next());
            System.out.println(rs.getString(1));
            System.out.println(rs.getString(2));
            Assert.assertTrue("OK".equalsIgnoreCase(rs.getString(1)));
            JdbcUtil.waitUntilVariableChangedMetaDb(connection, "transaction_policy", "XA", 5);
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = true");
            JdbcUtil.waitUntilVariableChanged(connection, "transaction_policy", "XA_TSO", 5);

            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = false");
            JdbcUtil.waitUntilVariableChanged(connection, "transaction_policy", "XA", 5);

            Thread.sleep(1000);

            JdbcUtil.executeUpdateSuccess(tddlConnection, "set global transaction_policy = XA");
            JdbcUtil.waitUntilVariableChangedMetaDb(connection, "transaction_policy", "XA", 5);

            Thread.sleep(1000);

            rs = JdbcUtil.executeQuerySuccess(tddlConnection, "call polardbx.fix_gdn_trx_policy(become_master)");
            Assert.assertTrue(rs.next());
            System.out.println(rs.getString(1));
            System.out.println(rs.getString(2));
            Assert.assertTrue("OK".equalsIgnoreCase(rs.getString(1)));
            JdbcUtil.waitUntilVariableChangedMetaDb(connection, "transaction_policy", "XA", 5);
            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = true");
            JdbcUtil.waitUntilVariableChanged(connection, "transaction_policy", "XA_TSO", 5);

            JdbcUtil.executeUpdateSuccess(connection, "set enable_xa_tso = false");
            JdbcUtil.waitUntilVariableChanged(connection, "transaction_policy", "XA", 5);
        }

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "call polardbx.fix_gdn_trx_policy(become_slave)");
        Assert.assertTrue(rs.next());
        System.out.println(rs.getString(1));
        System.out.println(rs.getString(2));
        Assert.assertTrue("OK".equalsIgnoreCase(rs.getString(1)));
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "call polardbx.fix_gdn_trx_policy(become_slave)");
        Assert.assertTrue(rs.next());
        System.out.println(rs.getString(1));
        System.out.println(rs.getString(2));
        Assert.assertTrue("OK".equalsIgnoreCase(rs.getString(1)));

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "call polardbx.fix_gdn_trx_policy(become_master)");
        Assert.assertTrue(rs.next());
        System.out.println(rs.getString(1));
        System.out.println(rs.getString(2));
        Assert.assertTrue("OK".equalsIgnoreCase(rs.getString(1)));
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "call polardbx.fix_gdn_trx_policy(become_master)");
        Assert.assertTrue(rs.next());
        System.out.println(rs.getString(1));
        System.out.println(rs.getString(2));
        Assert.assertTrue("OK".equalsIgnoreCase(rs.getString(1)));

        // wrong params
         rs =
            JdbcUtil.executeQuerySuccess(tddlConnection, "call polardbx.fix_gdn_trx_policy()");
        Assert.assertTrue(rs.next());
        System.out.println(rs.getString(1));
        System.out.println(rs.getString(2));
        Assert.assertTrue("FAIL".equalsIgnoreCase(rs.getString(1)));
        Assert.assertTrue(rs.getString(2).contains("fix_gdn_trx_policy() requires exactly one parameter"));

        rs =
            JdbcUtil.executeQuerySuccess(tddlConnection, "call polardbx.fix_gdn_trx_policy(become)");
        Assert.assertTrue(rs.next());
        System.out.println(rs.getString(1));
        System.out.println(rs.getString(2));
        Assert.assertTrue("FAIL".equalsIgnoreCase(rs.getString(1)));
        Assert.assertTrue(rs.getString(2).contains("parameter choices: become_slave, become_master"));
    }
}
