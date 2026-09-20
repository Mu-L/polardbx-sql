package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class ForbidWriteReadonlyDnTest extends DDLBaseNewDBTestCase {
    String DB_NAME = "test_forbid_write_readonly_dn_db";

    @After
    public void tearDown() throws SQLException {
        dropDb();
        setReadonlyDnList("");
    }

    private void dropDb() {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "drop database if exists " + DB_NAME);
    }

    @Test
    public void testPartitionTable() throws SQLException {
        String partitionDef = "PARTITION BY RANGE(id) (\n"
            + "    PARTITION p1 VALUES LESS THAN (100),\n"
            + "    PARTITION p2 VALUES LESS THAN (200),\n"
            + "    PARTITION p3 VALUES LESS THAN (MAXVALUE)\n"
            + ")";
        testSimpleCase(partitionDef);
    }

    @Test
    public void testBroadcast() throws SQLException {
        String partitionDef = "BROADCAST";
        testSimpleCase(partitionDef);
    }

    @Test
    public void testSingle() throws SQLException {
        String partitionDef = "SINGLE";
        testSimpleCase(partitionDef);
    }

    @Test
    public void testReplica() throws SQLException {
        List<String> dnList = getDnList();
        if (dnList.size() < 2) {
            return;
        }
        String partitionDef = "REPLICAS LOCALITY = 'db_set=ghdb_g1,ghdb_g2'";
        testSimpleCase(partitionDef);
    }

    @Test
    public void testSwitch() throws SQLException {
        List<String> dnList = getDnList();
        System.out.println(String.join(",", dnList));
        {
            dropDb();
            JdbcUtil.executeUpdateSuccess(tddlConnection, "create database " + DB_NAME + " mode=auto");
            try (Connection connection = getPolardbxConnection(DB_NAME)) {
                JdbcUtil.executeUpdateSuccess(connection,
                    "CREATE TABLE tb1 (\n"
                        + "    id BIGINT NOT NULL,\n"
                        + "    PRIMARY KEY (id)\n"
                        + ")\n"
                        + "PARTITION BY RANGE(id) (\n"
                        + "    PARTITION p1 VALUES LESS THAN (100),\n"
                        + "    PARTITION p2 VALUES LESS THAN (200),\n"
                        + "    PARTITION p3 VALUES LESS THAN (MAXVALUE)\n"
                        + ")"
                );
                setReadonlyDnList(String.join(",", dnList));
                checkCannotWrite(connection);
                // clear readonly dn list
                setReadonlyDnList("");
                JdbcUtil.executeUpdateSuccess(connection, "delete from tb1 where 1=1");
                checkCanWrite(connection);
            }
        }
    }

    private void testSimpleCase(String tableDef) throws SQLException {
        List<String> dnList = getDnList();
        System.out.println(String.join(",", dnList));
        String readonlyDn = dnList.get(0);
        // test can write if not set readonly dn list
        {
            dropDb();
            setReadonlyDnList("");
            if (tableDef.startsWith("REPLICAS")) {
                JdbcUtil.executeUpdateSuccess(tddlConnection, "create database " + DB_NAME
                    + " mode=auto locality='dble_config={\"group_config\":{ \"ghdb_g1\":[\"" + dnList.get(0)
                    + "\",\"ghdb1\"], \"ghdb_g2\":[\"" + dnList.get(1) + "\",\"ghdb2\"]}}';");
            } else {
                JdbcUtil.executeUpdateSuccess(tddlConnection, "create database " + DB_NAME + " mode=auto");
            }
            try (Connection connection = getPolardbxConnection(DB_NAME)) {
                JdbcUtil.executeUpdateSuccess(connection,
                    "CREATE TABLE tb1 (\n"
                        + "    id BIGINT NOT NULL,\n"
                        + "    PRIMARY KEY (id)\n"
                        + ")\n"
                        + tableDef
                );
                checkCanWrite(connection);
            }
        }

        // test can not write if set readonly dn list
        {
            dropDb();
            setReadonlyDnList(String.join(",", dnList));
            if (tableDef.startsWith("REPLICAS")) {
                JdbcUtil.executeUpdateSuccess(tddlConnection, "create database " + DB_NAME
                    + " mode=auto locality='dble_config={\"group_config\":{ \"ghdb_g1\":[\"" + dnList.get(0)
                    + "\",\"ghdb1\"], \"ghdb_g2\":[\"" + dnList.get(1) + "\",\"ghdb2\"]}}';");
            } else {
                JdbcUtil.executeUpdateSuccess(tddlConnection, "create database " + DB_NAME + " mode=auto");
            }
            try (Connection connection = getPolardbxConnection(DB_NAME)) {
                JdbcUtil.executeUpdateSuccess(connection,
                    "CREATE TABLE tb1 (\n"
                        + "    id BIGINT NOT NULL,\n"
                        + "    PRIMARY KEY (id)\n"
                        + ")\n"
                        + tableDef
                );
                if (tableDef.startsWith("REPLICAS") || tableDef.startsWith("BROADCAST")) {
                    JdbcUtil.executeUpdateSuccess(connection, "set ALLOW_BROADCAST_WRITE_FOR_READONLY_DN = false");
                }
                checkCannotWrite(connection);

                if (tableDef.startsWith("REPLICAS") || tableDef.startsWith("BROADCAST")) {
                    JdbcUtil.executeUpdateSuccess(connection, "set ALLOW_BROADCAST_WRITE_FOR_READONLY_DN = true");
                    checkCanWrite(connection);
                }
            }
        }

        // test can not write if set partial readonly dn list
        {
            dropDb();
            setReadonlyDnList(readonlyDn);
            if (tableDef.startsWith("REPLICAS")) {
                JdbcUtil.executeUpdateSuccess(tddlConnection, "create database " + DB_NAME
                    + " mode=auto locality='dble_config={\"group_config\":{ \"ghdb_g1\":[\"" + dnList.get(0)
                    + "\",\"ghdb1\"], \"ghdb_g2\":[\"" + dnList.get(1) + "\",\"ghdb2\"]}}';");
            } else {
                JdbcUtil.executeUpdateSuccess(tddlConnection, "create database " + DB_NAME + " mode=auto");
            }
            try (Connection connection = getPolardbxConnection(DB_NAME)) {
                JdbcUtil.executeUpdateSuccess(connection,
                    "CREATE TABLE tb1 (\n"
                        + "    id BIGINT NOT NULL,\n"
                        + "    PRIMARY KEY (id)\n"
                        + ")\n"
                        + tableDef
                );
                if (tableDef.startsWith("REPLICAS") || tableDef.startsWith("BROADCAST")) {
                    JdbcUtil.executeUpdateSuccess(connection, "set ALLOW_BROADCAST_WRITE_FOR_READONLY_DN = false");
                }
                checkCannotWrite(connection);

                if (tableDef.startsWith("REPLICAS") || tableDef.startsWith("BROADCAST")) {
                    JdbcUtil.executeUpdateSuccess(connection, "set ALLOW_BROADCAST_WRITE_FOR_READONLY_DN = true");
                    checkCanWrite(connection);
                }
            }
        }
    }

    private static void checkCannotWrite(Connection connection) throws SQLException {
        JdbcUtil.executeUpdateFailed(connection, "insert into tb1 values (10), (110), (210)",
            "ERR_WRITE_FOR_READ_ONLY_DN");
        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeUpdateFailed(connection, "insert into tb1 values (20), (120), (220)",
            "ERR_WRITE_FOR_READ_ONLY_DN");
        JdbcUtil.executeUpdateSuccess(connection, "rollback");

        JdbcUtil.executeUpdateFailed(connection, "delete from tb1 where id in (10, 110, 210)",
            "ERR_WRITE_FOR_READ_ONLY_DN");
        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeUpdateFailed(connection, "delete from tb1 where id in (20, 120, 220)",
            "ERR_WRITE_FOR_READ_ONLY_DN");
        JdbcUtil.executeUpdateSuccess(connection, "rollback");
    }

    private void checkCanWrite(Connection connection) throws SQLException {
        // can write
        JdbcUtil.executeUpdateSuccess(connection, "insert into tb1 values (10), (110), (210)");
        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeUpdateSuccess(connection, "insert into tb1 values (20), (120), (220)");
        JdbcUtil.executeUpdateSuccess(connection, "commit");
        // check
        checkResultSet(new HashSet<>(ImmutableList.of(10, 20, 110, 120, 210, 220)),
            JdbcUtil.executeQuerySuccess(connection, "select * from tb1"));

        JdbcUtil.executeUpdateSuccess(connection, "delete from tb1 where id in (10, 110, 210)");
        JdbcUtil.executeUpdateSuccess(connection, "begin");
        JdbcUtil.executeUpdateSuccess(connection, "delete from tb1 where id in (20, 120, 220)");
        JdbcUtil.executeUpdateSuccess(connection, "commit");
        // check
        Assert.assertFalse(JdbcUtil.executeQuerySuccess(connection, "select * from tb1").next());
    }

    private void checkResultSet(Set<Integer> expected, ResultSet rs) throws SQLException {
        while (rs.next()) {
            expected.remove(rs.getInt("id"));
        }
        Assert.assertEquals(0, expected.size());
    }

    private void setReadonlyDnList(String readonlyDn) throws SQLException {
        JdbcUtil.executeUpdateSuccess(tddlConnection, "set global READONLY_DN_LIST='" + readonlyDn + "'");
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show variables like 'READONLY_DN_LIST'");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(readonlyDn, rs.getString("Value"));
    }

    private List<String> getDnList() throws SQLException {
        List<String> dnList = new ArrayList<>();
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "show storage");
        while (rs.next()) {
            if (rs.getString("INST_KIND").equalsIgnoreCase("MASTER")) {
                dnList.add(rs.getString("STORAGE_INST_ID"));
            }
        }
        rs.close();
        return dnList;
    }
}
