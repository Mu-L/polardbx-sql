package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.ConnectionManager;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

public class AlterTableAddPartitionFailTest extends DDLBaseNewDBTestCase {
    private static final String DB_NAME = "AlterTableAddPartitionFailTest";
    private static final String TABLE_NAME1 = "t1";
    private static final String TABLE_NAME2 = "t2";
    private static final String TABLE_NAME3 = "t3";

    @Before
    public void setUp() throws SQLException {
        try (Connection conn = ConnectionManager.getInstance().getDruidPolardbxConnection()) {
            JdbcUtil.useDb(conn, "information_schema");
            conn.createStatement().execute("drop database if exists " + DB_NAME);
            conn.createStatement().execute("create database if not exists " + DB_NAME + " mode=auto");
            conn.createStatement().execute("create table " + DB_NAME + "." + TABLE_NAME1
                + " (id int, name varchar(20)) partition by range(id) (partition p1 values less than (100), partition p2 values less than (200))");
            conn.createStatement().execute("create table " + DB_NAME + "." + TABLE_NAME2
                + " (id int, name varchar(20)) partition by range(id) (partition p1 values less than (100), partition p2 values less than (200))");
            conn.createStatement().execute("create table " + DB_NAME + "." + TABLE_NAME3
                + " (id int, a int) partition by key(id) partitions 2 subpartition by range(a) (subpartition sp1 values less than(100), subpartition sp2 values less than(200)) ");
        }
    }

    @Test
    public void testAddPartitionFail() throws SQLException {
        JdbcUtil.useDb(tddlConnection, DB_NAME);
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "/*+TDDL:cmd_extra(FP_ROLLBACK_AFTER_DDL_TASK_EXECUTION='CreatePhyTableWithRollbackCheckTask')*/alter tablegroup by table "
                + TABLE_NAME1 + " add partition (partition p3 values less than (300))",
            "The DDL job has been cancelled or interrupted");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "/*+TDDL:cmd_extra(FP_ROLLBACK_AFTER_DDL_TASK_EXECUTION='CreatePhyTableWithRollbackCheckTask')*/alter tablegroup by table "
                + TABLE_NAME1 + " add partition (partition p4 values less than (400))",
            "The DDL job has been cancelled or interrupted");
        JdbcUtil.executeSuccess(tddlConnection,
            "alter tablegroup by table " + TABLE_NAME1 + " add partition (partition p5 values less than (500))");

        JdbcUtil.executeUpdateFailed(tddlConnection,
            "/*+TDDL:cmd_extra(FP_ROLLBACK_AFTER_DDL_TASK_EXECUTION='CreatePhyTableWithRollbackCheckTask')*/alter table "
                + TABLE_NAME3 + " add subpartition (subpartition sp3 values less than (300))",
            "The DDL job has been cancelled or interrupted");
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "/*+TDDL:cmd_extra(FP_ROLLBACK_AFTER_DDL_TASK_EXECUTION='CreatePhyTableWithRollbackCheckTask')*/alter table "
                + TABLE_NAME3
                + " add subpartition (subpartition sp3 values less than (300),subpartition sp4 values less than (400))",
            "The DDL job has been cancelled or interrupted");
        JdbcUtil.executeSuccess(tddlConnection, "alter table " + TABLE_NAME3
            + " add subpartition (subpartition sp3 values less than (300),subpartition sp5 values less than (500))");
    }
}
