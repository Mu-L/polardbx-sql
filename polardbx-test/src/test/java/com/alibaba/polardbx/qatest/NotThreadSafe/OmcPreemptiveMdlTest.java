package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.RandomUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;

public class OmcPreemptiveMdlTest extends DDLBaseNewDBTestCase {

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Before
    public void beforeMethod() {
        if (!isMySQL80()) {
            JdbcUtil.executeSuccess(tddlConnection, "set FORCE_USING_OMC_30 = true");
        }
        JdbcUtil.executeSuccess(tddlConnection, "set global PHYSICAL_DDL_MDL_WAITING_TIMEOUT = 5");
    }

    @After
    public void afterMethod() {
        JdbcUtil.executeSuccess(tddlConnection, "set global PHYSICAL_DDL_MDL_WAITING_TIMEOUT = 15");
    }

    // 测试读流量被抢占
    @Test
    public void testReadPreempt() {
        if (!isMySQL80()) {
            return;
        }
        String tableName = "omc_read_preempt_" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);

        String sql =
            String.format(
                "create table %s (a int primary key, b int, c int, index idx_f((b + c))) partition by key(a)",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "insert into " + tableName + " values (0, 1, 2)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Connection newConnection = getPolardbxConnection();
        sql = "set autocommit = 0";
        JdbcUtil.executeUpdateSuccess(newConnection, sql);
        sql = "select * from " + tableName;
        JdbcUtil.executeQuery(sql, newConnection);

        sql = String.format("alter table %s modify column c int, algorithm=omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "commit";
        JdbcUtil.executeUpdateFailed(newConnection, sql, "");
    }

    // 测试写流量被抢占
    @Test
    public void testWritePreempt() {
        if (!isMySQL80()) {
            return;
        }
        String tableName = "omc_write_preempt_" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);

        String sql =
            String.format(
                "create table %s (a int primary key, b int, c int, index idx_f((b + c))) partition by key(a)",
                tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "insert into " + tableName + " values (0, 1, 2)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Connection newConnection = getPolardbxConnection();
        sql = "set autocommit = 0";
        JdbcUtil.executeUpdateSuccess(newConnection, sql);
        sql = "insert into " + tableName + " values (1, 2, 3)";
        JdbcUtil.executeUpdate(newConnection, sql);

        sql = String.format("alter table %s modify column c int, algorithm=omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "commit";
        JdbcUtil.executeUpdateFailed(newConnection, sql, "");
    }

    // 测试物理表快照版本
    @Test
    public void testDefinitionOfTableRequiredChanged() {
        String tableName = "omc_table_" + RandomUtils.getStringBetween(1, 5);
        String tableName2 = "tmp_table_" + RandomUtils.getStringBetween(1, 5);
        dropTableIfExists(tableName);
        dropTableIfExists(tableName2);

        String sql = String.format("create table %s (a int primary key, b int, c int) partition by key(a)", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = String.format("create table %s (a int primary key, b int, c int) partition by key(a)", tableName2);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " values (0, 1, 2)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "insert into " + tableName2 + " values (0, 1, 2)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        Connection newConnection = getPolardbxConnection();
        sql = "set transaction_isolation = 'REPEATABLE-READ'";
        JdbcUtil.executeUpdateSuccess(newConnection, sql);
        sql = "set autocommit = 0";
        JdbcUtil.executeUpdateSuccess(newConnection, sql);
        sql = "select * from " + tableName2;
        JdbcUtil.executeUpdate(newConnection, sql);

        sql = String.format("alter table %s modify column c int, algorithm=omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "select * from " + tableName;
        if (isMySQL80()) {
            JdbcUtil.executeUpdateFailed(newConnection, sql, "The definition of the table required");
        } else {
            JdbcUtil.executeUpdateFailed(newConnection, sql, "Table definition has changed");
        }
    }
}
