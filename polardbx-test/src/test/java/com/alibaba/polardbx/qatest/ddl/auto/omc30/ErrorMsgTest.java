package com.alibaba.polardbx.qatest.ddl.auto.omc30;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.ReplicaIgnore;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Before;
import org.junit.Test;

@ReplicaIgnore(ignoreReason = "set session variables")
public class ErrorMsgTest extends DDLBaseNewDBTestCase {

    @Before
    public void beforeMethod() {
        JdbcUtil.executeSuccess(tddlConnection, "set FORCE_USING_OMC_30 = true");
        JdbcUtil.executeSuccess(tddlConnection, "set ALLOW_LOOSE_ALTER_COLUMN_WITH_GSI = true");
    }

    @Test
    public void testErrorMsg() {
        setSqlMode("STRICT_TRANS_TABLES", tddlConnection);
        String tableName = "omc30_err_msg_t1";
        String sql = String.format("create table %s (a int primary key, b int, c int) single", tableName);
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = String.format("insert into %s values (1, 1, 1)", tableName);
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column b bigint after b, algorithm=omc;", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Unknown column 'b'");

        sql = String.format("alter table %s modify column d bigint, algorithm=omc;", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Modify unknown column");

        sql = String.format("alter table %s modify column b bigint GENERATED ALWAYS AS (a) virtual, algorithm=omc",
            tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "can not be modified to a generated column");

        // with add column
        sql = String.format("alter table %s add column d bigint, drop column d, algorithm=omc", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Unknown column 'd'");

        sql = String.format("alter table %s modify column b bigint,add column d int,add column d int, algorithm=omc;",
            tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Duplicate column name 'd'");

        sql = String.format("alter table %s modify column b bigint, add column c int, algorithm=omc;", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Duplicate column name 'c'");

        sql = String.format("alter table %s modify column b bigint, add column d int not null, algorithm=omc;",
            tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Field 'd' doesn't have a default value");

        sql = String.format("alter table %s modify column b bigint, add column d int auto_increment, algorithm=omc;",
            tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Incorrect table definition");

        // with drop column
        sql = String.format("alter table %s modify column b bigint, drop column b, algorithm=omc", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Unknown column 'b'");

        sql = String.format("alter table %s modify column b bigint, drop column d, algorithm=omc", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Unknown column 'd'");

        sql = String.format("alter table %s modify column b bigint, drop column a, algorithm=omc", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Drop primary key is not supported");
    }

    @Test
    public void testOnlineModifyColumnErrorMsg2() {
        String tableName = "omc30_err_msg_t2";
        String sql = String.format("create table %s (a int primary key, b int, c int) partition by key(a)", tableName);
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = String.format("alter table %s change column b d bigint after b, algorithm=omc;", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Unknown column 'b'");

        sql = String.format("alter table %s change column a a bigint auto_increment, algorithm=omc;", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Do not support modify the column type of partition key");

        sql = String.format("alter table %s change column d d bigint, algorithm=omc;", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Modify unknown column");

        sql = String.format("alter table %s change column b d bigint GENERATED ALWAYS AS (a) virtual, algorithm=omc",
            tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "can not be changed to a generated column");

        sql = String.format("alter table %s change column a d bigint, algorithm=omc;", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Do not support modify the column type of partition key");
    }

    @Test
    public void testOnlineModifyColumnErrorMsg3() {
        String tableName = "omc30_err_msg_t3";
        String sql = String.format(
            "create table %s (a int primary key, b int, c int, d int GENERATED ALWAYS AS (a + b)) partition by key(a)",
            tableName);
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = String.format("alter table %s change column d d bigint, algorithm=omc;", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Can not change generated column");

        sql = String.format("alter table %s modify column d bigint, algorithm=omc", tableName);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "Can not modify generated column");

        sql = String.format("alter table %s change column b b bigint, algorithm=omc;", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = String.format("alter table %s modify column b bigint, algorithm=omc", tableName);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}
