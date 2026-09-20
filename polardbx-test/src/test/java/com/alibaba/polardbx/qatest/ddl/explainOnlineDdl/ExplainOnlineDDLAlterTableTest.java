package com.alibaba.polardbx.qatest.ddl.explainOnlineDdl;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ExplainOnlineDDLAlterTableTest extends ExplainOnlineDDLBaseTest {
    String tableName = "explain_online_alter_table";
    String localIndexName = "idx_b";

    @Before
    public void prepare() {
        String createTableSql =
            String.format(
                "create table %s (a int, b varchar(10), c int, d char(10), index %s(b)) partition by key(a)",
                tableName, localIndexName);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);

        setSupportInstant(tddlConnection);
    }

    @After
    public void clean() {
        String dropTableSql = String.format("drop table %s", tableName);
        JdbcUtil.executeSuccess(tddlConnection, dropTableSql);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    public void testAddColumn() {
        String sql = String.format("explain online_ddl alter table %s add column f varchar(20)", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.INSTANT);
    }

    @Test
    public void testDropColumn() {
        String sql = String.format("explain online_ddl alter table %s drop column d", tableName);
        DdlAlgorithm expectAlgorithm = isMySQL80() ? DdlAlgorithm.INSTANT : DdlAlgorithm.INPLACE;
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectAlgorithm);
    }

    @Test
    public void testRenameColumn() {
        String sql = String.format("explain online_ddl alter table %s change column c cc int", tableName);
        DdlAlgorithm expectAlgorithm = isMySQL80() ? DdlAlgorithm.INSTANT : DdlAlgorithm.INPLACE;
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectAlgorithm);
    }

    @Test
    public void testModifyColumnOrder() {
        String sql = String.format("explain online_ddl alter table %s modify column c int after d", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testModifyColumnDefaultValue() {
        String sql =
            String.format("explain online_ddl alter table %s modify column b varchar(10) default \"abc\"", tableName);
        DdlAlgorithm expectAlgorithm = isMySQL80() ? DdlAlgorithm.INSTANT : DdlAlgorithm.INPLACE;
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectAlgorithm);
    }

    @Test
    public void testSetDefaultValue() {
        String sql = String.format("explain online_ddl alter table %s alter column b set default \"abc\"", tableName);
        DdlAlgorithm expectAlgorithm = isMySQL80() ? DdlAlgorithm.INSTANT : DdlAlgorithm.INPLACE;
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectAlgorithm);
    }

    @Test
    public void testModifyColumnType() {
        String sql = String.format("explain online_ddl alter table %s modify column c bigint", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.LOCK_TABLE, DdlAlgorithm.COPY);

        sql = String.format("explain online_ddl alter table %s modify column c varchar(100)", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.LOCK_TABLE, DdlAlgorithm.COPY);

        sql = String.format("explain online_ddl alter table %s modify column c decimal(12,2)", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.LOCK_TABLE, DdlAlgorithm.COPY);
    }

    @Test
    public void testModifyVarcharType57() {
        if (isMySQL80()) {
            return;
        }
        String sql = String.format("explain online_ddl alter table %s modify column b varchar(11)", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.INPLACE);

        sql = String.format("explain online_ddl alter table %s modify column b varchar(8)", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.LOCK_TABLE, DdlAlgorithm.COPY);

        sql = String.format("explain online_ddl alter table %s modify column b varchar(300)", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testModifyVarcharType80() {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format("explain online_ddl alter table %s modify column b varchar(11)", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.INPLACE);

        sql = String.format("explain online_ddl alter table %s modify column b varchar(8)", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.LOCK_TABLE, DdlAlgorithm.COPY);

        sql = String.format("explain online_ddl alter table %s modify column b varchar(300)", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.LOCK_TABLE, DdlAlgorithm.COPY);
    }

    @Test
    public void testModifyCharType57() {
        if (isMySQL80()) {
            return;
        }
        String sql = String.format("explain online_ddl alter table %s modify column d char(11)", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.INPLACE);

        sql = String.format("explain online_ddl alter table %s modify column d char(8)", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.LOCK_TABLE, DdlAlgorithm.COPY);

        sql = String.format("explain online_ddl alter table %s modify column d char(200)", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testModifyCharType80() {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format("explain online_ddl alter table %s modify column d char(11)", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.LOCK_TABLE, DdlAlgorithm.COPY);

        sql = String.format("explain online_ddl alter table %s modify column d char(8)", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.LOCK_TABLE, DdlAlgorithm.COPY);

        sql = String.format("explain online_ddl alter table %s modify column d char(200)", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.LOCK_TABLE, DdlAlgorithm.COPY);
    }

    @Test
    public void testModifyColumnNullable() {
        String sqlMode = JdbcUtil.getSqlMode(tddlConnection);
        try {
            setSqlMode("", tddlConnection);
            String sql = String.format(
                "explain online_ddl alter table %s modify column c int not null", tableName);
            assertOnlineDdlResult(tddlConnection, sql, DdlType.LOCK_TABLE, DdlAlgorithm.COPY);
        } finally {
            setSqlMode(sqlMode, tddlConnection);
        }
    }

    @Test
    public void testModifyColumnNullableWithStrictMode() {
        String sqlMode = JdbcUtil.getSqlMode(tddlConnection);
        try {
            setSqlMode("STRICT_TRANS_TABLES", tddlConnection);
            String sql = String.format(
                "explain online_ddl alter table %s modify column c int not null", tableName);
            assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.INPLACE);
        } finally {
            setSqlMode(sqlMode, tddlConnection);
        }
    }

    @Test
    public void testMultiAlters() {
        String sql = String.format(
            "explain online_ddl alter table %s add column f int, drop column c, modify column d char(10) default \"test\"",
            tableName);
        DdlAlgorithm expectAlgorithm = isMySQL80() ? DdlAlgorithm.INSTANT : DdlAlgorithm.INPLACE;
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectAlgorithm);

        sql = String.format("explain online_ddl alter table %s modify column d varchar(10), drop column c", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.LOCK_TABLE, DdlAlgorithm.COPY);
    }

    @Test
    public void testAlterPrimaryKey() {
        String sql = String.format("explain online_ddl alter table %s add primary key(a)", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.OMC20);

        sql = String.format("explain online_ddl alter table %s drop primary key", tableName);
        JdbcUtil.executeFailed(tddlConnection, sql, "drop primary key is not supported yet");
    }
}
