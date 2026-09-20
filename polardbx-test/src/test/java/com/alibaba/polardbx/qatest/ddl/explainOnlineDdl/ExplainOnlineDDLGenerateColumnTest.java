package com.alibaba.polardbx.qatest.ddl.explainOnlineDdl;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ExplainOnlineDDLGenerateColumnTest extends ExplainOnlineDDLBaseTest {
    String tableName = "explain_online_ddl_generate_column";

    @Before
    public void prepare() {
        String createTableSql =
            String.format(
                "create table %s (a int, b int, d int generated always as (a + 1) virtual, e int) partition by key(a)",
                tableName);
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
    public void testAddVirtualColumn() {
        String sql =
            String.format("explain online_ddl alter table %s add column c int generated always as (a + 1) virtual",
                tableName);
        DdlAlgorithm expectAlgorithm = isMySQL80() ? DdlAlgorithm.INSTANT : DdlAlgorithm.INPLACE;
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectAlgorithm);
    }

    @Test
    public void testDropVirtualColumn() {
        String sql =
            String.format("alter table %s add column c int generated always as (a + 1) virtual", tableName);
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = String.format("explain online_ddl alter table %s drop column c", tableName);
        DdlAlgorithm expectAlgorithm = isMySQL80() ? DdlAlgorithm.INSTANT : DdlAlgorithm.INPLACE;
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectAlgorithm);
    }

    @Test
    public void testAddStoredColumn() {
        String sql =
            String.format("explain online_ddl alter table %s add column c int generated always as (a + 1) stored",
                tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.LOCK_TABLE, DdlAlgorithm.COPY);
    }

    @Test
    public void testDropStoredColumn() {
        String sql =
            String.format("alter table %s add column c int generated always as (a + 1) stored", tableName);
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = String.format("explain online_ddl alter table %s drop column c", tableName);
        DdlAlgorithm expectAlgorithm = isMySQL80() ? DdlAlgorithm.INSTANT : DdlAlgorithm.INPLACE;
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectAlgorithm);
    }

    @Test
    public void testAddLogicalColumn() {
        String sql =
            String.format("explain online_ddl alter table %s add column c int generated always as (a + 1) logical",
                tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testDropLogicalColumn() {
        String sql =
            String.format("alter table %s add column c int generated always as (a + 1) logical", tableName);
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = String.format("explain online_ddl alter table %s drop column c", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testAddColumnBeforeVirtualColumn57() {
        if (isMySQL80()) {
            return;
        }
        String sql = String.format("explain online_ddl alter table %s add column f int after b", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.LOCK_TABLE, DdlAlgorithm.COPY);
    }

    @Test
    public void testDropColumnBeforeVirtualColumn57() {
        if (isMySQL80()) {
            return;
        }
        String sql = String.format("explain online_ddl alter table %s drop column b", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.LOCK_TABLE, DdlAlgorithm.COPY);
    }

    @Test
    public void testAddColumnAfterVirtualColumn57() {
        if (isMySQL80()) {
            return;
        }
        String sql = String.format("explain online_ddl alter table %s add column f int after d", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testDropColumnAfterVirtualColumn57() {
        if (isMySQL80()) {
            return;
        }
        String sql = String.format("explain online_ddl alter table %s drop column e", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testAddColumnBeforeVirtualColumn80() {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format("explain online_ddl alter table %s add column f int after b", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.INSTANT);
    }

    @Test
    public void testDropColumnBeforeVirtualColumn80() {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format("explain online_ddl alter table %s drop column b", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.INSTANT);
    }

    @Test
    public void testAddColumnAfterVirtualColumn80() {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format("explain online_ddl alter table %s add column f int after d", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.INSTANT);
    }

    @Test
    public void testDropColumnAfterVirtualColumn80() {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format("explain online_ddl alter table %s drop column e", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.INSTANT);
    }
}
