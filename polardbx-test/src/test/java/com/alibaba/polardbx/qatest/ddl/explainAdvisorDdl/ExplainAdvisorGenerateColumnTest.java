package com.alibaba.polardbx.qatest.ddl.explainAdvisorDdl;

import com.alibaba.polardbx.qatest.ReplicaIgnore;
import com.alibaba.polardbx.qatest.ddl.explainOnlineDdl.ExplainOnlineDDLBaseTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@ReplicaIgnore(ignoreReason = "set session variables")
public class ExplainAdvisorGenerateColumnTest extends ExplainOnlineDDLBaseTest {
    String tableName = "explain_online_ddl_generate_column";

    @Before
    public void prepare() {
        String createTableSql = String.format(
            "create table %s (a int, b int, d int generated always as (a + 1) virtual, e int) partition by key(a)",
            tableName);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);

        setSupportInstant(tddlConnection);

        if (!isMySQL80()) {
            JdbcUtil.executeSuccess(tddlConnection, "set FORCE_USING_OMC_30 = true");
        }
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
            String.format("explain advisor alter table %s add column c int generated always as (a + 1) virtual",
                tableName);
        String expectedSql =
            String.format("alter table %s add column c int generated always as (a + 1) virtual", tableName);
        DdlAlgorithm expectAlgorithm = isMySQL80() ? DdlAlgorithm.INSTANT : DdlAlgorithm.INPLACE;
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, expectAlgorithm);

        sql = String.format("explain advisor alter table %s drop column c", tableName);
        expectedSql = String.format("alter table %s drop column c", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, expectAlgorithm);
    }

    @Test
    public void testAddStoredColumn() {
        String sql =
            String.format("explain advisor alter table %s add column c int generated always as (a + 1) stored",
                tableName);
        String expectedSql =
            String.format("alter table %s add column c int generated always as (a + 1) stored, algorithm = omc",
                tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OMC30);
    }

    @Test
    public void testDropStoredColumn() {
        String sql =
            String.format("alter table %s add column c int generated always as (a + 1) stored", tableName);
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = String.format("explain advisor alter table %s drop column c", tableName);
        String expectedSql = String.format("alter table %s drop column c", tableName);
        DdlAlgorithm expectAlgorithm = isMySQL80() ? DdlAlgorithm.INSTANT : DdlAlgorithm.INPLACE;
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, expectAlgorithm);
    }

    @Test
    public void testAddLogicalColumn() {
        String sql =
            String.format("explain advisor alter table %s add column c int generated always as (a + 1) logical",
                tableName);
        String expectedSql =
            String.format("alter table %s add column c int generated always as (a + 1) logical", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.INPLACE);

        sql = String.format("explain advisor alter table %s drop column c", tableName);
        expectedSql = String.format("alter table %s drop column c", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testAddColumnBeforeVirtualColumn57() {
        if (isMySQL80()) {
            return;
        }
        String sql = String.format("explain advisor alter table %s add column f int after b", tableName);
        String expectedSql = String.format("alter table %s add column f int after b, algorithm = omc", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OMC30);
    }

    @Test
    public void testDropColumnBeforeVirtualColumn57() {
        if (isMySQL80()) {
            return;
        }
        // todo: remove hint
        String sql = String.format("explain advisor alter table %s drop column b", tableName);
        String expectedSql = String.format("alter table %s drop column b, algorithm = omc", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OMC30);
    }

    @Test
    public void testAddColumnAfterVirtualColumn57() {
        if (isMySQL80()) {
            return;
        }
        String sql = String.format("explain advisor alter table %s add column f int after d", tableName);
        String expectedSql = String.format("alter table %s add column f int after d", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testDropColumnAfterVirtualColumn57() {
        if (isMySQL80()) {
            return;
        }
        String sql = String.format("explain advisor alter table %s drop column e", tableName);
        String expectedSql = String.format("alter table %s drop column e", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testAddColumnBeforeVirtualColumn80() {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format("explain advisor alter table %s add column f int after b", tableName);
        String expectedSql = String.format("alter table %s add column f int after b", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.INSTANT);
    }

    @Test
    public void testDropColumnBeforeVirtualColumn80() {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format("explain advisor alter table %s drop column b", tableName);
        String expectedSql = String.format("alter table %s drop column b", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.INSTANT);
    }

    @Test
    public void testAddColumnAfterVirtualColumn80() {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format("explain advisor alter table %s add column f int after d", tableName);
        String expectedSql = String.format("alter table %s add column f int after d", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.INSTANT);
    }

    @Test
    public void testDropColumnAfterVirtualColumn80() {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format("explain advisor alter table %s drop column e", tableName);
        String expectedSql = String.format("alter table %s drop column e", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.INSTANT);
    }
}
