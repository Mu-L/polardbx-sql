package com.alibaba.polardbx.qatest.ddl.explainAdvisorDdl;

import com.alibaba.polardbx.qatest.ReplicaIgnore;
import com.alibaba.polardbx.qatest.ddl.explainOnlineDdl.ExplainOnlineDDLBaseTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

@ReplicaIgnore(ignoreReason = "set session variables")
public class ExplainAdvisorAlterTableTest extends ExplainOnlineDDLBaseTest {
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
    public void testAddColumn() {
        String sql = String.format("explain advisor alter table %s add column f varchar(20)", tableName);
        String expectedSql = String.format("alter table %s add column f varchar(20)", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.INSTANT);
    }

    @Test
    public void testAddColumn2() {
        String sql = String.format("explain advisor alter table %s add column f varchar(20) after a", tableName);
        String expectedSql = String.format("alter table %s add column f varchar(20) after a", tableName);
        DdlAlgorithm expectAlgorithm = isMySQL80() ? DdlAlgorithm.INSTANT : DdlAlgorithm.INPLACE;
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, expectAlgorithm);
    }

    @Test
    public void testDropColumn() {
        String sql = String.format("explain advisor alter table %s drop column d", tableName);
        String expectedSql = String.format("alter table %s drop column d", tableName);
        DdlAlgorithm expectAlgorithm = isMySQL80() ? DdlAlgorithm.INSTANT : DdlAlgorithm.INPLACE;
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, expectAlgorithm);
    }

    @Test
    public void testRenameColumn() {
        String sql = String.format("explain advisor alter table %s change column c cc int", tableName);
        String expectedSql = String.format("alter table %s change column c cc int", tableName);
        DdlAlgorithm expectAlgorithm = isMySQL80() ? DdlAlgorithm.INSTANT : DdlAlgorithm.INPLACE;
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, expectAlgorithm);
    }

    @Test
    public void testRenameColumn2() {
        String sql = String.format("explain advisor alter table %s change column c cc bigint", tableName);
        String expectedSql = String.format("alter table %s change column c cc bigint, algorithm = omc", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OMC30);
    }

    @Test
    public void testModifyColumnOrder() {
        String sql = String.format("explain advisor alter table %s modify column c int after d", tableName);
        String expectedSql = String.format("alter table %s modify column c int after d", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testModifyColumnDefaultValue() {
        String sql =
            String.format("explain advisor alter table %s modify column b varchar(10) default \"abc\"", tableName);
        String expectedSql = String.format("alter table %s modify column b varchar(10) default 'abc'", tableName);
        DdlAlgorithm expectAlgorithm = isMySQL80() ? DdlAlgorithm.INSTANT : DdlAlgorithm.INPLACE;
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, expectAlgorithm);
    }

    @Test
    public void testSetDefaultValue() {
        String sql = String.format("explain advisor alter table %s alter column b set default \"abc\"", tableName);
        String expectedSql = String.format("alter table %s alter column b set default 'abc'", tableName);
        DdlAlgorithm expectAlgorithm = isMySQL80() ? DdlAlgorithm.INSTANT : DdlAlgorithm.INPLACE;
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, expectAlgorithm);
    }

    @Test
    public void testModifyColumnType() {
        String sql = String.format("explain advisor alter table %s modify column c bigint", tableName);
        String expectedSql = String.format("alter table %s modify column c bigint, algorithm = omc", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OMC30);

        sql = String.format("explain advisor alter table %s modify column c varchar(100)", tableName);
        expectedSql = String.format("alter table %s modify column c varchar(100), algorithm = omc", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OMC30);

        sql = String.format("explain advisor alter table %s modify column c decimal(12,2)", tableName);
        expectedSql = String.format("alter table %s modify column c decimal(12, 2), algorithm = omc", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OMC30);
    }

    @Test
    public void testModifyVarcharType57() {
        if (isMySQL80()) {
            return;
        }
        String sql = String.format("explain advisor alter table %s modify column b varchar(11)", tableName);
        String expected = String.format("alter table %s modify column b varchar(11)", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expected, DdlAlgorithm.INPLACE);

        sql = String.format("explain advisor alter table %s modify column b varchar(8)", tableName);
        expected = String.format("alter table %s modify column b varchar(8), algorithm = omc", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expected, DdlAlgorithm.OMC30);

        sql = String.format("explain advisor alter table %s modify column b varchar(300)", tableName);
        expected = String.format("alter table %s modify column b varchar(300)", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expected, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testModifyCharType57() {
        if (isMySQL80()) {
            return;
        }
        String sql = String.format("explain advisor alter table %s modify column d char(11)", tableName);
        String expectedSql = String.format("alter table %s modify column d char(11)", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.INPLACE);

        sql = String.format("explain advisor alter table %s modify column d char(8)", tableName);
        expectedSql = String.format("alter table %s modify column d char(8), algorithm = omc", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OMC30);

        sql = String.format("explain advisor alter table %s modify column d char(200)", tableName);
        expectedSql = String.format("alter table %s modify column d char(200)", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.INPLACE);
    }

    @Test
    public void testModifyVarcharType80() {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format("explain advisor alter table %s modify column b varchar(11)", tableName);
        String expected = String.format("alter table %s modify column b varchar(11)", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expected, DdlAlgorithm.INPLACE);

        sql = String.format("explain advisor alter table %s modify column b varchar(8)", tableName);
        expected = String.format("alter table %s modify column b varchar(8), algorithm = omc", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expected, DdlAlgorithm.OMC30);

        sql = String.format("explain advisor alter table %s modify column b varchar(300)", tableName);
        expected = String.format("alter table %s modify column b varchar(300), algorithm = omc", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expected, DdlAlgorithm.OMC30);
    }

    @Test
    public void testModifyCharType80() {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format("explain advisor alter table %s modify column d char(11)", tableName);
        String expectedSql = String.format("alter table %s modify column d char(11), algorithm = omc", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OMC30);

        sql = String.format("explain advisor alter table %s modify column d char(8)", tableName);
        expectedSql = String.format("alter table %s modify column d char(8), algorithm = omc", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OMC30);

        sql = String.format("explain advisor alter table %s modify column d char(200)", tableName);
        expectedSql = String.format("alter table %s modify column d char(200), algorithm = omc", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OMC30);
    }

    @Test
    public void testModifyColumnNullable() {
        String sqlMode = JdbcUtil.getSqlMode(tddlConnection);
        try {
            setSqlMode("", tddlConnection);
            String sql = String.format(
                "explain advisor alter table %s modify column c int not null", tableName);
            String expectedSql =
                String.format("alter table %s modify column c int not null, algorithm = omc", tableName);
            assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OMC30);
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
                "explain advisor alter table %s modify column c int not null", tableName);
            String expectedSql = String.format("alter table %s modify column c int not null", tableName);
            assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.INPLACE);
        } finally {
            setSqlMode(sqlMode, tddlConnection);
        }
    }

    @Test
    public void testMultiAlters() {
        String sql = String.format(
            "explain advisor alter table %s add column f int, drop column c, modify column d char(10) default \"test\"",
            tableName);
        String expectedSql = String.format(
            "alter table %s add column f int, drop column c, modify column d char(10) default 'test'", tableName);
        DdlAlgorithm expectAlgorithm = isMySQL80() ? DdlAlgorithm.INSTANT : DdlAlgorithm.INPLACE;
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, expectAlgorithm);
    }

    @Test
    public void testMultiAlters2() {
        String sql = String.format(
            "explain advisor alter table %s modify column d varchar(10), drop column c, add column f int",
            tableName);
        String expectedSql = String.format(
            "alter table %s modify column d varchar(10), drop column c, add column f int, algorithm = omc",
            tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OMC30);
    }

    @Test
    public void testAlterPrimaryKey() {
        String sql = String.format("explain advisor alter table %s add primary key(a)", tableName);
        String expectedSql = String.format("alter table %s add primary key (a)", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OMC20);

        sql = String.format("explain advisor alter table %s drop primary key", tableName);
        JdbcUtil.executeFailed(tddlConnection, sql, "Drop primary key is not supported");
    }
}
