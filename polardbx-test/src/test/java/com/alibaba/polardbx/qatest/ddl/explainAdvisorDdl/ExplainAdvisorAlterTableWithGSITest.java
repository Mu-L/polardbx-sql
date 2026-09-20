package com.alibaba.polardbx.qatest.ddl.explainAdvisorDdl;

import com.alibaba.polardbx.qatest.ReplicaIgnore;
import com.alibaba.polardbx.qatest.ddl.explainOnlineDdl.ExplainOnlineDDLBaseTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ExplainAdvisorAlterTableWithGSITest extends ExplainOnlineDDLBaseTest {
    String tableName = "explain_online_alter_table_gsi";
    String localIndexName = "idx_b";
    String globalIndexName = "gsi_b";

    @Before
    public void prepare() {
        String createTableSql = String.format(
            "create table %s ("
                + "a int,"
                + "b varchar(10),"
                + "c int,"
                + "d varchar(20),"
                + "e int,"
                + "index %s(b),"
                + "global index %s(b) covering(c, d) partition by key(b)"
                + ") partition by key(a)",
            tableName, localIndexName, globalIndexName);
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

    @ReplicaIgnore(ignoreReason = "set session variables")
    @Test
    public void testModifyColumnOnlyInPrimary() {
        String sql = String.format("explain advisor alter table %s modify column e bigint", tableName);
        String expectedSql = String.format("alter table %s modify column e bigint, algorithm = omc", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OMC30);
    }

    @Test
    public void testModifyColumnOnlyInPrimary2() {
        String sql = String.format("explain advisor alter table %s modify column e int comment '123'", tableName);
        String expectedSql = String.format("alter table %s modify column e int comment '123'", tableName);
        DdlAlgorithm expectAlgorithm = isMySQL80() ? DdlAlgorithm.INSTANT : DdlAlgorithm.INPLACE;
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, expectAlgorithm);
    }

    @Test
    public void testModifyPrimaryTablePartitionColumn() {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format("explain advisor alter table %s modify column a bigint", tableName);
        String expectedSql = String.format("alter table %s modify column a bigint", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OMC20);
    }

    @Test
    public void testModifyPrimaryTablePartitionColumn2() {
        String sql =
            String.format("explain advisor alter table %s modify column a int comment '123' default 123", tableName);
        String expectedSql = String.format("alter table %s modify column a int default 123 comment '123'", tableName);
        DdlAlgorithm expectAlgorithm = isMySQL80() ? DdlAlgorithm.INSTANT : DdlAlgorithm.INPLACE;
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, expectAlgorithm);
    }

    @Test
    public void testModifyGsiPartitionColumn() {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format("explain advisor alter table %s modify column b varchar(12)", tableName);
        String expectedSql = String.format("alter table %s modify column b varchar(12)", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OMC20);
    }

    @Test
    public void testModifyGsiCoveringColumn() {
        if (!isMySQL80()) {
            return;
        }
        String sql = String.format("explain advisor alter table %s modify column c bigint", tableName);
        String expectedSql = String.format("alter table %s modify column c bigint", tableName);
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, DdlAlgorithm.OMC20);
    }

    @Test
    public void testModifyGsiCoveringColumn2() {
        String sql = String.format("explain advisor alter table %s modify column c int comment '123'", tableName);
        String expectedSql = String.format("alter table %s modify column c int comment '123'", tableName);
        DdlAlgorithm expectAlgorithm = isMySQL80() ? DdlAlgorithm.INSTANT : DdlAlgorithm.INPLACE;
        assertExplainAdvisorResult(tddlConnection, sql, DdlType.ONLINE_DDL, expectedSql, expectAlgorithm);
    }
}
