package com.alibaba.polardbx.qatest.ddl.explainOnlineDdl;

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ExplainOnlineDDLAlterTableWithGSITest extends ExplainOnlineDDLBaseTest {
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
    public void testModifyColumnOnlyInPrimary() {
        String sql = String.format("explain online_ddl alter table %s modify column e bigint", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.LOCK_TABLE, DdlAlgorithm.COPY);
    }

    @Test
    public void testModifyPrimaryTablePartitionColumn() {
        String sql = String.format("explain online_ddl alter table %s modify column a bigint", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.OMC20);
    }

    @Test
    public void testModifyGsiPartitionColumn() {
        String sql = String.format("explain online_ddl alter table %s modify column b varchar(12)", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.OMC20);
    }

    @Test
    public void testModifyGsiCoveringColumn() {
        String sql = String.format("explain online_ddl alter table %s modify column c bigint", tableName);
        assertOnlineDdlResult(tddlConnection, sql, DdlType.ONLINE_DDL, DdlAlgorithm.OMC20);
    }
}
