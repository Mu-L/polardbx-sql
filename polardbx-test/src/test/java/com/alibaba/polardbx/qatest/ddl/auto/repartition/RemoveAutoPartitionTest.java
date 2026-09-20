package com.alibaba.polardbx.qatest.ddl.auto.repartition;

import com.alibaba.polardbx.qatest.ddl.explainOnlineDdl.ExplainOnlineDDLBaseTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.sql.SQLException;

public class RemoveAutoPartitionTest extends ExplainOnlineDDLBaseTest {

    @Test
    public void testRemoveAutoPartitionExplain() {
        String sql = "drop table if exists auto_tb123";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create table auto_tb123 (a int primary key auto_increment, b int)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "alter table auto_tb123 remove auto partition";
        assertExplainAdvisorResult(tddlConnection, "explain advisor " + sql, DdlType.ONLINE_DDL, sql,
            DdlAlgorithm.INSTANT);
    }

    @Test
    public void testRemoveAutoPartitionRollback() throws SQLException {
        String sql = "drop table if exists auto_tb124";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);
        sql = "create table auto_tb124 (a int primary key auto_increment, b int)";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        String showTable1 = showCreateTable(tddlConnection, "auto_tb124");

        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET @FP_FAILED_TABLE_SYNC = 'true'");
        sql = "/*+TDDL:cmd_extra(FP_FAILED_TABLE_SYNC=true)*/alter table auto_tb124 remove auto partition";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "FP_FAILED_TABLE_SYNC");

        JdbcUtil.executeUpdateSuccess(tddlConnection, "SET @FP_FAILED_TABLE_SYNC = 'false'");

        JobInfo ddlInfo = fetchCurrentJob("auto_tb124");
        JdbcUtil.executeUpdateSuccess(tddlConnection, "rollback ddl " + ddlInfo.parentJob.jobId);

        String showTable2 = showCreateTable(tddlConnection, "auto_tb124");

        Assert.assertEquals(showTable1, showTable2);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}
