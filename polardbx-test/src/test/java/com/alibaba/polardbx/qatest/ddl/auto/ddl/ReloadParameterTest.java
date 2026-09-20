package com.alibaba.polardbx.qatest.ddl.auto.ddl;

import com.alibaba.polardbx.qatest.CdcIgnore;
import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Ignore;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;

@CdcIgnore(ignoreReason = "修改 sql_mode，下游会中断，因为 sql_mode 不一致")
public class ReloadParameterTest extends DDLBaseNewDBTestCase {

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    public void testReloadParameter1() {
        String tableName = "reload_param_t1";
        String sql = "create table " + tableName + "(id int primary key, c1 int, c2 int) partition by key(id)";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "set sql_mode = 'STRICT_TRANS_TABLES'";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " values(1,1,1),(2,null,2),(3,3,3)";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        Long jobId = generateDdlJobId();
        String myHint = String.format("/*+TDDL:cmd_extra(ddl_job_id=%s,ENABLE_DRDS_MULTI_PHASE_DDL=false)*/", jobId);
        sql = myHint + "alter table " + tableName + " modify column c1 int not null";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        sql = "set RELOAD_DDL_PARAMETER = '{\"jobIds\":[%s],\"parameters\":{\"sql_mode\":\"\"}}'";
        JdbcUtil.executeSuccess(tddlConnection, String.format(sql, jobId));

        sql = "continue ddl " + jobId;
        JdbcUtil.executeSuccess(tddlConnection, sql);
    }

    @Ignore
    @Test
    public void testReloadParameter2() throws SQLException {
        JdbcUtil.executeSuccess(tddlConnection, "set global PHYSICAL_DDL_MDL_WAITING_TIMEOUT = 15");

        String tableName = "reload_param_t2";
        String sql = "create table " + tableName + "(id int primary key, c1 int, c2 int) partition by key(id)";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "insert into " + tableName + " values(1,1,1),(2,2,2),(3,3,3)";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        Connection conn = getPolardbxConnection0(tddlDatabase1);
        conn.setAutoCommit(false);
        sql = "select id,c1,c2 from " + tableName + " where id = 1";
        JdbcUtil.executeQuery(sql, conn);

        sql = "set lock_wait_timeout = 3";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        Long jobId = generateDdlJobId();
        String myHint = String.format("/*+TDDL:cmd_extra(ddl_job_id=%s,ENABLE_DRDS_MULTI_PHASE_DDL=false)*/", jobId);
        sql = myHint + "alter table " + tableName + " modify column c1 int not null";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "");

        sql = "set RELOAD_DDL_PARAMETER = '{\"jobIds\":[%s],\"parameters\":{\"lock_wait_timeout\":500}}'";
        JdbcUtil.executeSuccess(tddlConnection, String.format(sql, jobId));

        sql = "continue ddl " + jobId;
        JdbcUtil.executeSuccess(tddlConnection, sql);
    }
}
