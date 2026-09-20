package com.alibaba.polardbx.qatest.NotThreadSafe;

import com.alibaba.polardbx.qatest.DDLBaseNewDBTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Test;

public class ReloadTest extends DDLBaseNewDBTestCase {

    @Test
    public void testReloadTable() {
        String sql = "reload table";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "table name can not be empty");

        sql = "reload table t1 t2";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "syntax error");

        sql = "reload table t1,t2";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "syntax error");

        sql = "reload table t1";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "reload table t1 preemptive";
        JdbcUtil.executeSuccess(tddlConnection, sql);
    }

    @Test
    public void testReload() {
        String sql = "reload datasources";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "reload schema";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "reload user";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "reload functions";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "reload java functions";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "reload filestorage";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "reload COLUMNARMANAGER cache";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "reload COLUMNARMANAGER SNAPSHOT";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "reload COLUMNARMANAGER SCHEMA";
        JdbcUtil.executeSuccess(tddlConnection, sql);

        sql = "reload COLUMNARMANAGER";
        JdbcUtil.executeSuccess(tddlConnection, sql);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}
