package com.alibaba.polardbx.qatest.ddl.auto.repartition;

import com.alibaba.polardbx.qatest.ddl.explainOnlineDdl.ExplainOnlineDDLBaseTest;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class RepartitionSingleOptimizeTest extends ExplainOnlineDDLBaseTest {

    @Test
    public void testRepartitionSingle() {
        String sql = "drop table if exists re_single_t1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create table re_single_t1 (a int primary key, b int) single";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        List<String> phyTableNames = showTopology(tddlConnection, "re_single_t1");

        sql = "alter table re_single_t1 partition by key (a) partitions 1";
        assertOnlineDdlResult(tddlConnection, "explain online_ddl " + sql, DdlType.ONLINE_DDL, DdlAlgorithm.META_ONLY);
        assertExplainAdvisorResult(tddlConnection, "explain advisor " + sql, DdlType.ONLINE_DDL, sql,
            DdlAlgorithm.META_ONLY);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        List<String> phyTableNames2 = showTopology(tddlConnection, "re_single_t1");

        Assert.assertEquals(phyTableNames, phyTableNames2);
    }

    @Test
    public void testRepartitionSingle2() {
        String sql = "drop table if exists re_single_t2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create table re_single_t2 (a int primary key, b int) single";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        List<String> phyTableNames = showTopology(tddlConnection, "re_single_t2");

        sql = "alter table re_single_t2 partition by key (a, b) partitions 1";
        assertOnlineDdlResult(tddlConnection, "explain online_ddl " + sql, DdlType.ONLINE_DDL, DdlAlgorithm.META_ONLY);
        assertExplainAdvisorResult(tddlConnection, "explain advisor " + sql, DdlType.ONLINE_DDL, sql,
            DdlAlgorithm.META_ONLY);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        List<String> phyTableNames2 = showTopology(tddlConnection, "re_single_t2");

        Assert.assertEquals(phyTableNames, phyTableNames2);
    }

    @Test
    public void testRepartitionSingle3() {
        String sql = "drop table if exists re_single_t3";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create table re_single_t3 (a int primary key, b int) single";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        List<String> phyTableNames = showTopology(tddlConnection, "re_single_t3");

        sql = "alter table re_single_t3 partition by hash (a, b) partitions 1";
        assertOnlineDdlResult(tddlConnection, "explain online_ddl " + sql, DdlType.ONLINE_DDL, DdlAlgorithm.OSC);
        assertExplainAdvisorResult(tddlConnection, "explain advisor " + sql, DdlType.ONLINE_DDL, sql,
            DdlAlgorithm.OSC);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        List<String> phyTableNames2 = showTopology(tddlConnection, "re_single_t3");

        Assert.assertNotEquals(phyTableNames, phyTableNames2);
    }

    @Test
    public void testRepartitionSingle4() {
        String sql = "drop table if exists re_single_t4";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create table re_single_t4 (a int primary key, b int) single";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        List<String> phyTableNames = showTopology(tddlConnection, "re_single_t4");

        sql = "alter table re_single_t4 partition by hash (a) partitions 1";
        assertOnlineDdlResult(tddlConnection, "explain online_ddl " + sql, DdlType.ONLINE_DDL, DdlAlgorithm.META_ONLY);
        assertExplainAdvisorResult(tddlConnection, "explain advisor " + sql, DdlType.ONLINE_DDL, sql,
            DdlAlgorithm.META_ONLY);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        List<String> phyTableNames2 = showTopology(tddlConnection, "re_single_t4");

        Assert.assertEquals(phyTableNames, phyTableNames2);
    }

    @Test
    public void testRepartitionSingle5() {
        String sql = "drop table if exists re_single_t5";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create table re_single_t5 (a int primary key, b int) single";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        List<String> phyTableNames = showTopology(tddlConnection, "re_single_t5");

        sql = "alter table re_single_t5 single";
        assertOnlineDdlResult(tddlConnection, "explain online_ddl " + sql, DdlType.ONLINE_DDL, DdlAlgorithm.META_ONLY);
        assertExplainAdvisorResult(tddlConnection, "explain advisor " + sql, DdlType.ONLINE_DDL, sql,
            DdlAlgorithm.META_ONLY);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        List<String> phyTableNames2 = showTopology(tddlConnection, "re_single_t5");

        Assert.assertEquals(phyTableNames, phyTableNames2);
    }

    @Test
    public void testRepartitionSingle6() {
        String sql = "drop table if exists re_single_t6";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create table re_single_t6 (a int primary key, b int) partition by key(a) partitions 1";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        List<String> phyTableNames = showTopology(tddlConnection, "re_single_t6");

        sql = "alter table re_single_t6 partition by key (a) partitions 1";
        assertOnlineDdlResult(tddlConnection, "explain online_ddl " + sql, DdlType.ONLINE_DDL, DdlAlgorithm.META_ONLY);
        assertExplainAdvisorResult(tddlConnection, "explain advisor " + sql, DdlType.ONLINE_DDL, sql,
            DdlAlgorithm.META_ONLY);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        List<String> phyTableNames2 = showTopology(tddlConnection, "re_single_t6");

        Assert.assertEquals(phyTableNames, phyTableNames2);
    }

    @Test
    public void testRepartitionSingle7() {
        String sql = "drop table if exists re_single_t7";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        sql = "create table re_single_t7 (a int primary key, b int) partition by key(a) partitions 2";
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        List<String> phyTableNames = showTopology(tddlConnection, "re_single_t7");

        sql = "alter table re_single_t7 partition by key (a) partitions 1";
        assertOnlineDdlResult(tddlConnection, "explain online_ddl " + sql, DdlType.ONLINE_DDL, DdlAlgorithm.OSC);
        assertExplainAdvisorResult(tddlConnection, "explain advisor " + sql, DdlType.ONLINE_DDL, sql,
            DdlAlgorithm.OSC);
        JdbcUtil.executeUpdateSuccess(tddlConnection, sql);

        List<String> phyTableNames2 = showTopology(tddlConnection, "re_single_t7");

        Assert.assertNotEquals(phyTableNames, phyTableNames2);
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }
}
