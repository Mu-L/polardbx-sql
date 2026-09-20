package com.alibaba.polardbx.qatest.dql.auto.explain;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

public class ExplianAnalyzeExecuteTest extends ReadBaseTestCase {

    public ExplianAnalyzeExecuteTest(String baseOneTableName, String baseTwoTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
    }

    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableSelect.selectOneTableMultiRuleMode());
    }

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

    @Test
    public void testExplainAnalyzeExecute() throws Exception {
        if (!isMySQL80()) {
            System.out.println("dn is not 8.0");
            return;
        }

        com.alibaba.polardbx.qatest.dql.sharding.explain.ExplianAnalyzeExecuteTest.testExplainAnalyzeExecute(
            getPolardbxConnection(), baseOneTableName, "select * from " + baseOneTableName + " where pk > 1", true);
        com.alibaba.polardbx.qatest.dql.sharding.explain.ExplianAnalyzeExecuteTest.testExplainAnalyzeExecute(
            getPolardbxConnection(), baseOneTableName, "select * from " + baseOneTableName + " where pk = 1", true);
        com.alibaba.polardbx.qatest.dql.sharding.explain.ExplianAnalyzeExecuteTest.testExplainAnalyzeExecute(
            getPolardbxConnection(), baseOneTableName, "select * from " + baseOneTableName + " where pk in (1,2,3)",
            true);

    }

    @Test
    public void testExplainAnalyzeExecuteFailed() {
        if (!isMySQL80()) {
            System.out.println("dn is not 8.0");
            return;
        }

        Assert.assertTrue(JdbcUtil.executeUpdateFailedReturn(tddlConnection,
                "explain analyze execute insert into " + baseOneTableName + "(pk) values (1)")
            .contains("explain analyze execute"));
        Assert.assertTrue(JdbcUtil.executeUpdateFailedReturn(tddlConnection,
            "explain analyze execute update " + baseOneTableName + " set pk = 2").contains("explain analyze execute"));
        Assert.assertTrue(JdbcUtil.executeUpdateFailedReturn(tddlConnection,
                "explain analyze execute update " + baseOneTableName + " set integer_test = 2")
            .contains("explain analyze execute"));
        Assert.assertTrue(JdbcUtil.executeUpdateFailedReturn(tddlConnection,
                "explain analyze execute update " + baseOneTableName + " set integer_test = 2 where pk = 1")
            .contains("explain analyze execute"));
        Assert.assertTrue(JdbcUtil.executeUpdateFailedReturn(tddlConnection,
                "explain analyze execute update " + baseOneTableName + " set pk = 2 where integer_test = 1")
            .contains("explain analyze execute"));
        Assert.assertTrue(JdbcUtil.executeUpdateFailedReturn(tddlConnection,
                "explain analyze execute delete " + baseOneTableName + " where integer_test = 1")
            .contains("explain analyze execute"));
        Assert.assertTrue(JdbcUtil.executeUpdateFailedReturn(tddlConnection,
                "explain analyze execute delete " + baseOneTableName + " where pk = 1")
            .contains("explain analyze execute"));
    }

    @Test
    public void testExplainTreeExecute() throws Exception {
        if (!isMySQL80()) {
            System.out.println("dn is not 8.0");
            return;
        }

        com.alibaba.polardbx.qatest.dql.sharding.explain.ExplianAnalyzeExecuteTest.testExplainTreeExecute(
            getPolardbxConnection(), baseOneTableName, "select * from " + baseOneTableName + " where pk > 1", true);
        com.alibaba.polardbx.qatest.dql.sharding.explain.ExplianAnalyzeExecuteTest.testExplainTreeExecute(
            getPolardbxConnection(), baseOneTableName, "select * from " + baseOneTableName + " where pk = 1", true);
        com.alibaba.polardbx.qatest.dql.sharding.explain.ExplianAnalyzeExecuteTest.testExplainTreeExecute(
            getPolardbxConnection(), baseOneTableName, "select * from " + baseOneTableName + " where pk in (1,2,3)",
            true);

    }

    @Test
    public void testExplainJsonExecute() throws Exception {
        if (!isMySQL80()) {
            System.out.println("dn is not 8.0");
            return;
        }

        com.alibaba.polardbx.qatest.dql.sharding.explain.ExplianAnalyzeExecuteTest.testExplainJsonExecute(
            getPolardbxConnection(), baseOneTableName, "select * from " + baseOneTableName + " where pk > 1", true);
        com.alibaba.polardbx.qatest.dql.sharding.explain.ExplianAnalyzeExecuteTest.testExplainJsonExecute(
            getPolardbxConnection(), baseOneTableName, "select * from " + baseOneTableName + " where pk = 1", true);
        com.alibaba.polardbx.qatest.dql.sharding.explain.ExplianAnalyzeExecuteTest.testExplainJsonExecute(
            getPolardbxConnection(), baseOneTableName, "select * from " + baseOneTableName + " where pk in (1,2,3)",
            true);

    }

    @Test
    public void testExplainExecutePhyTbPattern() throws Exception {
        if (!isMySQL80()) {
            return;
        }

        com.alibaba.polardbx.qatest.dql.sharding.explain.ExplianAnalyzeExecuteTest.testExplainExecutePhyTbPattern(
            getPolardbxConnection(), baseTwoTableName, "^" + baseTwoTableName + ".*", "explain execute ",
            "select * from " + baseTwoTableName + " where pk > 1", true);
        com.alibaba.polardbx.qatest.dql.sharding.explain.ExplianAnalyzeExecuteTest.testExplainExecutePhyTbPattern(
            getPolardbxConnection(), baseTwoTableName, "^" + baseTwoTableName + ".*", "explain analyze_execute ",
            "select * from " + baseTwoTableName + " where pk > 1", true);
        com.alibaba.polardbx.qatest.dql.sharding.explain.ExplianAnalyzeExecuteTest.testExplainExecutePhyTbPattern(
            getPolardbxConnection(), baseTwoTableName, "^" + baseTwoTableName + ".*", "explain json_execute ",
            "select * from " + baseTwoTableName + " where pk > 1", true);
        com.alibaba.polardbx.qatest.dql.sharding.explain.ExplianAnalyzeExecuteTest.testExplainExecutePhyTbPattern(
            getPolardbxConnection(), baseTwoTableName, "^" + baseTwoTableName + ".*", "explain tree_execute ",
            "select * from " + baseTwoTableName + " where pk > 1", true);

    }

}
