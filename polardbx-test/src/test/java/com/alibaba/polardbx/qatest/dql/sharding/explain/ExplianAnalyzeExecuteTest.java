package com.alibaba.polardbx.qatest.dql.sharding.explain;

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
import java.util.*;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class ExplianAnalyzeExecuteTest extends ReadBaseTestCase {

    public ExplianAnalyzeExecuteTest(String baseOneTableName, String baseTwoTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
    }

    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableSelect.selectOneTableMultiRuleMode());
    }

    @Test
    public void testExplainAnalyzeExecute() throws Exception {
        if (!isMySQL80()) {
            return;
        }

        testExplainAnalyzeExecute(getPolardbxConnection(), baseOneTableName,
            "select * from " + baseOneTableName + " where pk > 1", false);
        testExplainAnalyzeExecute(getPolardbxConnection(), baseOneTableName,
            "select * from " + baseOneTableName + " where pk = 1", false);
        testExplainAnalyzeExecute(getPolardbxConnection(), baseOneTableName,
            "select * from " + baseOneTableName + " where pk in (1,2,3)", false);
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
            return;
        }

        testExplainTreeExecute(getPolardbxConnection(), baseOneTableName,
            "select * from " + baseOneTableName + " where pk > 1", false);
        testExplainTreeExecute(getPolardbxConnection(), baseOneTableName,
            "select * from " + baseOneTableName + " where pk = 1", false);
        testExplainTreeExecute(getPolardbxConnection(), baseOneTableName,
            "select * from " + baseOneTableName + " where pk in (1,2,3)", false);
    }

    @Test
    public void testExplainJsonExecute() throws Exception {
        if (!isMySQL80()) {
            return;
        }

        testExplainJsonExecute(getPolardbxConnection(), baseOneTableName,
            "select * from " + baseOneTableName + " where pk > 1", false);
        testExplainJsonExecute(getPolardbxConnection(), baseOneTableName,
            "select * from " + baseOneTableName + " where pk = 1", false);
        testExplainJsonExecute(getPolardbxConnection(), baseOneTableName,
            "select * from " + baseOneTableName + " where pk in (1,2,3)", false);
    }

    @Test
    public void testExplainExecutePhyTbPattern() throws Exception {
        if (!isMySQL80()) {
            return;
        }

        testExplainExecutePhyTbPattern(getPolardbxConnection(), baseTwoTableName, "^" + baseTwoTableName + ".*",
            "explain execute ",
            "select * from " + baseTwoTableName + " where pk > 1", false);
        testExplainExecutePhyTbPattern(getPolardbxConnection(), baseTwoTableName, "^" + baseTwoTableName + ".*",
            "explain analyze_execute ",
            "select * from " + baseTwoTableName + " where pk > 1", false);
        testExplainExecutePhyTbPattern(getPolardbxConnection(), baseTwoTableName, "^" + baseTwoTableName + ".*",
            "explain json_execute ",
            "select * from " + baseTwoTableName + " where pk > 1", false);
        testExplainExecutePhyTbPattern(getPolardbxConnection(), baseTwoTableName, "^" + baseTwoTableName + ".*",
            "explain tree_execute ",
            "select * from " + baseTwoTableName + " where pk > 1", false);

    }

    public static List<String> removeBracket(String showPhyTableName) {
        List<String> allPhyTableNames = new ArrayList<>();
        showPhyTableName = showPhyTableName.trim();
        if (showPhyTableName.startsWith("[") && showPhyTableName.endsWith("]")) {
            showPhyTableName = showPhyTableName.substring(1, showPhyTableName.length() - 1);
        }
        String[] phyTableNames = showPhyTableName.split(",");
        for (String phyTableName : phyTableNames) {
            if (phyTableName.startsWith("[") && phyTableName.endsWith("]")) {
                phyTableName = phyTableName.substring(1, phyTableName.length() - 1);
            }
            for (String name : phyTableName.split(",")) {
                allPhyTableNames.add(name.trim());
            }
        }
        return allPhyTableNames;
    }

    public static void testExplainAnalyzeExecute(Connection tddlConnection, String tableName, String sql,
                                                 boolean isNewPart) throws Exception {
        if (!isMySQL80()) {
            System.out.println("dn is not 8.0");
            return;
        }

        String explainShadingSql = "explain sharding " + sql;
        ResultSet explainShadingRs = JdbcUtil.executeQuery(explainShadingSql, tddlConnection);
        com.alibaba.polardbx.common.utils.Assert.assertTrue(explainShadingRs.next());
        int phyTableCount = explainShadingRs.getInt("SHARD_COUNT");

        String showTopologySql = "show topology from " + tableName;
        ResultSet topologyRs = JdbcUtil.executeQuery(showTopologySql, tddlConnection);
        String groupName = null;
        String phyTableName = null;
        Set<String> phyTableNames = new HashSet<>();
        while (topologyRs.next()) {
            groupName = topologyRs.getString("GROUP_NAME");
            phyTableName = topologyRs.getString("TABLE_NAME");
            if (isNewPart) {
                phyTableNames.add(phyTableName);
            } else {
                phyTableNames.add(groupName + "." + phyTableName);
            }
        }

        String explainSql = "explain analyze execute " + sql;
        List<List<Object>> res = JdbcUtil.getAllResult(JdbcUtil.executeQuery(explainSql, tddlConnection));
        Assert.assertEquals(1, res.size());
        List<Object> row = res.get(0);
        Assert.assertEquals(2, row.size());
        Assert.assertTrue(phyTableNames.containsAll(removeBracket(row.get(0).toString())));
        Assert.assertTrue(row.get(1).toString().contains("actual time"));
        Assert.assertTrue(row.get(1).toString().contains("rows"));
        Assert.assertTrue(row.get(1).toString().contains("loops"));

        List<List<Object>> showAllPhyTbRes = JdbcUtil.getAllResult(
            JdbcUtil.executeQuery("/*+TDDL:EXPLAIN_EXECUTE_PHYTB_LEVEL=2*/" + explainSql, tddlConnection));

        Set<String> showPhyTableNames = new HashSet<>();
        for (List<Object> phyTableRow : showAllPhyTbRes) {
            Assert.assertEquals(2, phyTableRow.size());
            List<String> showPhyTableName = removeBracket(phyTableRow.get(0).toString());
            Assert.assertTrue(phyTableNames.containsAll(showPhyTableName));
            showPhyTableNames.addAll(showPhyTableName);
            Assert.assertTrue(phyTableRow.get(1).toString().contains("actual time"));
            Assert.assertTrue(phyTableRow.get(1).toString().contains("rows"));
            Assert.assertTrue(phyTableRow.get(1).toString().contains("loops"));
        }

        Assert.assertEquals(phyTableCount, showPhyTableNames.size());
    }

    public static void testExplainTreeExecute(Connection tddlConnection, String tableName, String sql,
                                              boolean isNewPart) throws Exception {
        if (!isMySQL80()) {
            System.out.println("dn is not 8.0");
            return;
        }

        String explainShadingSql = "explain sharding " + sql;
        ResultSet explainShadingRs = JdbcUtil.executeQuery(explainShadingSql, tddlConnection);
        com.alibaba.polardbx.common.utils.Assert.assertTrue(explainShadingRs.next());
        int phyTableCount = explainShadingRs.getInt("SHARD_COUNT");

        String showTopologySql = "show topology from " + tableName;
        ResultSet topologyRs = JdbcUtil.executeQuery(showTopologySql, tddlConnection);
        String groupName = null;
        String phyTableName = null;
        Set<String> phyTableNames = new HashSet<>();
        while (topologyRs.next()) {
            groupName = topologyRs.getString("GROUP_NAME");
            phyTableName = topologyRs.getString("TABLE_NAME");
            if (isNewPart) {
                phyTableNames.add(phyTableName);
            } else {
                phyTableNames.add(groupName + "." + phyTableName);
            }
        }

        String explainSql = "explain tree_execute " + sql;
        List<List<Object>> res = JdbcUtil.getAllResult(JdbcUtil.executeQuery(explainSql, tddlConnection));
        Assert.assertEquals(1, res.size());
        List<Object> row = res.get(0);
        Assert.assertEquals(2, row.size());
        Assert.assertTrue(phyTableNames.containsAll(removeBracket(row.get(0).toString())));
        Assert.assertTrue(row.get(1).toString().contains("rows"));
        Assert.assertTrue(row.get(1).toString().contains("cost"));

        List<List<Object>> showAllPhyTbRes = JdbcUtil.getAllResult(
            JdbcUtil.executeQuery("/*+TDDL:EXPLAIN_EXECUTE_PHYTB_LEVEL=2*/" + explainSql, tddlConnection));

        Set<String> showPhyTableNames = new HashSet<>();
        for (List<Object> phyTableRow : showAllPhyTbRes) {
            Assert.assertEquals(2, phyTableRow.size());
            List<String> showPhyTableName = removeBracket(phyTableRow.get(0).toString());
            Assert.assertTrue(phyTableNames.containsAll(showPhyTableName));
            showPhyTableNames.addAll(showPhyTableName);
            Assert.assertTrue(phyTableRow.get(1).toString().contains("rows"));
            Assert.assertTrue(phyTableRow.get(1).toString().contains("cost"));
        }

        Assert.assertEquals(phyTableCount, showPhyTableNames.size());
    }

    public static void testExplainJsonExecute(Connection tddlConnection, String tableName, String sql,
                                              boolean isNewPart) throws Exception {
        if (!isMySQL80()) {
            System.out.println("dn is not 8.0");
            return;
        }

        String explainShadingSql = "explain sharding " + sql;
        ResultSet explainShadingRs = JdbcUtil.executeQuery(explainShadingSql, tddlConnection);
        com.alibaba.polardbx.common.utils.Assert.assertTrue(explainShadingRs.next());
        int phyTableCount = explainShadingRs.getInt("SHARD_COUNT");

        String showTopologySql = "show topology from " + tableName;
        ResultSet topologyRs = JdbcUtil.executeQuery(showTopologySql, tddlConnection);
        String groupName = null;
        String phyTableName = null;
        Set<String> phyTableNames = new HashSet<>();
        while (topologyRs.next()) {
            groupName = topologyRs.getString("GROUP_NAME");
            phyTableName = topologyRs.getString("TABLE_NAME");
            if (isNewPart) {
                phyTableNames.add(phyTableName);
            } else {
                phyTableNames.add(groupName + "." + phyTableName);
            }
        }

        String explainSql = "explain json_execute " + sql;
        List<List<Object>> res = JdbcUtil.getAllResult(JdbcUtil.executeQuery(explainSql, tddlConnection));
        Assert.assertEquals(1, res.size());
        List<Object> row = res.get(0);
        Assert.assertEquals(2, row.size());
        Assert.assertTrue(phyTableNames.containsAll(removeBracket(row.get(0).toString())));
        Assert.assertTrue(row.get(1).toString().contains("query_block"));
        Assert.assertTrue(row.get(1).toString().contains("table"));

        List<List<Object>> showAllPhyTbRes = JdbcUtil.getAllResult(
            JdbcUtil.executeQuery("/*+TDDL:EXPLAIN_EXECUTE_PHYTB_LEVEL=2*/" + explainSql, tddlConnection));

        Set<String> showPhyTableNames = new HashSet<>();
        for (List<Object> phyTableRow : showAllPhyTbRes) {
            Assert.assertEquals(2, phyTableRow.size());
            List<String> showPhyTableName = removeBracket(phyTableRow.get(0).toString());
            Assert.assertTrue(phyTableNames.containsAll(showPhyTableName));
            showPhyTableNames.addAll(showPhyTableName);
            Assert.assertTrue(phyTableRow.get(1).toString().contains("query_block"));
            Assert.assertTrue(phyTableRow.get(1).toString().contains("table"));
        }

        Assert.assertEquals(phyTableCount, showPhyTableNames.size());
    }

    public static void testExplainExecutePhyTbPattern(Connection tddlConnection, String tableName, String patternStr,
                                                      String explain, String sql,
                                                      boolean isNewPart) throws Exception {
        if (!isMySQL80()) {
            System.out.println("dn is not 8.0");
            return;
        }

        List<Pattern> patternList =
            Arrays.stream(patternStr.split(",")).map(Pattern::compile).collect(Collectors.toList());

        String showTopologySql = "show topology from " + tableName;
        ResultSet topologyRs = JdbcUtil.executeQuery(showTopologySql, tddlConnection);
        Set<String> phyTableNames = new HashSet<>();
        while (topologyRs.next()) {
            final String groupName = topologyRs.getString("GROUP_NAME");
            final String phyTableName = topologyRs.getString("TABLE_NAME");
            if (patternList.stream().noneMatch(p -> p.matcher(phyTableName).matches())) {
                continue;
            }
            if (isNewPart) {
                phyTableNames.add(phyTableName);
            } else {
                phyTableNames.add(groupName + "." + phyTableName);
            }
        }

        String explainSql = explain + " " + sql;
        List<List<Object>> showAllPhyTbRes = JdbcUtil.getAllResult(
            JdbcUtil.executeQuery("/*+TDDL:EXPLAIN_EXECUTE_PHYTB_PATTERN='" + patternStr + "'*/" + explainSql,
                tddlConnection));

        for (List<Object> phyTableRow : showAllPhyTbRes) {
            List<String> showPhyTableName = null;
            if (phyTableRow.size() != 2) {
                showPhyTableName = removeBracket(phyTableRow.get(2).toString());
            } else {
                showPhyTableName = removeBracket(phyTableRow.get(0).toString());
            }
            Assert.assertTrue(phyTableNames.containsAll(showPhyTableName));
        }
    }

}
