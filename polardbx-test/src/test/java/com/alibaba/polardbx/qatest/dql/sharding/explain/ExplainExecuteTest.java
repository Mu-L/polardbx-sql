/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.qatest.dql.sharding.explain;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.qatest.CrudBasedLockTestCase;
import com.alibaba.polardbx.qatest.data.ColumnDataGenerator;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.data.TableColumnGenerator;
import com.alibaba.polardbx.qatest.entity.ColumnEntity;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

import static com.alibaba.polardbx.qatest.validator.DataOperator.executeErrorAssert;

/**
 * test explain execute xxx
 *
 * @author roy
 * @since 5.3.8
 */

public class ExplainExecuteTest extends CrudBasedLockTestCase {

    private static final Log log = LogFactory.getLog(ExplainExecuteTest.class);
    private ColumnDataGenerator columnDataGenerator = new ColumnDataGenerator();

    @Parameterized.Parameters(name = "{index}:table0={0},table1={1}")
    public static List<String[]> prepareDate() {
        return Arrays.asList(ExecuteTableSelect.selectOneTableMultiRuleMode());
    }

    public ExplainExecuteTest(String baseOneTableName, String baseTwoTableName) {
        this.baseOneTableName = baseOneTableName;
        this.baseTwoTableName = baseTwoTableName;
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainSelectTest() {
        String sql = "explain execute /*+ TDDL: MIN_MERGE_UNION_SIZE=1 */ select * from " + baseOneTableName;
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            int rowsize = 0;
            while (rs.next()) {
                String actualExplainResult = rs.getString("select_type");
                Assert.assertTrue(actualExplainResult != null && !actualExplainResult.equals(""));
                rowsize++;
            }
            Assert.assertTrue(rowsize == 1);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainSelectWithPartitionFilterTest() {
        String sql = "explain execute select * from " + baseOneTableName + " where pk=1";
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            int rowsize = 0;
            while (rs.next()) {
                String actualExplainResult = rs.getString("select_type");
                Assert.assertTrue(actualExplainResult != null && !actualExplainResult.equals(""));
                rowsize++;
            }
            Assert.assertTrue(rowsize == 1);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainUpdateTest() {
        String sql = "explain execute update " + baseOneTableName + " set varchar_test='a'";
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            int rowsize = 0;
            while (rs.next()) {
                String actualExplainResult = rs.getString("select_type");
                Assert.assertTrue(actualExplainResult != null && !actualExplainResult.equals(""));
                String extra = rs.getString("Extra");
                Assert.assertTrue(extra == null || !extra.contains("XPlan"));
                rowsize++;
            }
            Assert.assertTrue(rowsize == 1);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    @Test
    public void explainJoinTest() {
        String sql = "explain execute select * from %s a join %s b on a.varchar_test = b.varchar_test and a.pk=1";
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(String.format(sql, baseOneTableName, baseTwoTableName));
            int rowsize = 0;
            while (rs.next()) {
                String actualExplainResult = rs.getString("select_type");
                Assert.assertTrue(actualExplainResult != null && !actualExplainResult.equals(""));
                String extra = rs.getString("Extra");
                Assert.assertTrue(extra == null || !extra.contains("XPlan"));
                rowsize++;
            }
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainUpdateWithPartitionFilterTest() {
        String sql = "explain execute update " + baseOneTableName + " set varchar_test='a' where pk=1";
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            int rowsize = 0;
            while (rs.next()) {
                String actualExplainResult = rs.getString("select_type");
                Assert.assertTrue(actualExplainResult != null && !actualExplainResult.equals(""));
                String extra = rs.getString("Extra");
                Assert.assertTrue(extra == null || !extra.contains("XPlan"));
                rowsize++;
            }
            Assert.assertTrue(rowsize == 1);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainDeleteTest() {
        String sql = "explain execute delete " + baseOneTableName;
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            int rowsize = 0;
            while (rs.next()) {
                String actualExplainResult = rs.getString("select_type");
                Assert.assertTrue(actualExplainResult != null && !actualExplainResult.equals(""));
                String extra = rs.getString("Extra");
                Assert.assertTrue(extra == null || !extra.contains("XPlan"));
                rowsize++;
            }
            Assert.assertTrue(rowsize == 1);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainDeleteWithPartitionFilterTest() {
        String sql = "explain execute delete " + baseOneTableName + " where varchar_test='a'";
        try {
            Statement statement = tddlConnection.createStatement();
            ResultSet rs = statement.executeQuery(sql);
            int rowsize = 0;
            while (rs.next()) {
                String actualExplainResult = rs.getString("select_type");
                Assert.assertTrue(actualExplainResult != null && !actualExplainResult.equals(""));
                String extra = rs.getString("Extra");
                Assert.assertTrue(extra == null || !extra.contains("XPlan"));
                rowsize++;
            }
            Assert.assertTrue(rowsize == 1);
        } catch (Exception e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainInsertTest() {
        List<ColumnEntity> columns = TableColumnGenerator.getAllTypeColum();
        String sql = "explain execute insert into " + baseOneTableName + " (";
        String values = " values ( ";

        for (int j = 0; j < columns.size(); j++) {
            String columnName = columns.get(j).getName();
            sql = sql + columnName + ",";
            values = values + " ?,";
        }

        sql = sql.substring(0, sql.length() - 1) + ") ";
        values = values.substring(0, values.length() - 1) + ")";

        sql = sql + values;
        List<Object> param = columnDataGenerator.getAllColumnValue(columns, PK_COLUMN_NAME, 1);

        executeErrorAssert(tddlConnection, sql, param, "not support");
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainDDLTest() {
        String sql = "explain execute CREATE TABLE `REGION` (\n" + "  `R_REGIONKEY` decimal(11,0) NOT NULL,\n"
            + "  `R_NAME` char(25) DEFAULT NULL,\n" + "  `R_COMMENT` varchar(152) DEFAULT NULL,\n"
            + "  PRIMARY KEY (`R_REGIONKEY`)\n" + ") ENGINE=InnoDB DEFAULT CHARSET=latin1";

        executeErrorAssert(tddlConnection, sql, null, "not support");
    }

    /**
     * 只适用于下发全分片的sql
     */
    public static void testExplainExecuteAllPhyTb(Connection connection, String tableName, String createTableSql,
                                                  String sql, String dropIndexName, boolean isNewPart)
        throws Exception {

        JdbcUtil.executeSuccess(connection, "set ENABLE_FORBID_PUSH_DML_WITH_HINT=false;");
        JdbcUtil.executeSuccess(connection, "set ENABLE_EXTERNAL_CONSISTENCY_FOR_WRITE_TRX=false;");

        JdbcUtil.dropTable(connection, tableName);
        JdbcUtil.executeSuccess(connection, createTableSql);

        String showTopologySql = "show topology from " + tableName;
        ResultSet topologyRs = JdbcUtil.executeQuery(showTopologySql, connection);

        String groupName = null;
        String phyTableName = null;
        String realPhyTableName = null;
        Set<String> allPhyTableNames = new HashSet<>();
        while (topologyRs.next()) {
            groupName = topologyRs.getString("GROUP_NAME");
            realPhyTableName = topologyRs.getString("TABLE_NAME");
            if (!isNewPart) {
                phyTableName = groupName + "." + realPhyTableName;
            } else {
                phyTableName = realPhyTableName;
            }
            allPhyTableNames.add(phyTableName);
        }

        //find SHARD_COUNT
//        String explainShadingSql = "explain sharding " + sql;
//        ResultSet explainShadingRs = JdbcUtil.executeQuery(explainShadingSql, connection);
//        Assert.assertTrue(explainShadingRs.next());
//        phyTableCount = explainShadingRs.getInt("SHARD_COUNT");

        //1. EXPLAIN_EXECUTE_ALL_PHYTB
        String explainExecuteAllPhyTb = "/*+TDDL:EXPLAIN_EXECUTE_PHYTB_LEVEL=0*/" + "explain execute " + sql;

        ResultSet explainExecuteAllPhyTbRs = JdbcUtil.executeQuery(explainExecuteAllPhyTb, connection);
        String extra;
        while (explainExecuteAllPhyTbRs.next()) {
            extra = explainExecuteAllPhyTbRs.getString("Extra");
            if (allPhyTableNames.contains(explainExecuteAllPhyTbRs.getString("table")) || tableName.equalsIgnoreCase(explainExecuteAllPhyTbRs.getString("table"))) {
                //扫描所有物理表，现在rows列的最大和最小值
                Assert.assertTrue(extra.contains("Scan rows"));
            }
        }

        //删除某个表的local索引
        String dropLocalIndex =
            String.format("/*+TDDL:node('%s')*/", groupName) + String.format(
                "alter table %s drop index " + dropIndexName,
                realPhyTableName);
        JdbcUtil.executeSuccess(connection, dropLocalIndex);

        explainExecuteAllPhyTbRs = JdbcUtil.executeQuery(explainExecuteAllPhyTb, connection);
        String key = null;
        while (explainExecuteAllPhyTbRs.next()) {
            extra = explainExecuteAllPhyTbRs.getString("Extra");
            if (allPhyTableNames.contains(explainExecuteAllPhyTbRs.getString("table")) || tableName.equalsIgnoreCase(explainExecuteAllPhyTbRs.getString("table"))) {
                //dn执行计划不一样时，显示物理表名
                Assert.assertTrue(extra.contains(realPhyTableName));
                Assert.assertTrue(extra.contains("Scan rows"));
                key = explainExecuteAllPhyTbRs.getString("key");
                break;
            }
        }

        //2. EXPLAIN_EXECUTE_SHOW_DIFF_PHYTB
        //String explainExecuteShowDiffPhyTb = "/*+TDDL:EXPLAIN_EXECUTE_PHYTB_LEVEL=1*/" + "explain execute " + sql;
        String explainExecuteShowDiffPhyTb = "explain diff_execute " + sql;
        ResultSet explainExecuteShowDiffPhyTbRs = JdbcUtil.executeQuery(explainExecuteShowDiffPhyTb, connection);
        while (explainExecuteShowDiffPhyTbRs.next()) {
            String table = explainExecuteShowDiffPhyTbRs.getString("table");
            if (Objects.equals(table, phyTableName)) {
                Assert.assertTrue(!Objects.equals(explainExecuteShowDiffPhyTbRs.getString("key"), key));
            } else if (allPhyTableNames.contains(table)) {
                Assert.assertTrue(Objects.equals(explainExecuteShowDiffPhyTbRs.getString("key"), key));
            }
        }

        //3. EXPLAIN_EXECUTE_SHOW_ALL_PHYTB
        Set<String> showAllPhyTbNames = new HashSet<>();
        //String explainExecuteShowAllPhyTb = "/*+TDDL:EXPLAIN_EXECUTE_PHYTB_LEVEL=2*/" + "explain execute " + sql;
        String explainExecuteShowAllPhyTb = "explain all_execute " + sql;
        ResultSet explainExecuteShowAllPhyTbRs = JdbcUtil.executeQuery(explainExecuteShowAllPhyTb, connection);

        while (explainExecuteShowAllPhyTbRs.next()) {
            if (allPhyTableNames.contains(explainExecuteShowAllPhyTbRs.getString("table"))) {
                showAllPhyTbNames.add(explainExecuteShowAllPhyTbRs.getString("table"));
                if (explainExecuteShowAllPhyTbRs.getString("table").equalsIgnoreCase(phyTableName)) {
                    Assert.assertTrue(!Objects.equals(explainExecuteShowAllPhyTbRs.getString("key"), key));
                }
            }
        }

        Assert.assertTrue(Objects.equals(showAllPhyTbNames, allPhyTableNames));

        JdbcUtil.dropTable(connection, tableName);
        JdbcUtil.executeSuccess(connection, "set ENABLE_FORBID_PUSH_DML_WITH_HINT=true;");
        JdbcUtil.executeSuccess(connection, "set ENABLE_EXTERNAL_CONSISTENCY_FOR_WRITE_TRX=true;");
    }

    public static void testExplainExecuteAllPhyTbWithPrunePartition(Connection connection, String tableName,
                                                                    String sql, boolean isNewPart) throws Exception {

        String explainShadingSql = "explain sharding " + sql;
        ResultSet explainShadingRs = JdbcUtil.executeQuery(explainShadingSql, connection);
        Assert.assertTrue(explainShadingRs.next());
        int phyTableCount = explainShadingRs.getInt("SHARD_COUNT");

        String showTopologySql = "show topology from " + tableName;
        ResultSet topologyRs = JdbcUtil.executeQuery(showTopologySql, connection);

        String groupName = null;
        String phyTableName = null;
        String realPhyTableName = null;
        Set<String> allPhyTableNames = new HashSet<>();
        while (topologyRs.next()) {
            groupName = topologyRs.getString("GROUP_NAME");
            realPhyTableName = topologyRs.getString("TABLE_NAME");
            if (!isNewPart) {
                phyTableName = groupName + "." + realPhyTableName;
            } else {
                phyTableName = realPhyTableName;
            }
            allPhyTableNames.add(phyTableName);
        }

        //EXPLAIN_EXECUTE_SHOW_ALL_PHYTB
        Set<String> showAllPhyTbNames = new HashSet<>();
        String explainExecuteShowAllPhyTb = "/*+TDDL:EXPLAIN_EXECUTE_PHYTB_LEVEL=2*/" + "explain execute " + sql;
        ResultSet explainExecuteShowAllPhyTbRs = JdbcUtil.executeQuery(explainExecuteShowAllPhyTb, connection);

        while (explainExecuteShowAllPhyTbRs.next()) {
            if (allPhyTableNames.contains(explainExecuteShowAllPhyTbRs.getString("table"))
                || tableName.equalsIgnoreCase(explainExecuteShowAllPhyTbRs.getString("table"))) {
                showAllPhyTbNames.add(explainExecuteShowAllPhyTbRs.getString("table"));
            }
        }
        Assert.assertTrue(showAllPhyTbNames.size() == phyTableCount);
    }

    @Test
    public void explainExecuteAllPhyTb() throws Exception {
        String tableName = "explain_execute_all_phy_tb";
        int partitionNum = ThreadLocalRandom.current().nextInt(10, 25);
        String createTableSql = String.format(
            "create table %s (id int primary key auto_increment, col1 int, col2 int, col3 int, key idx_col2(col2)) dbpartition by hash(col1) tbpartition by hash(col1) tbpartitions "
                + partitionNum
            , tableName);
        testExplainExecuteAllPhyTb(getPolardbxConnection(), tableName, createTableSql,
            String.format("select col2 from %s where col2 < 6 order by col2;", tableName), "idx_col2", false);
    }

    @Test
    public void explainExecuteAllPhyTbForUpdate() throws Exception {
        String tableName = "explain_execute_all_phy_tb";
        int partitionNum = ThreadLocalRandom.current().nextInt(20, 50);
        String createTableSql = String.format(
            "create table %s (id int primary key auto_increment, col1 int, col2 int, col3 int, key idx_col2(col2)) dbpartition by hash(col1) tbpartition by hash(col1) tbpartitions "
                + partitionNum
            , tableName);
        com.alibaba.polardbx.qatest.dql.sharding.explain.ExplainExecuteTest.testExplainExecuteAllPhyTb(
            getPolardbxConnection(), tableName, createTableSql,
            String.format("update %s set col1 = 1 where col2 < 6;", tableName), "idx_col2", false);
    }

    @Test
    public void explainExecuteAllPhyTbForDelete() throws Exception {
        String tableName = "explain_execute_all_phy_tb";
        int partitionNum = ThreadLocalRandom.current().nextInt(20, 50);
        String createTableSql = String.format(
            "create table %s (id int primary key auto_increment, col1 int, col2 int, col3 int, key idx_col2(col2)) dbpartition by hash(col1) tbpartition by hash(col1) tbpartitions "
                + partitionNum
            , tableName);
        com.alibaba.polardbx.qatest.dql.sharding.explain.ExplainExecuteTest.testExplainExecuteAllPhyTb(
            getPolardbxConnection(), tableName, createTableSql,
            String.format("delete %s  where col2 < 6;", tableName), "idx_col2", false);
    }

    @Test
    public void testExplainExecuteAllPhyTbWithPrunePartition() throws Exception {
        String tableName = "explain_execute_all_phy_tb";
        int partitionNum = ThreadLocalRandom.current().nextInt(20, 50);
        String createTableSql = String.format(
            "create table %s (id int primary key auto_increment, col1 int, col2 int, col3 int, key idx_col2(col2)) dbpartition by hash(col1) tbpartition by hash(col2) tbpartitions "
                + partitionNum
            , tableName);
        JdbcUtil.dropTable(tddlConnection, tableName);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);

        List<String> sqlList = new ArrayList<>();
        sqlList.add(String.format("select * from %s where col1 = 1", tableName));
        sqlList.add(String.format("select * from %s where col1 in (1,2,3)", tableName));
        sqlList.add(String.format("select * from %s where col2 = 1", tableName));
        sqlList.add(String.format("select * from %s where col2 in (1,2,3)", tableName));
        sqlList.add(String.format("select * from %s where col1 = 1 and col2 in (1,2,3)", tableName));
        sqlList.add(String.format("select * from %s where col1 = 1 and col2 = 1", tableName));
        sqlList.add(String.format("select * from %s where col1 in (1,2,3) and col2 in (1,2,3)", tableName));

        for (String sql : sqlList) {
            testExplainExecuteAllPhyTbWithPrunePartition(tddlConnection, tableName, sql, false);
        }

        JdbcUtil.dropTable(tddlConnection, tableName);

    }

    @Test
    public void testExplainExecuteAllPhyTbWithPrunePartition1() throws Exception {
        String tableName = "explain_execute_all_phy_tb";
        int partitionNum = ThreadLocalRandom.current().nextInt(20, 50);
        String createTableSql = String.format(
            "create table %s (id int primary key auto_increment, col1 int, col2 int, col3 int, key idx_col2(col2)) dbpartition by hash(col1) tbpartition by hash(col1) tbpartitions "
                + partitionNum
            , tableName);
        JdbcUtil.dropTable(tddlConnection, tableName);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);

        List<String> sqlList = new ArrayList<>();
        sqlList.add(String.format("select * from %s where col1 = 1", tableName));
        sqlList.add(String.format("select * from %s where col1 in (1,2,3)", tableName));
        sqlList.add(String.format("select * from %s where col2 = 1", tableName));
        sqlList.add(String.format("select * from %s where col2 in (1,2,3)", tableName));
        sqlList.add(String.format("select * from %s where col1 = 1 and col2 in (1,2,3)", tableName));
        sqlList.add(String.format("select * from %s where col1 = 1 and col2 = 1", tableName));
        sqlList.add(String.format("select * from %s where col1 in (1,2,3) and col2 in (1,2,3)", tableName));

        for (String sql : sqlList) {
            testExplainExecuteAllPhyTbWithPrunePartition(tddlConnection, tableName, sql, false);
        }

        JdbcUtil.dropTable(tddlConnection, tableName);

    }

}
