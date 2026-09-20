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

package com.alibaba.polardbx.qatest.dql.auto.explain;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.qatest.ReadBaseTestCase;
import com.alibaba.polardbx.qatest.data.ExecuteTableSelect;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.google.common.collect.ImmutableList;
import org.apache.commons.lang.StringUtils;
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

import static com.google.common.truth.Truth.assertThat;

public class ExplainExecuteTest extends ReadBaseTestCase {

    @Override
    public boolean usingNewPartDb() {
        return true;
    }

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
        String sql = "select * from " + baseOneTableName;
        assertThat(explainExecuteXplan(sql)).isFalse();
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainSelectWithPartitionFilterTest() {
        String sql = "select * from " + baseOneTableName + " where pk=1";
        assertThat(explainExecuteXplan(sql)).isEqualTo(useXproto(tddlConnection));
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainUpdateTest() {
        String sql = "update " + baseOneTableName + " set varchar_test='a'";
        assertThat(explainExecuteXplan(sql)).isFalse();
    }

    @Test
    public void explainJoinTest() {
        String sql = "select * from %s a join %s b on a.varchar_test = b.varchar_test and a.pk=1";
        assertThat(explainExecuteXplan(String.format(sql, baseOneTableName, baseTwoTableName))).isFalse();
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainUpdateWithPartitionFilterTest() {
        String sql = "update " + baseOneTableName + " set varchar_test='a' where pk=1";
        assertThat(explainExecuteXplan(sql)).isFalse();
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainDeleteTest() {
        String sql = "delete " + baseOneTableName;
        assertThat(explainExecuteXplan(sql)).isFalse();
    }

    /**
     * @since 5.3.8
     */
    @Test
    public void explainDeleteWithPartitionFilterTest() {
        String sql = "delete " + baseOneTableName + " where varchar_test='a'";
        assertThat(explainExecuteXplan(sql)).isFalse();
    }

    private boolean explainExecuteXplan(String sql) {
        final List<List<String>> res =
            JdbcUtil.getAllStringResult(JdbcUtil.executeQuery("explain execute " + sql, tddlConnection), false,
                ImmutableList.of());
        boolean useXplan = (!StringUtils.isEmpty(res.get(0).get(11))) && res.get(0).get(11).contains("Using XPlan");
        for (List<String> result : res) {
            assertThat(useXplan == ((!StringUtils.isEmpty(result.get(11))) && result.get(11).contains("Using XPlan")))
                .isTrue();
            if (useXplan) {
                assertThat(result.get(5)).contains(result.get(6));
                assertThat(result.get(6)).isNotEmpty();
                assertThat(Double.valueOf(result.get(10))).isAtMost(100D);
                assertThat(Double.valueOf(result.get(10))).isAtLeast(0D);
            }
        }
        return useXplan;
    }

    @Test
    public void explainExecuteAllPhyTbForSelect() throws Exception {
        String tableName = "explain_execute_all_phy_tb";
        int partitionNum = ThreadLocalRandom.current().nextInt(20, 50);
        String createTableSql = String.format(
            "create table %s (id int primary key auto_increment, col1 int, col2 int, col3 int, key idx_col2(col2)) partition by hash(col1) partitions "
                + partitionNum
            , tableName);
        com.alibaba.polardbx.qatest.dql.sharding.explain.ExplainExecuteTest.testExplainExecuteAllPhyTb(
            getPolardbxConnection(), tableName, createTableSql,
            String.format("select col2 from %s where col2 < 6 order by col2;", tableName), "idx_col2", true);
    }

    @Test
    public void explainExecuteAllPhyTbForSelect1() throws Exception {
        String tableName = "explain_execute_all_phy_tb";
        int partitionNum = ThreadLocalRandom.current().nextInt(20, 50);
        String createTableSql = String.format(
            "create table %s (id int primary key auto_increment, col1 int, col2 int, col3 int, key idx_col2(col2)) partition by hash(col1) partitions "
                + partitionNum
                + " SUBPARTITION BY RANGE(col3) (\n" +
                "  SUBPARTITION sp1 VALUES LESS THAN(1000),\n" +
                "  SUBPARTITION sp2 VALUES LESS THAN(5000),\n" +
                "  SUBPARTITION sp3 VALUES LESS THAN(10000),\n" +
                "  SUBPARTITION sp4 VALUES LESS THAN(MAXVALUE)\n" +
                ")"
            , tableName);
        com.alibaba.polardbx.qatest.dql.sharding.explain.ExplainExecuteTest.testExplainExecuteAllPhyTb(
            getPolardbxConnection(), tableName, createTableSql,
            String.format("select col2 from %s where col2 < 6 order by col2;", tableName), "idx_col2", true);
    }

    @Test
    public void explainExecuteAllPhyTbForUpdate() throws Exception {
        String tableName = "explain_execute_all_phy_tb";
        int partitionNum = ThreadLocalRandom.current().nextInt(20, 50);
        String createTableSql = String.format(
            "create table %s (id int primary key auto_increment, col1 int, col2 int, col3 int, key idx_col2(col2)) partition by hash(col1) partitions "
                + partitionNum
            , tableName);
        com.alibaba.polardbx.qatest.dql.sharding.explain.ExplainExecuteTest.testExplainExecuteAllPhyTb(
            getPolardbxConnection(), tableName, createTableSql,
            String.format("update %s set col1 = 1 where col2 < 6;", tableName), "idx_col2", true);
    }

    @Test
    public void explainExecuteAllPhyTbForDelete() throws Exception {
        String tableName = "explain_execute_all_phy_tb";
        int partitionNum = ThreadLocalRandom.current().nextInt(20, 50);
        String createTableSql = String.format(
            "create table %s (id int primary key auto_increment, col1 int, col2 int, col3 int, key idx_col2(col2)) partition by hash(col1) partitions "
                + partitionNum
            , tableName);
        com.alibaba.polardbx.qatest.dql.sharding.explain.ExplainExecuteTest.testExplainExecuteAllPhyTb(
            getPolardbxConnection(), tableName, createTableSql,
            String.format("delete %s  where col2 < 6;", tableName), "idx_col2", true);
    }

    @Test
    public void testExplainExecuteAllPhyTbWithPrunePartition() throws Exception {
        String tableName = "explain_execute_all_phy_tb";
        int partitionNum = ThreadLocalRandom.current().nextInt(20, 50);
        String createTableSql = String.format(
            "create table %s (id int primary key auto_increment, col1 int, col2 int, col3 int, key idx_col2(col2)) partition by hash(col2) partitions "
                + partitionNum
                + " SUBPARTITION BY RANGE(col3) (\n" +
                "  SUBPARTITION sp1 VALUES LESS THAN(1000),\n" +
                "  SUBPARTITION sp2 VALUES LESS THAN(5000),\n" +
                "  SUBPARTITION sp3 VALUES LESS THAN(10000),\n" +
                "  SUBPARTITION sp4 VALUES LESS THAN(MAXVALUE)\n" +
                ")"
            , tableName);
        JdbcUtil.dropTable(tddlConnection, tableName);
        JdbcUtil.executeSuccess(tddlConnection, createTableSql);

        List<String> sqlList = new ArrayList<>();
        sqlList.add(String.format("select * from %s where col2 = 1", tableName));
        sqlList.add(String.format("select * from %s where col2 in (1,2,3)", tableName));
        sqlList.add(String.format("select * from %s where col3 > 2000", tableName));
        sqlList.add(String.format("select * from %s where col3 < 6000 and col3 > 2000", tableName));
        sqlList.add(String.format("select * from %s where col3 < 6000 and col3 > 2000 and col2 = 1", tableName));
        sqlList.add(String.format("select * from %s where col3 < 6000 and col3 > 2000 and col2 in (1,2,3)", tableName));

        for (String sql : sqlList) {
            com.alibaba.polardbx.qatest.dql.sharding.explain.ExplainExecuteTest.testExplainExecuteAllPhyTbWithPrunePartition(
                tddlConnection, tableName, sql, true);
        }

        JdbcUtil.dropTable(tddlConnection, tableName);

    }

}
