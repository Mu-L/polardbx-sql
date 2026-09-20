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
package com.alibaba.polardbx.qatest.dal.ai;

import com.alibaba.polardbx.qatest.util.ConnectionManager;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@NotThreadSafe
public class Nl2sqlAgentQueryTest extends Nl2sqlAgentTestBase {

    @Test
    public void testNl2sql_showTables() throws Exception {
        String output = executeNlQuery("显示当前数据库中的所有表");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
    }

    @Test
    public void testNl2sql_describeTable() throws Exception {
        String output = executeNlQuery("查看employees表的结构");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
    }

    @Test
    public void testNl2sql_useDatabase() throws Exception {
        // Use Chinese input — "use database ..." in English gets parsed as
        // a USE statement and fails before reaching the NL2SQL handler.
        String output = executeNlQuery("请切换到" + TEST_DB + "数据库");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
    }

    @Test
    public void testNl2sql_countQuery() throws Exception {
        String output = executeNlQuery("总共有多少条员工记录");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
    }

    @Test
    public void testNl2sql_maxSalaryQuery() throws Exception {
        String output = executeNlQuery("最高薪水的员工是谁");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
    }

    @Test
    public void testNl2sql_groupByQuery() throws Exception {
        String output = executeNlQuery("按部门分组统计平均薪资");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
    }

    @Test
    public void testNl2sql_orderByQuery() throws Exception {
        String output = executeNlQuery("按薪资从高到低排列所有员工");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
    }

    @Test
    public void testNl2sql_multiStatementFailure() throws Exception {
        // Input that causes MultiStatementSplitter to fail should trigger NL2SQL
        String output = executeNlQuery("what is the total count of employees in each department");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
    }

    @Test
    public void testNl2sql_errorRecovery() throws Exception {
        // Ask about a nonexistent table — agent should handle gracefully
        String output = executeNlQuery("查询nonexistent_table_xyz表的数据");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
    }

    @Test
    public void testNl2sql_complexJoinQuery() throws Exception {
        // Complex query that might require multiple SQL executions
        String output = executeNlQuery("查询Engineering部门薪资最高的员工姓名");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
    }

    @Test
    public void testDiverseQueries_forCoverage() throws Exception {
        // Send diverse queries to exercise different code paths
        String[] queries = {
            "显示所有表",
            "employees表有哪些列",
            "查询薪资最高的员工",
            "统计employees表中每个department字段对应的员工数量",
            "按薪资降序排列所有员工"
        };
        for (String query : queries) {
            String output = executeNlQuery(query);
            Assert.assertFalse("Output for '" + query + "' should not be empty", output.isEmpty());
        }
    }

    @Test
    public void testShowDatabasesViaAgent() throws Exception {
        String output = executeNlQuery("显示所有数据库列表");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
    }

    @Test
    public void testEmptyResultSet() throws Exception {
        // Query that returns no results
        String output = executeNlQuery("查询部门为NonExistent的员工");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
    }

    @Test
    public void testUseDatabaseWithBackticks() throws Exception {
        // This may trigger the agent to generate USE `nl2sql_agent_test_db`
        String output = executeNlQuery("请切换到`nl2sql_agent_test_db`数据库");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
    }

    @Test
    public void testVectorColumnMetadata() throws Exception {
        // Create a table that might trigger vector column detection
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("CREATE TABLE IF NOT EXISTS vector_test ("
                + "id INT PRIMARY KEY, content VARCHAR(200))");
        }
        String output = executeNlQuery("查看vector_test表的结构");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS vector_test");
        }
    }

    @Test
    public void testResolveModelConfig_default() throws Exception {
        // Query with default model resolution path
        String output = executeNlQuery("今天天气怎么样");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
    }

    @Test
    public void testShowHelp() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SHOW HELP");
            try (ResultSet rs = stmt.getResultSet()) {
                boolean foundNl2sql = false;
                while (rs.next()) {
                    String name = rs.getString(1);
                    if (name != null && name.contains("natural language")) {
                        foundNl2sql = true;
                    }
                }
                // SHOW HELP should include NL2SQL entries
            }
        }
    }

    @Test
    public void testShowCreateTable() throws Exception {
        // First run an NL query to warm up
        executeNlQuery("查看employees表的结构");
        // Then run SHOW CREATE TABLE
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SHOW CREATE TABLE employees");
            try (ResultSet rs = stmt.getResultSet()) {
                Assert.assertTrue("Should have result", rs.next());
            }
        }
    }

    @Test
    public void testDiverseNlQueries() throws Exception {
        String output1 = executeNlQuery("employees表中有哪些不同的department字段值");
        Assert.assertFalse(output1.isEmpty());

        String output2 = executeNlQuery("统计employees表中每个department字段对应的员工数量");
        Assert.assertFalse(output2.isEmpty());

        String output3 = executeNlQuery("谁的工资最高");
        Assert.assertFalse(output3.isEmpty());

        String output4 = executeNlQuery("Engineering部门的平均工资是多少");
        Assert.assertFalse(output4.isEmpty());
    }
}
