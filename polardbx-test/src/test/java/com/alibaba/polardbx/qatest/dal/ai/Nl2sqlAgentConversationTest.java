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
public class Nl2sqlAgentConversationTest extends Nl2sqlAgentTestBase {

    @Test
    public void testClearContext() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            // CLEAR CONTEXT should succeed without error
            stmt.execute("CLEAR CONTEXT");
        }
    }

    @Test
    public void testClearContext_afterConversation() throws Exception {
        // Have a conversation first
        executeNlQuery("查询所有员工");
        // Clear context
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("CLEAR CONTEXT");
        }
        // Should be able to start a new conversation
        String output = executeNlQuery("employees表中有多少个不同部门");
        Assert.assertFalse("Output should not be empty after CLEAR CONTEXT", output.isEmpty());
    }

    @Test
    public void testMultiTurnConversation() throws Exception {
        // First query
        String output1 = executeNlQuery("查询Engineering部门的员工");
        Assert.assertFalse("First query output should not be empty", output1.isEmpty());

        // Follow-up query (uses context from first)
        String output2 = executeNlQuery("他们的平均薪资是多少");
        Assert.assertFalse("Follow-up query output should not be empty", output2.isEmpty());
    }

    @Test
    public void testConversationWithClear() throws Exception {
        // First conversation
        String output1 = executeNlQuery("查询所有员工");
        Assert.assertFalse(output1.isEmpty());

        // Clear context
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("CLEAR CONTEXT");
        }

        // New conversation — should not reference previous context
        String output2 = executeNlQuery("查询employees表中的所有不同部门");
        Assert.assertFalse(output2.isEmpty());
    }

    @Test
    public void testLongConversation_multipleQueries() throws Exception {
        // Send multiple queries to build up context
        for (int i = 0; i < 5; i++) {
            String output = executeNlQuery("查询所有员工信息");
            Assert.assertFalse("Query " + i + " output should not be empty", output.isEmpty());
        }
    }

    @Test
    public void testSqlRecordingAfterNl2sql() throws Exception {
        // Run an NL2SQL query first to create the agentSession
        executeNlQuery("查询所有员工");
        // Then run a regular SQL on the same connection — the post-execution
        // hook in ServerConnection should record it in the agentSession
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SELECT * FROM employees");
        }
        // Run another NL2SQL query — the agent should have context about the SQL
        String output = executeNlQuery("刚才查询的表有多少行数据");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
    }

    @Test
    public void testContextCompression() throws Exception {
        // NL2SQL_MAX_CONTEXT_TOKENS is set globally to 8000 (threshold: 6400 tokens)
        // Run multiple diverse queries to build up context and trigger compression
        String[] queries = {
            "查询所有员工信息，包括姓名、部门和薪资",
            "统计每个部门有多少员工",
            "查询Engineering部门的平均薪资",
            "找出薪资最高的员工姓名",
            "按薪资从高到低排列所有员工",
            "查询薪资大于70000的员工",
            "统计总共有多少条员工记录",
            "查询Sales部门的员工信息",
            "计算所有员工的平均薪资",
            "查询薪资最低的员工",
            "按部门分组统计薪资总和",
            "查询Marketing部门的员工详情",
            "找出每个部门薪资最高的员工",
            "统计每个部门的薪资范围",
            "查询所有员工的完整信息并按部门排序"
        };
        for (int i = 0; i < queries.length; i++) {
            String output = executeNlQuery(queries[i]);
            Assert.assertFalse("Query " + i + " output should not be empty", output.isEmpty());
        }
    }

    @Test
    public void testFallbackAnswer_maxIterations() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SET NL2SQL_MAX_ITERATIONS=1");
        }
        // With max_iterations=1, the agent uses its 1 iteration on a tool call
        // and should produce a fallback answer
        String output = executeNlQuery("查询所有员工信息");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
        // Reset to default
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SET NL2SQL_MAX_ITERATIONS=100");
        }
    }

    @Test
    public void testKillQueryCancellation() throws Exception {
        // Start an NL2SQL query in a background thread, then kill it
        Thread bgThread = new Thread(() -> {
            try (Connection bgConn = getPolardbxConnection(TEST_DB)) {
                try (Statement stmt = bgConn.createStatement()) {
                    stmt.execute("SET ENABLE_NL2SQL=true");
                    stmt.execute("USE " + TEST_DB);
                    stmt.execute("查询所有员工的详细信息，包括姓名、部门、薪资，并按薪资排序");
                }
            } catch (Exception e) {
                // Expected: query may be killed
            }
        });
        bgThread.start();
        // Wait a bit for the query to start
        Thread.sleep(2000);
        // Kill the query on this connection's background thread
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SHOW PROCESSLIST");
            try (ResultSet rs = stmt.getResultSet()) {
                while (rs.next()) {
                    String state = rs.getString("State");
                    String info = rs.getString("Info");
                    if (info != null && info.contains("查询")) {
                        long pid = rs.getLong("Id");
                        try (Statement killStmt = tddlConnection.createStatement()) {
                            killStmt.execute("KILL QUERY " + pid);
                        } catch (Exception e) {
                            // Ignore kill errors
                        }
                        break;
                    }
                }
            }
        }
        bgThread.join(10000);
    }

    @Test
    public void testConsecutiveQueries() throws Exception {
        String output1 = executeNlQuery("查询员工总数");
        Assert.assertFalse("First query should return result", output1.isEmpty());
        String output2 = executeNlQuery("查询平均薪资");
        Assert.assertFalse("Second query should return result", output2.isEmpty());
        String output3 = executeNlQuery("查询最高薪资的员工姓名");
        Assert.assertFalse("Third query should return result", output3.isEmpty());
    }
}
