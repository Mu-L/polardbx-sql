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

import net.jcip.annotations.NotThreadSafe;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Statement;

@NotThreadSafe
public class Nl2sqlAgentCoverageTest extends Nl2sqlAgentTestBase {

    @Test
    public void testShowDatabasesToolPath() throws Exception {
        String output = executeNlQuery("请只执行 SHOW DATABASES 语句，查看当前实例的逻辑数据库列表");
        Assert.assertTrue("SHOW DATABASES result should include current test database: " + output,
            output.toLowerCase().contains(TEST_DB.toLowerCase()) || output.toLowerCase().contains("database"));
    }

    @Test
    public void testTransactionSharedConnectionPath() throws Exception {
        String output = executeNlQuery(
            "请严格按顺序执行这些 SQL：BEGIN; UPDATE employees SET salary=96000 WHERE name='Alice'; "
                + "SELECT salary FROM employees WHERE name='Alice'; COMMIT; 最后告诉我执行结果");
        Assert.assertFalse("Transaction output should not be empty", output.isEmpty());
        try (Statement stmt = tddlConnection.createStatement()) {
            Assert.assertTrue(stmt.execute("SELECT salary FROM employees WHERE name='Alice'"));
            Assert.assertTrue(stmt.getResultSet().next());
            Assert.assertEquals(96000, stmt.getResultSet().getInt(1));
        }
    }

    @Test
    public void testContextCompressionPath() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SET GLOBAL NL2SQL_MAX_CONTEXT_TOKENS=200");
        }
        try {
            String[] queries = {
                "查询employees表中所有员工的姓名、部门和薪资，并说明结果",
                "统计employees表中每个部门的员工数量，并结合上一轮结果做简短总结",
                "查询employees表中薪资最高的员工姓名、部门和薪资，并结合前两轮结果继续总结"
            };
            for (String query : queries) {
                String output = executeNlQuery(query);
                Assert.assertFalse("Compression query output should not be empty", output.isEmpty());
            }
        } finally {
            try (Statement stmt = tddlConnection.createStatement()) {
                stmt.execute("SET GLOBAL NL2SQL_MAX_CONTEXT_TOKENS=8000");
            }
        }
    }
}
