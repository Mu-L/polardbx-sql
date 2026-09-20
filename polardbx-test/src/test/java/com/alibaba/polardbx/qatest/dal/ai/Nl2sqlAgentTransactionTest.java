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
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@NotThreadSafe
public class Nl2sqlAgentTransactionTest extends Nl2sqlAgentTestBase {

    @Before
    public void enableAgentWritePolicy() throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SET GLOBAL NL2SQL_ALLOWED_SQL_TYPES='READ_WRITE'");
        }
    }

    @After
    public void restoreAgentWritePolicy() throws SQLException {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SET GLOBAL NL2SQL_ALLOWED_SQL_TYPES='ALL'");
        }
    }

    @Test
    public void testNl2sql_showStepsEnabled() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SET NL2SQL_SHOW_STEPS=true");
        }
        String output = executeNlQuery("查询所有员工姓名");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
    }

    @Test
    public void testExecuteSqlViaAgent() throws Exception {
        // Ask the agent to insert data — should use execute_sql tool
        String output = executeNlQuery("向employees表中插入一条记录，姓名为TestUser，部门为QA，薪资为50000");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM employees WHERE name = 'TestUser'")) {
            Assert.assertTrue(rs.next());
            Assert.assertEquals(1, rs.getInt(1));
        }
    }

    @Test
    public void testShowStepsWithComplexQuery() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SET NL2SQL_SHOW_STEPS=true");
        }
        // Complex multi-step query that might trigger set_plan
        String output =
            executeNlQuery("分析employees表的数据：先统计总人数，再按部门分组统计平均薪资，最后找出薪资最高的员工");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
    }

    @Test
    public void testTransactionViaAgent() throws Exception {
        // Ask the agent to perform a transactional operation
        String output = executeNlQuery("开启一个事务，将Alice的薪资更新为98000，然后提交事务");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
    }

    @Test
    public void testUpdateViaAgent() throws Exception {
        String output = executeNlQuery("将员工Bob的薪资更新为68000");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
    }

    @Test
    public void testDeleteViaAgent() throws Exception {
        // First insert a row to delete
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("INSERT INTO employees (name, department, salary) VALUES ('ToDelete', 'Temp', 10000)");
        }
        String output = executeNlQuery("删除姓名为ToDelete的员工记录");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
    }

    @Test
    public void testTransaction_beginCommit() throws Exception {
        String output = executeNlQuery(
            "请开启一个事务，然后执行BEGIN，接着执行UPDATE employees SET salary=99000 WHERE name='Alice'，最后执行COMMIT提交事务");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
    }

    @Test
    public void testTransaction_beginRollback() throws Exception {
        String output = executeNlQuery(
            "请开启事务执行BEGIN，然后执行UPDATE employees SET salary=1 WHERE name='Bob'，最后执行ROLLBACK回滚事务");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
    }

    @Test
    public void testTransaction_commitWithoutBegin() throws Exception {
        String output = executeNlQuery("请直接执行COMMIT语句");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
    }

    @Test
    public void testTransaction_rollbackWithoutBegin() throws Exception {
        String output = executeNlQuery("请直接执行ROLLBACK语句");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
    }

    @Test
    public void testTransaction_savepoint() throws Exception {
        String output = executeNlQuery(
            "请执行BEGIN开启事务，然后执行SAVEPOINT sp1创建保存点，接着执行UPDATE employees SET salary=2 WHERE name='Charlie'，最后执行ROLLBACK TO SAVEPOINT sp1回滚到保存点");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
    }
}
