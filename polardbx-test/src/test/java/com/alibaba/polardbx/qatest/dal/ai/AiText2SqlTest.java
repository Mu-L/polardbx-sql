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

import com.alibaba.polardbx.qatest.BaseTestCase;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Integration tests for AI_TEXT2SQL function.
 *
 * <p>Verifies that AI_TEXT2SQL converts natural language queries to SQL
 * using table metadata from the current schema.
 *
 * <p>Requires environment variable {@code DASHSCOPE_API_KEY} to be set.
 */
@NotThreadSafe
public class AiText2SqlTest extends AiFunctionTestBase {

    private static final String TEST_DB = "ai_text2sql_test_db";

    private Connection tddlConnection;

    @Before
    public void setUp() throws SQLException {
        tddlConnection = getPolardbxConnection();

        // Create test database and tables
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("CREATE DATABASE IF NOT EXISTS " + TEST_DB + " MODE='AUTO'");
        }
        tddlConnection.setCatalog(TEST_DB);

        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("DROP TABLE IF EXISTS employees");
            stmt.execute("CREATE TABLE employees ("
                + "id BIGINT PRIMARY KEY AUTO_INCREMENT, "
                + "name VARCHAR(100) NOT NULL, "
                + "department VARCHAR(50), "
                + "salary DECIMAL(10,2), "
                + "hire_date DATE"
                + ")");

            stmt.execute("DROP TABLE IF EXISTS departments");
            stmt.execute("CREATE TABLE departments ("
                + "id BIGINT PRIMARY KEY AUTO_INCREMENT, "
                + "dept_name VARCHAR(100) NOT NULL, "
                + "manager_id BIGINT"
                + ")");

            stmt.execute("DROP TABLE IF EXISTS orders");
            stmt.execute("CREATE TABLE orders ("
                + "id BIGINT PRIMARY KEY AUTO_INCREMENT, "
                + "customer_name VARCHAR(100) NOT NULL, "
                + "product VARCHAR(100) NOT NULL, "
                + "quantity INT NOT NULL, "
                + "total_price DECIMAL(10,2) NOT NULL, "
                + "order_date DATE NOT NULL"
                + ")");

            // Insert some test data
            stmt.execute("INSERT INTO employees (name, department, salary, hire_date) VALUES "
                + "('Alice', 'Engineering', 95000.00, '2020-01-15'), "
                + "('Bob', 'Marketing', 75000.00, '2019-06-01'), "
                + "('Charlie', 'Engineering', 110000.00, '2018-03-10')");

            stmt.execute("INSERT INTO departments (dept_name, manager_id) VALUES "
                + "('Engineering', 3), "
                + "('Marketing', 2)");

            stmt.execute("INSERT INTO orders (customer_name, product, quantity, total_price, order_date) VALUES "
                + "('Alice', 'Laptop', 1, 1299.99, '2024-01-15'), "
                + "('Bob', 'Phone', 2, 1599.98, '2024-01-20')");
        }

        registerModel(AiTestModelConfig.DASHSCOPE_QWEN_PLUS);

        // Set the registered model as the default for AI_TEXT2SQL so tests
        // can call AI_TEXT2SQL without specifying model name
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SELECT AI_UPDATE_FUNCTION('AI_TEXT2SQL', '"
                + AiTestModelConfig.DASHSCOPE_QWEN_PLUS.name + "')");
        }
    }

    @After
    public void tearDown() {
        if (tddlConnection != null) {
            // Clear default model for AI_TEXT2SQL to keep clean state for other tests
            try (Statement stmt = tddlConnection.createStatement()) {
                stmt.execute("SELECT AI_UPDATE_FUNCTION('AI_TEXT2SQL', NULL)");
            } catch (Throwable ignore) {
            }
            try (Statement stmt = tddlConnection.createStatement()) {
                stmt.execute("DROP DATABASE IF EXISTS " + TEST_DB);
            } catch (SQLException ignore) {
            }
            JdbcUtil.close(tddlConnection);
        }
    }

    private String executeQuery(String sql) throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue("Query should return a result: " + sql, rs.next());
            return rs.getString(1);
        } finally {
            JdbcUtil.close(rs);
        }
    }

    // ==================== Basic functionality ====================

    @Test
    public void testText2SqlBasicSelect() throws SQLException {
        String result = executeQuery(
            "SELECT AI_TEXT2SQL('Find all employees')");
        Assert.assertNotNull("Result should not be null", result);
        String lower = result.toLowerCase();
        Assert.assertTrue("Should contain SELECT", lower.contains("select"));
        Assert.assertTrue("Should reference employees table", lower.contains("employees"));
        System.out.println("[testText2SqlBasicSelect] Generated SQL: " + result);
    }

    @Test
    public void testText2SqlWithFilter() throws SQLException {
        String result = executeQuery(
            "SELECT AI_TEXT2SQL('Find employees with salary greater than 80000')");
        Assert.assertNotNull("Result should not be null", result);
        String lower = result.toLowerCase();
        Assert.assertTrue("Should contain SELECT", lower.contains("select"));
        Assert.assertTrue("Should contain WHERE", lower.contains("where"));
        Assert.assertTrue("Should reference salary", lower.contains("salary"));
        System.out.println("[testText2SqlWithFilter] Generated SQL: " + result);
    }

    @Test
    public void testText2SqlWithAggregation() throws SQLException {
        String result = executeQuery(
            "SELECT AI_TEXT2SQL('Calculate the average salary of employees by department')");
        Assert.assertNotNull("Result should not be null", result);
        String lower = result.toLowerCase();
        Assert.assertTrue("Should contain SELECT", lower.contains("select"));
        Assert.assertTrue("Should contain AVG or average logic",
            lower.contains("avg") || lower.contains("average"));
        Assert.assertTrue("Should contain GROUP BY", lower.contains("group by"));
        System.out.println("[testText2SqlWithAggregation] Generated SQL: " + result);
    }

    @Test
    public void testText2SqlCrossTable() throws SQLException {
        String result = executeQuery(
            "SELECT AI_TEXT2SQL('Show the total order amount for each customer')");
        Assert.assertNotNull("Result should not be null", result);
        String lower = result.toLowerCase();
        Assert.assertTrue("Should contain SELECT", lower.contains("select"));
        Assert.assertTrue("Should reference orders table", lower.contains("orders"));
        System.out.println("[testText2SqlCrossTable] Generated SQL: " + result);
    }

    @Test
    public void testText2SqlWithModelName() throws SQLException {
        String result = executeQuery(
            "SELECT AI_TEXT2SQL('List all departments', 'dashscope_qwen_plus')");
        Assert.assertNotNull("Result should not be null", result);
        String lower = result.toLowerCase();
        Assert.assertTrue("Should contain SELECT", lower.contains("select"));
        Assert.assertTrue("Should reference departments table", lower.contains("departments"));
        System.out.println("[testText2SqlWithModelName] Generated SQL: " + result);
    }

    // ==================== Chinese language support ====================

    @Test
    public void testText2SqlChinese() throws SQLException {
        String result = executeQuery(
            "SELECT AI_TEXT2SQL('查询所有员工的姓名和工资')");
        Assert.assertNotNull("Result should not be null", result);
        String lower = result.toLowerCase();
        Assert.assertTrue("Should contain SELECT", lower.contains("select"));
        Assert.assertTrue("Should reference employees table", lower.contains("employees"));
        System.out.println("[testText2SqlChinese] Generated SQL: " + result);
    }
}
