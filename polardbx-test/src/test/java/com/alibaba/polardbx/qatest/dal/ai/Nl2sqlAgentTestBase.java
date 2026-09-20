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
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public abstract class Nl2sqlAgentTestBase extends AiFunctionTestBase {

    protected static final String TEST_DB = "nl2sql_agent_test_db";
    protected static final String NL2SQL_USER = "nl2sql_it_user";
    protected static final String NL2SQL_USER_NO_PRIV = "nl2sql_it_user_no_priv";
    protected static final String NORMAL_USER_PASSWORD = "test123";
    protected static final AiTestModelConfig NL2SQL_MODEL = new AiTestModelConfig(
        "openai_qwen_plus_nl2sql", AiTestModelConfig.OPENAI_QWEN_PLUS.model,
        AiTestModelConfig.OPENAI_QWEN_PLUS.provider, AiTestModelConfig.OPENAI_QWEN_PLUS.endpoint,
        AiTestModelConfig.OPENAI_QWEN_PLUS.apiKeyEnvVar);

    @BeforeClass
    public static void prepareTestDatabase() throws SQLException {
        try (Connection conn = getPolardbxConnection0()) {
            JdbcUtil.executeSuccess(conn, "DROP DATABASE IF EXISTS " + TEST_DB);
            JdbcUtil.executeSuccess(conn, "CREATE DATABASE IF NOT EXISTS " + TEST_DB + " MODE='AUTO'");
        }
    }

    @AfterClass
    public static void dropTestDatabase() throws SQLException {
        try (Connection conn = getPolardbxConnection0()) {
            JdbcUtil.executeSuccess(conn, "DROP DATABASE IF EXISTS " + TEST_DB);
        }
    }

    @Before
    public void setUp() throws Exception {
        registerSharedModel(NL2SQL_MODEL);
        createTables();
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SET GLOBAL NL2SQL_MODEL_NAME='" + NL2SQL_MODEL.name + "'");
            stmt.execute("SET ENABLE_NL2SQL=true");
        }
    }

    @After
    public void tearDown() {
        cleanupNormalUsers();
    }

    protected void createTables() throws Exception {
        for (int attempt = 0; attempt < 3; attempt++) {
            try (Statement stmt = tddlConnection.createStatement()) {
                stmt.execute("USE " + TEST_DB);
                stmt.execute("CREATE TABLE IF NOT EXISTS employees ("
                    + "id INT PRIMARY KEY AUTO_INCREMENT, "
                    + "name VARCHAR(100), "
                    + "department VARCHAR(50), "
                    + "salary DECIMAL(10,2))");
                stmt.execute("DELETE FROM employees");
                stmt.execute("INSERT INTO employees (name, department, salary) VALUES "
                    + "('Alice', 'Engineering', 95000), "
                    + "('Bob', 'Sales', 65000), "
                    + "('Charlie', 'Engineering', 80000), "
                    + "('Diana', 'Marketing', 70000)");
                return;
            } catch (SQLException e) {
                if (attempt < 2 && e.getMessage() != null
                    && e.getMessage().contains("cancelled or interrupted")) {
                    Thread.sleep(5000);
                } else {
                    throw e;
                }
            }
        }
    }

    protected void cleanupNormalUsers() {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("DROP USER IF EXISTS '" + NL2SQL_USER + "'@'%'");
            stmt.execute("DROP USER IF EXISTS '" + NL2SQL_USER_NO_PRIV + "'@'%'");
        } catch (Exception ignored) {
        }
    }

    protected Connection getNormalUserConnection(String userName) {
        ConnectionManager manager = ConnectionManager.getInstance();
        return getPolardbxDirectConnection(
            manager.getPolardbxAddress(), userName, TEST_DB, NORMAL_USER_PASSWORD, manager.getPolardbxPort());
    }

    protected String executeNlQuery(String naturalLanguage) throws Exception {
        StringBuilder output = new StringBuilder();
        try (Statement stmt = tddlConnection.createStatement()) {
            boolean hasResult = stmt.execute(naturalLanguage);
            collectResultSets(stmt, hasResult, output);
        }
        String result = output.toString();
        assertNl2sqlSuccess(result);
        return result;
    }

    protected void assertNl2sqlSuccess(String output) {
        Assert.assertFalse("Output should not be empty", output.trim().isEmpty());
        String lower = output.toLowerCase();
        Assert.assertFalse("NL2SQL should not return a system error response: " + output,
            lower.contains("nl agent error")
                || lower.contains("no model configured")
                || lower.contains("model not found")
                || lower.contains("ai api call failed"));
    }

    protected void collectResultSets(Statement stmt, boolean hasResult, StringBuilder output) throws SQLException {
        while (true) {
            if (hasResult) {
                try (ResultSet rs = stmt.getResultSet()) {
                    int colCount = rs.getMetaData().getColumnCount();
                    while (rs.next()) {
                        for (int i = 1; i <= colCount; i++) {
                            String val = rs.getString(i);
                            if (val != null) {
                                output.append(val).append('\n');
                            }
                        }
                    }
                }
            } else if (stmt.getUpdateCount() == -1) {
                break;
            }
            hasResult = stmt.getMoreResults();
        }
    }
}
