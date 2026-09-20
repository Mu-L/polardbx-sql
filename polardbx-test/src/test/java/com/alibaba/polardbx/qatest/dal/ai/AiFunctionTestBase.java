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
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

public abstract class AiFunctionTestBase extends BaseTestCase {

    protected Connection tddlConnection;

    private final List<String> registeredModels = new ArrayList<>();

    @Before
    public void setUpAiFunctionTest() {
        tddlConnection = getPolardbxConnection();
        String apiKey = getConfigProperty("DASHSCOPE_API_KEY");
        Assert.assertNotNull(
            "DASHSCOPE_API_KEY must be set to run AI function tests", apiKey);
        Assert.assertFalse(
            "DASHSCOPE_API_KEY must not be empty", apiKey.trim().isEmpty());
    }

    @After
    public void tearDownAiFunctionTest() {
        for (String name : registeredModels) {
            cleanupModel(name);
        }
        registeredModels.clear();
        JdbcUtil.close(tddlConnection);
    }

    protected String requireApiKey(String envVarKey) {
        String apiKey = getConfigProperty(envVarKey);
        Assert.assertNotNull(
            "Property '" + envVarKey + "' is not set. "
                + "Please set the environment variable before running AI function tests.",
            apiKey);
        Assert.assertFalse(
            "Property '" + envVarKey + "' is empty.",
            apiKey.trim().isEmpty());
        return apiKey;
    }

    protected void registerModel(AiTestModelConfig config) {
        String apiKey = requireApiKey(config.apiKeyEnvVar);

        cleanupModel(config.name);

        String optionsJson = String.format("{\"api_key\":\"%s\"}", apiKey.replace("\"", "\\\""));
        String sql = String.format(
            "SELECT AI_REGISTER_MODEL('%s', '%s', '%s', '%s', '%s')",
            config.name,
            config.provider,
            config.endpoint,
            config.model,
            optionsJson.replace("'", "\\'")
        );

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue("AI_REGISTER_MODEL should return a result", rs.next());
            String result = rs.getString(1);
            Assert.assertTrue("Model registration should succeed: " + result,
                result.contains("registered successfully"));
        } catch (SQLException e) {
            Assert.fail("Failed to read AI_REGISTER_MODEL result: " + e.getMessage());
        } finally {
            JdbcUtil.close(rs);
        }

        registeredModels.add(config.name);
    }

    protected void registerSharedModel(AiTestModelConfig config) {
        String apiKey = requireApiKey(config.apiKeyEnvVar);
        String optionsJson = String.format("{\"api_key\":\"%s\"}", apiKey.replace("\"", "\\\""));
        String sql = String.format(
            "SELECT AI_REGISTER_MODEL('%s', '%s', '%s', '%s', '%s')",
            config.name,
            config.provider,
            config.endpoint,
            config.model,
            optionsJson.replace("'", "\\'")
        );

        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            Assert.assertTrue("AI_REGISTER_MODEL should return a result", rs.next());
            String result = rs.getString(1);
            Assert.assertTrue("Model registration should succeed: " + result,
                result.contains("registered successfully"));
        } catch (SQLException e) {
            if (!e.getMessage().toLowerCase().contains("already exists")) {
                Assert.fail("Failed to register shared model: " + e.getMessage());
            }
        }
    }

    protected void registerModelWithOptions(AiTestModelConfig config, String extraOptions) {
        String apiKey = requireApiKey(config.apiKeyEnvVar);

        cleanupModel(config.name);

        String optionsJson = String.format("{\"api_key\":\"%s\",%s}",
            apiKey.replace("\"", "\\\""), extraOptions);
        String sql = String.format(
            "SELECT AI_REGISTER_MODEL('%s', '%s', '%s', '%s', '%s')",
            config.name,
            config.provider,
            config.endpoint,
            config.model,
            optionsJson.replace("'", "\\'")
        );

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue(rs.next());
            Assert.assertTrue(rs.getString(1).contains("registered successfully"));
        } catch (SQLException e) {
            Assert.fail("Failed to register model with options: " + e.getMessage());
        } finally {
            JdbcUtil.close(rs);
        }

        registeredModels.add(config.name);
    }

    protected String executeAiFunction(String sql) throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue("AI function should return a result", rs.next());
            return rs.getString(1);
        } finally {
            JdbcUtil.close(rs);
        }
    }

    protected void executeAiFunctionExpectError(String sql, String expectedMessage) {
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, expectedMessage);
    }

    protected static String getConfigProperty(String key) {
        String value = System.getProperty(key);
        if (value == null || value.isEmpty()) {
            value = System.getenv(key);
        }
        if (value == null || value.isEmpty()) {
            value = PropertiesUtil.configProp.getProperty(key);
        }
        return value;
    }

    private void cleanupModel(String name) {
        try {
            tddlConnection.createStatement()
                .execute(String.format("SELECT AI_DROP_MODEL('%s')", name));
        } catch (Exception e) {
            // Ignore — model may not exist
        }
    }
}
