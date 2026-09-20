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

import com.alibaba.polardbx.qatest.util.JdbcUtil;
import com.alibaba.polardbx.qatest.util.PropertiesUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

@NotThreadSafe
public class AiDefaultModelConfigTest extends AiFunctionTestBase {

    private static final String TEST_MODEL_NAME = "test_default_config_model";
    private static final String TEST_MODEL_NAME_2 = "test_default_config_model_2";
    private static final String DASHSCOPE_ENDPOINT =
        "https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation";

    private String originalDefaultModel = null;

    @Before
    public void setUp() {
        String apiKey = requireApiKey("DASHSCOPE_API_KEY");
        registerModelDirect(TEST_MODEL_NAME, "dashscope", DASHSCOPE_ENDPOINT, "qwen-plus", apiKey);
        registerModelDirect(TEST_MODEL_NAME_2, "dashscope", DASHSCOPE_ENDPOINT, "qwen-turbo", apiKey);
    }

    @After
    public void tearDown() {
        // Clear default model for AI_PROMPT to keep clean state for other tests
        try {
            tddlConnection.createStatement().execute(
                "SELECT AI_UPDATE_FUNCTION('AI_PROMPT', NULL)");
        } catch (Throwable e) {
            // best effort cleanup
        }
        cleanupModelDirect(TEST_MODEL_NAME);
        cleanupModelDirect(TEST_MODEL_NAME_2);
    }

    @Test
    public void testShowAiFunctionReturnsAllFunctions() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW AI FUNCTION");
        int count = 0;
        while (rs.next()) {
            String functionName = rs.getString("FUNCTION");
            Assert.assertNotNull("FUNCTION column should not be null", functionName);
            count++;
        }
        JdbcUtil.close(rs);
        Assert.assertEquals("Should have 10 AI functions registered", 10, count);
    }

    @Test
    public void testUpdateFunctionDefaultModel() throws SQLException {
        originalDefaultModel = getDefaultModelForFunction("AI_PROMPT");

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT AI_UPDATE_FUNCTION('AI_PROMPT', '" + TEST_MODEL_NAME + "')");
        Assert.assertTrue(rs.next());
        Assert.assertEquals("OK", rs.getString(1));
        JdbcUtil.close(rs);
    }

    @Test
    public void testShowReflectsUpdatedDefault() throws SQLException {
        originalDefaultModel = getDefaultModelForFunction("AI_PROMPT");

        JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT AI_UPDATE_FUNCTION('AI_PROMPT', '" + TEST_MODEL_NAME + "')");

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW AI FUNCTION FROM AI_PROMPT");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(TEST_MODEL_NAME, rs.getString("DEFAULT_MODEL"));
        JdbcUtil.close(rs);
    }

    @Test
    public void testUpdateInvalidFunctionNameFails() {
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "SELECT AI_UPDATE_FUNCTION('INVALID_FUNC', '" + TEST_MODEL_NAME + "')",
            "Unknown AI function");
    }

    @Test
    public void testUpdateInvalidModelNameFails() {
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "SELECT AI_UPDATE_FUNCTION('AI_PROMPT', 'nonexistent_model_xyz')",
            "Model not found");
    }

    @Test
    public void testUpdateFunctionNameCaseInsensitive() throws SQLException {
        originalDefaultModel = getDefaultModelForFunction("AI_PROMPT");

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT AI_UPDATE_FUNCTION('ai_prompt', '" + TEST_MODEL_NAME + "')");
        Assert.assertTrue(rs.next());
        Assert.assertEquals("OK", rs.getString(1));
        JdbcUtil.close(rs);

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW AI FUNCTION FROM AI_PROMPT");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(TEST_MODEL_NAME, rs.getString("DEFAULT_MODEL"));
        JdbcUtil.close(rs);
    }

    @Test
    public void testPromptUsesUpdatedDefaultModel() throws SQLException {
        originalDefaultModel = getDefaultModelForFunction("AI_PROMPT");

        JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT AI_UPDATE_FUNCTION('AI_PROMPT', '" + TEST_MODEL_NAME + "')");

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT AI_PROMPT('hello')");
        Assert.assertTrue(rs.next());
        String result = rs.getString(1);
        Assert.assertNotNull(result);
        Assert.assertFalse(result.trim().isEmpty());
        JdbcUtil.close(rs);
    }

    @Test
    public void testPromptWithoutDefaultModelFails() {
        String current = getDefaultModelForFunction("AI_PROMPT");
        if (current != null && !current.isEmpty()) {
            Assume.assumeTrue("Default model already set, cannot test error case", false);
        }
        JdbcUtil.executeUpdateFailed(tddlConnection,
            "SELECT AI_PROMPT('hello')",
            "no model specified");
    }

    @Test
    public void testSwitchDefaultModel() throws SQLException {
        originalDefaultModel = getDefaultModelForFunction("AI_PROMPT");

        JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT AI_UPDATE_FUNCTION('AI_PROMPT', '" + TEST_MODEL_NAME + "')");

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW AI FUNCTION FROM AI_PROMPT");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(TEST_MODEL_NAME, rs.getString("DEFAULT_MODEL"));
        JdbcUtil.close(rs);

        JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT AI_UPDATE_FUNCTION('AI_PROMPT', '" + TEST_MODEL_NAME_2 + "')");

        rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW AI FUNCTION FROM AI_PROMPT");
        Assert.assertTrue(rs.next());
        Assert.assertEquals(TEST_MODEL_NAME_2, rs.getString("DEFAULT_MODEL"));
        JdbcUtil.close(rs);
    }

    private String getDefaultModelForFunction(String functionName) {
        try {
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                "SHOW AI FUNCTION FROM " + functionName);
            if (rs.next()) {
                String model = rs.getString("DEFAULT_MODEL");
                JdbcUtil.close(rs);
                return model;
            }
            JdbcUtil.close(rs);
        } catch (Exception e) {
            // ignore
        }
        return null;
    }

    private void registerModelDirect(String name, String provider, String endpoint, String model, String apiKey) {
        try {
            tddlConnection.createStatement().execute(
                String.format("SELECT AI_DROP_MODEL('%s')", name));
        } catch (Exception e) {
            // ignore
        }
        String optionsJson = String.format("{\"api_key\":\"%s\"}", apiKey.replace("\"", "\\\""));
        String sql = String.format(
            "SELECT AI_REGISTER_MODEL('%s', '%s', '%s', '%s', '%s')",
            name, provider, endpoint, model, optionsJson.replace("'", "\\'"));
        JdbcUtil.executeQuerySuccess(tddlConnection, sql);
    }

    private void cleanupModelDirect(String name) {
        try {
            tddlConnection.createStatement().execute(
                String.format("SELECT AI_DROP_MODEL('%s')", name));
        } catch (Exception e) {
            // ignore
        }
    }
}
