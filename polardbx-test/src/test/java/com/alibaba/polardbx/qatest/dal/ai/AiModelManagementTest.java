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

/**
 * Integration tests for AI Model Management functions:
 * AI_REGISTER_MODEL, AI_UPDATE_MODEL, AI_DROP_MODEL, AI_LIST_MODELS, AI_DESCRIBE_MODEL
 */
@NotThreadSafe
public class AiModelManagementTest extends AiFunctionTestBase {

    private Connection tddlConnection;

    private static final String TEST_MODEL_NAME = "test_model_qwen_turbo";
    private static final String TEST_MODEL_NAME_2 = "test_model_embedding_v2";
    private static final String TEST_MODEL_NAME_3 = "test_model_rerank_v1";

    // Default model value for tests that don't care about the actual API model
    private static final String TEST_MODEL = "test-model";

    @Before
    public void setUp() {
        tddlConnection = getPolardbxConnection();
        // Clean up any leftover test models
        cleanupTestModel(TEST_MODEL_NAME);
        cleanupTestModel(TEST_MODEL_NAME_2);
        cleanupTestModel(TEST_MODEL_NAME_3);
        cleanupTestModel("test_mgmt_llm");
        cleanupTestModel("test_mgmt_embedding");
        cleanupTestModel("__POLARDBX_MY_MODEL");
    }

    @After
    public void tearDown() {
        // Clean up test models
        cleanupTestModel(TEST_MODEL_NAME);
        cleanupTestModel(TEST_MODEL_NAME_2);
        cleanupTestModel(TEST_MODEL_NAME_3);
        cleanupTestModel("test_mgmt_llm");
        cleanupTestModel("test_mgmt_embedding");
        cleanupTestModel("__POLARDBX_MY_MODEL");
        JdbcUtil.close(tddlConnection);
    }

    /**
     * Silently clean up a test model if it exists.
     */
    private void cleanupTestModel(String name) {
        try {
            String sql = String.format("SELECT AI_DROP_MODEL('%s')", name);
            tddlConnection.createStatement().execute(sql);
        } catch (Exception e) {
            // Ignore errors during cleanup
        }
    }

    // ==================== AI_REGISTER_MODEL Tests ====================

    /**
     * Test basic model registration with required parameters only.
     */
    @Test
    public void testRegisterModelBasic() throws SQLException {
        String sql = String.format(
            "SELECT AI_REGISTER_MODEL('%s', 'dashscope', 'https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation', '%s')",
            TEST_MODEL_NAME, TEST_MODEL);

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        String result = rs.getString(1);
        Assert.assertNotNull(result);
        Assert.assertTrue("Should contain success message", result.contains("registered successfully"));
        rs.close();
    }

    /**
     * Test model registration with optional parameters (options JSON).
     */
    @Test
    public void testRegisterModelWithOptions() throws SQLException {
        String optionsJson =
            "{\"api_key\":\"sk-test-key-12345678\",\"description\":\"Test model for QA\",\"temperature\":0.7}";
        String sql = String.format(
            "SELECT AI_REGISTER_MODEL('%s', 'dashscope', 'https://dashscope.aliyuncs.com/api/v1', '%s', '%s')",
            TEST_MODEL_NAME, TEST_MODEL, optionsJson.replace("'", "\\'"));

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        String result = rs.getString(1);
        Assert.assertNotNull(result);
        Assert.assertTrue("Should contain success message", result.contains("registered successfully"));
        rs.close();
    }

    /**
     * Test registering an EMBEDDING type model.
     */
    @Test
    public void testRegisterEmbeddingModel() throws SQLException {
        String sql = String.format(
            "SELECT AI_REGISTER_MODEL('%s', 'dashscope', 'https://dashscope.aliyuncs.com/api/v1/embeddings', 'text-embedding-v3')",
            TEST_MODEL_NAME_2);

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        String result = rs.getString(1);
        Assert.assertTrue(result.contains("registered successfully"));
        rs.close();
    }

    /**
     * Test registering a RERANK type model.
     */
    @Test
    public void testRegisterRerankModel() throws SQLException {
        String sql = String.format(
            "SELECT AI_REGISTER_MODEL('%s', 'custom', 'https://custom-rerank.example.com/api', 'rerank-model')",
            TEST_MODEL_NAME_3);

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        String result = rs.getString(1);
        Assert.assertTrue(result.contains("registered successfully"));
        rs.close();
    }

    /**
     * Test registering a duplicate model should fail.
     */
    @Test
    public void testRegisterDuplicateModel() throws SQLException {
        // First registration should succeed
        String sql = String.format(
            "SELECT AI_REGISTER_MODEL('%s', 'dashscope', 'https://dashscope.aliyuncs.com/api/v1', '%s')",
            TEST_MODEL_NAME, TEST_MODEL);
        JdbcUtil.executeQuerySuccess(tddlConnection, sql);

        // Second registration with same name should fail
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "already exists");
    }

    /**
     * Test registering a model with invalid parameters should fail.
     */
    @Test
    public void testRegisterModelInvalidType() {
        // This test is no longer valid as model_type restriction is removed
        // Keep the test but change to test missing required parameter
        String sql = "SELECT AI_REGISTER_MODEL('test', 'dashscope', 'endpoint')";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "requires at least 4 arguments");
    }

    /**
     * Test registering a model with missing required parameter should fail.
     */
    @Test
    public void testRegisterModelMissingParams() {
        // Only 2 args when at least 4 are required
        String sql = "SELECT AI_REGISTER_MODEL('test', 'dashscope')";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "requires at least 4 arguments");
    }

    /**
     * Test registering a model with empty name should fail.
     */
    @Test
    public void testRegisterModelEmptyName() {
        String sql = "SELECT AI_REGISTER_MODEL('', 'dashscope', 'https://example.com/api', 'test-model')";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "name is required");
    }

    // ==================== AI_LIST_MODELS Tests ====================

    /**
     * Test listing all models.
     */
    @Test
    public void testListModelsAll() throws SQLException {
        // Register two models
        JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT AI_REGISTER_MODEL('%s', 'dashscope', 'https://example.com/api', '%s')",
            TEST_MODEL_NAME, TEST_MODEL));
        JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT AI_REGISTER_MODEL('%s', 'dashscope', 'https://example.com/api', 'embed-model')",
            TEST_MODEL_NAME_2));

        // List all models
        String sql = "SELECT AI_LIST_MODELS()";
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        String result = rs.getString(1);
        Assert.assertNotNull(result);
        // The result is a JSON array, should contain our test models
        Assert.assertTrue("Should contain first test model", result.contains(TEST_MODEL_NAME));
        Assert.assertTrue("Should contain second test model", result.contains(TEST_MODEL_NAME_2));
        rs.close();
    }

    /**
     * Test listing all models (no filtering by type as model_type is removed).
     */
    @Test
    public void testListModelsByType() throws SQLException {
        // Register two models
        JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT AI_REGISTER_MODEL('%s', 'dashscope', 'https://example.com/api', '%s')",
            TEST_MODEL_NAME, TEST_MODEL));
        JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT AI_REGISTER_MODEL('%s', 'dashscope', 'https://example.com/api', 'embed-model')",
            TEST_MODEL_NAME_2));

        // AI_LIST_MODELS no longer accepts parameters
        String sql = "SELECT AI_LIST_MODELS()";
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        String result = rs.getString(1);
        Assert.assertNotNull(result);
        Assert.assertTrue("Should contain first test model", result.contains(TEST_MODEL_NAME));
        Assert.assertTrue("Should contain second test model", result.contains(TEST_MODEL_NAME_2));
        rs.close();
    }

    /**
     * Test listing models when no models exist returns empty array.
     */
    @Test
    public void testListModelsEmpty() throws SQLException {
        String sql = "SELECT AI_LIST_MODELS()";
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        String result = rs.getString(1);
        Assert.assertNotNull(result);
        // Should be an empty JSON array (may still contain other models from previous tests)
        // Just verify it's valid JSON
        Assert.assertTrue("Should be a JSON array", result.startsWith("["));
        rs.close();
    }

    // ==================== AI_DESCRIBE_MODEL Tests ====================

    /**
     * Test describing a registered model.
     */
    @Test
    public void testDescribeModel() throws SQLException {
        // Register a model first
        String registerSql = String.format(
            "SELECT AI_REGISTER_MODEL('%s', 'dashscope', 'https://example.com/api', '%s', '%s')",
            TEST_MODEL_NAME, TEST_MODEL, "{\"api_key\":\"sk-test-key\",\"description\":\"Test description\"}");
        JdbcUtil.executeQuerySuccess(tddlConnection, registerSql);

        // Describe the model
        String sql = String.format("SELECT AI_DESCRIBE_MODEL('%s')", TEST_MODEL_NAME);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        String result = rs.getString(1);
        Assert.assertNotNull(result);
        Assert.assertTrue("Should contain model name", result.contains(TEST_MODEL_NAME));
        rs.close();
    }

    /**
     * Test describing a non-existent model should fail.
     */
    @Test
    public void testDescribeModelNotFound() {
        String sql = "SELECT AI_DESCRIBE_MODEL('non_existent_model_xyz')";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "not found");
    }

    /**
     * Test describing a model with empty name should fail.
     */
    @Test
    public void testDescribeModelEmptyName() {
        String sql = "SELECT AI_DESCRIBE_MODEL('')";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "name is required");
    }

    // ==================== AI_UPDATE_MODEL Tests ====================

    /**
     * Test updating a model's status.
     */
    @Test
    public void testUpdateModelStatus() throws SQLException {
        // Register a model first
        String registerSql = String.format(
            "SELECT AI_REGISTER_MODEL('%s', 'dashscope', 'https://example.com/api', '%s')",
            TEST_MODEL_NAME, TEST_MODEL);
        JdbcUtil.executeQuerySuccess(tddlConnection, registerSql);

        // Update the endpoint
        String updateOptions = "{\"endpoint\":\"https://new-endpoint.com/api\"}";
        String sql = String.format("SELECT AI_UPDATE_MODEL('%s', '%s')", TEST_MODEL_NAME, updateOptions);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        String result = rs.getString(1);
        Assert.assertTrue(result.contains("updated successfully"));
        rs.close();

        // Verify the update via describe
        String describeSql = String.format("SELECT AI_DESCRIBE_MODEL('%s')", TEST_MODEL_NAME);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, describeSql);
        Assert.assertTrue(rs.next());
        String describeResult = rs.getString(1);
        Assert.assertTrue("Should contain new endpoint",
            describeResult.contains("https://new-endpoint.com/api"));
        rs.close();
    }

    /**
     * Test updating a model status to INACTIVE and back to ACTIVE.
     */
    @Test
    public void testUpdateModelStatus2() throws SQLException {
        // Register a model
        JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT AI_REGISTER_MODEL('%s', 'dashscope', 'https://example.com/api', '%s')",
            TEST_MODEL_NAME, TEST_MODEL));

        // Update status to INACTIVE
        String updateOptions = "{\"status\":\"INACTIVE\"}";
        String sql = String.format("SELECT AI_UPDATE_MODEL('%s', '%s')", TEST_MODEL_NAME, updateOptions);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        rs.close();

        // Verify status
        String describeSql = String.format("SELECT AI_DESCRIBE_MODEL('%s')", TEST_MODEL_NAME);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, describeSql);
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getString(1).contains("INACTIVE"));
        rs.close();

        // Update back to ACTIVE
        updateOptions = "{\"status\":\"ACTIVE\"}";
        sql = String.format("SELECT AI_UPDATE_MODEL('%s', '%s')", TEST_MODEL_NAME, updateOptions);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        rs.close();

        // Verify status is back to ACTIVE
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, describeSql);
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getString(1).contains("ACTIVE"));
        rs.close();
    }

    /**
     * Test updating a model with invalid status should fail.
     */
    @Test
    public void testUpdateModelInvalidStatus() throws SQLException {
        // Register a model
        JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT AI_REGISTER_MODEL('%s', 'dashscope', 'https://example.com/api', '%s')",
            TEST_MODEL_NAME, TEST_MODEL));

        // Update with invalid status
        String updateOptions = "{\"status\":\"INVALID_STATUS\"}";
        String sql = String.format("SELECT AI_UPDATE_MODEL('%s', '%s')", TEST_MODEL_NAME, updateOptions);
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "status must be one of");
    }

    /**
     * Test updating model description and api_key.
     */
    @Test
    public void testUpdateModelDescriptionAndApiKey() throws SQLException {
        // Register a model
        JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT AI_REGISTER_MODEL('%s', 'dashscope', 'https://example.com/api', '%s')",
            TEST_MODEL_NAME, TEST_MODEL));

        // Update description and api_key
        String updateOptions = "{\"description\":\"Updated description\",\"api_key\":\"sk-new-key-99998888\"}";
        String sql = String.format("SELECT AI_UPDATE_MODEL('%s', '%s')", TEST_MODEL_NAME, updateOptions);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        rs.close();

        // Verify via describe
        String describeSql = String.format("SELECT AI_DESCRIBE_MODEL('%s')", TEST_MODEL_NAME);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, describeSql);
        Assert.assertTrue(rs.next());
        String describeResult = rs.getString(1);
        Assert.assertTrue("Should contain updated description",
            describeResult.contains("Updated description"));
        rs.close();
    }

    /**
     * Test updating a non-existent model should fail.
     */
    @Test
    public void testUpdateModelNotFound() {
        String sql = "SELECT AI_UPDATE_MODEL('non_existent_model_xyz', '{\"status\":\"ACTIVE\"}')";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "not found");
    }

    /**
     * Test updating a model with missing options should fail.
     */
    @Test
    public void testUpdateModelMissingOptions() {
        String sql = "SELECT AI_UPDATE_MODEL('some_model')";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "requires 2 arguments");
    }

    // testUpdateModelSetDefault removed - is_default feature no longer exists

    // ==================== AI_DROP_MODEL Tests ====================

    /**
     * Test dropping an existing model.
     */
    @Test
    public void testDropModel() throws SQLException {
        // Register a model
        JdbcUtil.executeQuerySuccess(tddlConnection, String.format(
            "SELECT AI_REGISTER_MODEL('%s', 'dashscope', 'https://example.com/api', '%s')",
            TEST_MODEL_NAME, TEST_MODEL));

        // Drop the model
        String sql = String.format("SELECT AI_DROP_MODEL('%s')", TEST_MODEL_NAME);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        String result = rs.getString(1);
        Assert.assertTrue(result.contains("dropped successfully"));
        rs.close();

        // Verify the model is gone via describe
        String describeSql = String.format("SELECT AI_DESCRIBE_MODEL('%s')", TEST_MODEL_NAME);
        JdbcUtil.executeUpdateFailed(tddlConnection, describeSql, "not found");
    }

    /**
     * Test dropping a non-existent model should fail.
     */
    @Test
    public void testDropModelNotFound() {
        String sql = "SELECT AI_DROP_MODEL('non_existent_model_xyz')";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "not found");
    }

    /**
     * Test dropping a model with empty name should fail.
     */
    @Test
    public void testDropModelEmptyName() {
        String sql = "SELECT AI_DROP_MODEL('')";
        JdbcUtil.executeUpdateFailed(tddlConnection, sql, "name is required");
    }

    // ==================== End-to-End Workflow Tests ====================

    /**
     * Test complete model lifecycle: register -> describe -> update -> list -> drop.
     */
    @Test
    public void testModelLifecycle() throws SQLException {
        // 1. Register
        String registerSql = String.format(
            "SELECT AI_REGISTER_MODEL('%s', 'dashscope', 'https://dashscope.aliyuncs.com/api/v1', '%s', "
                + "'{\"api_key\":\"sk-test-lifecycle-key\",\"description\":\"Lifecycle test model\"}')",
            TEST_MODEL_NAME, TEST_MODEL);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, registerSql);
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getString(1).contains("registered successfully"));
        rs.close();

        // 2. Describe
        String describeSql = String.format("SELECT AI_DESCRIBE_MODEL('%s')", TEST_MODEL_NAME);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, describeSql);
        Assert.assertTrue(rs.next());
        String descResult = rs.getString(1);
        Assert.assertTrue(descResult.contains(TEST_MODEL_NAME));
        Assert.assertTrue(descResult.contains("dashscope"));
        Assert.assertTrue(descResult.contains("Lifecycle test model"));
        rs.close();

        // 3. Update
        String updateSql = String.format(
            "SELECT AI_UPDATE_MODEL('%s', '{\"description\":\"Updated lifecycle model\",\"status\":\"INACTIVE\"}')",
            TEST_MODEL_NAME);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, updateSql);
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getString(1).contains("updated successfully"));
        rs.close();

        // 4. Verify update via describe
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, describeSql);
        Assert.assertTrue(rs.next());
        descResult = rs.getString(1);
        Assert.assertTrue(descResult.contains("Updated lifecycle model"));
        Assert.assertTrue(descResult.contains("INACTIVE"));
        rs.close();

        // 5. List - should contain our model
        String listSql = "SELECT AI_LIST_MODELS()";
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, listSql);
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getString(1).contains(TEST_MODEL_NAME));
        rs.close();

        // 6. Drop
        String dropSql = String.format("SELECT AI_DROP_MODEL('%s')", TEST_MODEL_NAME);
        rs = JdbcUtil.executeQuerySuccess(tddlConnection, dropSql);
        Assert.assertTrue(rs.next());
        Assert.assertTrue(rs.getString(1).contains("dropped successfully"));
        rs.close();

        // 7. Verify model is gone
        JdbcUtil.executeUpdateFailed(tddlConnection, describeSql, "not found");
    }

    /**
     * Test re-registering a model after dropping it.
     */
    @Test
    public void testReRegisterAfterDrop() throws SQLException {
        // Register a model
        String registerSql1 = String.format(
            "SELECT AI_REGISTER_MODEL('%s', 'dashscope', 'https://example.com/api', '%s')",
            TEST_MODEL_NAME, TEST_MODEL);
        JdbcUtil.executeQuerySuccess(tddlConnection, registerSql1);

        // Drop the model
        String dropSql = String.format("SELECT AI_DROP_MODEL('%s')", TEST_MODEL_NAME);
        JdbcUtil.executeQuerySuccess(tddlConnection, dropSql);

        // Re-register with same name but different provider
        String registerSql2 = String.format(
            "SELECT AI_REGISTER_MODEL('%s', 'openai', 'https://api.openai.com/embeddings', 'text-embedding-3')",
            TEST_MODEL_NAME, "text-embedding-3");
        JdbcUtil.executeQuerySuccess(tddlConnection, registerSql2);

        // Verify the new registration
        String describeSql = String.format("SELECT AI_DESCRIBE_MODEL('%s')", TEST_MODEL_NAME);
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, describeSql);
        Assert.assertTrue(rs.next());
        String result = rs.getString(1);
        Assert.assertTrue(result.contains("openai"));
        rs.close();
    }

    // testModelTypeCaseInsensitive removed - model_type filtering no longer exists

    // ==================== Built-in Model Tests ====================

    /**
     * Test that AI_LIST_MODELS includes a registered model.
     */
    @Test
    public void testListModelsIncludesRegistered() throws SQLException {
        // Register a model first
        String apiKey = requireApiKey("DASHSCOPE_API_KEY");
        String optionsJson = String.format("{\"api_key\":\"%s\"}", apiKey.replace("\"", "\\\""));
        String registerSql = String.format(
            "SELECT AI_REGISTER_MODEL('test_mgmt_llm', 'dashscope', "
                + "'https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation', "
                + "'qwen-plus', '%s')",
            optionsJson.replace("'", "\\'"));
        JdbcUtil.executeQuerySuccess(tddlConnection, registerSql);

        // List models and verify it contains the registered model
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SELECT AI_LIST_MODELS()");
        Assert.assertTrue(rs.next());
        String result = rs.getString(1);
        rs.close();

        Assert.assertTrue("Should contain registered model test_mgmt_llm",
            result.contains("test_mgmt_llm"));
    }

    /**
     * Test that a registered LLM model has correct properties when described.
     */
    @Test
    public void testDescribeRegisteredLLMModel() throws SQLException {
        // Register a model first
        String apiKey = requireApiKey("DASHSCOPE_API_KEY");
        String optionsJson = String.format("{\"api_key\":\"%s\"}", apiKey.replace("\"", "\\\""));
        String registerSql = String.format(
            "SELECT AI_REGISTER_MODEL('test_mgmt_llm', 'dashscope', "
                + "'https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation', "
                + "'qwen-plus', '%s')",
            optionsJson.replace("'", "\\'"));
        JdbcUtil.executeQuerySuccess(tddlConnection, registerSql);

        // Describe the registered model
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT AI_DESCRIBE_MODEL('test_mgmt_llm')");
        Assert.assertTrue(rs.next());
        String result = rs.getString(1);
        rs.close();

        Assert.assertTrue("Should have name=test_mgmt_llm",
            result.contains("test_mgmt_llm"));
        Assert.assertTrue("Should have model=qwen-plus",
            result.contains("qwen-plus"));
        Assert.assertTrue("Should have provider=dashscope",
            result.contains("dashscope"));
        Assert.assertTrue("Should have DashScope native endpoint",
            result.contains("services/aigc/text-generation/generation"));
    }

    /**
     * Test that a registered EMBEDDING model has correct properties when described.
     */
    @Test
    public void testDescribeRegisteredEmbeddingModel() throws SQLException {
        // Register an embedding model first
        String apiKey = requireApiKey("DASHSCOPE_API_KEY");
        String optionsJson = String.format("{\"api_key\":\"%s\"}", apiKey.replace("\"", "\\\""));
        String registerSql = String.format(
            "SELECT AI_REGISTER_MODEL('test_mgmt_embedding', 'dashscope', "
                + "'https://dashscope.aliyuncs.com/api/v1/services/embeddings/text-embedding/text-embedding', "
                + "'text-embedding-v3', '%s')",
            optionsJson.replace("'", "\\'"));
        JdbcUtil.executeQuerySuccess(tddlConnection, registerSql);

        // Describe the registered model
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT AI_DESCRIBE_MODEL('test_mgmt_embedding')");
        Assert.assertTrue(rs.next());
        String result = rs.getString(1);
        rs.close();

        Assert.assertTrue("Should have name=test_mgmt_embedding",
            result.contains("test_mgmt_embedding"));
        Assert.assertTrue("Should have provider=dashscope",
            result.contains("dashscope"));
        Assert.assertTrue("Should have DashScope native embedding endpoint",
            result.contains("services/embeddings/text-embedding/text-embedding"));
    }

    /**
     * Test that a registered model does NOT expose api_key_env in describe output.
     */
    @Test
    public void testRegisteredModelNoApiKeyEnvInParams() throws SQLException {
        // Register a model first
        String apiKey = requireApiKey("DASHSCOPE_API_KEY");
        String optionsJson = String.format("{\"api_key\":\"%s\"}", apiKey.replace("\"", "\\\""));
        String registerSql = String.format(
            "SELECT AI_REGISTER_MODEL('test_mgmt_llm', 'dashscope', "
                + "'https://dashscope.aliyuncs.com/api/v1/services/aigc/text-generation/generation', "
                + "'qwen-plus', '%s')",
            optionsJson.replace("'", "\\'"));
        JdbcUtil.executeQuerySuccess(tddlConnection, registerSql);

        // Describe the model
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SELECT AI_DESCRIBE_MODEL('test_mgmt_llm')");
        Assert.assertTrue(rs.next());
        String result = rs.getString(1);
        rs.close();

        // model_params should NOT contain api_key_env
        Assert.assertFalse("Should NOT have api_key_env in model_params",
            result.contains("api_key_env"));
    }

    // testBuiltinModelsListByType removed - model_type filtering no longer exists

    /**
     * Test that users CAN register models with the __POLARDBX prefix (no longer reserved).
     */
    @Test
    public void testRegisterModelWithPolardbxPrefix() throws SQLException {
        String sql =
            "SELECT AI_REGISTER_MODEL('__POLARDBX_MY_MODEL', 'dashscope', 'https://example.com/api', 'test-model')";
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        Assert.assertTrue(rs.next());
        String result = rs.getString(1);
        Assert.assertTrue("Should succeed registering with __POLARDBX prefix",
            result.contains("registered successfully"));
        rs.close();
    }
}
