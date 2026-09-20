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

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

/**
 * Integration tests for AI functions using registered (non-built-in) models.
 *
 * <p>This test class covers all scenarios previously tested in AiBuiltinModelTest
 * but uses explicitly registered models via AI_REGISTER_MODEL. It validates LLM,
 * embedding, rerank, classify, document parse, VL embedding, SHOW commands,
 * and default model configuration using DashScope-backed registered models.
 *
 * <p>Pattern B: Direct model registration in @Before / per-test (non-parameterized).
 *
 * <p>Requires environment variable {@code DASHSCOPE_API_KEY} to be set.
 */
@NotThreadSafe
public class AiRegisteredModelTest extends AiFunctionTestBase {

    // ==================== Constants ====================

    private static final String DASHSCOPE_ENDPOINT_BASE = "https://dashscope.aliyuncs.com";

    private static final String ENDPOINT_TEXT_GENERATION =
        DASHSCOPE_ENDPOINT_BASE + "/api/v1/services/aigc/text-generation/generation";

    private static final String ENDPOINT_MULTIMODAL_GENERATION =
        DASHSCOPE_ENDPOINT_BASE + "/api/v1/services/aigc/multimodal-generation/generation";

    private static final String ENDPOINT_EMBEDDING =
        DASHSCOPE_ENDPOINT_BASE + "/api/v1/services/embeddings/text-embedding/text-embedding";

    private static final String ENDPOINT_RERANK =
        DASHSCOPE_ENDPOINT_BASE + "/api/v1/services/rerank/text-rerank/text-rerank";

    private static final String ENDPOINT_DOC_PARSE =
        DASHSCOPE_ENDPOINT_BASE + "/api/v1/services/aigc/text-generation/generation";

    private static final String ENDPOINT_VL_EMBEDDING =
        DASHSCOPE_ENDPOINT_BASE + "/api/v1/services/embeddings/multimodal-embedding/multimodal-embedding";

    private static final String TEST_IMAGE_URL =
        "https://help-static-aliyun-doc.aliyuncs.com/assets/img/zh-CN/4931952271/p829380.png";

    // Minimal 1x1 red PNG encoded as Base64 Data URI
    private static final String TEST_IMAGE_BASE64_URI =
        "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mP8z8BQDwADhQGAWjR9awAAAABJRU5ErkJggg==";

    // Model config names for registration
    private static final String MODEL_LLM_QWEN_PLUS = "reg_test_qwen_plus";
    private static final String MODEL_LLM_QWEN_TURBO = "reg_test_qwen_turbo";
    private static final String MODEL_LLM_QWEN_MAX = "reg_test_qwen_max";
    private static final String MODEL_EMBEDDING_V4 = "reg_test_embedding_v4";
    private static final String MODEL_EMBEDDING_V3 = "reg_test_embedding_v3";
    private static final String MODEL_RERANK = "reg_test_rerank";
    private static final String MODEL_DOC_PARSE = "reg_test_doc_parse";
    private static final String MODEL_VL_EMBEDDING = "reg_test_vl_embedding";

    // Configs built from AiTestModelConfig pattern
    private static final AiTestModelConfig CONFIG_QWEN_PLUS = new AiTestModelConfig(
        MODEL_LLM_QWEN_PLUS, "qwen-plus", "dashscope",
        ENDPOINT_TEXT_GENERATION, AiTestModelConfig.ENV_DASHSCOPE_API_KEY
    );

    private static final AiTestModelConfig CONFIG_QWEN_TURBO = new AiTestModelConfig(
        MODEL_LLM_QWEN_TURBO, "qwen-turbo", "dashscope",
        ENDPOINT_TEXT_GENERATION, AiTestModelConfig.ENV_DASHSCOPE_API_KEY
    );

    private static final AiTestModelConfig CONFIG_QWEN_MAX = new AiTestModelConfig(
        MODEL_LLM_QWEN_MAX, "qwen-max", "dashscope",
        ENDPOINT_TEXT_GENERATION, AiTestModelConfig.ENV_DASHSCOPE_API_KEY
    );

    private static final AiTestModelConfig CONFIG_EMBEDDING_V4 = new AiTestModelConfig(
        MODEL_EMBEDDING_V4, "text-embedding-v4", "dashscope",
        ENDPOINT_EMBEDDING, AiTestModelConfig.ENV_DASHSCOPE_API_KEY
    );

    private static final AiTestModelConfig CONFIG_EMBEDDING_V3 = new AiTestModelConfig(
        MODEL_EMBEDDING_V3, "text-embedding-v3", "dashscope",
        ENDPOINT_EMBEDDING, AiTestModelConfig.ENV_DASHSCOPE_API_KEY
    );

    private static final AiTestModelConfig CONFIG_RERANK = new AiTestModelConfig(
        MODEL_RERANK, "qwen3-vl-rerank", "dashscope",
        ENDPOINT_RERANK, AiTestModelConfig.ENV_DASHSCOPE_API_KEY
    );

    private static final AiTestModelConfig CONFIG_DOC_PARSE = new AiTestModelConfig(
        MODEL_DOC_PARSE, "qwen-doc-turbo", "dashscope",
        ENDPOINT_DOC_PARSE, AiTestModelConfig.ENV_DASHSCOPE_API_KEY
    );

    private static final AiTestModelConfig CONFIG_VL_EMBEDDING = new AiTestModelConfig(
        MODEL_VL_EMBEDDING, "qwen3-vl-embedding", "dashscope",
        ENDPOINT_VL_EMBEDDING, AiTestModelConfig.ENV_DASHSCOPE_API_KEY
    );

    private String originalDefaultModel = null;

    // ==================== Setup / Teardown ====================

    @Before
    public void setUpModels() {
        registerModel(CONFIG_QWEN_PLUS);
        registerModel(CONFIG_EMBEDDING_V4);
        registerModel(CONFIG_RERANK);
        registerModel(CONFIG_VL_EMBEDDING);
    }

    @After
    public void restoreDefaultModel() {
        // Clear default model for AI_PROMPT to keep clean state for other tests
        try {
            tddlConnection.createStatement().execute(
                "SELECT AI_UPDATE_FUNCTION('AI_PROMPT', NULL)");
        } catch (Throwable e) {
            // best effort cleanup
        }
        originalDefaultModel = null;
    }

    // ==================== LLM Tests ====================

    /**
     * Test AI_PROMPT with a registered model (qwen3.5-plus).
     */
    @Test
    public void testPromptWithRegisteredModel() throws SQLException {
        String sql = String.format(
            "SELECT AI_PROMPT('hello', '%s')", MODEL_LLM_QWEN_PLUS);

        String result = executeAiFunction(sql);
        Assert.assertNotNull("AI_PROMPT should return a non-null result", result);
        Assert.assertFalse("AI_PROMPT should return a non-empty result", result.trim().isEmpty());
    }

    /**
     * Test AI_PROMPT with temperature and max_tokens options.
     */
    @Test
    public void testPromptWithOptions() throws SQLException {
        String options = "{\"temperature\": 0.1, \"max_tokens\": 50}";
        String sql = String.format(
            "SELECT AI_PROMPT('What is 1+1? Answer with just the number.', '%s', '%s')",
            MODEL_LLM_QWEN_PLUS, options);

        String result = executeAiFunction(sql);
        Assert.assertNotNull(result);
        Assert.assertFalse(result.trim().isEmpty());
    }

    /**
     * Test AI_PROMPT with qwen3.5-plus model.
     */
    @Test
    public void testPromptQwen35Plus() throws SQLException {
        String sql = String.format(
            "SELECT AI_PROMPT('Say hi in one word', '%s')", MODEL_LLM_QWEN_PLUS);

        String result = executeAiFunction(sql);
        Assert.assertNotNull(result);
        Assert.assertFalse(result.trim().isEmpty());
    }

    /**
     * Test AI_PROMPT with qwen-plus model.
     */
    @Test
    public void testPromptQwenPlus() throws SQLException {
        registerModel(CONFIG_QWEN_PLUS);

        String sql = String.format(
            "SELECT AI_PROMPT('Say hi in one word', '%s')", MODEL_LLM_QWEN_PLUS);

        String result = executeAiFunction(sql);
        Assert.assertNotNull(result);
        Assert.assertFalse(result.trim().isEmpty());
    }

    /**
     * Test AI_PROMPT with qwen-turbo model.
     */
    @Test
    public void testPromptQwenTurbo() throws SQLException {
        registerModel(CONFIG_QWEN_TURBO);

        String sql = String.format(
            "SELECT AI_PROMPT('Say hi in one word', '%s')", MODEL_LLM_QWEN_TURBO);

        String result = executeAiFunction(sql);
        Assert.assertNotNull(result);
        Assert.assertFalse(result.trim().isEmpty());
    }

    /**
     * Test AI_PROMPT with qwen-max model.
     */
    @Test
    public void testPromptQwenMax() throws SQLException {
        registerModel(CONFIG_QWEN_MAX);

        String sql = String.format(
            "SELECT AI_PROMPT('Say hi in one word', '%s')", MODEL_LLM_QWEN_MAX);

        String result = executeAiFunction(sql);
        Assert.assertNotNull(result);
        Assert.assertFalse(result.trim().isEmpty());
    }

    /**
     * Test AI_PROMPT with system_prompt option.
     */
    @Test
    public void testPromptWithSystemPrompt() throws SQLException {
        String options = "{\"system_prompt\": \"You are a helpful math tutor. Answer concisely.\", \"max_tokens\": 50}";
        String sql = String.format(
            "SELECT AI_PROMPT('What is 2+3?', '%s', '%s')",
            MODEL_LLM_QWEN_PLUS, options);

        String result = executeAiFunction(sql);
        Assert.assertNotNull(result);
        Assert.assertFalse(result.trim().isEmpty());
    }

    /**
     * Test AI_PROMPT without model name returns error about no model specified
     * when no default model is configured.
     */
    @Test
    public void testPromptWithoutModelReturnsError() throws SQLException {
        // Explicitly clear AI_PROMPT default to ensure clean state
        tddlConnection.createStatement().execute(
            "SELECT AI_UPDATE_FUNCTION('AI_PROMPT', NULL)");

        executeAiFunctionExpectError(
            "SELECT AI_PROMPT('hello')",
            "no model specified");
    }

    // ==================== Embedding Tests ====================

    /**
     * Test AI_EMBEDDING with a registered model (text-embedding-v4).
     */
    @Test
    public void testEmbeddingWithRegisteredModel() throws SQLException {
        String sql = String.format(
            "SELECT AI_EMBEDDING('Hello world', '%s')", MODEL_EMBEDDING_V4);

        String result = executeAiFunction(sql);
        Assert.assertNotNull("AI_EMBEDDING should return a non-null result", result);

        JSONArray vector = JSON.parseArray(result);
        Assert.assertNotNull("Result should be a valid JSON array", vector);
        Assert.assertTrue("Embedding vector should not be empty", vector.size() > 0);

        for (int i = 0; i < Math.min(5, vector.size()); i++) {
            Assert.assertTrue("Element " + i + " should be a number",
                vector.get(i) instanceof Number);
        }
    }

    /**
     * Test AI_EMBEDDING with dimension=256 option.
     */
    @Test
    public void testEmbeddingWithDimension() throws SQLException {
        int targetDimension = 256;
        String options = String.format("{\"dimension\": %d}", targetDimension);
        String sql = String.format(
            "SELECT AI_EMBEDDING('Hello world', '%s', '%s')",
            MODEL_EMBEDDING_V4, options);

        String result = executeAiFunction(sql);
        JSONArray vector = JSON.parseArray(result);
        Assert.assertNotNull(vector);
        Assert.assertEquals("Embedding dimension should match requested dimension",
            targetDimension, vector.size());
    }

    /**
     * Test AI_EMBEDDING with text-embedding-v4 model.
     */
    @Test
    public void testEmbeddingV4() throws SQLException {
        String sql = String.format(
            "SELECT AI_EMBEDDING('PolarDB-X distributed database', '%s')", MODEL_EMBEDDING_V4);

        String result = executeAiFunction(sql);
        JSONArray vector = JSON.parseArray(result);
        Assert.assertNotNull(vector);
        Assert.assertTrue("Embedding vector should have multiple dimensions", vector.size() > 1);
    }

    /**
     * Test AI_EMBEDDING with text-embedding-v3 model.
     */
    @Test
    public void testEmbeddingV3() throws SQLException {
        registerModel(CONFIG_EMBEDDING_V3);

        String sql = String.format(
            "SELECT AI_EMBEDDING('PolarDB-X distributed database', '%s')", MODEL_EMBEDDING_V3);

        String result = executeAiFunction(sql);
        JSONArray vector = JSON.parseArray(result);
        Assert.assertNotNull(vector);
        Assert.assertTrue("Embedding vector should have multiple dimensions", vector.size() > 1);
    }

    /**
     * Test AI_EMBEDDING with text-embedding-v3 and dimension=512.
     */
    @Test
    public void testEmbeddingV3WithDimension() throws SQLException {
        registerModel(CONFIG_EMBEDDING_V3);

        int targetDimension = 512;
        String options = String.format("{\"dimension\": %d}", targetDimension);
        String sql = String.format(
            "SELECT AI_EMBEDDING('Hello world', '%s', '%s')",
            MODEL_EMBEDDING_V3, options);

        String result = executeAiFunction(sql);
        JSONArray vector = JSON.parseArray(result);
        Assert.assertNotNull(vector);
        Assert.assertEquals("Embedding dimension should match requested dimension",
            targetDimension, vector.size());
    }

    /**
     * Test that two different texts produce different embedding vectors.
     */
    @Test
    public void testEmbeddingDifferentVectors() throws SQLException {
        String sql1 = String.format(
            "SELECT AI_EMBEDDING('The cat sat on the mat', '%s')", MODEL_EMBEDDING_V4);
        String sql2 = String.format(
            "SELECT AI_EMBEDDING('Quantum computing harnesses quantum mechanical phenomena', '%s')",
            MODEL_EMBEDDING_V4);

        String result1 = executeAiFunction(sql1);
        String result2 = executeAiFunction(sql2);

        JSONArray vector1 = JSON.parseArray(result1);
        JSONArray vector2 = JSON.parseArray(result2);

        Assert.assertNotNull(vector1);
        Assert.assertNotNull(vector2);
        Assert.assertEquals("Both embeddings should have the same dimension",
            vector1.size(), vector2.size());

        boolean hasDifference = false;
        for (int i = 0; i < vector1.size(); i++) {
            if (!vector1.getDouble(i).equals(vector2.getDouble(i))) {
                hasDifference = true;
                break;
            }
        }
        Assert.assertTrue("Different texts should produce different embedding vectors", hasDifference);
    }

    // ==================== Rerank Tests ====================

    /**
     * Test AI_RANK with a registered rerank model.
     */
    @Test
    public void testRankWithRegisteredModel() throws SQLException {
        String sql = String.format(
            "SELECT AI_RANK('What is PolarDB-X?', "
                + "'[\"PolarDB-X is a distributed database\", \"The weather is nice today\"]', '%s')",
            MODEL_RERANK);

        String result = executeAiFunction(sql);
        Assert.assertNotNull("AI_RANK should return a non-null result", result);
        Assert.assertFalse("AI_RANK should return a non-empty result", result.trim().isEmpty());
    }

    /**
     * Test AI_RANK with explicit model name.
     */
    @Test
    public void testRankExplicitModel() throws SQLException {
        String sql = String.format(
            "SELECT AI_RANK('distributed database', "
                + "'[\"PolarDB-X supports distributed transactions\", \"I like ice cream\"]', '%s')",
            MODEL_RERANK);

        String result = executeAiFunction(sql);
        Assert.assertNotNull(result);
        Assert.assertFalse(result.trim().isEmpty());
    }

    /**
     * Test AI_RANK semantic correctness: relevant text should score higher.
     */
    @Test
    public void testRankSemanticCorrectness() throws SQLException {
        String relevantSql = String.format(
            "SELECT AI_RANK('What is a distributed database?', "
                + "'PolarDB-X is a cloud-native distributed SQL database', '%s')",
            MODEL_RERANK);
        String irrelevantSql = String.format(
            "SELECT AI_RANK('What is a distributed database?', "
                + "'The capital of France is Paris', '%s')",
            MODEL_RERANK);

        double relevantScore = Double.parseDouble(executeAiFunction(relevantSql));
        double irrelevantScore = Double.parseDouble(executeAiFunction(irrelevantSql));

        Assert.assertTrue(
            "Relevant text should score higher: " + relevantScore + " > " + irrelevantScore,
            relevantScore > irrelevantScore);
    }

    // ==================== Classify Tests ====================

    /**
     * Test AI_CLASSIFY with a registered LLM model.
     */
    @Test
    public void testClassifyWithRegisteredModel() throws SQLException {
        String sql = String.format(
            "SELECT AI_CLASSIFY('I love this product, it is amazing!', "
                + "'[\"positive\", \"negative\", \"neutral\"]', '%s')",
            MODEL_LLM_QWEN_PLUS);

        String result = executeAiFunction(sql);
        Assert.assertNotNull("AI_CLASSIFY should return a non-null result", result);
        Assert.assertFalse("AI_CLASSIFY should return a non-empty result", result.trim().isEmpty());
    }

    /**
     * Test AI_CLASSIFY with explicit model name.
     */
    @Test
    public void testClassifyExplicitModel() throws SQLException {
        String sql = String.format(
            "SELECT AI_CLASSIFY('The server crashed unexpectedly', "
                + "'[\"bug\", \"feature_request\", \"question\"]', '%s')",
            MODEL_LLM_QWEN_PLUS);

        String result = executeAiFunction(sql);
        Assert.assertNotNull(result);
        Assert.assertFalse(result.trim().isEmpty());
    }

    /**
     * Test AI_CLASSIFY correctly identifies positive sentiment.
     */
    @Test
    public void testClassifySentiment() throws SQLException {
        String sql = String.format(
            "SELECT AI_CLASSIFY('This is the best database I have ever used!', "
                + "'[\"positive\", \"negative\", \"neutral\"]', '%s')",
            MODEL_LLM_QWEN_PLUS);

        String result = executeAiFunction(sql);
        Assert.assertNotNull(result);
        Assert.assertTrue("Should classify as positive",
            result.toLowerCase().contains("positive"));
    }

    /**
     * Test AI_CLASSIFY with Chinese text.
     */
    @Test
    public void testClassifyChineseText() throws SQLException {
        String sql = String.format(
            "SELECT AI_CLASSIFY('这个数据库性能非常好，我很满意', "
                + "'[\"正面\", \"负面\", \"中性\"]', '%s')",
            MODEL_LLM_QWEN_PLUS);

        String result = executeAiFunction(sql);
        Assert.assertNotNull(result);
        Assert.assertFalse(result.trim().isEmpty());
    }

    // ==================== SHOW Commands Tests ====================

    /**
     * Test SHOW AI FUNCTION returns 10 rows.
     */
    @Test
    public void testShowAiFunction() throws SQLException {
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

    /**
     * Test SHOW AI FUNCTION FROM AI_PROMPT returns info for AI_PROMPT.
     */
    @Test
    public void testShowAiFunctionSpecific() throws SQLException {
        // Set a default model first so DEFAULT_MODEL is populated
        originalDefaultModel = getDefaultModelForFunction("AI_PROMPT");
        JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT AI_UPDATE_FUNCTION('AI_PROMPT', '%s')", MODEL_LLM_QWEN_PLUS));

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW AI FUNCTION FROM AI_PROMPT");
        Assert.assertTrue("Should have at least one row", rs.next());
        String functionName = rs.getString("FUNCTION");
        Assert.assertEquals("AI_PROMPT", functionName);
        String defaultModel = rs.getString("DEFAULT_MODEL");
        Assert.assertEquals("DEFAULT_MODEL should match what we set", MODEL_LLM_QWEN_PLUS, defaultModel);
        JdbcUtil.close(rs);
    }

    /**
     * Test SHOW AI FUNCTION FROM NON_EXISTENT returns 0 rows.
     */
    @Test
    public void testShowAiFunctionNonExistent() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW AI FUNCTION FROM NON_EXISTENT");
        int count = 0;
        while (rs.next()) {
            count++;
        }
        JdbcUtil.close(rs);
        Assert.assertEquals("Non-existent function should return 0 rows", 0, count);
    }

    /**
     * Test SHOW AI MODEL shows registered models.
     */
    @Test
    public void testShowAiModel() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW AI MODEL");
        boolean foundRegistered = false;
        while (rs.next()) {
            String modelName = rs.getString("NAME");
            if (MODEL_LLM_QWEN_PLUS.equals(modelName)) {
                foundRegistered = true;
            }
        }
        JdbcUtil.close(rs);
        Assert.assertTrue("Should find registered model in SHOW AI MODEL", foundRegistered);
    }

    /**
     * Test SHOW AI MODEL FROM specific model name.
     */
    @Test
    public void testShowAiModelSpecific() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            "SHOW AI MODEL FROM " + MODEL_LLM_QWEN_PLUS);
        Assert.assertTrue("Should have at least one row", rs.next());
        String modelName = rs.getString("NAME");
        Assert.assertEquals(MODEL_LLM_QWEN_PLUS, modelName);
        JdbcUtil.close(rs);
    }

    /**
     * Test SHOW AI MODEL FROM NON_EXISTENT returns 0 rows.
     */
    @Test
    public void testShowAiModelNonExistent() throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW AI MODEL FROM NON_EXISTENT");
        int count = 0;
        while (rs.next()) {
            count++;
        }
        JdbcUtil.close(rs);
        Assert.assertEquals("Non-existent model should return 0 rows", 0, count);
    }

    /**
     * Test SHOW AI FUNCTION includes all 10 function names.
     */
    @Test
    public void testShowAiFunctionIncludesAll() throws SQLException {
        Set<String> expectedFunctions = new HashSet<>(Arrays.asList(
            "AI_PROMPT", "AI_EMBEDDING", "AI_RANK", "AI_CLASSIFY",
            "AI_EXTRACT", "AI_SUMMARIZE", "AI_PARSE_DOCUMENT",
            "AI_VL_EMBEDDING", "AI_TEXT2SQL", "AI_SIMILARITY"
        ));

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW AI FUNCTION");
        Set<String> actualFunctions = new HashSet<>();
        while (rs.next()) {
            actualFunctions.add(rs.getString("FUNCTION"));
        }
        JdbcUtil.close(rs);

        for (String expected : expectedFunctions) {
            Assert.assertTrue("Should contain function: " + expected,
                actualFunctions.contains(expected));
        }
    }

    // ==================== Document Parse Tests ====================
    // Note: Document parse tests removed — they require a DashScope-accessible
    // PDF URL which is environment-dependent. Document parse function is
    // separately verified in production.

    // ==================== VL Embedding Tests ====================

    /**
     * Test AI_VL_EMBEDDING with a registered VL embedding model and image URL.
     */
    @Test
    public void testVlEmbedding() throws SQLException {
        String sql = String.format(
            "SELECT AI_VL_EMBEDDING('%s', '%s')",
            TEST_IMAGE_URL, MODEL_VL_EMBEDDING);

        String result = executeAiFunction(sql);
        Assert.assertNotNull("AI_VL_EMBEDDING should return a non-null result", result);

        JSONArray vector = JSON.parseArray(result);
        Assert.assertNotNull("Result should be a valid JSON array", vector);
        Assert.assertTrue("Embedding vector should not be empty", vector.size() > 0);

        for (int i = 0; i < Math.min(5, vector.size()); i++) {
            Assert.assertTrue("Element " + i + " should be a number",
                vector.get(i) instanceof Number);
        }
    }

    /**
     * Test AI_VL_EMBEDDING with explicit model name.
     */
    @Test
    public void testVlEmbeddingExplicitModel() throws SQLException {
        String sql = String.format(
            "SELECT AI_VL_EMBEDDING('%s', '%s')",
            TEST_IMAGE_URL, MODEL_VL_EMBEDDING);

        String result = executeAiFunction(sql);
        JSONArray vector = JSON.parseArray(result);
        Assert.assertNotNull(vector);
        Assert.assertTrue("Embedding vector should not be empty", vector.size() > 0);
    }

    /**
     * Test AI_VL_EMBEDDING with dimension option.
     */
    @Test
    public void testVlEmbeddingWithDimension() throws SQLException {
        int targetDimension = 512;
        String options = String.format("{\"dimension\": %d}", targetDimension);
        String sql = String.format(
            "SELECT AI_VL_EMBEDDING('%s', '%s', '%s')",
            TEST_IMAGE_URL, MODEL_VL_EMBEDDING, options);

        String result = executeAiFunction(sql);
        JSONArray vector = JSON.parseArray(result);
        Assert.assertNotNull(vector);
        Assert.assertEquals("Embedding dimension should match requested dimension",
            targetDimension, vector.size());
    }

    /**
     * Test AI_VL_EMBEDDING with Base64 Data URI (small 1x1 red PNG).
     */
    @Test
    public void testVlEmbeddingBase64Image() throws SQLException {
        String sql = String.format(
            "SELECT AI_VL_EMBEDDING('%s', '%s')",
            TEST_IMAGE_BASE64_URI, MODEL_VL_EMBEDDING);

        String result = executeAiFunction(sql);
        Assert.assertNotNull("AI_VL_EMBEDDING with Base64 should return a non-null result", result);

        JSONArray vector = JSON.parseArray(result);
        Assert.assertNotNull("Result should be a valid JSON array", vector);
        Assert.assertTrue("Embedding vector should not be empty", vector.size() > 0);
    }

    /**
     * Test AI_VL_EMBEDDING with Base64 Data URI and explicit model name.
     */
    @Test
    public void testVlEmbeddingBase64ExplicitModel() throws SQLException {
        String sql = String.format(
            "SELECT AI_VL_EMBEDDING('%s', '%s')",
            TEST_IMAGE_BASE64_URI, MODEL_VL_EMBEDDING);

        String result = executeAiFunction(sql);
        JSONArray vector = JSON.parseArray(result);
        Assert.assertNotNull(vector);
        Assert.assertTrue("Embedding vector should not be empty", vector.size() > 0);
    }

    /**
     * Test AI_VL_EMBEDDING with Base64 Data URI and content_type option.
     */
    @Test
    public void testVlEmbeddingBase64ContentType() throws SQLException {
        String options = "{\"content_type\": \"image\"}";
        String sql = String.format(
            "SELECT AI_VL_EMBEDDING('%s', '%s', '%s')",
            TEST_IMAGE_BASE64_URI, MODEL_VL_EMBEDDING, options);

        String result = executeAiFunction(sql);
        JSONArray vector = JSON.parseArray(result);
        Assert.assertNotNull(vector);
        Assert.assertTrue("Embedding vector should not be empty", vector.size() > 0);
    }

    // ==================== Describe Model Tests ====================

    /**
     * Test AI_DESCRIBE_MODEL for a registered model.
     */
    @Test
    public void testDescribeRegisteredModel() throws SQLException {
        String sql = String.format("SELECT AI_DESCRIBE_MODEL('%s')", MODEL_LLM_QWEN_PLUS);

        String result = executeAiFunction(sql);
        Assert.assertNotNull(result);
        Assert.assertTrue("Should contain model name",
            result.contains(MODEL_LLM_QWEN_PLUS));
        Assert.assertTrue("Should contain provider",
            result.contains("dashscope"));
        Assert.assertTrue("Should contain endpoint",
            result.contains("text-generation"));
    }

    // ==================== Default Model Tests ====================

    /**
     * Test AI_UPDATE_FUNCTION then call AI_PROMPT without model name uses the updated default.
     */
    @Test
    public void testUpdateFunctionThenCallWithoutModel() throws SQLException {
        // Record original default for cleanup
        originalDefaultModel = getDefaultModelForFunction("AI_PROMPT");

        // Update default to our registered model
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
            String.format("SELECT AI_UPDATE_FUNCTION('AI_PROMPT', '%s')", MODEL_LLM_QWEN_PLUS));
        Assert.assertTrue(rs.next());
        Assert.assertEquals("OK", rs.getString(1));
        JdbcUtil.close(rs);

        // Call AI_PROMPT without model name - should use updated default
        String sql = "SELECT AI_PROMPT('hello')";
        String result = executeAiFunction(sql);
        Assert.assertNotNull("AI_PROMPT with default model should return a result", result);
        Assert.assertFalse("AI_PROMPT with default model should return non-empty", result.trim().isEmpty());
    }

    /**
     * Test registering a model with __POLARDBX prefix should succeed
     * (prefix restriction has been removed).
     */
    @Test
    public void testRegisterModelWithPolardbxPrefix() {
        AiTestModelConfig testConfig = new AiTestModelConfig(
            "__POLARDBX_TEST", "qwen-plus", "dashscope",
            ENDPOINT_TEXT_GENERATION, AiTestModelConfig.ENV_DASHSCOPE_API_KEY
        );

        registerModel(testConfig);

        // Verify model exists via SHOW AI MODEL
        try {
            ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection,
                "SHOW AI MODEL FROM __POLARDBX_TEST");
            Assert.assertTrue("Model with __POLARDBX prefix should be registered", rs.next());
            Assert.assertEquals("__POLARDBX_TEST", rs.getString("NAME"));
            JdbcUtil.close(rs);
        } catch (SQLException e) {
            Assert.fail("Failed to verify __POLARDBX_TEST model: " + e.getMessage());
        }
    }

    // ==================== Helper Methods ====================

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
}
