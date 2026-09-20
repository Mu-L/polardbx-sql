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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;

/**
 * Integration tests for AI functions using the OpenAI provider format.
 *
 * <p>Verifies that AI functions work correctly when models are registered with
 * provider="openai" using OpenAI-compatible endpoints (e.g., DashScope's
 * compatible-mode endpoint, OpenAI itself, or any OpenAI-compatible service).
 *
 * <p>Tests cover:
 * <ul>
 *   <li>AI_PROMPT with multiple OpenAI provider models (qwen-plus, qwen-turbo, qwen-max)</li>
 *   <li>AI_EMBEDDING with OpenAI provider</li>
 *   <li>AI_CLASSIFY / AI_EXTRACT / AI_SUMMARIZE / AI_TEXT2SQL with OpenAI LLM</li>
 *   <li>Mixed registration: DashScope and OpenAI models coexisting</li>
 *   <li>OpenAI-specific options: temperature, max_tokens, system_prompt</li>
 * </ul>
 */
@NotThreadSafe
public class AiOpenAiProviderTest extends AiFunctionTestBase {

    @Before
    public void setUpModels() {
        registerModel(AiTestModelConfig.OPENAI_QWEN_PLUS);
        registerModel(AiTestModelConfig.OPENAI_EMBEDDING_V3);
    }

    // ==================== Basic LLM tests ====================

    /**
     * Test AI_PROMPT with OpenAI provider.
     */
    @Test
    public void testPromptOpenAiProvider() throws SQLException {
        String sql = String.format(
            "SELECT AI_PROMPT('Say hello in one word', '%s')",
            AiTestModelConfig.OPENAI_QWEN_PLUS.name);

        String result = executeAiFunction(sql);
        Assert.assertNotNull(result);
        Assert.assertFalse(result.trim().isEmpty());
    }

    /**
     * Test AI_PROMPT with OpenAI provider qwen-turbo model.
     */
    @Test
    public void testPromptOpenAiQwenTurbo() throws SQLException {
        registerModel(AiTestModelConfig.OPENAI_QWEN_TURBO);

        String sql = String.format(
            "SELECT AI_PROMPT('What is 1+1?', '%s')",
            AiTestModelConfig.OPENAI_QWEN_TURBO.name);

        String result = executeAiFunction(sql);
        Assert.assertNotNull(result);
        Assert.assertFalse(result.trim().isEmpty());
    }

    /**
     * Test AI_PROMPT with OpenAI provider qwen-max model.
     */
    @Test
    public void testPromptOpenAiQwenMax() throws SQLException {
        registerModel(AiTestModelConfig.OPENAI_QWEN_MAX);

        String sql = String.format(
            "SELECT AI_PROMPT('Say hi', '%s')",
            AiTestModelConfig.OPENAI_QWEN_MAX.name);

        String result = executeAiFunction(sql);
        Assert.assertNotNull(result);
        Assert.assertFalse(result.trim().isEmpty());
    }

    /**
     * Test AI_PROMPT with OpenAI provider and temperature option.
     */
    @Test
    public void testPromptOpenAiWithTemperature() throws SQLException {
        String options = "{\"temperature\": 0.1, \"max_tokens\": 50}";
        String sql = String.format(
            "SELECT AI_PROMPT('What is the capital of France?', '%s', '%s')",
            AiTestModelConfig.OPENAI_QWEN_PLUS.name, options);

        String result = executeAiFunction(sql);
        Assert.assertNotNull(result);
        Assert.assertFalse(result.trim().isEmpty());
    }

    /**
     * Test AI_PROMPT with OpenAI provider and system_prompt option.
     */
    @Test
    public void testPromptOpenAiWithSystemPrompt() throws SQLException {
        String options = "{\"system_prompt\": \"You are a concise assistant.\", \"max_tokens\": 30}";
        String sql = String.format(
            "SELECT AI_PROMPT('What is PolarDB-X?', '%s', '%s')",
            AiTestModelConfig.OPENAI_QWEN_PLUS.name, options);

        String result = executeAiFunction(sql);
        Assert.assertNotNull(result);
        Assert.assertFalse(result.trim().isEmpty());
    }

    // ==================== Embedding tests ====================

    /**
     * Test AI_EMBEDDING with OpenAI provider.
     */
    @Test
    public void testEmbeddingOpenAiProvider() throws SQLException {
        String sql = String.format(
            "SELECT AI_EMBEDDING('Hello world', '%s')",
            AiTestModelConfig.OPENAI_EMBEDDING_V3.name);

        String result = executeAiFunction(sql);
        Assert.assertNotNull(result);

        JSONArray vector = JSON.parseArray(result);
        Assert.assertNotNull("Embedding should be a JSON array", vector);
        Assert.assertTrue("Embedding should be non-empty", vector.size() > 0);
        Assert.assertTrue("Embedding elements should be numbers", vector.get(0) instanceof Number);
    }

    /**
     * Test AI_EMBEDDING with OpenAI provider and dimension option.
     */
    @Test
    public void testEmbeddingOpenAiWithDimension() throws SQLException {
        String options = "{\"dimension\": 256}";
        String sql = String.format(
            "SELECT AI_EMBEDDING('test text', '%s', '%s')",
            AiTestModelConfig.OPENAI_EMBEDDING_V3.name, options);

        String result = executeAiFunction(sql);
        Assert.assertNotNull(result);

        JSONArray vector = JSON.parseArray(result);
        Assert.assertNotNull(vector);
        Assert.assertEquals("Embedding dimension should be 256", 256, vector.size());
    }

    /**
     * Test AI_EMBEDDING with OpenAI provider produces different vectors for different text.
     */
    @Test
    public void testEmbeddingOpenAiDifferentVectors() throws SQLException {
        String sql1 = String.format(
            "SELECT AI_EMBEDDING('cloud database', '%s')",
            AiTestModelConfig.OPENAI_EMBEDDING_V3.name);
        String sql2 = String.format(
            "SELECT AI_EMBEDDING('blue sky weather', '%s')",
            AiTestModelConfig.OPENAI_EMBEDDING_V3.name);

        String result1 = executeAiFunction(sql1);
        String result2 = executeAiFunction(sql2);

        Assert.assertNotEquals("Different texts should produce different embeddings",
            result1, result2);
    }

    // ==================== Higher-level functions with OpenAI provider ====================

    /**
     * Test AI_CLASSIFY with OpenAI provider LLM.
     */
    @Test
    public void testClassifyOpenAiProvider() throws SQLException {
        String sql = String.format(
            "SELECT AI_CLASSIFY('I love this product, it works great!', "
                + "'[\"positive\", \"negative\", \"neutral\"]', '%s')",
            AiTestModelConfig.OPENAI_QWEN_PLUS.name);

        String result = executeAiFunction(sql);
        Assert.assertNotNull(result);
        Assert.assertFalse(result.trim().isEmpty());
    }

    /**
     * Test AI_EXTRACT with OpenAI provider LLM.
     */
    @Test
    public void testExtractOpenAiProvider() throws SQLException {
        String sql = String.format(
            "SELECT AI_EXTRACT('Contact: John, phone 13800138000', "
                + "'{\"name\":\"contact name\",\"phone\":\"phone number\"}', '%s')",
            AiTestModelConfig.OPENAI_QWEN_PLUS.name);

        String result = executeAiFunction(sql);
        Assert.assertNotNull(result);
        Assert.assertFalse(result.trim().isEmpty());
        // Result should be valid JSON
        Assert.assertNotNull(JSON.parseObject(result));
    }

    /**
     * Test AI_SUMMARIZE with OpenAI provider LLM.
     */
    @Test
    public void testSummarizeOpenAiProvider() throws SQLException {
        String text = "PolarDB-X is a cloud-native distributed SQL database developed by Alibaba. "
            + "It supports horizontal scaling, distributed transactions, and MySQL compatibility. "
            + "The system is widely used in e-commerce, finance, and gaming industries.";
        String sql = String.format(
            "SELECT AI_SUMMARIZE('%s', 100, '%s')",
            text, AiTestModelConfig.OPENAI_QWEN_PLUS.name);

        String result = executeAiFunction(sql);
        Assert.assertNotNull(result);
        Assert.assertFalse(result.trim().isEmpty());
    }

    /**
     * Test AI_SIMILARITY using OpenAI provider embedding model.
     */
    @Test
    public void testSimilarityWithOpenAiEmbedding() throws SQLException {
        // Pre-set default model for AI_SIMILARITY to OpenAI embedding
        try {
            tddlConnection.createStatement().execute(
                "SELECT AI_UPDATE_FUNCTION('AI_SIMILARITY', '"
                    + AiTestModelConfig.OPENAI_EMBEDDING_V3.name + "')");
        } catch (Exception e) {
            // ignore
        }

        String sql = String.format(
            "SELECT AI_SIMILARITY('cloud database', 'distributed SQL database', 'cosine', '%s')",
            AiTestModelConfig.OPENAI_EMBEDDING_V3.name);

        String result = executeAiFunction(sql);
        Assert.assertNotNull(result);
        double similarity = Double.parseDouble(result);
        Assert.assertTrue("Similarity should be in [0, 1]: " + similarity,
            similarity >= 0.0 && similarity <= 1.0);
    }

    // ==================== AI_RANK with OpenAI provider ====================
    // Note: AI_RANK with OpenAI provider requires a Cohere-compatible /rerank endpoint.
    // DashScope's compatible-mode does not currently expose a /rerank endpoint, so
    // OpenAI-provider rerank tests are not included here. The OpenAI rerank protocol
    // is verified by unit tests in OpenAiApiProviderTest (request body construction).

    // ==================== Unsupported functions on OpenAI provider ====================

    /**
     * Test AI_PARSE_DOCUMENT errors when called with an OpenAI provider model
     * (OpenAI protocol does not support doc_url content type).
     */
    @Test
    public void testParseDocumentOpenAiProviderUnsupported() {
        // Use the LLM model registered in setUp; parseDocument should reject openai provider
        executeAiFunctionExpectError(
            String.format(
                "SELECT AI_PARSE_DOCUMENT('https://example.com/doc.pdf', 'auto', '%s')",
                AiTestModelConfig.OPENAI_QWEN_PLUS.name),
            "not supported by the OpenAI protocol");
    }

    /**
     * Test AI_VL_EMBEDDING errors when called with an OpenAI provider model
     * (OpenAI protocol does not support multimodal embedding).
     */
    @Test
    public void testVlEmbeddingOpenAiProviderUnsupported() {
        executeAiFunctionExpectError(
            String.format(
                "SELECT AI_VL_EMBEDDING('https://example.com/image.jpg', '%s')",
                AiTestModelConfig.OPENAI_EMBEDDING_V3.name),
            "not supported by the OpenAI protocol");
    }

    // ==================== Mixed provider scenarios ====================

    /**
     * Test that DashScope and OpenAI provider models can coexist and be queried independently.
     */
    @Test
    public void testMixedProvidersCoexist() throws SQLException {
        // OpenAI model already registered in setUp
        // Register a DashScope model alongside
        registerModel(AiTestModelConfig.DASHSCOPE_QWEN_PLUS);

        // Call OpenAI provider model
        String openaiResult = executeAiFunction(String.format(
            "SELECT AI_PROMPT('Say A in one letter', '%s')",
            AiTestModelConfig.OPENAI_QWEN_PLUS.name));
        Assert.assertNotNull(openaiResult);
        Assert.assertFalse(openaiResult.trim().isEmpty());

        // Call DashScope provider model
        String dashscopeResult = executeAiFunction(String.format(
            "SELECT AI_PROMPT('Say B in one letter', '%s')",
            AiTestModelConfig.DASHSCOPE_QWEN_PLUS.name));
        Assert.assertNotNull(dashscopeResult);
        Assert.assertFalse(dashscopeResult.trim().isEmpty());
    }

    /**
     * Test SHOW AI MODEL displays both DashScope and OpenAI provider models with correct provider field.
     */
    @Test
    public void testShowAiModelShowsProvider() throws SQLException {
        registerModel(AiTestModelConfig.DASHSCOPE_QWEN_PLUS);

        java.sql.ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, "SHOW AI MODEL");
        boolean foundOpenAi = false;
        boolean foundDashscope = false;
        try {
            while (rs.next()) {
                String name = rs.getString("NAME");
                String provider = rs.getString("PROVIDER");
                if (AiTestModelConfig.OPENAI_QWEN_PLUS.name.equals(name)) {
                    Assert.assertEquals("openai", provider);
                    foundOpenAi = true;
                }
                if (AiTestModelConfig.DASHSCOPE_QWEN_PLUS.name.equals(name)) {
                    Assert.assertEquals("dashscope", provider);
                    foundDashscope = true;
                }
            }
        } finally {
            JdbcUtil.close(rs);
        }
        Assert.assertTrue("OpenAI provider model should be present", foundOpenAi);
        Assert.assertTrue("DashScope provider model should be present", foundDashscope);
    }

    /**
     * Test AI_DESCRIBE_MODEL on an OpenAI provider model returns correct provider info.
     */
    @Test
    public void testDescribeOpenAiModel() throws SQLException {
        String sql = String.format("SELECT AI_DESCRIBE_MODEL('%s')",
            AiTestModelConfig.OPENAI_QWEN_PLUS.name);

        String result = executeAiFunction(sql);
        Assert.assertNotNull(result);
        Assert.assertTrue("Result should mention provider 'openai'", result.contains("openai"));
        Assert.assertTrue("Result should contain model name",
            result.contains(AiTestModelConfig.OPENAI_QWEN_PLUS.name));
    }
}
