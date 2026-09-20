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
import net.jcip.annotations.NotThreadSafe;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Parameterized integration tests for AI_EMBEDDING function.
 *
 * <p>Each test runs against every EMBEDDING model config defined in
 * {@link AiTestModelConfig#embeddingModels()}, covering multiple providers.
 *
 * <h3>How to extend:</h3>
 * <ul>
 *   <li><b>Add a new embedding model:</b> Add it to {@link AiTestModelConfig#embeddingModels()}</li>
 *   <li><b>Add a new test case:</b> Add a new {@code @Test} method below</li>
 * </ul>
 */
@NotThreadSafe
public class AiEmbeddingFunctionTest extends AiFunctionTestBase {

    private final AiTestModelConfig modelConfig;

    public AiEmbeddingFunctionTest(AiTestModelConfig modelConfig) {
        this.modelConfig = modelConfig;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        List<Object[]> params = new ArrayList<>();
        for (AiTestModelConfig config : AiTestModelConfig.embeddingModels()) {
            params.add(new Object[] {config});
        }
        return params;
    }

    @Before
    public void setUpModel() {
        registerModel(modelConfig);
    }

    // ==================== Basic functionality ====================

    /**
     * Test basic AI_EMBEDDING call with explicit model name.
     */
    @Test
    public void testBasicEmbedding() throws SQLException {
        String sql = String.format(
            "SELECT AI_EMBEDDING('Hello world', '%s')",
            modelConfig.name);

        String result = executeAiFunction(sql);
        Assert.assertNotNull("AI_EMBEDDING should return a non-null result", result);

        // Result should be a valid JSON array of numbers
        JSONArray vector = JSON.parseArray(result);
        Assert.assertNotNull("Result should be a valid JSON array", vector);
        Assert.assertTrue("Embedding vector should not be empty", vector.size() > 0);

        // Each element should be a number
        for (int i = 0; i < vector.size(); i++) {
            Assert.assertNotNull("Element " + i + " should not be null", vector.get(i));
            Assert.assertTrue("Element " + i + " should be a number",
                vector.get(i) instanceof Number);
        }
    }

    /**
     * Test AI_EMBEDDING with a longer text input.
     */
    @Test
    public void testEmbeddingLongerText() throws SQLException {
        String sql = String.format(
            "SELECT AI_EMBEDDING('PolarDB-X is a cloud-native distributed SQL database "
                + "that provides horizontal scalability, distributed transactions, "
                + "and MySQL compatibility.', '%s')",
            modelConfig.name);

        String result = executeAiFunction(sql);
        JSONArray vector = JSON.parseArray(result);
        Assert.assertNotNull(vector);
        Assert.assertTrue("Embedding vector should have multiple dimensions", vector.size() > 1);
    }

    /**
     * Test AI_EMBEDDING with dimension option.
     */
    @Test
    public void testEmbeddingWithDimension() throws SQLException {
        int targetDimension = 256;
        String options = String.format("{\"dimension\": %d}", targetDimension);
        String sql = String.format(
            "SELECT AI_EMBEDDING('Hello world', '%s', '%s')",
            modelConfig.name, options);

        String result = executeAiFunction(sql);
        JSONArray vector = JSON.parseArray(result);
        Assert.assertNotNull(vector);
        Assert.assertEquals("Embedding dimension should match requested dimension",
            targetDimension, vector.size());
    }

    /**
     * Test that different texts produce different embeddings.
     */
    @Test
    public void testDifferentTextsProduceDifferentEmbeddings() throws SQLException {
        String sql1 = String.format(
            "SELECT AI_EMBEDDING('The cat sat on the mat', '%s')",
            modelConfig.name);
        String sql2 = String.format(
            "SELECT AI_EMBEDDING('Quantum computing harnesses quantum mechanical phenomena', '%s')",
            modelConfig.name);

        String result1 = executeAiFunction(sql1);
        String result2 = executeAiFunction(sql2);

        JSONArray vector1 = JSON.parseArray(result1);
        JSONArray vector2 = JSON.parseArray(result2);

        Assert.assertNotNull(vector1);
        Assert.assertNotNull(vector2);
        Assert.assertEquals("Both embeddings should have the same dimension",
            vector1.size(), vector2.size());

        // At least some elements should differ
        boolean hasDifference = false;
        for (int i = 0; i < vector1.size(); i++) {
            if (!vector1.getDouble(i).equals(vector2.getDouble(i))) {
                hasDifference = true;
                break;
            }
        }
        Assert.assertTrue("Different texts should produce different embedding vectors", hasDifference);
    }

    // ==================== Error handling ====================

    /**
     * Test AI_EMBEDDING with empty text should fail.
     */
    @Test
    public void testEmbeddingEmptyInput() {
        String sql = String.format("SELECT AI_EMBEDDING('', '%s')", modelConfig.name);
        executeAiFunctionExpectError(sql, "text cannot be empty");
    }

    /**
     * Test AI_EMBEDDING with non-existent model should fail.
     */
    @Test
    public void testEmbeddingNonExistentModel() {
        String sql = "SELECT AI_EMBEDDING('hello', 'non_existent_model_xyz_12345')";
        executeAiFunctionExpectError(sql, "not found");
    }

    /**
     * Test AI_EMBEDDING with invalid options JSON should fail.
     */
    @Test
    public void testEmbeddingInvalidOptionsJson() {
        String sql = String.format(
            "SELECT AI_EMBEDDING('hello', '%s', 'not-valid-json')",
            modelConfig.name);
        executeAiFunctionExpectError(sql, "invalid options JSON");
    }
}
