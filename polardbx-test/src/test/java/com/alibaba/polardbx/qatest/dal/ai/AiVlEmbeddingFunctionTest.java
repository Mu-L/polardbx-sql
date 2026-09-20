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

import java.sql.SQLException;

/**
 * Integration tests for AI_VL_EMBEDDING function with text input support.
 *
 * <p>Verifies that AI_VL_EMBEDDING can embed plain text (in addition to images/videos)
 * into the same multimodal semantic space, enabling cross-modal similarity search
 * (e.g., searching images by text description).
 *
 * <p>Uses the DashScope qwen3-vl-embedding model which supports text, image, and video
 * content types in a unified embedding space.
 *
 * <p>Requires environment variable {@code DASHSCOPE_API_KEY} to be set.
 */
@NotThreadSafe
public class AiVlEmbeddingFunctionTest extends AiFunctionTestBase {

    private static final AiTestModelConfig VL_EMBEDDING_MODEL = AiTestModelConfig.DASHSCOPE_QWEN3_VL_EMBEDDING;

    private static final String TEST_IMAGE_URL =
        "https://help-static-aliyun-doc.aliyuncs.com/assets/img/zh-CN/4931952271/p829380.png";

    @Before
    public void setUpModel() {
        registerModel(VL_EMBEDDING_MODEL);
    }

    // ==================== Text embedding basics ====================

    /**
     * Test AI_VL_EMBEDDING with plain text input - core text support test.
     */
    @Test
    public void testVlEmbeddingTextInput() throws SQLException {
        String sql = String.format(
            "SELECT AI_VL_EMBEDDING('PolarDB-X is a distributed database', '%s')",
            VL_EMBEDDING_MODEL.name);

        String result = executeAiFunction(sql);
        Assert.assertNotNull("AI_VL_EMBEDDING with text should return non-null", result);

        JSONArray vector = JSON.parseArray(result);
        Assert.assertNotNull("Result should be a valid JSON array", vector);
        Assert.assertTrue("Embedding vector should not be empty", vector.size() > 0);

        for (int i = 0; i < Math.min(5, vector.size()); i++) {
            Assert.assertTrue("Element " + i + " should be a number",
                vector.get(i) instanceof Number);
        }
    }

    /**
     * Test AI_VL_EMBEDDING with explicit content_type=text option.
     */
    @Test
    public void testVlEmbeddingTextWithExplicitContentType() throws SQLException {
        String options = "{\"content_type\": \"text\"}";
        String sql = String.format(
            "SELECT AI_VL_EMBEDDING('Hello world', '%s', '%s')",
            VL_EMBEDDING_MODEL.name, options);

        String result = executeAiFunction(sql);
        JSONArray vector = JSON.parseArray(result);
        Assert.assertNotNull("Result should be a valid JSON array", vector);
        Assert.assertTrue("Embedding vector should not be empty", vector.size() > 0);
    }

    /**
     * Test AI_VL_EMBEDDING with Chinese text.
     */
    @Test
    public void testVlEmbeddingChineseText() throws SQLException {
        String sql = String.format(
            "SELECT AI_VL_EMBEDDING('PolarDB-X 是一款云原生分布式数据库', '%s')",
            VL_EMBEDDING_MODEL.name);

        String result = executeAiFunction(sql);
        JSONArray vector = JSON.parseArray(result);
        Assert.assertNotNull(vector);
        Assert.assertTrue("Embedding vector should not be empty", vector.size() > 0);
    }

    /**
     * Test AI_VL_EMBEDDING text with dimension option.
     */
    @Test
    public void testVlEmbeddingTextWithDimension() throws SQLException {
        int targetDimension = 512;
        String options = String.format("{\"dimension\": %d}", targetDimension);
        String sql = String.format(
            "SELECT AI_VL_EMBEDDING('Hello world', '%s', '%s')",
            VL_EMBEDDING_MODEL.name, options);

        String result = executeAiFunction(sql);
        JSONArray vector = JSON.parseArray(result);
        Assert.assertNotNull(vector);
        Assert.assertEquals("Embedding dimension should match requested dimension",
            targetDimension, vector.size());
    }

    // ==================== Different texts produce different vectors ====================

    /**
     * Test that different texts produce different VL embedding vectors.
     */
    @Test
    public void testVlEmbeddingDifferentTexts() throws SQLException {
        String sql1 = String.format(
            "SELECT AI_VL_EMBEDDING('a cute cat sitting on a sofa', '%s')",
            VL_EMBEDDING_MODEL.name);
        String sql2 = String.format(
            "SELECT AI_VL_EMBEDDING('quantum computing algorithms', '%s')",
            VL_EMBEDDING_MODEL.name);

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
        Assert.assertTrue("Different texts should produce different embeddings", hasDifference);
    }

    // ==================== Cross-modal: text and image in same space ====================

    /**
     * Test that text and image embeddings have the same dimension,
     * confirming they share the same semantic space.
     */
    @Test
    public void testVlEmbeddingTextAndImageSameDimension() throws SQLException {
        String textSql = String.format(
            "SELECT AI_VL_EMBEDDING('a red flower in a garden', '%s')",
            VL_EMBEDDING_MODEL.name);
        String imageSql = String.format(
            "SELECT AI_VL_EMBEDDING('%s', '%s')",
            TEST_IMAGE_URL, VL_EMBEDDING_MODEL.name);

        String textResult = executeAiFunction(textSql);
        String imageResult = executeAiFunction(imageSql);

        JSONArray textVector = JSON.parseArray(textResult);
        JSONArray imageVector = JSON.parseArray(imageResult);

        Assert.assertNotNull(textVector);
        Assert.assertNotNull(imageVector);
        Assert.assertEquals(
            "Text and image embeddings should have the same dimension for cross-modal search",
            textVector.size(), imageVector.size());
    }

    // ==================== Auto-detection of content type ====================

    /**
     * Test that plain text is auto-detected as text content type (no URL prefix).
     */
    @Test
    public void testVlEmbeddingAutoDetectText() throws SQLException {
        // This string does not start with http:// or data: so should be auto-detected as text
        String sql = String.format(
            "SELECT AI_VL_EMBEDDING('This is plain text, not a URL', '%s')",
            VL_EMBEDDING_MODEL.name);

        String result = executeAiFunction(sql);
        JSONArray vector = JSON.parseArray(result);
        Assert.assertNotNull("Auto-detected text should produce valid embedding", vector);
        Assert.assertTrue("Embedding should not be empty", vector.size() > 0);
    }

    /**
     * Test that an image URL is still correctly auto-detected as image content type.
     */
    @Test
    public void testVlEmbeddingAutoDetectImageUrl() throws SQLException {
        String sql = String.format(
            "SELECT AI_VL_EMBEDDING('%s', '%s')",
            TEST_IMAGE_URL, VL_EMBEDDING_MODEL.name);

        String result = executeAiFunction(sql);
        JSONArray vector = JSON.parseArray(result);
        Assert.assertNotNull("Image URL should produce valid embedding", vector);
        Assert.assertTrue("Embedding should not be empty", vector.size() > 0);
    }

    // ==================== Error handling ====================

    /**
     * Test AI_VL_EMBEDDING with empty content should fail.
     */
    @Test
    public void testVlEmbeddingEmptyContent() {
        String sql = String.format("SELECT AI_VL_EMBEDDING('', '%s')", VL_EMBEDDING_MODEL.name);
        executeAiFunctionExpectError(sql, "content cannot be empty");
    }

    /**
     * Test AI_VL_EMBEDDING with invalid content_type option should fail.
     */
    @Test
    public void testVlEmbeddingInvalidContentType() {
        String sql = String.format(
            "SELECT AI_VL_EMBEDDING('hello', '%s', '{\"content_type\": \"audio\"}')",
            VL_EMBEDDING_MODEL.name);
        executeAiFunctionExpectError(sql, "invalid content_type");
    }

    /**
     * Test AI_VL_EMBEDDING with non-existent model should fail.
     */
    @Test
    public void testVlEmbeddingNonExistentModel() {
        String sql = "SELECT AI_VL_EMBEDDING('hello', 'non_existent_model_xyz_12345')";
        executeAiFunctionExpectError(sql, "not found");
    }
}
