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
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runners.Parameterized;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Integration tests for AI_SIMILARITY function.
 *
 * <p>Tests cover:
 * <ul>
 *   <li>Vector-to-vector similarity (cosine, euclidean, dot)</li>
 *   <li>Text-to-text similarity via automatic embedding</li>
 *   <li>Complex table-based semantic search scenario</li>
 *   <li>Error handling</li>
 * </ul>
 */
@NotThreadSafe
public class AiSimilarityFunctionTest extends AiFunctionTestBase {

    private static final String TEST_TABLE = "ai_similarity_test_docs";

    private final AiTestModelConfig modelConfig;

    public AiSimilarityFunctionTest(AiTestModelConfig modelConfig) {
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
        // Set as default for AI_SIMILARITY so tests can call it without model name
        try {
            tddlConnection.createStatement().execute(
                "SELECT AI_UPDATE_FUNCTION('AI_SIMILARITY', '" + modelConfig.name + "')");
        } catch (Exception e) {
            // Ignore — will be caught by test failures if needed
        }
    }

    @After
    public void cleanUpTable() {
        try {
            JdbcUtil.executeUpdate(tddlConnection,
                "DROP TABLE IF EXISTS " + TEST_TABLE);
        } catch (Exception e) {
            // Ignore cleanup errors
        }
        // Clear default model for AI_SIMILARITY to keep clean state for other tests
        try {
            tddlConnection.createStatement().execute(
                "SELECT AI_UPDATE_FUNCTION('AI_SIMILARITY', NULL)");
        } catch (Throwable e) {
            // best effort cleanup
        }
    }

    // ==================== Vector-to-vector similarity ====================

    /**
     * Test cosine similarity with known vectors.
     * Vectors [1,0,0] and [0,1,0] are orthogonal, cosine = 0.
     */
    @Test
    public void testCosineOrthogonalVectors() throws SQLException {
        String sql = "SELECT AI_SIMILARITY('[1,0,0]', '[0,1,0]', 'cosine')";
        String result = executeAiFunction(sql);
        double similarity = Double.parseDouble(result);
        Assert.assertEquals("Orthogonal vectors should have cosine similarity 0", 0.0, similarity, 0.001);
    }

    /**
     * Test cosine similarity with identical vectors.
     * Identical vectors should have cosine similarity 1.
     */
    @Test
    public void testCosineIdenticalVectors() throws SQLException {
        String sql = "SELECT AI_SIMILARITY('[0.5, 0.3, 0.8]', '[0.5, 0.3, 0.8]', 'cosine')";
        String result = executeAiFunction(sql);
        double similarity = Double.parseDouble(result);
        Assert.assertEquals("Identical vectors should have cosine similarity 1", 1.0, similarity, 0.001);
    }

    /**
     * Test euclidean distance with known vectors.
     * Distance between [0,0] and [3,4] should be 5.
     */
    @Test
    public void testEuclideanDistance() throws SQLException {
        String sql = "SELECT AI_SIMILARITY('[0,0]', '[3,4]', 'euclidean')";
        String result = executeAiFunction(sql);
        double distance = Double.parseDouble(result);
        Assert.assertEquals("Euclidean distance of [0,0] and [3,4] should be 5", 5.0, distance, 0.001);
    }

    /**
     * Test dot product with known vectors.
     * [1,2,3] · [4,5,6] = 1*4 + 2*5 + 3*6 = 32
     */
    @Test
    public void testDotProduct() throws SQLException {
        String sql = "SELECT AI_SIMILARITY('[1,2,3]', '[4,5,6]', 'dot')";
        String result = executeAiFunction(sql);
        double dotProduct = Double.parseDouble(result);
        Assert.assertEquals("Dot product of [1,2,3] and [4,5,6] should be 32", 32.0, dotProduct, 0.001);
    }

    /**
     * Test default similarity type (should be cosine).
     */
    @Test
    public void testDefaultSimilarityType() throws SQLException {
        String sqlDefault = "SELECT AI_SIMILARITY('[1,2,3]', '[4,5,6]')";
        String sqlCosine = "SELECT AI_SIMILARITY('[1,2,3]', '[4,5,6]', 'cosine')";

        double resultDefault = Double.parseDouble(executeAiFunction(sqlDefault));
        double resultCosine = Double.parseDouble(executeAiFunction(sqlCosine));

        Assert.assertEquals("Default similarity type should be cosine",
            resultCosine, resultDefault, 0.0001);
    }

    // ==================== Text-to-text similarity ====================

    /**
     * Test text similarity: semantically similar texts should have high cosine similarity.
     */
    @Test
    public void testTextSimilaritySemanticallyClose() throws SQLException {
        String sql = String.format(
            "SELECT AI_SIMILARITY("
                + "'PolarDB-X is a distributed database', "
                + "'PolarDB-X is a cloud-native distributed SQL database', "
                + "'cosine', '%s')",
            modelConfig.name);

        String result = executeAiFunction(sql);
        double similarity = Double.parseDouble(result);
        Assert.assertTrue(
            "Semantically similar texts should have cosine similarity > 0.7, got: " + similarity,
            similarity > 0.7);
    }

    /**
     * Test text similarity: semantically different texts should have lower similarity.
     */
    @Test
    public void testTextSimilaritySemanticallyDifferent() throws SQLException {
        String sqlSimilar = String.format(
            "SELECT AI_SIMILARITY("
                + "'database performance optimization', "
                + "'SQL query tuning and indexing', "
                + "'cosine', '%s')",
            modelConfig.name);

        String sqlDifferent = String.format(
            "SELECT AI_SIMILARITY("
                + "'database performance optimization', "
                + "'chocolate cake recipe with strawberries', "
                + "'cosine', '%s')",
            modelConfig.name);

        double similarScore = Double.parseDouble(executeAiFunction(sqlSimilar));
        double differentScore = Double.parseDouble(executeAiFunction(sqlDifferent));

        Assert.assertTrue(
            "Related texts should have higher similarity than unrelated texts. "
                + "Related: " + similarScore + ", Unrelated: " + differentScore,
            similarScore > differentScore);
    }

    // ==================== Complex table-based semantic search ====================

    /**
     * End-to-end semantic search scenario:
     * 1. Create a documents table with title, content, and embedding columns
     * 2. Insert documents and generate embeddings via AI_EMBEDDING
     * 3. Use AI_SIMILARITY to find the most relevant document for a query
     * 4. Verify the top result is semantically correct
     */
    @Test
    public void testSemanticSearchWithTable() throws SQLException {
        // Step 1: Create table
        String createTableSql = "CREATE TABLE " + TEST_TABLE + " ("
            + "  id INT PRIMARY KEY AUTO_INCREMENT,"
            + "  title VARCHAR(200),"
            + "  content TEXT,"
            + "  embedding TEXT"
            + ")";
        JdbcUtil.executeUpdate(tddlConnection, createTableSql);

        // Step 2: Insert documents with embeddings
        String[] documents = {
            "PolarDB-X Architecture|PolarDB-X is a cloud-native distributed SQL database designed for horizontal scalability and high availability",
            "MySQL Performance Tuning|This guide covers indexing strategies, query optimization, and buffer pool configuration for MySQL databases",
            "Introduction to Machine Learning|Machine learning is a subset of artificial intelligence that enables systems to learn from data",
            "Cooking Italian Pasta|Learn how to make authentic Italian pasta with fresh ingredients and traditional techniques",
            "Distributed Transaction Processing|XA transactions and two-phase commit protocols ensure data consistency across distributed database nodes"
        };

        for (String doc : documents) {
            String[] parts = doc.split("\\|");
            String title = parts[0];
            String content = parts[1];
            String insertSql = String.format(
                "INSERT INTO %s (title, content, embedding) VALUES ('%s', '%s', AI_EMBEDDING('%s', '%s'))",
                TEST_TABLE,
                title.replace("'", "\\'"),
                content.replace("'", "\\'"),
                content.replace("'", "\\'"),
                modelConfig.name);
            JdbcUtil.executeUpdate(tddlConnection, insertSql);
        }

        // Step 3: Semantic search — query about distributed databases
        String searchQuery = "distributed database architecture and scalability";
        String searchSql = String.format(
            "SELECT title, "
                + "AI_SIMILARITY(embedding, AI_EMBEDDING('%s', '%s'), 'cosine') AS score "
                + "FROM %s "
                + "ORDER BY score DESC "
                + "LIMIT 3",
            searchQuery, modelConfig.name, TEST_TABLE);

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, searchSql);
        try {
            // Verify we get results
            Assert.assertTrue("Search should return results", rs.next());
            String topTitle = rs.getString("title");
            double topScore = rs.getDouble("score");

            // The top result should be about PolarDB-X or distributed transactions
            Assert.assertTrue(
                "Top result should be about databases, got: '" + topTitle + "' with score " + topScore,
                topTitle.contains("PolarDB") || topTitle.contains("Distributed"));

            // Top score should be reasonably high
            Assert.assertTrue(
                "Top similarity score should be > 0.5, got: " + topScore,
                topScore > 0.5);

            // Verify ordering: scores should be descending
            if (rs.next()) {
                double secondScore = rs.getDouble("score");
                Assert.assertTrue(
                    "Results should be ordered by similarity descending. "
                        + "First: " + topScore + ", Second: " + secondScore,
                    topScore >= secondScore);
            }
        } finally {
            JdbcUtil.close(rs);
        }

        // Step 4: Search for cooking — should rank the pasta document highest
        String cookingQuery = "how to cook food and recipes";
        String cookingSql = String.format(
            "SELECT title, "
                + "AI_SIMILARITY(embedding, AI_EMBEDDING('%s', '%s'), 'cosine') AS score "
                + "FROM %s "
                + "ORDER BY score DESC "
                + "LIMIT 1",
            cookingQuery, modelConfig.name, TEST_TABLE);

        ResultSet cookingRs = JdbcUtil.executeQuerySuccess(tddlConnection, cookingSql);
        try {
            Assert.assertTrue("Cooking search should return results", cookingRs.next());
            String cookingTopTitle = cookingRs.getString("title");
            Assert.assertTrue(
                "Cooking query should find the pasta document, got: '" + cookingTopTitle + "'",
                cookingTopTitle.contains("Pasta") || cookingTopTitle.contains("Cooking"));
        } finally {
            JdbcUtil.close(cookingRs);
        }
    }

    /**
     * Test similarity with WHERE clause filtering — find documents above a threshold.
     */
    @Test
    public void testSimilarityWithThresholdFilter() throws SQLException {
        // Create and populate table
        String createTableSql = "CREATE TABLE " + TEST_TABLE + " ("
            + "  id INT PRIMARY KEY AUTO_INCREMENT,"
            + "  title VARCHAR(200),"
            + "  embedding TEXT"
            + ")";
        JdbcUtil.executeUpdate(tddlConnection, createTableSql);

        String[] titles = {
            "Cloud Native Database",
            "Distributed SQL Engine",
            "Chocolate Cake Recipe"
        };

        for (String title : titles) {
            String insertSql = String.format(
                "INSERT INTO %s (title, embedding) VALUES ('%s', AI_EMBEDDING('%s', '%s'))",
                TEST_TABLE, title, title, modelConfig.name);
            JdbcUtil.executeUpdate(tddlConnection, insertSql);
        }

        // Query with threshold: only database-related docs should match
        String querySql = String.format(
            "SELECT title, "
                + "AI_SIMILARITY(embedding, AI_EMBEDDING('database technology', '%s'), 'cosine') AS score "
                + "FROM %s "
                + "HAVING score > 0.5 "
                + "ORDER BY score DESC",
            modelConfig.name, TEST_TABLE);

        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, querySql);
        try {
            int count = 0;
            while (rs.next()) {
                String title = rs.getString("title");
                double score = rs.getDouble("score");
                Assert.assertTrue(
                    "Filtered result '" + title + "' should have score > 0.5, got: " + score,
                    score > 0.5);
                Assert.assertFalse(
                    "Cooking recipe should not appear in database query results",
                    title.contains("Cake"));
                count++;
            }
            Assert.assertTrue("Should find at least one matching document", count > 0);
        } finally {
            JdbcUtil.close(rs);
        }
    }

    // ==================== Error handling ====================

    /**
     * Test AI_SIMILARITY with mismatched vector dimensions should fail.
     */
    @Test
    public void testMismatchedDimensions() {
        String sql = "SELECT AI_SIMILARITY('[1,2,3]', '[4,5]', 'cosine')";
        executeAiFunctionExpectError(sql, "dimensions do not match");
    }

    /**
     * Test AI_SIMILARITY with unsupported similarity type should fail.
     */
    @Test
    public void testUnsupportedSimilarityType() {
        String sql = "SELECT AI_SIMILARITY('[1,2]', '[3,4]', 'manhattan')";
        executeAiFunctionExpectError(sql, "unsupported similarity type");
    }

    /**
     * Test AI_SIMILARITY with non-existent model should fail.
     */
    @Test
    public void testNonExistentModel() {
        String sql = "SELECT AI_SIMILARITY('hello', 'world', 'cosine', 'non_existent_model_xyz')";
        executeAiFunctionExpectError(sql, "not found");
    }

    /**
     * Test AI_SIMILARITY with empty input should fail.
     */
    @Test
    public void testEmptyInput() {
        String sql = "SELECT AI_SIMILARITY('', '[1,2,3]')";
        executeAiFunctionExpectError(sql, "cannot be empty");
    }
}
