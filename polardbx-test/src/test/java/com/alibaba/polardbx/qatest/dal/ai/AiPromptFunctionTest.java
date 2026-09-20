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
 * Parameterized integration tests for AI_PROMPT function.
 *
 * <p>Each test runs against every LLM model config defined in
 * {@link AiTestModelConfig#llmModels()}, covering multiple providers
 * (DashScope, OpenAI, etc.).
 *
 * <h3>How to extend:</h3>
 * <ul>
 *   <li><b>Add a new LLM model:</b> Add it to {@link AiTestModelConfig#llmModels()}</li>
 *   <li><b>Add a new test case:</b> Add a new {@code @Test} method below</li>
 * </ul>
 */
@NotThreadSafe
public class AiPromptFunctionTest extends AiFunctionTestBase {

    private final AiTestModelConfig modelConfig;

    public AiPromptFunctionTest(AiTestModelConfig modelConfig) {
        this.modelConfig = modelConfig;
    }

    @Parameterized.Parameters(name = "{0}")
    public static Collection<Object[]> data() {
        List<Object[]> params = new ArrayList<>();
        for (AiTestModelConfig config : AiTestModelConfig.llmModels()) {
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
     * Test basic AI_PROMPT call with explicit model name.
     */
    @Test
    public void testBasicPrompt() throws SQLException {
        String sql = String.format(
            "SELECT AI_PROMPT('Say hello in one word', '%s')",
            modelConfig.name);

        String result = executeAiFunction(sql);
        Assert.assertNotNull("AI_PROMPT should return a non-null result", result);
        Assert.assertFalse("AI_PROMPT should return a non-empty result", result.trim().isEmpty());
    }

    /**
     * Test AI_PROMPT with options parameter (temperature, max_tokens).
     */
    @Test
    public void testPromptWithOptions() throws SQLException {
        String options = "{\"temperature\": 0.1, \"max_tokens\": 50}";
        String sql = String.format(
            "SELECT AI_PROMPT('What is 1+1? Answer with just the number.', '%s', '%s')",
            modelConfig.name, options);

        String result = executeAiFunction(sql);
        Assert.assertNotNull(result);
        Assert.assertFalse(result.trim().isEmpty());
    }

    /**
     * Test AI_PROMPT with system_prompt in options.
     */
    @Test
    public void testPromptWithSystemPrompt() throws SQLException {
        String options = "{\"system_prompt\": \"You are a helpful math tutor. Answer concisely.\", \"max_tokens\": 50}";
        String sql = String.format(
            "SELECT AI_PROMPT('What is 2+3?', '%s', '%s')",
            modelConfig.name, options);

        String result = executeAiFunction(sql);
        Assert.assertNotNull(result);
        Assert.assertFalse(result.trim().isEmpty());
    }

    // ==================== Error handling ====================

    /**
     * Test AI_PROMPT with empty prompt should fail.
     */
    @Test
    public void testPromptEmptyInput() {
        String sql = String.format("SELECT AI_PROMPT('', '%s')", modelConfig.name);
        executeAiFunctionExpectError(sql, "prompt cannot be empty");
    }

    /**
     * Test AI_PROMPT with non-existent model should fail.
     */
    @Test
    public void testPromptNonExistentModel() {
        String sql = "SELECT AI_PROMPT('hello', 'non_existent_model_xyz_12345')";
        executeAiFunctionExpectError(sql, "not found");
    }

    /**
     * Test AI_PROMPT with invalid options JSON should fail.
     */
    @Test
    public void testPromptInvalidOptionsJson() {
        String sql = String.format(
            "SELECT AI_PROMPT('hello', '%s', 'not-valid-json')",
            modelConfig.name);
        executeAiFunctionExpectError(sql, "invalid options JSON");
    }
}
