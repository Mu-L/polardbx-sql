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
import net.jcip.annotations.NotThreadSafe;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Integration tests for AI skill management UDFs.
 * Covers AI_REGISTER_SKILL, AI_LIST_SKILLS, AI_DESCRIBE_SKILL, AI_UPDATE_SKILL,
 * AI_DROP_SKILL, AI_GET_SKILL_PROMPT, AI_ADD_SKILL_REFERENCE, AI_GET_REFERENCE,
 * AI_REMOVE_SKILL_REFERENCE.
 */
@NotThreadSafe
public class Nl2sqlAgentSkillTest extends BaseTestCase {

    private static final String TEST_SKILL_NAME = "it_test_skill";
    private static final String TEST_SKILL_PROMPT = "You are a test skill. Answer questions about testing.";
    private static final String TEST_SKILL_DESC = "Integration test skill";
    private static final String TEST_REF_NAME = "test_ref";
    private static final String TEST_REF_CONTENT = "This is test reference content for integration testing.";

    private Connection tddlConnection;

    @Before
    public void setUp() {
        tddlConnection = getPolardbxConnection();
        cleanupTestSkill();
    }

    @After
    public void tearDown() {
        cleanupTestSkill();
    }

    private void cleanupTestSkill() {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SELECT AI_DROP_SKILL('" + TEST_SKILL_NAME + "')");
        } catch (Exception e) {
            // ignore if skill doesn't exist
        }
    }

    private String executeFunction(String sql) throws SQLException {
        try (Statement stmt = tddlConnection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            Assert.assertTrue("Expected at least one row", rs.next());
            return rs.getString(1);
        }
    }

    private void executeFunctionExpectError(String sql, String expectedMessage) {
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.executeQuery(sql);
            Assert.fail("Expected error but query succeeded: " + sql);
        } catch (SQLException e) {
            Assert.assertTrue("Error message should contain: " + expectedMessage
                    + ", but got: " + e.getMessage(),
                e.getMessage().contains(expectedMessage));
        }
    }

    // ==================== Register & List ====================

    @Test
    public void testRegisterSkill_basic() throws Exception {
        String result = executeFunction(
            "SELECT AI_REGISTER_SKILL('" + TEST_SKILL_NAME + "', '" + TEST_SKILL_PROMPT + "')");
        Assert.assertTrue(result.contains("registered successfully"));
    }

    @Test
    public void testRegisterSkill_withOptions() throws Exception {
        String result = executeFunction(
            "SELECT AI_REGISTER_SKILL('" + TEST_SKILL_NAME + "', '" + TEST_SKILL_PROMPT + "', "
                + "'{\"description\":\"" + TEST_SKILL_DESC + "\",\"priority\":50}')");
        Assert.assertTrue(result.contains("registered successfully"));
    }

    @Test
    public void testRegisterSkill_duplicate() throws Exception {
        executeFunction("SELECT AI_REGISTER_SKILL('" + TEST_SKILL_NAME + "', '" + TEST_SKILL_PROMPT + "')");
        executeFunctionExpectError(
            "SELECT AI_REGISTER_SKILL('" + TEST_SKILL_NAME + "', '" + TEST_SKILL_PROMPT + "')",
            "already exists");
    }

    @Test
    public void testRegisterSkill_overwriteBuiltin() {
        executeFunctionExpectError(
            "SELECT AI_REGISTER_SKILL('skill-guide', 'overwrite builtin')",
            "Cannot overwrite built-in skill");
    }

    @Test
    public void testRegisterSkill_emptyName() {
        executeFunctionExpectError(
            "SELECT AI_REGISTER_SKILL('', 'prompt')",
            "skill name is required");
    }

    @Test
    public void testRegisterSkill_emptyPrompt() {
        executeFunctionExpectError(
            "SELECT AI_REGISTER_SKILL('" + TEST_SKILL_NAME + "', '')",
            "skill prompt is required");
    }

    @Test
    public void testListSkills_includesTestSkill() throws Exception {
        executeFunction("SELECT AI_REGISTER_SKILL('" + TEST_SKILL_NAME + "', '" + TEST_SKILL_PROMPT + "')");
        String result = executeFunction("SELECT AI_LIST_SKILLS()");
        Assert.assertTrue("List should contain test skill", result.contains(TEST_SKILL_NAME));
    }

    @Test
    public void testListSkills_includesBuiltin() throws Exception {
        String result = executeFunction("SELECT AI_LIST_SKILLS()");
        Assert.assertTrue("List should contain builtin skill-guide", result.contains("skill-guide"));
    }

    // ==================== Describe & GetPrompt ====================

    @Test
    public void testDescribeSkill_basic() throws Exception {
        executeFunction("SELECT AI_REGISTER_SKILL('" + TEST_SKILL_NAME + "', '" + TEST_SKILL_PROMPT + "', "
            + "'{\"description\":\"" + TEST_SKILL_DESC + "\"}')");
        String result = executeFunction("SELECT AI_DESCRIBE_SKILL('" + TEST_SKILL_NAME + "')");
        Assert.assertTrue(result.contains(TEST_SKILL_NAME));
        Assert.assertTrue(result.contains(TEST_SKILL_DESC));
        Assert.assertTrue(result.contains(TEST_SKILL_PROMPT));
    }

    @Test
    public void testDescribeSkill_notFound() {
        executeFunctionExpectError(
            "SELECT AI_DESCRIBE_SKILL('nonexistent_skill_xyz')",
            "not found");
    }

    @Test
    public void testGetSkillPrompt_basic() throws Exception {
        executeFunction("SELECT AI_REGISTER_SKILL('" + TEST_SKILL_NAME + "', '" + TEST_SKILL_PROMPT + "', "
            + "'{\"description\":\"" + TEST_SKILL_DESC + "\"}')");
        String result = executeFunction("SELECT AI_GET_SKILL_PROMPT('" + TEST_SKILL_NAME + "')");
        Assert.assertTrue(result.contains(TEST_SKILL_NAME));
        Assert.assertTrue(result.contains(TEST_SKILL_PROMPT));
    }

    @Test
    public void testGetSkillPrompt_notFound() throws Exception {
        String result = executeFunction("SELECT AI_GET_SKILL_PROMPT('nonexistent_skill_xyz')");
        Assert.assertTrue(result.contains("not found"));
    }

    @Test
    public void testGetSkillPrompt_includesReferenceList() throws Exception {
        executeFunction("SELECT AI_REGISTER_SKILL('" + TEST_SKILL_NAME + "', '" + TEST_SKILL_PROMPT + "')");
        executeFunction("SELECT AI_ADD_SKILL_REFERENCE('" + TEST_SKILL_NAME + "', '" + TEST_REF_NAME + "', '"
            + TEST_REF_CONTENT + "')");
        String result = executeFunction("SELECT AI_GET_SKILL_PROMPT('" + TEST_SKILL_NAME + "')");
        Assert.assertTrue("Prompt should list references", result.contains(TEST_REF_NAME));
        Assert.assertTrue("Prompt should show AI_GET_REFERENCE usage", result.contains("AI_GET_REFERENCE"));
    }

    // ==================== Update ====================

    @Test
    public void testUpdateSkill_description() throws Exception {
        executeFunction("SELECT AI_REGISTER_SKILL('" + TEST_SKILL_NAME + "', '" + TEST_SKILL_PROMPT + "')");
        String result = executeFunction(
            "SELECT AI_UPDATE_SKILL('" + TEST_SKILL_NAME + "', '{\"description\":\"updated desc\"}')");
        Assert.assertTrue(result.contains("updated successfully"));

        String desc = executeFunction("SELECT AI_DESCRIBE_SKILL('" + TEST_SKILL_NAME + "')");
        Assert.assertTrue(desc.contains("updated desc"));
    }

    @Test
    public void testUpdateSkill_prompt() throws Exception {
        executeFunction("SELECT AI_REGISTER_SKILL('" + TEST_SKILL_NAME + "', '" + TEST_SKILL_PROMPT + "')");
        String result = executeFunction(
            "SELECT AI_UPDATE_SKILL('" + TEST_SKILL_NAME + "', '{\"prompt\":\"new prompt content\"}')");
        Assert.assertTrue(result.contains("updated successfully"));

        String desc = executeFunction("SELECT AI_DESCRIBE_SKILL('" + TEST_SKILL_NAME + "')");
        Assert.assertTrue(desc.contains("new prompt content"));
    }

    @Test
    public void testUpdateSkill_status() throws Exception {
        executeFunction("SELECT AI_REGISTER_SKILL('" + TEST_SKILL_NAME + "', '" + TEST_SKILL_PROMPT + "')");
        executeFunction(
            "SELECT AI_UPDATE_SKILL('" + TEST_SKILL_NAME + "', '{\"status\":\"INACTIVE\"}')");

        String desc = executeFunction("SELECT AI_DESCRIBE_SKILL('" + TEST_SKILL_NAME + "')");
        Assert.assertTrue(desc.contains("INACTIVE"));
    }

    @Test
    public void testUpdateSkill_priority() throws Exception {
        executeFunction("SELECT AI_REGISTER_SKILL('" + TEST_SKILL_NAME + "', '" + TEST_SKILL_PROMPT + "')");
        executeFunction(
            "SELECT AI_UPDATE_SKILL('" + TEST_SKILL_NAME + "', '{\"priority\":10}')");

        String desc = executeFunction("SELECT AI_DESCRIBE_SKILL('" + TEST_SKILL_NAME + "')");
        Assert.assertTrue(desc.contains("10"));
    }

    @Test
    public void testUpdateSkill_notFound() {
        executeFunctionExpectError(
            "SELECT AI_UPDATE_SKILL('nonexistent_skill_xyz', '{\"description\":\"test\"}')",
            "not found");
    }

    @Test
    public void testUpdateSkill_emptyOptions() {
        executeFunctionExpectError(
            "SELECT AI_UPDATE_SKILL('" + TEST_SKILL_NAME + "', '')",
            "options are required");
    }

    // ==================== Reference Management ====================

    @Test
    public void testAddReference_basic() throws Exception {
        executeFunction("SELECT AI_REGISTER_SKILL('" + TEST_SKILL_NAME + "', '" + TEST_SKILL_PROMPT + "')");
        String result = executeFunction(
            "SELECT AI_ADD_SKILL_REFERENCE('" + TEST_SKILL_NAME + "', '" + TEST_REF_NAME + "', '"
                + TEST_REF_CONTENT + "')");
        Assert.assertTrue(result.contains("added to skill"));
    }

    @Test
    public void testAddReference_skillNotFound() {
        executeFunctionExpectError(
            "SELECT AI_ADD_SKILL_REFERENCE('nonexistent_skill', 'ref', 'content')",
            "not found");
    }

    @Test
    public void testAddReference_emptyContent() throws Exception {
        executeFunction("SELECT AI_REGISTER_SKILL('" + TEST_SKILL_NAME + "', '" + TEST_SKILL_PROMPT + "')");
        executeFunctionExpectError(
            "SELECT AI_ADD_SKILL_REFERENCE('" + TEST_SKILL_NAME + "', 'ref', '')",
            "content is required");
    }

    @Test
    public void testGetReference_basic() throws Exception {
        executeFunction("SELECT AI_REGISTER_SKILL('" + TEST_SKILL_NAME + "', '" + TEST_SKILL_PROMPT + "')");
        executeFunction("SELECT AI_ADD_SKILL_REFERENCE('" + TEST_SKILL_NAME + "', '" + TEST_REF_NAME + "', '"
            + TEST_REF_CONTENT + "')");
        String result = executeFunction(
            "SELECT AI_GET_REFERENCE('" + TEST_SKILL_NAME + "', '" + TEST_REF_NAME + "')");
        Assert.assertEquals(TEST_REF_CONTENT, result);
    }

    @Test
    public void testGetReference_notFound() throws Exception {
        executeFunction("SELECT AI_REGISTER_SKILL('" + TEST_SKILL_NAME + "', '" + TEST_SKILL_PROMPT + "')");
        String result = executeFunction(
            "SELECT AI_GET_REFERENCE('" + TEST_SKILL_NAME + "', 'nonexistent_ref')");
        Assert.assertTrue(result.contains("not found"));
    }

    @Test
    public void testRemoveReference_basic() throws Exception {
        executeFunction("SELECT AI_REGISTER_SKILL('" + TEST_SKILL_NAME + "', '" + TEST_SKILL_PROMPT + "')");
        executeFunction("SELECT AI_ADD_SKILL_REFERENCE('" + TEST_SKILL_NAME + "', '" + TEST_REF_NAME + "', '"
            + TEST_REF_CONTENT + "')");
        String result = executeFunction(
            "SELECT AI_REMOVE_SKILL_REFERENCE('" + TEST_SKILL_NAME + "', '" + TEST_REF_NAME + "')");
        Assert.assertTrue(result.contains("removed from skill"));
    }

    @Test
    public void testRemoveReference_notFound() throws Exception {
        executeFunction("SELECT AI_REGISTER_SKILL('" + TEST_SKILL_NAME + "', '" + TEST_SKILL_PROMPT + "')");
        executeFunctionExpectError(
            "SELECT AI_REMOVE_SKILL_REFERENCE('" + TEST_SKILL_NAME + "', 'nonexistent_ref')",
            "not found");
    }

    @Test
    public void testReferenceLifecycle_full() throws Exception {
        executeFunction("SELECT AI_REGISTER_SKILL('" + TEST_SKILL_NAME + "', '" + TEST_SKILL_PROMPT + "')");

        String ref1 = "ref1";
        String ref2 = "ref2";
        String content1 = "content for ref1";
        String content2 = "content for ref2";

        executeFunction("SELECT AI_ADD_SKILL_REFERENCE('" + TEST_SKILL_NAME + "', '" + ref1 + "', '"
            + content1 + "')");
        executeFunction("SELECT AI_ADD_SKILL_REFERENCE('" + TEST_SKILL_NAME + "', '" + ref2 + "', '"
            + content2 + "')");

        String desc = executeFunction("SELECT AI_DESCRIBE_SKILL('" + TEST_SKILL_NAME + "')");
        Assert.assertTrue(desc.contains(ref1));
        Assert.assertTrue(desc.contains(ref2));

        String got1 = executeFunction("SELECT AI_GET_REFERENCE('" + TEST_SKILL_NAME + "', '" + ref1 + "')");
        Assert.assertEquals(content1, got1);

        executeFunction("SELECT AI_REMOVE_SKILL_REFERENCE('" + TEST_SKILL_NAME + "', '" + ref1 + "')");
        String descAfter = executeFunction("SELECT AI_DESCRIBE_SKILL('" + TEST_SKILL_NAME + "')");
        Assert.assertFalse(descAfter.contains(ref1));
        Assert.assertTrue(descAfter.contains(ref2));
    }

    // ==================== Drop ====================

    @Test
    public void testDropSkill_basic() throws Exception {
        executeFunction("SELECT AI_REGISTER_SKILL('" + TEST_SKILL_NAME + "', '" + TEST_SKILL_PROMPT + "')");
        String result = executeFunction("SELECT AI_DROP_SKILL('" + TEST_SKILL_NAME + "')");
        Assert.assertTrue(result.contains("dropped successfully"));
    }

    @Test
    public void testDropSkill_notFound() {
        executeFunctionExpectError(
            "SELECT AI_DROP_SKILL('nonexistent_skill_xyz')",
            "not found");
    }

    @Test
    public void testDropSkill_builtin() {
        executeFunctionExpectError(
            "SELECT AI_DROP_SKILL('skill-guide')",
            "Cannot drop built-in skill");
    }

    @Test
    public void testDropSkill_removesReferences() throws Exception {
        executeFunction("SELECT AI_REGISTER_SKILL('" + TEST_SKILL_NAME + "', '" + TEST_SKILL_PROMPT + "')");
        executeFunction("SELECT AI_ADD_SKILL_REFERENCE('" + TEST_SKILL_NAME + "', '" + TEST_REF_NAME + "', '"
            + TEST_REF_CONTENT + "')");
        executeFunction("SELECT AI_DROP_SKILL('" + TEST_SKILL_NAME + "')");

        executeFunctionExpectError(
            "SELECT AI_DESCRIBE_SKILL('" + TEST_SKILL_NAME + "')",
            "not found");
    }

    // ==================== Full Lifecycle ====================

    @Test
    public void testFullLifecycle() throws Exception {
        // Register
        String regResult = executeFunction(
            "SELECT AI_REGISTER_SKILL('" + TEST_SKILL_NAME + "', '" + TEST_SKILL_PROMPT + "', "
                + "'{\"description\":\"" + TEST_SKILL_DESC + "\",\"priority\":50}')");
        Assert.assertTrue(regResult.contains("registered successfully"));

        // List - should contain our skill
        String listResult = executeFunction("SELECT AI_LIST_SKILLS()");
        Assert.assertTrue(listResult.contains(TEST_SKILL_NAME));

        // Describe
        String descResult = executeFunction("SELECT AI_DESCRIBE_SKILL('" + TEST_SKILL_NAME + "')");
        Assert.assertTrue(descResult.contains(TEST_SKILL_NAME));
        Assert.assertTrue(descResult.contains(TEST_SKILL_DESC));

        // Get prompt
        String promptResult = executeFunction("SELECT AI_GET_SKILL_PROMPT('" + TEST_SKILL_NAME + "')");
        Assert.assertTrue(promptResult.contains(TEST_SKILL_PROMPT));

        // Add reference
        String addRefResult = executeFunction(
            "SELECT AI_ADD_SKILL_REFERENCE('" + TEST_SKILL_NAME + "', '" + TEST_REF_NAME + "', '"
                + TEST_REF_CONTENT + "')");
        Assert.assertTrue(addRefResult.contains("added to skill"));

        // Get reference
        String getRefResult = executeFunction(
            "SELECT AI_GET_REFERENCE('" + TEST_SKILL_NAME + "', '" + TEST_REF_NAME + "')");
        Assert.assertEquals(TEST_REF_CONTENT, getRefResult);

        // Update
        String updateResult = executeFunction(
            "SELECT AI_UPDATE_SKILL('" + TEST_SKILL_NAME + "', '{\"description\":\"updated\"}')");
        Assert.assertTrue(updateResult.contains("updated successfully"));

        // Remove reference
        String removeRefResult = executeFunction(
            "SELECT AI_REMOVE_SKILL_REFERENCE('" + TEST_SKILL_NAME + "', '" + TEST_REF_NAME + "')");
        Assert.assertTrue(removeRefResult.contains("removed from skill"));

        // Drop
        String dropResult = executeFunction("SELECT AI_DROP_SKILL('" + TEST_SKILL_NAME + "')");
        Assert.assertTrue(dropResult.contains("dropped successfully"));
    }
}
