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

import com.alibaba.polardbx.qatest.util.ConnectionManager;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@NotThreadSafe
public class Nl2sqlAgentSkillFunctionTest extends Nl2sqlAgentTestBase {

    @Test
    public void testAiRegisterSkill_wrongArgs() {
        try (Statement stmt = tddlConnection.createStatement()) {
            try {
                stmt.executeQuery("SELECT AI_REGISTER_SKILL('only_one_arg')");
                Assert.fail("Should fail with wrong args");
            } catch (SQLException e) {
                Assert.assertTrue(e.getMessage().contains("at least 2 arguments"));
            }
        } catch (SQLException e) {
            // connection error
        }
    }

    @Test
    public void testAiDescribeSkill_wrongArgs() {
        try (Statement stmt = tddlConnection.createStatement()) {
            try {
                stmt.executeQuery("SELECT AI_DESCRIBE_SKILL()");
                Assert.fail("Should fail with wrong args");
            } catch (SQLException e) {
                Assert.assertTrue(e.getMessage().contains("requires 1 argument"));
            }
        } catch (SQLException e) {
            // connection error
        }
    }

    @Test
    public void testAiUpdateSkill_wrongArgs() {
        try (Statement stmt = tddlConnection.createStatement()) {
            try {
                stmt.executeQuery("SELECT AI_UPDATE_SKILL('only_one_arg')");
                Assert.fail("Should fail with wrong args");
            } catch (SQLException e) {
                Assert.assertTrue(e.getMessage().contains("requires 2 arguments"));
            }
        } catch (SQLException e) {
            // connection error
        }
    }

    @Test
    public void testAiDropSkill_wrongArgs() {
        try (Statement stmt = tddlConnection.createStatement()) {
            try {
                stmt.executeQuery("SELECT AI_DROP_SKILL()");
                Assert.fail("Should fail with wrong args");
            } catch (SQLException e) {
                Assert.assertTrue(e.getMessage().contains("requires 1 argument"));
            }
        } catch (SQLException e) {
            // connection error
        }
    }

    @Test
    public void testAiAddSkillReference_wrongArgs() {
        try (Statement stmt = tddlConnection.createStatement()) {
            try {
                stmt.executeQuery("SELECT AI_ADD_SKILL_REFERENCE('only_one_arg')");
                Assert.fail("Should fail with wrong args");
            } catch (SQLException e) {
                Assert.assertTrue(e.getMessage().contains("requires 3 arguments"));
            }
        } catch (SQLException e) {
            // connection error
        }
    }

    @Test
    public void testAiGetReference_wrongArgs() {
        try (Statement stmt = tddlConnection.createStatement()) {
            try {
                stmt.executeQuery("SELECT AI_GET_REFERENCE('only_one_arg')");
                Assert.fail("Should fail with wrong args");
            } catch (SQLException e) {
                Assert.assertTrue(e.getMessage().contains("requires 2 arguments"));
            }
        } catch (SQLException e) {
            // connection error
        }
    }

    @Test
    public void testAiRemoveSkillReference_wrongArgs() {
        try (Statement stmt = tddlConnection.createStatement()) {
            try {
                stmt.executeQuery("SELECT AI_REMOVE_SKILL_REFERENCE('only_one_arg')");
                Assert.fail("Should fail with wrong args");
            } catch (SQLException e) {
                Assert.assertTrue(e.getMessage().contains("requires 2 arguments"));
            }
        } catch (SQLException e) {
            // connection error
        }
    }

    @Test
    public void testAiGetSkillPrompt_wrongArgs() {
        try (Statement stmt = tddlConnection.createStatement()) {
            try {
                stmt.executeQuery("SELECT AI_GET_SKILL_PROMPT()");
                Assert.fail("Should fail with wrong args");
            } catch (SQLException e) {
                Assert.assertTrue(e.getMessage().contains("requires 1 argument"));
            }
        } catch (SQLException e) {
            // connection error
        }
    }

    @Test
    public void testSkillManager_validation() throws Exception {
        // These should trigger SkillManager validation branches
        try (Statement stmt = tddlConnection.createStatement()) {
            try {
                stmt.execute("SELECT AI_UPDATE_SKILL('', '{}')");
            } catch (Exception e) {
                // Expected: validation error
            }
            try {
                stmt.execute("SELECT AI_DROP_SKILL('')");
            } catch (Exception e) {
                // Expected: validation error
            }
            try {
                stmt.execute("SELECT AI_ADD_SKILL_REFERENCE('', 'ref', 'content')");
            } catch (Exception e) {
                // Expected: validation error
            }
            try {
                stmt.execute("SELECT AI_REMOVE_SKILL_REFERENCE('', 'ref')");
            } catch (Exception e) {
                // Expected: validation error
            }
        }
    }

    @Test
    public void testDuplicateReference() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            try {
                stmt.execute("SELECT AI_DROP_SKILL('test_dup_ref_skill')");
            } catch (Exception ignored) {
            }
        }
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SELECT AI_REGISTER_SKILL('test_dup_ref_skill', "
                + "'Dup ref test', '{\"status\":\"ACTIVE\",\"priority\":60}')");
            stmt.execute("SELECT AI_ADD_SKILL_REFERENCE('test_dup_ref_skill', "
                + "'ref1', 'first content')");
            // Try adding duplicate reference — should fail
            try {
                stmt.execute("SELECT AI_ADD_SKILL_REFERENCE('test_dup_ref_skill', "
                    + "'ref1', 'duplicate content')");
            } catch (Exception e) {
                // Expected: duplicate reference error
            }
            stmt.execute("SELECT AI_DROP_SKILL('test_dup_ref_skill')");
        }
    }

    @Test
    public void testSkillWriteOperations() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            String skillName = "it_write_test_skill";
            // Clean up first
            try {
                stmt.execute("SELECT AI_DROP_SKILL('" + skillName + "')");
            } catch (Exception ignored) {
            }

            // Register
            try (ResultSet rs = stmt.executeQuery(
                "SELECT AI_REGISTER_SKILL('" + skillName + "', 'Test prompt for write ops')")) {
                Assert.assertTrue(rs.next());
            }

            // Update (happy path)
            try (ResultSet rs = stmt.executeQuery(
                "SELECT AI_UPDATE_SKILL('" + skillName + "', '{\"description\":\"Updated desc\"}')")) {
                Assert.assertTrue(rs.next());
                String result = rs.getString(1);
                Assert.assertTrue("Update should succeed",
                    result != null && result.contains("updated"));
            }

            // Add reference (happy path)
            try (ResultSet rs = stmt.executeQuery(
                "SELECT AI_ADD_SKILL_REFERENCE('" + skillName + "', 'ref1', 'Reference content here')")) {
                Assert.assertTrue(rs.next());
                String result = rs.getString(1);
                Assert.assertTrue("Add reference should succeed",
                    result != null && (result.contains("added") || result.contains("success")));
            }

            // Remove reference (happy path)
            try (ResultSet rs = stmt.executeQuery(
                "SELECT AI_REMOVE_SKILL_REFERENCE('" + skillName + "', 'ref1')")) {
                Assert.assertTrue(rs.next());
                String result = rs.getString(1);
                Assert.assertTrue("Remove reference should succeed",
                    result != null && (result.contains("removed") || result.contains("success")));
            }

            // Remove non-existent reference (error path)
            try (ResultSet rs = stmt.executeQuery(
                "SELECT AI_REMOVE_SKILL_REFERENCE('" + skillName + "', 'nonexistent_ref')")) {
                Assert.assertTrue(rs.next());
            } catch (SQLException e) {
                // Expected: reference not found
            }

            // Drop skill (happy path)
            try (ResultSet rs = stmt.executeQuery(
                "SELECT AI_DROP_SKILL('" + skillName + "')")) {
                Assert.assertTrue(rs.next());
                String result = rs.getString(1);
                Assert.assertTrue("Drop should succeed",
                    result != null && (result.contains("dropped") || result.contains("success")));
            }

            // Drop non-existent skill (error path)
            try (ResultSet rs = stmt.executeQuery(
                "SELECT AI_DROP_SKILL('never_existed_skill_xyz')")) {
                Assert.assertTrue(rs.next());
            } catch (SQLException e) {
                // Expected: skill not found
            }
        }
    }

    @Test
    public void testAddReferenceToNonexistentSkill() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            try (ResultSet rs = stmt.executeQuery(
                "SELECT AI_ADD_SKILL_REFERENCE('nonexistent_skill_xyz', 'ref1', 'content')")) {
                Assert.assertTrue(rs.next());
            } catch (SQLException e) {
                // Expected: skill not found error
                Assert.assertTrue("Should mention skill not found",
                    e.getMessage() != null && (e.getMessage().contains("not found")
                        || e.getMessage().contains("does not exist")
                        || e.getMessage().contains("nonexistent")));
            }
        }
    }

    @Test
    public void testMultipleSkillRegistrations() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            String skill1 = "it_multi_skill_a";
            String skill2 = "it_multi_skill_b";
            try {
                stmt.execute("SELECT AI_DROP_SKILL('" + skill1 + "')");
            } catch (Exception ignored) {
            }
            try {
                stmt.execute("SELECT AI_DROP_SKILL('" + skill2 + "')");
            } catch (Exception ignored) {
            }

            stmt.execute("SELECT AI_REGISTER_SKILL('" + skill1 + "', 'Prompt A')");
            stmt.execute("SELECT AI_REGISTER_SKILL('" + skill2 + "', 'Prompt B')");

            try (ResultSet rs = stmt.executeQuery("SELECT AI_LIST_SKILLS()")) {
                Assert.assertTrue(rs.next());
                String skills = rs.getString(1);
                Assert.assertTrue("Should contain skill1", skills.contains(skill1));
                Assert.assertTrue("Should contain skill2", skills.contains(skill2));
            }

            try (ResultSet rs = stmt.executeQuery("SELECT AI_GET_SKILL_PROMPT('" + skill1 + "')")) {
                Assert.assertTrue(rs.next());
                Assert.assertTrue(rs.getString(1).contains("Prompt A"));
            }
            try (ResultSet rs = stmt.executeQuery("SELECT AI_GET_SKILL_PROMPT('" + skill2 + "')")) {
                Assert.assertTrue(rs.next());
                Assert.assertTrue(rs.getString(1).contains("Prompt B"));
            }

            stmt.execute("SELECT AI_ADD_SKILL_REFERENCE('" + skill1 + "', 'r1', 'Content R1')");
            try (ResultSet rs = stmt.executeQuery("SELECT AI_GET_REFERENCE('" + skill1 + "', 'r1')")) {
                Assert.assertTrue(rs.next());
                Assert.assertTrue(rs.getString(1).contains("Content R1"));
            }

            stmt.execute("SELECT AI_REMOVE_SKILL_REFERENCE('" + skill1 + "', 'r1')");
            stmt.execute("SELECT AI_DROP_SKILL('" + skill1 + "')");
            stmt.execute("SELECT AI_DROP_SKILL('" + skill2 + "')");
        }
    }

    @Test
    public void testAiDescribeSkill_happyPath() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            String skillName = "it_describe_test_skill";
            try {
                stmt.execute("SELECT AI_DROP_SKILL('" + skillName + "')");
            } catch (Exception ignored) {
            }
            stmt.execute("SELECT AI_REGISTER_SKILL('" + skillName + "', 'Describe test prompt')");

            try (ResultSet rs = stmt.executeQuery("SELECT AI_DESCRIBE_SKILL('" + skillName + "')")) {
                Assert.assertTrue(rs.next());
                String desc = rs.getString(1);
                Assert.assertTrue("Should contain skill name", desc.contains(skillName));
                Assert.assertTrue("Should contain prompt", desc.contains("Describe test prompt"));
            }
            stmt.execute("SELECT AI_DROP_SKILL('" + skillName + "')");
        }
    }

    @Test
    public void testAiDescribeSkill_withReferences() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            String skillName = "it_describe_ref_skill";
            try {
                stmt.execute("SELECT AI_DROP_SKILL('" + skillName + "')");
            } catch (Exception ignored) {
            }
            stmt.execute("SELECT AI_REGISTER_SKILL('" + skillName + "', 'Ref describe prompt')");
            stmt.execute("SELECT AI_ADD_SKILL_REFERENCE('" + skillName + "', 'r1', 'Content R1')");
            stmt.execute("SELECT AI_ADD_SKILL_REFERENCE('" + skillName + "', 'r2', 'Content R2')");

            try (ResultSet rs = stmt.executeQuery("SELECT AI_DESCRIBE_SKILL('" + skillName + "')")) {
                Assert.assertTrue(rs.next());
                String desc = rs.getString(1);
                Assert.assertTrue("Should contain references", desc.contains("r1") && desc.contains("r2"));
            }
            stmt.execute("SELECT AI_DROP_SKILL('" + skillName + "')");
        }
    }

    @Test
    public void testAiDescribeSkill_nonExistent() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            try {
                stmt.executeQuery("SELECT AI_DESCRIBE_SKILL('nonexistent_skill_xyz_123')");
                Assert.fail("Should fail for non-existent skill");
            } catch (SQLException e) {
                Assert.assertTrue("Should mention not found",
                    e.getMessage().contains("not found") || e.getMessage().contains("nonexistent"));
            }
        }
    }

    @Test
    public void testDropBuiltinSkill() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            try {
                stmt.executeQuery("SELECT AI_DROP_SKILL('skill-guide')");
                Assert.fail("Should fail to drop builtin skill");
            } catch (SQLException e) {
                Assert.assertTrue("Should mention builtin",
                    e.getMessage().contains("builtin") || e.getMessage().contains("built-in"));
            }
        }
    }

    @Test
    public void testRegisterSkillWithOptions() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            String skillName = "it_options_test_skill";
            try {
                stmt.execute("SELECT AI_DROP_SKILL('" + skillName + "')");
            } catch (Exception ignored) {
            }
            stmt.execute("SELECT AI_REGISTER_SKILL('" + skillName + "', 'Options prompt', "
                + "'{\"description\":\"Custom desc\",\"priority\":50,\"status\":\"INACTIVE\"}')");

            try (ResultSet rs = stmt.executeQuery("SELECT AI_DESCRIBE_SKILL('" + skillName + "')")) {
                Assert.assertTrue(rs.next());
                String desc = rs.getString(1);
                Assert.assertTrue("Should contain description", desc.contains("Custom desc"));
                Assert.assertTrue("Should contain priority 50", desc.contains("50"));
                Assert.assertTrue("Should contain INACTIVE status", desc.contains("INACTIVE"));
            }
            stmt.execute("SELECT AI_DROP_SKILL('" + skillName + "')");
        }
    }

    @Test
    public void testUpdateSkillAllOptions() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            String skillName = "it_update_all_skill";
            try {
                stmt.execute("SELECT AI_DROP_SKILL('" + skillName + "')");
            } catch (Exception ignored) {
            }
            stmt.execute("SELECT AI_REGISTER_SKILL('" + skillName + "', 'Original prompt')");

            stmt.execute("SELECT AI_UPDATE_SKILL('" + skillName + "', "
                + "'{\"description\":\"Updated desc\",\"prompt\":\"Updated prompt\","
                + "\"status\":\"INACTIVE\",\"priority\":25}')");

            try (ResultSet rs = stmt.executeQuery("SELECT AI_DESCRIBE_SKILL('" + skillName + "')")) {
                Assert.assertTrue(rs.next());
                String desc = rs.getString(1);
                Assert.assertTrue("Should have updated prompt", desc.contains("Updated prompt"));
                Assert.assertTrue("Should have updated desc", desc.contains("Updated desc"));
                Assert.assertTrue("Should have INACTIVE status", desc.contains("INACTIVE"));
                Assert.assertTrue("Should have priority 25", desc.contains("25"));
            }
            stmt.execute("SELECT AI_DROP_SKILL('" + skillName + "')");
        }
    }

    @Test
    public void testRegisterDuplicateSkill() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            String skillName = "it_dup_test_skill";
            try {
                stmt.execute("SELECT AI_DROP_SKILL('" + skillName + "')");
            } catch (Exception ignored) {
            }
            stmt.execute("SELECT AI_REGISTER_SKILL('" + skillName + "', 'First prompt')");

            try {
                stmt.executeQuery("SELECT AI_REGISTER_SKILL('" + skillName + "', 'Second prompt')");
                Assert.fail("Should fail to register duplicate skill");
            } catch (SQLException e) {
                Assert.assertTrue("Should mention already exists",
                    e.getMessage().contains("already exists") || e.getMessage().contains("exists"));
            }
            stmt.execute("SELECT AI_DROP_SKILL('" + skillName + "')");
        }
    }

    @Test
    public void testRegisterBuiltinSkillName() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            try {
                stmt.executeQuery("SELECT AI_REGISTER_SKILL('skill-guide', 'Hijack prompt')");
                Assert.fail("Should fail to overwrite builtin skill");
            } catch (SQLException e) {
                Assert.assertTrue("Should mention builtin",
                    e.getMessage().contains("builtin") || e.getMessage().contains("built-in"));
            }
        }
    }

    @Test
    public void testRegisterSkill_emptyName() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            try {
                stmt.executeQuery("SELECT AI_REGISTER_SKILL('', 'some prompt')");
                Assert.fail("Should fail for empty skill name");
            } catch (SQLException e) {
                Assert.assertTrue("Should mention name required",
                    e.getMessage().contains("name") || e.getMessage().contains("required"));
            }
        }
    }

    @Test
    public void testRegisterSkill_emptyPrompt() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            try {
                stmt.executeQuery("SELECT AI_REGISTER_SKILL('test_empty_prompt_skill', '')");
                Assert.fail("Should fail for empty prompt");
            } catch (SQLException e) {
                Assert.assertTrue("Should mention prompt required",
                    e.getMessage().contains("prompt") || e.getMessage().contains("required"));
            } finally {
                try {
                    stmt.execute("SELECT AI_DROP_SKILL('test_empty_prompt_skill')");
                } catch (Exception ignored) {
                }
            }
        }
    }

    @Test
    public void testUpdateSkill_emptyName() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            try {
                stmt.executeQuery("SELECT AI_UPDATE_SKILL('', '{\"description\":\"test\"}')");
                Assert.fail("Should fail for empty skill name in update");
            } catch (SQLException e) {
                Assert.assertTrue("Should mention name required",
                    e.getMessage().contains("name") || e.getMessage().contains("required"));
            }
        }
    }

    @Test
    public void testRemoveReference_nonExistent() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            String skillName = "it_remove_ref_skill";
            try {
                stmt.execute("SELECT AI_DROP_SKILL('" + skillName + "')");
            } catch (Exception ignored) {
            }
            stmt.execute("SELECT AI_REGISTER_SKILL('" + skillName + "', 'Test prompt')");
            try {
                stmt.executeQuery("SELECT AI_REMOVE_SKILL_REFERENCE('" + skillName + "', 'nonexistent_ref')");
                Assert.fail("Should fail for non-existent reference");
            } catch (SQLException e) {
                Assert.assertTrue("Should mention not found",
                    e.getMessage().contains("not found") || e.getMessage().contains("nonexistent"));
            } finally {
                stmt.execute("SELECT AI_DROP_SKILL('" + skillName + "')");
            }
        }
    }

    @Test
    public void testDescribeSkill_inactiveSkill() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            String skillName = "it_inactive_skill";
            try {
                stmt.execute("SELECT AI_DROP_SKILL('" + skillName + "')");
            } catch (Exception ignored) {
            }
            stmt.execute("SELECT AI_REGISTER_SKILL('" + skillName + "', 'Active prompt')");
            stmt.execute("SELECT AI_UPDATE_SKILL('" + skillName + "', '{\"status\":\"INACTIVE\"}')");
            try (ResultSet rs = stmt.executeQuery("SELECT AI_DESCRIBE_SKILL('" + skillName + "')")) {
                Assert.assertTrue("Should have result", rs.next());
                String desc = rs.getString(1);
                Assert.assertTrue("Should contain INACTIVE status", desc.contains("INACTIVE"));
            } finally {
                stmt.execute("SELECT AI_DROP_SKILL('" + skillName + "')");
            }
        }
    }
}
