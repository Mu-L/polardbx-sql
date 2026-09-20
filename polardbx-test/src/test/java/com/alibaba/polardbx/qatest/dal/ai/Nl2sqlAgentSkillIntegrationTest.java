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
public class Nl2sqlAgentSkillIntegrationTest extends Nl2sqlAgentTestBase {

    @Test
    public void testSkillIntegrationWithAgent() throws Exception {
        // Clean up any leftover skill from previous runs
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SELECT AI_DROP_SKILL('data_analysis_guide')");
        } catch (Exception e) {
            // Ignore if skill doesn't exist
        }
        // Register a custom skill with a reference document
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SELECT AI_REGISTER_SKILL('data_analysis_guide', "
                + "'指导用户如何分析员工数据，包括统计、分组、排序等操作', "
                + "'{\"status\":\"ACTIVE\",\"priority\":50}')");
            stmt.execute("SELECT AI_ADD_SKILL_REFERENCE('data_analysis_guide', "
                + "'analysis_tips', '分析数据时请先统计总量，再按维度分组，最后找出极值')");
        }
        // Ask a question that should trigger the agent to look up the skill
        String output = executeNlQuery("使用data_analysis_guide技能分析员工数据");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
        // Cleanup
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SELECT AI_DROP_SKILL('data_analysis_guide')");
        }
    }

    @Test
    public void testListSkillsViaAgent() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            try {
                stmt.execute("SELECT AI_DROP_SKILL('test_list_skill')");
            } catch (Exception ignored) {
            }
        }
        // Register a skill first
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SELECT AI_REGISTER_SKILL('test_list_skill', "
                + "'A test skill for listing', '{\"status\":\"ACTIVE\",\"priority\":90}')");
        }
        // Ask agent to list available skills
        String output = executeNlQuery("请通过 SELECT AI_LIST_SKILLS() 列出当前可用的AI技能列表");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
        // Cleanup
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SELECT AI_DROP_SKILL('test_list_skill')");
        }
    }

    @Test
    public void testGetSkillPromptViaAgent() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            try {
                stmt.execute("SELECT AI_DROP_SKILL('test_prompt_skill')");
            } catch (Exception ignored) {
            }
        }
        // Register a skill
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SELECT AI_REGISTER_SKILL('test_prompt_skill', "
                + "'Skill for prompt test', '{\"status\":\"ACTIVE\",\"priority\":80}')");
        }
        // Ask agent to show the skill's prompt
        String output = executeNlQuery("请查看test_prompt_skill技能的详细提示信息");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
        // Cleanup
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SELECT AI_DROP_SKILL('test_prompt_skill')");
        }
    }

    @Test
    public void testGetReferenceViaAgent() throws Exception {
        try (Statement stmt = tddlConnection.createStatement()) {
            try {
                stmt.execute("SELECT AI_DROP_SKILL('test_ref_skill')");
            } catch (Exception ignored) {
            }
        }
        // Register a skill with reference
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SELECT AI_REGISTER_SKILL('test_ref_skill', "
                + "'Skill for ref test', '{\"status\":\"ACTIVE\",\"priority\":70}')");
            stmt.execute("SELECT AI_ADD_SKILL_REFERENCE('test_ref_skill', "
                + "'my_ref', 'This is reference content for testing')");
        }
        // Ask agent to get the reference
        String output = executeNlQuery("请查看test_ref_skill技能的my_ref参考文档内容");
        Assert.assertFalse("Output should not be empty", output.isEmpty());
        // Cleanup
        try (Statement stmt = tddlConnection.createStatement()) {
            stmt.execute("SELECT AI_DROP_SKILL('test_ref_skill')");
        }
    }
}
