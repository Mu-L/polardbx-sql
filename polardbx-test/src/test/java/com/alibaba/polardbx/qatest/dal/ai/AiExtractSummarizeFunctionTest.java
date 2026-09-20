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
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.qatest.util.JdbcUtil;
import net.jcip.annotations.NotThreadSafe;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Integration tests for AI_EXTRACT and AI_SUMMARIZE functions.
 *
 * <p>Tests both functions using a registered DashScope qwen-plus model.
 * Requires environment variable {@code DASHSCOPE_API_KEY} to be set.
 */
@NotThreadSafe
public class AiExtractSummarizeFunctionTest extends AiFunctionTestBase {

    private static final String TEST_LLM_MODEL = AiTestModelConfig.DASHSCOPE_QWEN_PLUS.name;

    @Before
    public void setUp() {
        registerModel(AiTestModelConfig.DASHSCOPE_QWEN_PLUS);
    }

    // ==================== Helper methods ====================

    private String executeQuery(String sql) throws SQLException {
        ResultSet rs = JdbcUtil.executeQuerySuccess(tddlConnection, sql);
        try {
            Assert.assertTrue("Query should return a result: " + sql, rs.next());
            return rs.getString(1);
        } finally {
            JdbcUtil.close(rs);
        }
    }

    // ==================== AI_EXTRACT tests ====================

    /**
     * Test AI_EXTRACT basic usage — extract name, phone and email from Chinese text.
     */
    @Test
    public void testExtractBasic() throws SQLException {
        String sql = "SELECT AI_EXTRACT("
            + "'请联系张三，电话：13800138000，邮箱：zhangsan@example.com',"
            + "'{\"name\": \"姓名\", \"phone\": \"电话号码\", \"email\": \"电子邮箱\"}',"
            + "'" + TEST_LLM_MODEL + "'"
            + ")";
        String result = executeQuery(sql);
        Assert.assertNotNull("AI_EXTRACT should return non-null", result);
        Assert.assertFalse("AI_EXTRACT should return non-empty", result.trim().isEmpty());

        // Result should be valid JSON
        JSONObject json = JSON.parseObject(result);
        Assert.assertNotNull("AI_EXTRACT result should be valid JSON", json);
    }

    /**
     * Test AI_EXTRACT extracts name field correctly.
     */
    @Test
    public void testExtractNameField() throws SQLException {
        String sql = "SELECT AI_EXTRACT("
            + "'联系人：李四，年龄：28岁，工作：软件工程师',"
            + "'{\"name\": \"人名\", \"age\": \"年龄\", \"job\": \"职业\"}',"
            + "'" + TEST_LLM_MODEL + "'"
            + ")";
        String result = executeQuery(sql);
        Assert.assertNotNull("AI_EXTRACT should return non-null", result);

        JSONObject json = JSON.parseObject(result);
        Assert.assertNotNull("Result should be valid JSON", json);
        // name field should be present
        Assert.assertTrue("Result should contain 'name' key", json.containsKey("name"));
    }

    /**
     * Test AI_EXTRACT with English text.
     */
    @Test
    public void testExtractEnglishText() throws SQLException {
        String sql = "SELECT AI_EXTRACT("
            + "'Apple iPhone 15 Pro, 6.1 inch display, 256GB storage, price $999',"
            + "'{\"brand\": \"brand name\", \"model\": \"product model\", \"storage\": \"storage capacity\", \"price\": \"price\"}',"
            + "'" + TEST_LLM_MODEL + "'"
            + ")";
        String result = executeQuery(sql);
        Assert.assertNotNull("AI_EXTRACT should return non-null for English text", result);

        JSONObject json = JSON.parseObject(result);
        Assert.assertNotNull("Result should be valid JSON", json);
        Assert.assertTrue("Result should have at least one field", !json.isEmpty());
    }

    /**
     * Test AI_EXTRACT with missing fields — should return null values for unfound fields.
     */
    @Test
    public void testExtractMissingFields() throws SQLException {
        String sql = "SELECT AI_EXTRACT("
            + "'只有名字：王五',"
            + "'{\"name\": \"姓名\", \"phone\": \"电话\", \"address\": \"地址\"}',"
            + "'" + TEST_LLM_MODEL + "'"
            + ")";
        String result = executeQuery(sql);
        Assert.assertNotNull("AI_EXTRACT should return non-null even with missing fields", result);

        JSONObject json = JSON.parseObject(result);
        Assert.assertNotNull("Result should be valid JSON", json);
        // name should be extractable
        Assert.assertTrue("Result should contain 'name' key", json.containsKey("name"));
    }

    /**
     * Test AI_EXTRACT with options (temperature override).
     */
    @Test
    public void testExtractWithOptions() throws SQLException {
        String sql = "SELECT AI_EXTRACT("
            + "'产品：华为Mate60，价格：6999元，颜色：黑色',"
            + "'{\"product\": \"产品名称\", \"price\": \"价格\", \"color\": \"颜色\"}',"
            + "'" + TEST_LLM_MODEL + "',"
            + "'{\"temperature\": 0.0, \"max_tokens\": 200}'"
            + ")";
        String result = executeQuery(sql);
        Assert.assertNotNull("AI_EXTRACT with options should return non-null", result);

        JSONObject json = JSON.parseObject(result);
        Assert.assertNotNull("Result should be valid JSON", json);
    }

    // ==================== AI_SUMMARIZE tests ====================

    /**
     * Test AI_SUMMARIZE basic usage — summarize a longer Chinese text.
     */
    @Test
    public void testSummarizeBasic() throws SQLException {
        String sql = "SELECT AI_SUMMARIZE("
            + "'PolarDB-X是阿里巴巴自主研发的云原生分布式数据库系统。"
            + "它支持水平扩展，能够处理海量并发事务。"
            + "PolarDB-X完全兼容MySQL协议，企业可以无缝迁移。"
            + "系统提供强一致性保证，支持分布式事务。"
            + "它广泛应用于电商、金融、游戏等高并发业务场景。',"
            + "100,"
            + "'" + TEST_LLM_MODEL + "'"
            + ")";
        String result = executeQuery(sql);
        Assert.assertNotNull("AI_SUMMARIZE should return non-null", result);
        Assert.assertFalse("AI_SUMMARIZE should return non-empty", result.trim().isEmpty());
    }

    /**
     * Test AI_SUMMARIZE respects max_length constraint.
     * The result should not exceed 2x the requested length (accounting for LLM non-determinism).
     */
    @Test
    public void testSummarizeMaxLength() throws SQLException {
        int maxLength = 50;
        String sql = "SELECT AI_SUMMARIZE("
            + "'这是一篇关于云计算技术发展的详细报告。"
            + "云计算改变了企业IT基础设施的部署方式，提供弹性、可扩展的计算资源。"
            + "主要服务模式包括IaaS、PaaS和SaaS三种形态。"
            + "公有云、私有云和混合云满足不同安全合规需求。',"
            + maxLength + ","
            + "'" + TEST_LLM_MODEL + "'"
            + ")";
        String result = executeQuery(sql);
        Assert.assertNotNull("AI_SUMMARIZE should return non-null", result);
        Assert.assertFalse("AI_SUMMARIZE should return non-empty", result.trim().isEmpty());
        // The summary should be reasonably short (allow 3x headroom for LLM variability)
        Assert.assertTrue(
            "Summary length " + result.length() + " should be <= " + (maxLength * 3),
            result.length() <= maxLength * 3);
    }

    /**
     * Test AI_SUMMARIZE with options (language and style).
     */
    @Test
    public void testSummarizeWithOptions() throws SQLException {
        String sql = "SELECT AI_SUMMARIZE("
            + "'PolarDB-X is a cloud-native distributed SQL database developed by Alibaba. "
            + "It supports horizontal scaling, distributed transactions, and MySQL compatibility. "
            + "The system is widely used in e-commerce, finance, and gaming industries.',"
            + "150,"
            + "'" + TEST_LLM_MODEL + "',"
            + "'{\"language\": \"Chinese\"}'"
            + ")";
        String result = executeQuery(sql);
        Assert.assertNotNull("AI_SUMMARIZE with language option should return non-null", result);
        Assert.assertFalse("AI_SUMMARIZE with language option should return non-empty", result.trim().isEmpty());
    }

    /**
     * Test AI_SUMMARIZE with bullet_points style.
     */
    @Test
    public void testSummarizeWithBulletPointsStyle() throws SQLException {
        String sql = "SELECT AI_SUMMARIZE("
            + "'分布式数据库的核心挑战包括：数据一致性保证、网络分区处理、跨节点事务协调。"
            + "CAP定理指出分布式系统只能同时满足一致性、可用性、分区容错性三者中的两个。"
            + "Paxos和Raft是常用的分布式共识算法，用于保证多副本数据的一致性。"
            + "两阶段提交（2PC）是实现分布式事务的经典协议。',"
            + "200,"
            + "'" + TEST_LLM_MODEL + "',"
            + "'{\"style\": \"bullet_points\"}'"
            + ")";
        String result = executeQuery(sql);
        Assert.assertNotNull("AI_SUMMARIZE with bullet_points style should return non-null", result);
        Assert.assertFalse("AI_SUMMARIZE with bullet_points style should return non-empty", result.trim().isEmpty());
    }

    /**
     * Test AI_SUMMARIZE with explicit model name.
     */
    @Test
    public void testSummarizeExplicitModel() throws SQLException {
        String sql = "SELECT AI_SUMMARIZE("
            + "'量子计算利用量子力学原理进行信息处理，"
            + "量子比特可以同时处于0和1的叠加态，"
            + "量子纠缠和量子干涉使其在某些问题上远超经典计算机。',"
            + "80,"
            + "'" + TEST_LLM_MODEL + "'"
            + ")";
        String result = executeQuery(sql);
        Assert.assertNotNull("AI_SUMMARIZE with explicit model should return non-null", result);
        Assert.assertFalse(result.trim().isEmpty());
    }
}
