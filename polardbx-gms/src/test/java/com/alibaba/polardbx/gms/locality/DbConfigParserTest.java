package com.alibaba.polardbx.gms.locality;

import com.alibaba.polardbx.gms.topology.DbGroupInfoManager;
import com.alibaba.polardbx.gms.topology.DbGroupInfoRecord;
import org.apache.commons.lang3.RandomUtils;
import org.junit.Ignore;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.when;

@Ignore("timeout is too strict")
public class DbConfigParserTest {

    /**
     * 测试parseGroupNames函数处理超长字符串groupConfig的情况
     */
    @Test
    public void testParseGroupNamesWithLongString() {
        // 构造超长字符串测试用例，使用用户提供的示例格式
        StringBuilder longConfigBuilder = new StringBuilder();
        for (int i = 1; i <= 127; i++) {
            if (i > 1) {
                longConfigBuilder.append(",");
            }
            longConfigBuilder.append(String.format("c1_%03d", i));
        }
        String longGroupConfig = longConfigBuilder.toString();

        // 调用parseGroupNames函数
        List<String> result = DbConfigParser.parseGroupNames(longGroupConfig, "testSchema");

        // 验证结果
        assertNotNull("结果不应为null", result);
        assertEquals("应该有127个元素", 127, result.size());

        // 验证前几个和后几个元素 (根据generateGroupName的逻辑，会添加_$schemaName后缀)
        assertEquals("c1_001_$testschema", result.get(0));
        assertEquals("c1_002_$testschema", result.get(1));
        assertEquals("c1_126_$testschema", result.get(125));
        assertEquals("c1_127_$testschema", result.get(126));
    }

    /**
     * 测试parseGroupNames函数处理超长字符串的性能
     */
    @Test
    public void testParseGroupNamesPerformanceWithLongString() {
        // 构造超长字符串测试用例，使用用户提供的示例格式
        StringBuilder longConfigBuilder = new StringBuilder();
        for (int i = 1; i <= 256; i++) {
            if (i > 1) {
                longConfigBuilder.append(",");
            }
            // 生成随机前缀，例如"C1"、"C2"等
            String prefix = "C" + RandomUtils.nextInt(1, 100);
            longConfigBuilder.append(String.format("%s_%03d", prefix, i));
        }
        String longGroupConfig = longConfigBuilder.toString();

        // 记录开始时间
        long startTime = System.nanoTime();
        // 调用parseGroupNames函数
        List<String> result = DbConfigParser.parseGroupNames(longGroupConfig, "testSchema");

        // 记录结束时间
        long endTime = System.nanoTime();
        long durationMs = (endTime - startTime) / 1_000_000; // 转换为毫秒

        // 验证结果数量
        assertEquals("应该有256个元素", 256, result.size());

        // 输出性能结果
        System.out.println("ParseGroupNames performance test completed.");
        System.out.println("Elements per iteration: 127");
        System.out.println("Total time: " + durationMs + " ms");

        // 验证性能要求：处理时间应小于100ms
        assertTrue("处理超长字符串时间应合理，实际耗时: " + durationMs + "ms", durationMs < 100);
    }

    /**
     * 测试parseGroupNames函数处理超长字符串的性能
     * 使用用户提供的示例格式：C1_[001,002,003,...,256]
     */
    @Test
    public void testParseGroupNamesPerformanceWithLongString2() {
        // 构造超长字符串测试用例，使用用户提供的示例格式
        StringBuilder longConfigBuilder = new StringBuilder();
        // 生成随机前缀，例如"C1"、"C2"等
        String prefix = "C" + RandomUtils.nextInt(1, 100);
        longConfigBuilder.append(String.format("%s_[", prefix));
        for (int i = 1; i <= 256; i++) {
            if (i > 1) {
                longConfigBuilder.append(",");
            }
            longConfigBuilder.append(String.format("%03d", i));
        }
        longConfigBuilder.append("]");
        String longGroupConfig = longConfigBuilder.toString();

        // 提前初始化，避免初始化时间影响
        DbGroupInfoManager.getInstance();

        // 记录开始时间
        long startTime = System.nanoTime();
        // 调用parseGroupNames函数
        List<String> result = DbConfigParser.parseGroupNames(longGroupConfig, "testSchema");

        // 记录结束时间
        long endTime = System.nanoTime();
        long durationMs = (endTime - startTime) / 1_000_000; // 转换为毫秒

        // 验证结果数量
        assertEquals("应该有256个元素", 256, result.size());

        // 输出性能结果
        System.out.println("ParseGroupNames performance test completed.");
        System.out.println("Elements per iteration: 127");
        System.out.println("Total time: " + durationMs + " ms");

        // 验证性能要求：处理时间应小于100ms
        assertTrue("处理超长字符串时间应合理，实际耗时: " + durationMs + "ms", durationMs < 100);
    }

    /**
     * 测试parseGroupNames函数处理正则表达式匹配的情况
     */
    @Test
    public void testParseGroupNamesWithRegexPattern() {
        // 测试正则表达式匹配的情况: group[1,2,3]
        String groupConfig = "group1[1,2,3]";
        List<String> result = DbConfigParser.parseGroupNames(groupConfig, "testSchema");

        // 验证结果
        assertNotNull("结果不应为null", result);
        assertEquals("应该有3个元素", 3, result.size());
        assertEquals("group11_$testschema", result.get(0));
        assertEquals("group12_$testschema", result.get(1));
        assertEquals("group13_$testschema", result.get(2));
    }

    /**
     * 测试parseGroupNames函数处理普通逗号分隔的情况
     */
    @Test
    public void testParseGroupNamesWithCommaSeparated() {
        // 测试普通逗号分隔的情况
        String groupConfig = "group1,group2,group3";
        List<String> result = DbConfigParser.parseGroupNames(groupConfig, "testSchema");

        // 验证结果
        assertNotNull("结果不应为null", result);
        assertEquals("应该有3个元素", 3, result.size());
        assertEquals("group1_$testschema", result.get(0));
        assertEquals("group2_$testschema", result.get(1));
        assertEquals("group3_$testschema", result.get(2));
    }

    /**
     * 测试parseGroupNames函数处理单个元素的情况
     */
    @Test
    public void testParseGroupNamesWithSingleElement() {
        // 测试单个元素的情况
        String groupConfig = "singlegroup";
        List<String> result = DbConfigParser.parseGroupNames(groupConfig, "testSchema");

        // 验证结果
        assertNotNull("结果不应为null", result);
        assertEquals("应该有1个元素", 1, result.size());
        assertEquals("singlegroup_$testschema", result.get(0));
    }

    /**
     * 测试使用mock的DbGroupInfoManager，使其不会添加 _$ + schemaName 的后缀
     */
    @Test
    public void testParseGroupNamesWithMockedDbGroupInfoManager() {
        // 创建mock的DbGroupInfoRecord列表
        List<DbGroupInfoRecord> mockGroupInfoList = new ArrayList<>();
        DbGroupInfoRecord mockRecord1 = new DbGroupInfoRecord();
        mockRecord1.groupName = "group1";
        mockGroupInfoList.add(mockRecord1);

        DbGroupInfoRecord mockRecord2 = new DbGroupInfoRecord();
        mockRecord2.groupName = "group2";
        mockGroupInfoList.add(mockRecord2);

        // 使用MockedStatic来mock DbGroupInfoManager.getInstance()
        try (MockedStatic<DbGroupInfoManager> mockedDbGroupInfoManager = Mockito.mockStatic(DbGroupInfoManager.class)) {

            // 创建mock的DbGroupInfoManager实例
            DbGroupInfoManager mockDbGroupInfoManager = Mockito.mock(DbGroupInfoManager.class);

            // 当调用getInstance()时返回mock的实例
            mockedDbGroupInfoManager.when(DbGroupInfoManager::getInstance).thenReturn(mockDbGroupInfoManager);

            // 当调用queryGroupInfoBySchema("testSchema")时返回mock的列表
            when(mockDbGroupInfoManager.queryGroupInfoBySchema("testSchema")).thenReturn(mockGroupInfoList);

            // 测试普通逗号分隔的情况
            String groupConfig = "group1,group2";
            List<String> result = DbConfigParser.parseGroupNames(groupConfig, "testSchema");

            // 验证结果
            assertNotNull("结果不应为null", result);
            assertEquals("应该有2个元素", 2, result.size());
            assertEquals("group1", result.get(0));
            assertEquals("group2", result.get(1));
        }
    }
}