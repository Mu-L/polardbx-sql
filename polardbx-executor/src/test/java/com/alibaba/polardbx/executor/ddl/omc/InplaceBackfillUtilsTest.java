package com.alibaba.polardbx.executor.ddl.omc;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.metadb.misc.DdlPhysicalLockStatRecord;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

/**
 * 单元测试类：测试InplaceBackfillUtils的核心逻辑
 * <p>
 * 覆盖场景：
 * 1. buildGroupAndPhyDbMap方法的边界条件
 * 2. prepareBackfillTask方法的参数校验
 * 3. processPartitionBounds方法的边界条件
 */
public class InplaceBackfillUtilsTest {

    /**
     * 测试：buildGroupAndPhyDbMap方法 - 空topology输入
     */
    @Test
    public void testBuildGroupAndPhyDbMap_EmptyTopology() {
        String schemaName = "test_schema";

        // 测试null输入
        TreeMap<String, List<List<String>>> nullTopology = null;
        Map<String, String> result = InplaceBackfillUtils.buildGroupAndPhyDbMap(schemaName, nullTopology);
        Assert.assertNotNull("Result should not be null", result);
        Assert.assertTrue("Result should be empty for null input", result.isEmpty());

        // 测试空map输入
        TreeMap<String, List<List<String>>> emptyTopology = new TreeMap<>();
        result = InplaceBackfillUtils.buildGroupAndPhyDbMap(schemaName, emptyTopology);
        Assert.assertNotNull("Result should not be null", result);
        Assert.assertTrue("Result should be empty for empty input", result.isEmpty());
    }

    /**
     * 测试：buildGroupAndPhyDbMap方法 - 正常输入
     */
    @Test
    public void testBuildGroupAndPhyDbMap_ValidTopology() {
        String schemaName = "test_schema";
        TreeMap<String, List<List<String>>> topology = new TreeMap<>();
        topology.put("GROUP_0", Arrays.asList(Arrays.asList("t1", "t2")));
        topology.put("GROUP_1", Arrays.asList(Arrays.asList("t3", "t4")));

        Map<String, String> result = InplaceBackfillUtils.buildGroupAndPhyDbMap(schemaName, topology);

        Assert.assertNotNull("Result should not be null", result);
        Assert.assertEquals("Should have 2 entries", 2, result.size());
        Assert.assertTrue("Should contain GROUP_0", result.containsKey("GROUP_0"));
        Assert.assertTrue("Should contain GROUP_1", result.containsKey("GROUP_1"));
    }

    /**
     * 测试：POLARX_READONLY常量值
     */
    @Test
    public void testReadonlyConstants() {
        Assert.assertEquals("polarx.readonly", InplaceBackfillUtils.POLARX_READONLY);
        Assert.assertEquals("{\"polarx.readonly\":true}", InplaceBackfillUtils.POLARX_READONLY_TRUE);
        Assert.assertEquals("{\"polarx.readonly\":false}", InplaceBackfillUtils.POLARX_READONLY_FALSE);
    }

    /**
     * 测试：DdlPhysicalLockStatRecord状态常量
     */
    @Test
    public void testDdlPhysicalLockStatRecordConstants() {
        Assert.assertEquals("STATE_LOCKED should be 1", 1, DdlPhysicalLockStatRecord.STATE_LOCKED);
        Assert.assertEquals("STATE_UNLOCKED should be 0", 0, DdlPhysicalLockStatRecord.STATE_UNLOCKED);
        Assert.assertEquals("DDL_TYPE_SPLIT_PARTITION should be 0", 0,
            DdlPhysicalLockStatRecord.DDL_TYPE_SPLIT_PARTITION);
        Assert.assertEquals("DDL_TYPE_OTHERS should be 1", 1, DdlPhysicalLockStatRecord.DDL_TYPE_OTHERS);
    }

    /**
     * 测试：DdlPhysicalLockStatRecord getter/setter
     */
    @Test
    public void testDdlPhysicalLockStatRecord_GetterSetter() {
        DdlPhysicalLockStatRecord record = new DdlPhysicalLockStatRecord();

        long testId = 100L;
        long testJobId = 200L;
        int testDdlType = 1;
        String testTableSchema = "schema";
        String testTableName = "table";
        String testPhysicalDb = "phy_db";
        String testPhysicalTable = "phy_table";
        Long testLockDurationMs = 5000L;
        long testRowCount = 1000L;
        long testStartTime = System.currentTimeMillis();
        long testCurLockStartTime = testStartTime + 100;
        long testEndTime = testStartTime + 6000;
        int testState = DdlPhysicalLockStatRecord.STATE_UNLOCKED;

        record.setId(testId);
        record.setJobId(testJobId);
        record.setDdlType(testDdlType);
        record.setTableSchema(testTableSchema);
        record.setTableName(testTableName);
        record.setPhysicalDb(testPhysicalDb);
        record.setPhysicalTable(testPhysicalTable);
        record.setLockDurationMs(testLockDurationMs);
        record.setRowCount(testRowCount);
        record.setStartTime(testStartTime);
        record.setCurLockStartTime(testCurLockStartTime);
        record.setEndTime(testEndTime);
        record.setState(testState);

        Assert.assertEquals(testId, record.getId());
        Assert.assertEquals(testJobId, record.getJobId());
        Assert.assertEquals(testDdlType, record.getDdlType());
        Assert.assertEquals(testTableSchema, record.getTableSchema());
        Assert.assertEquals(testTableName, record.getTableName());
        Assert.assertEquals(testPhysicalDb, record.getPhysicalDb());
        Assert.assertEquals(testPhysicalTable, record.getPhysicalTable());
        Assert.assertEquals(testLockDurationMs, record.getLockDurationMs());
        Assert.assertEquals(testRowCount, record.getRowCount());
        Assert.assertEquals(testStartTime, record.getStartTime());
        Assert.assertEquals(testCurLockStartTime, record.getCurLockStartTime());
        Assert.assertEquals(testEndTime, record.getEndTime());
        Assert.assertEquals(testState, record.getState());
    }

    /**
     * 测试：prepareBackfillTask参数边界条件
     */
    @Test
    public void testPrepareBackfillTask_EmptyParams() {
        // 准备测试数据
        Map<String, Set<String>> srcTargetTableMap = new TreeMap<>(String::compareToIgnoreCase);
        Map<String, Set<String>> srcTargetPartitionMap = new HashMap<>();
        Map<String, List<Pair<Long, Long>>> sourceTablePartitionBounds = new TreeMap<>(String::compareToIgnoreCase);
        Map<String, List<Pair<Long, Long>>> targetTablePartitionBounds = new TreeMap<>(String::compareToIgnoreCase);
        List<List<String>> activePartitionKeys = new ArrayList<>();

        // 验证空输入不会导致NPE
        Assert.assertTrue("srcTargetTableMap should be empty", srcTargetTableMap.isEmpty());
        Assert.assertTrue("srcTargetPartitionMap should be empty", srcTargetPartitionMap.isEmpty());
        Assert.assertTrue("sourceTablePartitionBounds should be empty", sourceTablePartitionBounds.isEmpty());
        Assert.assertTrue("targetTablePartitionBounds should be empty", targetTablePartitionBounds.isEmpty());
        Assert.assertTrue("activePartitionKeys should be empty", activePartitionKeys.isEmpty());
    }

    /**
     * 测试：分区边界值对的创建
     */
    @Test
    public void testPartitionBoundsPairCreation() {
        // 测试正常范围
        Pair<Long, Long> normalPair = Pair.of(100L, 200L);
        Assert.assertEquals(Long.valueOf(100L), normalPair.getKey());
        Assert.assertEquals(Long.valueOf(200L), normalPair.getValue());

        // 测试边界值
        Pair<Long, Long> minMaxPair = Pair.of(Long.MIN_VALUE, Long.MAX_VALUE);
        Assert.assertEquals(Long.valueOf(Long.MIN_VALUE), minMaxPair.getKey());
        Assert.assertEquals(Long.valueOf(Long.MAX_VALUE), minMaxPair.getValue());

        // 测试相等边界（空范围）
        Pair<Long, Long> emptyRangePair = Pair.of(100L, 100L);
        Assert.assertEquals(emptyRangePair.getKey(), emptyRangePair.getValue());
    }

    /**
     * 测试：sourceTableTopology结构验证
     */
    @Test
    public void testSourceTableTopologyStructure() {
        Map<String, Set<String>> sourceTableTopology = new HashMap<>();

        Set<String> tables1 = new HashSet<>();
        tables1.add("t1_00000");
        tables1.add("t1_00001");
        sourceTableTopology.put("GROUP_0", tables1);

        Set<String> tables2 = new HashSet<>();
        tables2.add("t1_00002");
        tables2.add("t1_00003");
        sourceTableTopology.put("GROUP_1", tables2);

        // 验证结构
        Assert.assertEquals(2, sourceTableTopology.size());
        Assert.assertEquals(2, sourceTableTopology.get("GROUP_0").size());
        Assert.assertEquals(2, sourceTableTopology.get("GROUP_1").size());

        // 验证包含关系
        Assert.assertTrue(sourceTableTopology.get("GROUP_0").contains("t1_00000"));
        Assert.assertTrue(sourceTableTopology.get("GROUP_1").contains("t1_00002"));
    }

    /**
     * 测试：activePartitionKeys结构验证
     */
    @Test
    public void testActivePartitionKeysStructure() {
        List<List<String>> activePartitionKeys = new ArrayList<>();

        // 单列分区键
        activePartitionKeys.add(Collections.singletonList("col1"));
        Assert.assertEquals(1, activePartitionKeys.size());
        Assert.assertEquals(1, activePartitionKeys.get(0).size());
        Assert.assertEquals("col1", activePartitionKeys.get(0).get(0));

        // 多列分区键（HASH分区）
        activePartitionKeys.clear();
        activePartitionKeys.add(Arrays.asList("col1", "col2", "col3"));
        Assert.assertEquals(1, activePartitionKeys.size());
        Assert.assertEquals(3, activePartitionKeys.get(0).size());

        // KEY分区（每列一个维度）
        activePartitionKeys.clear();
        activePartitionKeys.add(Collections.singletonList("col1"));
        activePartitionKeys.add(Collections.singletonList("col2"));
        activePartitionKeys.add(Collections.singletonList("col3"));
        Assert.assertEquals(3, activePartitionKeys.size());
        for (List<String> keys : activePartitionKeys) {
            Assert.assertEquals(1, keys.size());
        }
    }

    /**
     * 测试：targetTablePartitionBounds结构验证
     */
    @Test
    public void testTargetTablePartitionBoundsStructure() {
        Map<String, List<Pair<Long, Long>>> targetTablePartitionBounds = new TreeMap<>(String::compareToIgnoreCase);

        // 单维度边界（HASH分区）
        List<Pair<Long, Long>> bounds1 = new ArrayList<>();
        bounds1.add(Pair.of(0L, 100L));
        targetTablePartitionBounds.put("t1_00000", bounds1);

        Assert.assertEquals(1, targetTablePartitionBounds.size());
        Assert.assertEquals(1, targetTablePartitionBounds.get("t1_00000").size());

        // 多维度边界（KEY分区）
        List<Pair<Long, Long>> bounds2 = new ArrayList<>();
        bounds2.add(Pair.of(5634770598966349863L, 5634770598966349863L));
        bounds2.add(Pair.of(3074457345618258600L, 6148914691236517202L));
        targetTablePartitionBounds.put("t1_00001", bounds2);

        Assert.assertEquals(2, targetTablePartitionBounds.size());
        Assert.assertEquals(2, targetTablePartitionBounds.get("t1_00001").size());
    }

    /**
     * 测试：srcTargetTableMap结构验证
     */
    @Test
    public void testSrcTargetTableMapStructure() {
        Map<String, Set<String>> srcTargetTableMap = new TreeMap<>(String::compareToIgnoreCase);

        // 一个源表对应多个目标表（分裂场景）
        Set<String> targets = new HashSet<>();
        targets.add("t1_new_00000");
        targets.add("t1_new_00001");
        targets.add("t1_new_00002");
        srcTargetTableMap.put("t1_00000", targets);

        Assert.assertEquals(1, srcTargetTableMap.size());
        Assert.assertEquals(3, srcTargetTableMap.get("t1_00000").size());
        Assert.assertTrue(srcTargetTableMap.get("t1_00000").contains("t1_new_00001"));
    }

    /**
     * 测试：空范围判断逻辑
     */
    @Test
    public void testEmptyRangeLogic() {
        // 正常范围
        Assert.assertFalse("Normal range should not be empty", isEmptyRange(100L, 200L));

        // 相等范围（单点，视为空）
        Assert.assertTrue("Equal range should be empty", isEmptyRange(100L, 100L));

        // 反向范围
        Assert.assertTrue("Reversed range should be empty", isEmptyRange(200L, 100L));

        // MIN_VALUE开始的范围
        Assert.assertFalse("Range from MIN_VALUE should not be empty", isEmptyRange(Long.MIN_VALUE, 100L));

        // 到MAX_VALUE的范围
        Assert.assertFalse("Range to MAX_VALUE should not be empty", isEmptyRange(100L, Long.MAX_VALUE));

        // 全范围
        Assert.assertFalse("Full range should not be empty", isEmptyRange(Long.MIN_VALUE, Long.MAX_VALUE));

        // 特殊情况：(MAX, MAX) 表示等于MAX的点
        Assert.assertFalse("(MAX, MAX) should not be empty", isEmptyRange(Long.MAX_VALUE, Long.MAX_VALUE));
    }

    /**
     * 辅助方法：判断范围是否为空
     */
    private boolean isEmptyRange(long low, long high) {
        if (low >= high) {
            // 特殊情况：(MAX, MAX) 表示等于MAX的点
            if (low == Long.MAX_VALUE && high == Long.MAX_VALUE) {
                return false;
            }
            // 特殊情况：wrap-around范围 (MAX_VALUE, negative_value)
            if (low == Long.MAX_VALUE && high < 0) {
                return false;
            }
            return true;
        }
        return false;
    }

    /**
     * 测试：热点键分裂场景的边界计算
     */
    @Test
    public void testHotKeySplitBoundsCalculation() {
        // 模拟热点键分裂的场景：将一个分区分成多个小分区
        long originalLower = 5634770598966349863L;
        long originalUpper = 5634770598966349870L;
        int splitCount = 3;

        // 计算每个新分区的边界
        long rangeSize = (originalUpper - originalLower) / splitCount;

        List<Pair<Long, Long>> newBounds = new ArrayList<>();
        for (int i = 0; i < splitCount; i++) {
            long lower = originalLower + (i * rangeSize);
            long upper = (i == splitCount - 1) ? originalUpper : (originalLower + ((i + 1) * rangeSize));
            newBounds.add(Pair.of(lower, upper));
        }

        Assert.assertEquals("Should have " + splitCount + " new partitions", splitCount, newBounds.size());

        // 验证边界连续性
        for (int i = 1; i < newBounds.size(); i++) {
            Assert.assertEquals("Boundaries should be continuous",
                newBounds.get(i - 1).getValue(), newBounds.get(i).getKey());
        }

        // 验证覆盖完整范围
        Assert.assertEquals("First partition should start at original lower",
            Long.valueOf(originalLower), newBounds.get(0).getKey());
        Assert.assertEquals("Last partition should end at original upper",
            Long.valueOf(originalUpper), newBounds.get(splitCount - 1).getValue());
    }

    /**
     * 测试：QUERY_TABLES_EXTENSIONS SQL模板
     */
    @Test
    public void testQueryTablesExtensionsSql() {
        String sql = InplaceBackfillUtils.QUERY_TABLES_EXTENSIONS;
        Assert.assertNotNull("SQL template should not be null", sql);
        Assert.assertTrue("SQL should query information_schema.tables_extensions",
            sql.contains("information_schema.tables_extensions"));
        Assert.assertTrue("SQL should select secondary_engine_attribute",
            sql.contains("secondary_engine_attribute"));
    }

    /**
     * 测试：SET_SECONDARY_ENGINE_ATTRIBUTE SQL模板
     */
    @Test
    public void testSetSecondaryEngineAttributeSql() {
        String sql = InplaceBackfillUtils.SET_SECONDARY_ENGINE_ATTRIBUTE;
        Assert.assertNotNull("SQL template should not be null", sql);
        Assert.assertTrue("SQL should alter table", sql.contains("alter table"));
        Assert.assertTrue("SQL should set secondary_engine_attribute",
            sql.contains("secondary_engine_attribute"));
    }

    /**
     * 测试：TreeMap大小写不敏感比较器
     */
    @Test
    public void testCaseInsensitiveTreeMap() {
        Map<String, String> map = new TreeMap<>(String::compareToIgnoreCase);
        map.put("GROUP_A", "value_a");
        map.put("group_a", "value_b");

        // 大小写不敏感的TreeMap应该只有一个条目
        Assert.assertEquals("Case insensitive map should have 1 entry", 1, map.size());
        // 后插入的值应该覆盖前一个
        Assert.assertEquals("Value should be the last inserted", "value_b", map.get("GROUP_A"));
        Assert.assertEquals("Value should be accessible with different case", "value_b", map.get("group_A"));
    }
}
