package com.alibaba.polardbx.planner.ddl;

import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupSplitPartitionPreparedData;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Unit tests for AlterTableGroupSplitPartitionPreparedData multi-partition split logic.
 * Tests rebuildPartitionRelationship() which is the core method building srcTargetPartitionMap.
 */
public class MultiSplitPartitionPreparedDataTest {

    @Test
    public void test_rebuildPartitionRelationship_multiPartition_2sources() {
        AlterTableGroupSplitPartitionPreparedData pd = new AlterTableGroupSplitPartitionPreparedData();
        pd.setOldPartitionNames(new ArrayList<>(Arrays.asList("p1", "p2")));
        pd.setNewPartitionNames(new ArrayList<>(Arrays.asList("p1_0", "p1_1", "p2_0", "p2_1")));

        // Multi-partition path: getOldPartitionNames().size() > 1
        // This path doesn't use PartitionInfo, so null is safe
        pd.rebuildPartitionRelationship(null, null);

        Map<String, Set<String>> map = pd.getSrcTargetPartitionMap();
        Assert.assertEquals(2, map.size());
        Assert.assertTrue(map.containsKey("p1"));
        Assert.assertTrue(map.containsKey("p2"));
        Assert.assertEquals(2, map.get("p1").size());
        Assert.assertTrue(map.get("p1").contains("p1_0"));
        Assert.assertTrue(map.get("p1").contains("p1_1"));
        Assert.assertEquals(2, map.get("p2").size());
        Assert.assertTrue(map.get("p2").contains("p2_0"));
        Assert.assertTrue(map.get("p2").contains("p2_1"));
    }

    @Test
    public void test_rebuildPartitionRelationship_multiPartition_3sources() {
        AlterTableGroupSplitPartitionPreparedData pd = new AlterTableGroupSplitPartitionPreparedData();
        pd.setOldPartitionNames(new ArrayList<>(Arrays.asList("p1", "p2", "p3")));
        pd.setNewPartitionNames(new ArrayList<>(Arrays.asList(
            "p1_a", "p1_b", "p1_c",
            "p2_a", "p2_b", "p2_c",
            "p3_a", "p3_b", "p3_c"
        )));

        pd.rebuildPartitionRelationship(null, null);

        Map<String, Set<String>> map = pd.getSrcTargetPartitionMap();
        Assert.assertEquals(3, map.size());
        Assert.assertEquals(3, map.get("p1").size());
        Assert.assertEquals(3, map.get("p2").size());
        Assert.assertEquals(3, map.get("p3").size());
        Assert.assertTrue(map.get("p3").contains("p3_a"));
        Assert.assertTrue(map.get("p3").contains("p3_b"));
        Assert.assertTrue(map.get("p3").contains("p3_c"));
    }

    @Test
    public void test_rebuildPartitionRelationship_multiPartition_firstLevelActive() {
        AlterTableGroupSplitPartitionPreparedData pd = new AlterTableGroupSplitPartitionPreparedData();
        pd.setOldPartitionNames(new ArrayList<>(Arrays.asList("p1", "p2")));
        pd.setNewPartitionNames(new ArrayList<>(Arrays.asList("np1", "np2", "np3", "np4")));

        pd.rebuildPartitionRelationship(null, null);

        Assert.assertTrue(pd.isFirstPartitionLevelActiveForInplaceBackfill());
    }

    @Test
    public void test_rebuildPartitionRelationship_calledTwice_clearsMap() {
        AlterTableGroupSplitPartitionPreparedData pd = new AlterTableGroupSplitPartitionPreparedData();
        pd.setOldPartitionNames(new ArrayList<>(Arrays.asList("p1", "p2")));
        pd.setNewPartitionNames(new ArrayList<>(Arrays.asList("a", "b", "c", "d")));

        pd.rebuildPartitionRelationship(null, null);
        Assert.assertEquals(2, pd.getSrcTargetPartitionMap().size());

        // Change names and rebuild - map should be refreshed
        pd.setOldPartitionNames(new ArrayList<>(Arrays.asList("x", "y", "z")));
        pd.setNewPartitionNames(new ArrayList<>(Arrays.asList("x1", "x2", "y1", "y2", "z1", "z2")));
        pd.rebuildPartitionRelationship(null, null);

        Map<String, Set<String>> map = pd.getSrcTargetPartitionMap();
        Assert.assertEquals(3, map.size());
        Assert.assertFalse(map.containsKey("p1"));
        Assert.assertTrue(map.containsKey("x"));
        Assert.assertTrue(map.containsKey("y"));
        Assert.assertTrue(map.containsKey("z"));
    }

    @Test
    public void test_getSplitPartitions_delegatesToOldPartitionNames() {
        AlterTableGroupSplitPartitionPreparedData pd = new AlterTableGroupSplitPartitionPreparedData();
        List<String> names = Arrays.asList("p1", "p2", "p3");
        pd.setSplitPartitions(new ArrayList<>(names));

        Assert.assertEquals(3, pd.getSplitPartitions().size());
        Assert.assertEquals("p1", pd.getSplitPartitions().get(0));
        Assert.assertEquals("p2", pd.getSplitPartitions().get(1));
        Assert.assertEquals("p3", pd.getSplitPartitions().get(2));

        // Same reference as getOldPartitionNames()
        Assert.assertSame(pd.getSplitPartitions(), pd.getOldPartitionNames());
    }

    @Test
    public void test_rebuildPartitionRelationship_unevenDivision() {
        AlterTableGroupSplitPartitionPreparedData pd = new AlterTableGroupSplitPartitionPreparedData();
        pd.setInplaceBackfill(true);
        pd.setOldPartitionNames(new ArrayList<>(Arrays.asList("p1", "p2")));
        // 5 new partitions for 2 sources = not evenly divisible
        pd.setNewPartitionNames(new ArrayList<>(Arrays.asList("n1", "n2", "n3", "n4", "n5")));
        pd.rebuildPartitionRelationship(null, null);

        // Redistribution case: inplaceBackfill should be disabled
        Assert.assertFalse(pd.isInplaceBackfill());
        // srcTargetPartitionMap should remain empty (changeset falls back to full catch-up)
        Assert.assertTrue(pd.getSrcTargetPartitionMap().isEmpty());
    }

    @Test
    public void test_rebuildPartitionRelationship_emptyOldPartitions() {
        AlterTableGroupSplitPartitionPreparedData pd = new AlterTableGroupSplitPartitionPreparedData();
        pd.setOldPartitionNames(new ArrayList<>());
        pd.setNewPartitionNames(new ArrayList<>(Arrays.asList("n1", "n2")));
        // Empty oldPartitionNames: size() == 0, not > 1, so takes single-partition path
        // which requires non-null PartitionInfo -> expect NPE or similar
        try {
            pd.rebuildPartitionRelationship(null, null);
            Assert.fail("Expected exception for empty oldPartitionNames with null PartitionInfo");
        } catch (NullPointerException e) {
            // expected: single-partition path dereferences partitionInfo
        }
    }

    @Test
    public void test_rebuildPartitionRelationship_emptyNewPartitions() {
        AlterTableGroupSplitPartitionPreparedData pd = new AlterTableGroupSplitPartitionPreparedData();
        pd.setOldPartitionNames(new ArrayList<>(Arrays.asList("p1", "p2")));
        pd.setNewPartitionNames(new ArrayList<>());
        // 0 new partitions / 2 old = 0 per source, 0 % 2 == 0 passes divisibility check
        // newPerSource = 0, inner loop never executes, computeIfAbsent never called -> empty map
        pd.rebuildPartitionRelationship(null, null);

        Map<String, Set<String>> map = pd.getSrcTargetPartitionMap();
        Assert.assertTrue("Map should be empty when no new partitions are provided", map.isEmpty());
    }
}
