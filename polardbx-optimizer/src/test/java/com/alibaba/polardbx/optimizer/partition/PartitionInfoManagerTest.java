package com.alibaba.polardbx.optimizer.partition;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.common.PartitionLocation;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link PartitionInfoManager#getRandomPhysicalPartition(String)}.
 *
 * @author fangwu
 */
public class PartitionInfoManagerTest {

    private static final String TABLE_NAME = "test_table";
    private static final String GROUP_KEY = "group_0";
    private static final String PHY_TABLE = "test_table_0";
    private static final long PART_ID = 42L;
    private static final PartKeyLevel PART_LEVEL = PartKeyLevel.PARTITION_KEY;
    private static final String PART_NAME = "p1";

    private PartitionInfoManager partitionInfoManager;

    @Before
    public void setUp() {
        // Use the two-arg constructor (schemaName, appName) which does not
        // trigger doInit() / MetaDB access.
        partitionInfoManager = new PartitionInfoManager("test_schema", "test_app");
    }

    // -----------------------------------------------------------------------
    // getRandomPhysicalPartition – table not registered
    // -----------------------------------------------------------------------

    /**
     * When the table name is not present in the cache, the method must return null.
     */
    @Test
    public void testGetRandomPhysicalPartitionReturnsNullWhenTableNotFound() {
        PhysicalPartitionInfo result = partitionInfoManager.getRandomPhysicalPartition("non_existent_table");

        Assert.assertNull("Should return null for an unknown table", result);
    }

    // -----------------------------------------------------------------------
    // getRandomPhysicalPartition – single partition
    // -----------------------------------------------------------------------

    /**
     * When the table has exactly one physical partition, the method must always
     * return that partition's info (no randomness involved).
     */
    @Test
    public void testGetRandomPhysicalPartitionWithSinglePartition() {
        PartitionSpec partitionSpec = buildMockPartitionSpec(PART_ID, PART_LEVEL, PART_NAME, GROUP_KEY, PHY_TABLE);
        registerTableWithPartitions(TABLE_NAME, Collections.singletonList(partitionSpec));

        PhysicalPartitionInfo result = partitionInfoManager.getRandomPhysicalPartition(TABLE_NAME);

        Assert.assertNotNull("Result must not be null for a registered table", result);
        Assert.assertEquals("groupKey must match", GROUP_KEY, result.getGroupKey());
        Assert.assertEquals("phyTable must match", PHY_TABLE, result.getPhyTable());
        Assert.assertEquals("partId must match", PART_ID, (long) result.getPartId());
        Assert.assertEquals("partLevel must match", PART_LEVEL, result.getPartLevel());
        Assert.assertEquals("partName must match", PART_NAME, result.getPartName());
    }

    // -----------------------------------------------------------------------
    // getRandomPhysicalPartition – multiple partitions
    // -----------------------------------------------------------------------

    /**
     * When the table has multiple physical partitions, the returned partition
     * must be one of the registered partitions (groupKey must be one of the
     * known values).
     */
    @Test
    public void testGetRandomPhysicalPartitionWithMultiplePartitionsReturnsValidPartition() {
        PartitionSpec spec0 = buildMockPartitionSpec(1L, PartKeyLevel.PARTITION_KEY, "p0", "group_0", "t_0");
        PartitionSpec spec1 = buildMockPartitionSpec(2L, PartKeyLevel.PARTITION_KEY, "p1", "group_1", "t_1");
        PartitionSpec spec2 = buildMockPartitionSpec(3L, PartKeyLevel.PARTITION_KEY, "p2", "group_2", "t_2");
        registerTableWithPartitions(TABLE_NAME, Arrays.asList(spec0, spec1, spec2));

        // Run several times to exercise the random selection path.
        for (int iteration = 0; iteration < 20; iteration++) {
            PhysicalPartitionInfo result = partitionInfoManager.getRandomPhysicalPartition(TABLE_NAME);

            Assert.assertNotNull("Result must not be null", result);
            Assert.assertTrue(
                "groupKey must be one of the registered groups",
                result.getGroupKey().equals("group_0")
                    || result.getGroupKey().equals("group_1")
                    || result.getGroupKey().equals("group_2"));
            Assert.assertTrue(
                "partName must be one of the registered partition names",
                result.getPartName().equals("p0")
                    || result.getPartName().equals("p1")
                    || result.getPartName().equals("p2"));
        }
    }

    /**
     * The partBitSetIdx stored in the result must equal the index of the chosen
     * partition inside the physicalPartitions list.  We verify this by
     * registering a single-element list so the index is always 0.
     */
    @Test
    public void testGetRandomPhysicalPartitionPartBitSetIdxMatchesListIndex() {
        PartitionSpec spec = buildMockPartitionSpec(10L, PartKeyLevel.SUBPARTITION_KEY, "px", "grp_x", "tbl_x");
        registerTableWithPartitions(TABLE_NAME, Collections.singletonList(spec));

        PhysicalPartitionInfo result = partitionInfoManager.getRandomPhysicalPartition(TABLE_NAME);

        Assert.assertNotNull(result);
        Assert.assertEquals("partBitSetIdx must equal the list index (0 for single-element list)",
            0, (long) result.getPartBitSetIdx());
    }

    // -----------------------------------------------------------------------
    // Helpers
    // -----------------------------------------------------------------------

    /**
     * Build a mock {@link PartitionSpec} that returns the given field values.
     */
    private PartitionSpec buildMockPartitionSpec(long partId, PartKeyLevel partLevelIgnored, String partName,
                                                 String groupKey, String phyTableName) {
        PartitionLocation location = mock(PartitionLocation.class);
        when(location.getGroupKey()).thenReturn(groupKey);
        when(location.getPhyTableName()).thenReturn(phyTableName);

        PartitionSpec spec = mock(PartitionSpec.class);
        when(spec.getLocation()).thenReturn(location);
        when(spec.getId()).thenReturn(partId);
        when(spec.getPartLevel()).thenReturn(PartKeyLevel.PARTITION_KEY);
        when(spec.getName()).thenReturn(partName);
        return spec;
    }

    // -----------------------------------------------------------------------
    // getFirstPhysicalPartition – table not registered
    // -----------------------------------------------------------------------

    /**
     * When the table name is not present in the cache, getFirstPhysicalPartition must return null.
     */
    @Test
    public void testGetFirstPhysicalPartitionReturnsNullWhenTableNotFound() {
        PhysicalPartitionInfo result = partitionInfoManager.getFirstPhysicalPartition("non_existent_table");

        Assert.assertNull("Should return null for an unknown table", result);
    }

    // -----------------------------------------------------------------------
    // getFirstPhysicalPartition – normal partitioned table (physicalPartitions non-empty)
    // -----------------------------------------------------------------------

    /**
     * Normal path: physicalPartitions is non-empty – must return info from the first physical partition.
     */
    @Test
    public void testGetFirstPhysicalPartitionWithPhysicalPartitions() {
        PartitionSpec partitionSpec = buildMockPartitionSpec(PART_ID, PART_LEVEL, PART_NAME, GROUP_KEY, PHY_TABLE);
        registerTableWithFirstPhysicalPartitions(TABLE_NAME,
            Collections.singletonList(partitionSpec), Collections.emptyList());

        PhysicalPartitionInfo result = partitionInfoManager.getFirstPhysicalPartition(TABLE_NAME);

        Assert.assertNotNull("Result must not be null", result);
        Assert.assertEquals("groupKey must match", GROUP_KEY, result.getGroupKey());
        Assert.assertEquals("phyTable must match", PHY_TABLE, result.getPhyTable());
        Assert.assertEquals("partId must match", PART_ID, (long) result.getPartId());
        Assert.assertEquals("partLevel must match", PART_LEVEL, result.getPartLevel());
        Assert.assertEquals("partName must match", PART_NAME, result.getPartName());
        Assert.assertEquals("partBitSetIdx must be 0", 0, (long) result.getPartBitSetIdx());
    }

    // -----------------------------------------------------------------------
    // getFirstPhysicalPartition – single-table CCI (physicalPartitions empty, falls back to logical)
    // -----------------------------------------------------------------------

    /**
     * Single-table CCI scenario: physicalPartitions is empty, must fall back to logical partitions.
     */
    @Test
    public void testGetFirstPhysicalPartitionFallsBackToLogicalPartitionsWhenPhysicalEmpty() {
        PartitionSpec logicalSpec = buildMockPartitionSpec(77L, PartKeyLevel.PARTITION_KEY, "lp0", "grp_l", "tbl_l");
        registerTableWithFirstPhysicalPartitions(TABLE_NAME,
            Collections.emptyList(), Collections.singletonList(logicalSpec));

        PhysicalPartitionInfo result = partitionInfoManager.getFirstPhysicalPartition(TABLE_NAME);

        Assert.assertNotNull("Result must not be null when falling back to logical partitions", result);
        Assert.assertEquals("groupKey must come from logical partition", "grp_l", result.getGroupKey());
        Assert.assertEquals("phyTable must come from logical partition", "tbl_l", result.getPhyTable());
        Assert.assertEquals("partId must come from logical partition", 77L, (long) result.getPartId());
        Assert.assertEquals("partName must come from logical partition", "lp0", result.getPartName());
    }

    // -----------------------------------------------------------------------
    // getFirstPhysicalPartition – physicalPartitions null, falls back to logical
    // -----------------------------------------------------------------------

    /**
     * physicalPartitions is null (not just empty) – must fall back to logical partitions.
     */
    @Test
    public void testGetFirstPhysicalPartitionFallsBackWhenPhysicalPartitionsNull() {
        PartitionSpec logicalSpec = buildMockPartitionSpec(88L, PartKeyLevel.PARTITION_KEY, "lp1", "grp_n", "tbl_n");
        registerTableWithFirstPhysicalPartitions(TABLE_NAME, null, Collections.singletonList(logicalSpec));

        PhysicalPartitionInfo result = partitionInfoManager.getFirstPhysicalPartition(TABLE_NAME);

        Assert.assertNotNull("Result must not be null when physicalPartitions is null", result);
        Assert.assertEquals("groupKey must come from logical partition", "grp_n", result.getGroupKey());
        Assert.assertEquals("partId must come from logical partition", 88L, (long) result.getPartId());
    }

    // -----------------------------------------------------------------------
    // getFirstPhysicalPartition – partitionBy is null → exception
    // -----------------------------------------------------------------------

    /**
     * When PartitionBy is null, the method must throw a TddlRuntimeException.
     */
    @Test(expected = TddlRuntimeException.class)
    public void testGetFirstPhysicalPartitionThrowsWhenPartitionByNull() {
        registerTableWithNullPartitionBy(TABLE_NAME);

        partitionInfoManager.getFirstPhysicalPartition(TABLE_NAME);
    }

    // -----------------------------------------------------------------------
    // getFirstPhysicalPartition – both physical and logical empty → exception
    // -----------------------------------------------------------------------

    /**
     * When both physicalPartitions and logicalPartitions are empty, the method must throw.
     */
    @Test(expected = TddlRuntimeException.class)
    public void testGetFirstPhysicalPartitionThrowsWhenBothPartitionListsEmpty() {
        registerTableWithFirstPhysicalPartitions(TABLE_NAME,
            Collections.emptyList(), Collections.emptyList());

        partitionInfoManager.getFirstPhysicalPartition(TABLE_NAME);
    }

    // -----------------------------------------------------------------------
    // Helpers – registerTableWithPartitions / registerTableWithNullPartitionBy
    // -----------------------------------------------------------------------

    /**
     * Register a table in the manager's cache with the given physical partitions.
     * Uses the package-accessible {@code partInfoCtxCache} field directly.
     */
    private void registerTableWithPartitions(String tableName, List<PartitionSpec> physicalPartitions) {
        PartitionByDefinition partitionByDefinition = mock(PartitionByDefinition.class);
        when(partitionByDefinition.getPhysicalPartitions()).thenReturn(physicalPartitions);

        PartitionInfo partitionInfo = mock(PartitionInfo.class);
        when(partitionInfo.getPartitionBy()).thenReturn(partitionByDefinition);

        PartitionInfoManager.PartInfoCtx partInfoCtx = mock(PartitionInfoManager.PartInfoCtx.class);
        when(partInfoCtx.getPartInfo()).thenReturn(partitionInfo);

        partitionInfoManager.partInfoCtxCache.put(tableName, partInfoCtx);
    }

    /**
     * Register a table with separate physicalPartitions and logicalPartitions lists,
     * to support testing the getFirstPhysicalPartition fallback logic.
     */
    private void registerTableWithFirstPhysicalPartitions(String tableName,
                                                          List<PartitionSpec> physicalPartitions,
                                                          List<PartitionSpec> logicalPartitions) {
        PartitionByDefinition partitionByDefinition = mock(PartitionByDefinition.class);
        when(partitionByDefinition.getPhysicalPartitions()).thenReturn(physicalPartitions);
        when(partitionByDefinition.getPartitions()).thenReturn(logicalPartitions);

        PartitionInfo partitionInfo = mock(PartitionInfo.class);
        when(partitionInfo.getPartitionBy()).thenReturn(partitionByDefinition);

        PartitionInfoManager.PartInfoCtx partInfoCtx = mock(PartitionInfoManager.PartInfoCtx.class);
        when(partInfoCtx.getPartInfo()).thenReturn(partitionInfo);

        partitionInfoManager.partInfoCtxCache.put(tableName, partInfoCtx);
    }

    /**
     * Register a table whose PartitionInfo returns null for getPartitionBy().
     */
    private void registerTableWithNullPartitionBy(String tableName) {
        PartitionInfo partitionInfo = mock(PartitionInfo.class);
        when(partitionInfo.getPartitionBy()).thenReturn(null);

        PartitionInfoManager.PartInfoCtx partInfoCtx = mock(PartitionInfoManager.PartInfoCtx.class);
        when(partInfoCtx.getPartInfo()).thenReturn(partitionInfo);

        partitionInfoManager.partInfoCtxCache.put(tableName, partInfoCtx);
    }
}
