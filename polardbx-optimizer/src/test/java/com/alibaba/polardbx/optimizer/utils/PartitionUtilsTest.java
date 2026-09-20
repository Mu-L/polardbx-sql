package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartSpecSearcher;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Covers the O(1) search path introduced by commit d1171a12 (to #65131863).
 * Only two branches are asserted here, strictly aligned with the diff:
 * 1) PartSpecSearcher.getPartSpec returns a spec with position  -> calcPartition == position - 1
 * 2) PartSpecSearcher.getPartSpec returns null                   -> calcPartition == -2
 */
public class PartitionUtilsTest {

    private static final String LOGICAL_SCHEMA = "ls";
    private static final String LOGICAL_TABLE = "lt";
    private static final String PHYSICAL_SCHEMA = "g";
    private static final String PHYSICAL_TABLE = "p";

    @Test
    public void testCalcPartition_hit_returnsPositionMinusOne() {
        PartitionSpec spec = mock(PartitionSpec.class);
        when(spec.getPosition()).thenReturn(5L);

        PartSpecSearcher searcher = mock(PartSpecSearcher.class);
        when(searcher.getPartSpec(PHYSICAL_SCHEMA, PHYSICAL_TABLE)).thenReturn(spec);

        PartitionInfo partInfo = mock(PartitionInfo.class);
        when(partInfo.getPartSpecSearcher()).thenReturn(searcher);

        TableMeta tableMeta = mock(TableMeta.class);
        when(tableMeta.getPartitionInfo()).thenReturn(partInfo);

        SchemaManager schemaManager = mock(SchemaManager.class);
        when(schemaManager.getTable(LOGICAL_TABLE)).thenReturn(tableMeta);

        ExecutionContext ec = mock(ExecutionContext.class);
        when(ec.getSchemaManager(LOGICAL_SCHEMA)).thenReturn(schemaManager);

        int result = PartitionUtils.calcPartition(
            LOGICAL_SCHEMA, LOGICAL_TABLE, PHYSICAL_SCHEMA, PHYSICAL_TABLE, ec);

        Assert.assertEquals(4, result);
    }

    @Test
    public void testCalcPartition_miss_returnsMinusTwo() {
        PartSpecSearcher searcher = mock(PartSpecSearcher.class);
        when(searcher.getPartSpec(PHYSICAL_SCHEMA, PHYSICAL_TABLE)).thenReturn(null);

        PartitionInfo partInfo = mock(PartitionInfo.class);
        when(partInfo.getPartSpecSearcher()).thenReturn(searcher);

        TableMeta tableMeta = mock(TableMeta.class);
        when(tableMeta.getPartitionInfo()).thenReturn(partInfo);

        SchemaManager schemaManager = mock(SchemaManager.class);
        when(schemaManager.getTable(LOGICAL_TABLE)).thenReturn(tableMeta);

        ExecutionContext ec = mock(ExecutionContext.class);
        when(ec.getSchemaManager(LOGICAL_SCHEMA)).thenReturn(schemaManager);

        int result = PartitionUtils.calcPartition(
            LOGICAL_SCHEMA, LOGICAL_TABLE, PHYSICAL_SCHEMA, PHYSICAL_TABLE, ec);

        Assert.assertEquals(-2, result);
    }
}
