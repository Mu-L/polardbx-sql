package com.alibaba.polardbx.executor.columnar.pruning.index.builder;

import com.alibaba.polardbx.executor.columnar.pruning.index.MultiSortKeyIndex;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class MultiSortKeyIndexBuilderTest {

    @Test
    public void testAppendColumn() {
        MultiSortKeyIndexBuilder builder = new MultiSortKeyIndexBuilder();
        builder.appendColumn(0, DataTypes.IntegerType);
        assertEquals(DataTypes.IntegerType, builder.dtMap.get(0));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAppendColumnWithInvalidData() {
        MultiSortKeyIndexBuilder builder = new MultiSortKeyIndexBuilder();
        builder.appendColumn(-1, null);
    }

    @Test
    public void testAppendNull() {
        MultiSortKeyIndexBuilder builder = new MultiSortKeyIndexBuilder();
        builder.appendNull(0, true);
        assertTrue(builder.nullValMap.get(0).get(0));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAppendNullWithInvalidData() {
        MultiSortKeyIndexBuilder builder = new MultiSortKeyIndexBuilder();
        builder.appendNull(-1, null);
    }

    @Test
    public void testAppendIntegerData() {
        MultiSortKeyIndexBuilder builder = new MultiSortKeyIndexBuilder();
        builder.appendIntegerData(0, 123);
        assertEquals(123, (int) builder.dataMap.get(0).get(0));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAppendIntegerDataWithInvalidData() {
        MultiSortKeyIndexBuilder builder = new MultiSortKeyIndexBuilder();
        builder.appendIntegerData(-1, null);
    }

    @Test
    public void testAppendLongData() {
        MultiSortKeyIndexBuilder builder = new MultiSortKeyIndexBuilder();
        builder.appendLongData(0, 123L);
        assertEquals(123L, (long) builder.dataMap.get(0).get(0));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAppendLongDataWithInvalidData() {
        MultiSortKeyIndexBuilder builder = new MultiSortKeyIndexBuilder();
        builder.appendLongData(-1, null);
    }

    @Test
    public void testAppendStringData() {
        MultiSortKeyIndexBuilder builder = new MultiSortKeyIndexBuilder();
        builder.appendStringData(0, "test");
        assertEquals("test", builder.dataMap.get(0).get(0));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testAppendStringDataWithInvalidData() {
        MultiSortKeyIndexBuilder builder = new MultiSortKeyIndexBuilder();
        builder.appendStringData(-1, null);
    }

    @Test
    public void testBuild() {
        MultiSortKeyIndexBuilder builder = new MultiSortKeyIndexBuilder();
        builder.appendColumn(0, DataTypes.IntegerType);
        builder.appendIntegerData(0, 1);
        builder.appendIntegerData(0, 2);
        builder.appendNull(0, false);
        builder.appendNull(0, true);

        MultiSortKeyIndex index = builder.build();
        assertNotNull(index);
        assertEquals(1, index.rgNum());
        assertEquals(DataTypes.IntegerType, index.getColumnDataType(0));
    }

    @Test
    public void testBuildWithNoData() {
        MultiSortKeyIndexBuilder builder = new MultiSortKeyIndexBuilder();
        MultiSortKeyIndex index = builder.build();
        assertNull(index);
    }
}

