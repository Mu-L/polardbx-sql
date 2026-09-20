package com.alibaba.polardbx.executor.columnar.pruning.index;

import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.apache.orc.OrcProto;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class MultiSortKeyIndexTest {
    @Test
    public void testDate() {
        IndexPruner.IndexPrunerBuilder builder = new IndexPruner.IndexPrunerBuilder("", false);
        builder.appendMultiSortKeyColumn(0, OrcProto.DateStatistics.newBuilder().build());
    }

    @Test
    public void testBuild() {
        long rgNum = 2;
        Map<Integer, DataType> dtMap = new HashMap<>();
        dtMap.put(0, DataTypes.IntegerType);
        dtMap.put(1, DataTypes.LongType);

        Map<Integer, ArrayList<Object>> dataMap = new HashMap<>();
        dataMap.put(0, new ArrayList<>(Arrays.asList(1, 2, 3, 4)));
        dataMap.put(1, new ArrayList<>(Arrays.asList(1L, 2L, 3L, 4L)));

        Map<Integer, RoaringBitmap> nullValMap = new HashMap<>();
        nullValMap.put(0, RoaringBitmap.bitmapOf(0, 1));
        nullValMap.put(1, RoaringBitmap.bitmapOf(0));

        MultiSortKeyIndex index = MultiSortKeyIndex.build(rgNum, dtMap, dataMap, nullValMap);

        assertNotNull(index);
        assertEquals(rgNum, index.rgNum());
        assertEquals(dtMap, index.dtMap);
        assertEquals(dataMap, index.dataMap);
        assertEquals(nullValMap, index.nullValMap);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testBuildWithInvalidData() {
        MultiSortKeyIndex.build(0, null, null, null);
    }

    @Test
    public void testPruneNull() {
        long rgNum = 2;
        Map<Integer, DataType> dtMap = new HashMap<>();
        dtMap.put(0, DataTypes.IntegerType);

        Map<Integer, ArrayList<Object>> dataMap = new HashMap<>();
        dataMap.put(0, new ArrayList<>(Arrays.asList(1, 2, 3, 4)));

        Map<Integer, RoaringBitmap> nullValMap = new HashMap<>();
        nullValMap.put(0, RoaringBitmap.bitmapOf(0, 1));

        MultiSortKeyIndex index = MultiSortKeyIndex.build(rgNum, dtMap, dataMap, nullValMap);

        RoaringBitmap cur = new RoaringBitmap();
        cur.add(0);
        cur.add(1);

        index.pruneNull(0, cur);
    }

    @Test
    public void testPruneRange() {
        long rgNum = 2;
        Map<Integer, DataType> dtMap = new HashMap<>();
        dtMap.put(0, DataTypes.IntegerType);

        Map<Integer, ArrayList<Object>> dataMap = new HashMap<>();
        dataMap.put(0, new ArrayList<>(Arrays.asList(1, 2, 3, 4)));

        Map<Integer, RoaringBitmap> nullValMap = new HashMap<>();

        MultiSortKeyIndex index = MultiSortKeyIndex.build(rgNum, dtMap, dataMap, nullValMap);

        RoaringBitmap cur = new RoaringBitmap();
        cur.add(0);
        cur.add(1);

        index.prune(0, 1, true, 3, true, cur, null);

        assertTrue(cur.contains(0));
    }

    @Test
    public void testCheckSupport() {
        long rgNum = 2;
        Map<Integer, DataType> dtMap = new HashMap<>();
        dtMap.put(0, DataTypes.IntegerType);

        Map<Integer, ArrayList<Object>> dataMap = new HashMap<>();
        dataMap.put(0, new ArrayList<>(Arrays.asList(1, 2, 3, 4)));

        Map<Integer, RoaringBitmap> nullValMap = new HashMap<>();

        MultiSortKeyIndex index = MultiSortKeyIndex.build(rgNum, dtMap, dataMap, nullValMap);

        assertTrue(index.checkSupport(0, org.apache.calcite.sql.type.SqlTypeName.INTEGER));
        assertFalse(index.checkSupport(0, org.apache.calcite.sql.type.SqlTypeName.VARCHAR));
    }

    @Test
    public void testGetColumnDataType() {
        long rgNum = 2;
        Map<Integer, DataType> dtMap = new HashMap<>();
        dtMap.put(0, DataTypes.IntegerType);

        Map<Integer, ArrayList<Object>> dataMap = new HashMap<>();
        dataMap.put(0, new ArrayList<>(Arrays.asList(1, 2, 3, 4)));

        Map<Integer, RoaringBitmap> nullValMap = new HashMap<>();

        MultiSortKeyIndex index = MultiSortKeyIndex.build(rgNum, dtMap, dataMap, nullValMap);

        assertEquals(DataTypes.IntegerType, index.getColumnDataType(0));
    }

    @Test
    public void testColIds() {
        long rgNum = 2;
        Map<Integer, DataType> dtMap = new HashMap<>();
        dtMap.put(0, DataTypes.IntegerType);
        dtMap.put(1, DataTypes.LongType);

        Map<Integer, ArrayList<Object>> dataMap = new HashMap<>();
        dataMap.put(0, new ArrayList<>(Arrays.asList(1, 2, 3, 4)));
        dataMap.put(1, new ArrayList<>(Arrays.asList(1L, 2L, 3L, 4L)));

        Map<Integer, RoaringBitmap> nullValMap = new HashMap<>();

        MultiSortKeyIndex index = MultiSortKeyIndex.build(rgNum, dtMap, dataMap, nullValMap);

        assertEquals("0,1", index.colIds());
    }

    @Test
    public void testGroupSize() {
        long rgNum = 2;
        Map<Integer, DataType> dtMap = new HashMap<>();
        dtMap.put(0, DataTypes.IntegerType);

        Map<Integer, ArrayList<Object>> dataMap = new HashMap<>();
        dataMap.put(0, new ArrayList<>(Arrays.asList(1, 2, 3, 4)));

        Map<Integer, RoaringBitmap> nullValMap = new HashMap<>();

        MultiSortKeyIndex index = MultiSortKeyIndex.build(rgNum, dtMap, dataMap, nullValMap);
    }

    @Test
    public void testGetSizeInBytes() {
        long rgNum = 2;
        Map<Integer, DataType> dtMap = new HashMap<>();
        dtMap.put(0, DataTypes.IntegerType);

        Map<Integer, ArrayList<Object>> dataMap = new HashMap<>();
        dataMap.put(0, new ArrayList<>(Arrays.asList(1, 2, 3, 4)));

        Map<Integer, RoaringBitmap> nullValMap = new HashMap<>();
        nullValMap.put(0, RoaringBitmap.bitmapOf(0, 1));

        MultiSortKeyIndex index = MultiSortKeyIndex.build(rgNum, dtMap, dataMap, nullValMap);

        long sizeInBytes = index.getSizeInBytes();
        assertTrue(sizeInBytes > 0);
    }
}
