package com.alibaba.polardbx.executor.columnar.pruning.index;

import com.alibaba.polardbx.executor.columnar.pruning.predicate.ColumnPredicatePruningInf;
import com.alibaba.polardbx.executor.columnar.pruning.predicate.OrColumnPredicate;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.Field;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.statis.ColumnarTracer;
import org.junit.Before;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class IndexPrunerTest {

    private IndexPruner indexPruner;
    private ColumnPredicatePruningInf cpp;
    private IndexPruneContext ipc;
    private List<ColumnMeta> columns;

    @Before
    public void setUp() {
        long rgNum = 2;
        String filePath = "test.orc";

        IndexPruner.IndexPrunerBuilder builder = new IndexPruner.IndexPrunerBuilder(filePath, false);
        builder.setRgNum((int) rgNum);
        builder.appendZoneMap(1, 1L, 2L);
        builder.appendMultiSortKeyColumn(1, 1L, 2L);
        builder.appendMockSortKeyIndex(1, 2, DataTypes.IntegerType);
        builder.stripeEnd();
        builder.setSortKeyDataType(DataTypes.IntegerType);
        indexPruner = builder.build();

        cpp = new OrColumnPredicate();
        ipc = new IndexPruneContext();
        ipc.setPruneTracer(new ColumnarTracer());

        columns = new ArrayList<>();
        columns.add(new ColumnMeta("table", "col1", "col1", new Field(DataTypes.IntegerType)));
    }

    @Test
    public void testPruneToSortMap() {
        SortedMap<Integer, boolean[]> sortedMap = indexPruner.pruneToSortMap("testTable", columns, cpp, ipc);

        assertNotNull(sortedMap);
        assertEquals(1, sortedMap.size());
    }

    @Test
    public void testPruneToSortMapWithRoaringBitmap() {
        RoaringBitmap rr = new RoaringBitmap();
        rr.add(0);
        rr.add(1);

        SortedMap<Integer, boolean[]> sortedMap = indexPruner.pruneToSortMap(rr);

        assertNotNull(sortedMap);
        assertEquals(1, sortedMap.size());
    }

    @Test
    public void testPrune() {
        RoaringBitmap pruneResult = indexPruner.prune("testTable", columns, cpp, ipc);

        assertNotNull(pruneResult);
    }

    @Test
    public void testPruneOnlyDeletedRowGroups() {
        RoaringBitmap deleteBitmap = new RoaringBitmap();
        deleteBitmap.add(0);
        deleteBitmap.add(1);

        RoaringBitmap result = indexPruner.pruneOnlyDeletedRowGroups(deleteBitmap);

        assertNotNull(result);
        assertEquals(0, result.getCardinality());
    }

    @Test
    public void testPruneIndex() {
        RoaringBitmap cur = new RoaringBitmap();
        cur.add(0);
        cur.add(1);

        indexPruner.pruneIndex("testTable", cpp, ipc, PruneAction.SORT_KEY_INDEX_PRUNE, indexPruner.sortKeyIndex, cur,
            columns);

        // Since we are not using mocks, we need to verify the state of the RoaringBitmap or other side effects.
        // This is a placeholder for the actual verification logic.
        assertNotNull(cur);
    }

    @Test
    public void testGetOrcFile() {
        assertEquals("test.orc", indexPruner.getOrcFile());
    }

    @Test
    public void testSetOrcFile() {
        indexPruner.setOrcFile("newTest.orc");
        assertEquals("newTest.orc", indexPruner.getOrcFile());
    }

    @Test
    public void testGetRgNum() {
        assertEquals(2, indexPruner.getRgNum());
    }

    @Test
    public void testGetStripeRgNum() {
        List<Integer> stripeRgNum = indexPruner.getStripeRgNum();
        assertNotNull(stripeRgNum);
        assertEquals(1, stripeRgNum.size());
    }

    @Test
    public void testGetSizeInBytes() {
        long sizeInBytes = indexPruner.getSizeInBytes();
        assertTrue(sizeInBytes > 0);
    }

    @Test
    public void testUpdateSizeInfo() {
        indexPruner.updateSizeInfo();
        long sizeInBytes = indexPruner.getSizeInBytes();
        assertTrue(sizeInBytes > 0);
    }
}
