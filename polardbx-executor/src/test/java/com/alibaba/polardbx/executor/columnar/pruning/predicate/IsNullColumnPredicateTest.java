package com.alibaba.polardbx.executor.columnar.pruning.predicate;

import com.alibaba.polardbx.executor.columnar.pruning.index.IndexPruneContext;
import com.alibaba.polardbx.executor.columnar.pruning.index.MultiSortKeyIndex;
import com.alibaba.polardbx.executor.columnar.pruning.index.ZoneMapIndex;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Covers the IS NULL pruning path where the column type is carried from the
 * predicate to the index layer via {@link SqlTypeName}. Before the fix,
 * {@link IsNullColumnPredicate} had a single-argument constructor that passed
 * {@code null} to the {@link ColumnPredicate} base, which caused every
 * {@code checkSupport(colId, null)} call to return {@code false} and silently
 * skipped pruneNull for every IS NULL predicate.
 *
 * <p>These tests use the two-argument constructor {@code IsNullColumnPredicate(
 * SqlTypeName type, int colId)}. If the constructor signature regresses to a
 * single-argument shape, compilation here will fail — that is the intended red
 * signal.
 */
public class IsNullColumnPredicateTest {

    private static final int COL_ID = 0;

    private IndexPruneContext ipc;

    @Before
    public void setUp() {
        ipc = new IndexPruneContext();
    }

    @Test
    public void zoneMap_supportedType_callsPruneNull() {
        ZoneMapIndex zoneMap = mock(ZoneMapIndex.class);
        when(zoneMap.checkSupport(COL_ID, SqlTypeName.BIGINT)).thenReturn(true);

        IsNullColumnPredicate predicate = new IsNullColumnPredicate(SqlTypeName.BIGINT, COL_ID);
        RoaringBitmap cur = RoaringBitmap.bitmapOfRange(0, 10);

        predicate.zoneMap(zoneMap, ipc, cur);

        verify(zoneMap, times(1)).pruneNull(eq(COL_ID), any(RoaringBitmap.class));
    }

    @Test
    public void zoneMap_unsupportedType_skipsPruneNull() {
        ZoneMapIndex zoneMap = mock(ZoneMapIndex.class);
        when(zoneMap.checkSupport(COL_ID, SqlTypeName.DECIMAL)).thenReturn(false);

        IsNullColumnPredicate predicate = new IsNullColumnPredicate(SqlTypeName.DECIMAL, COL_ID);
        RoaringBitmap cur = RoaringBitmap.bitmapOfRange(0, 10);

        predicate.zoneMap(zoneMap, ipc, cur);

        verify(zoneMap, never()).pruneNull(anyInt(), any(RoaringBitmap.class));
    }

    @Test
    public void multiSortKey_supportedType_callsPruneNull() {
        MultiSortKeyIndex multiSortKey = mock(MultiSortKeyIndex.class);
        when(multiSortKey.checkSupport(COL_ID, SqlTypeName.INTEGER)).thenReturn(true);

        IsNullColumnPredicate predicate = new IsNullColumnPredicate(SqlTypeName.INTEGER, COL_ID);
        RoaringBitmap cur = RoaringBitmap.bitmapOfRange(0, 10);

        predicate.multiSortKey(multiSortKey, ipc, cur);

        verify(multiSortKey, times(1)).pruneNull(eq(COL_ID), any(RoaringBitmap.class));
    }

    @Test
    public void multiSortKey_unsupportedType_skipsPruneNull() {
        MultiSortKeyIndex multiSortKey = mock(MultiSortKeyIndex.class);
        when(multiSortKey.checkSupport(COL_ID, SqlTypeName.VARCHAR)).thenReturn(false);

        IsNullColumnPredicate predicate = new IsNullColumnPredicate(SqlTypeName.VARCHAR, COL_ID);
        RoaringBitmap cur = RoaringBitmap.bitmapOfRange(0, 10);

        predicate.multiSortKey(multiSortKey, ipc, cur);

        verify(multiSortKey, never()).pruneNull(anyInt(), any(RoaringBitmap.class));
    }

    private static int eq(int value) {
        return org.mockito.ArgumentMatchers.eq(value);
    }
}
