package com.alibaba.polardbx.executor.columnar.pruning.index.builder;

import com.alibaba.polardbx.executor.columnar.pruning.index.LongSortKeyIndex;
import com.alibaba.polardbx.executor.columnar.pruning.index.SortKeyIndex;
import com.alibaba.polardbx.executor.columnar.pruning.index.StringSortKeyIndex;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.junit.Test;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * Covers the graceful-skip branches of {@link SortKeyIndexBuilder#build()} introduced
 * by iteration 001 of Aone Bug #72283876. When the caller chooses not to populate the
 * builder (e.g. DECIMAL sort-key falling back to full scan), build() must return
 * {@code null} instead of asserting via Preconditions so the IndexPruner can treat
 * sort-key pruning as disabled for this file.
 *
 * <p>Three null-return branches are exercised here:
 * <ul>
 *     <li>{@code dt} left unset</li>
 *     <li>string-typed entry list is empty</li>
 *     <li>long-typed entry list is empty</li>
 * </ul>
 * Plus two positive paths to ensure the happy construction is not regressed.
 */
public class SortKeyIndexBuilderTest {

    @Test
    public void build_dtUnset_returnsNull() {
        SortKeyIndexBuilder builder = new SortKeyIndexBuilder();
        builder.setColId(0);
        builder.setAsc(true);
        assertNull("dt==null must yield a null SortKeyIndex (graceful skip)", builder.build());
    }

    @Test
    public void build_stringTypeEmptyData_returnsNull() {
        SortKeyIndexBuilder builder = new SortKeyIndexBuilder();
        builder.setColId(0);
        builder.setAsc(true);
        builder.setDt(DataTypes.VarcharType);
        assertNull("string dt with empty data must yield a null SortKeyIndex", builder.build());
    }

    @Test
    public void build_longTypeEmptyData_returnsNull() {
        SortKeyIndexBuilder builder = new SortKeyIndexBuilder();
        builder.setColId(0);
        builder.setAsc(true);
        builder.setDt(DataTypes.LongType);
        assertNull("long dt with empty data must yield a null SortKeyIndex", builder.build());
    }

    @Test
    public void build_stringTypeWithData_returnsStringSortKeyIndex() {
        SortKeyIndexBuilder builder = new SortKeyIndexBuilder();
        builder.setColId(1);
        builder.setAsc(true);
        builder.setDt(DataTypes.VarcharType);
        builder.appendMockDataEntry("aaa", "zzz", DataTypes.VarcharType);
        SortKeyIndex index = builder.build();
        assertNotNull(index);
        assertTrue(index instanceof StringSortKeyIndex);
    }

    @Test
    public void build_longTypeWithData_returnsLongSortKeyIndex() {
        SortKeyIndexBuilder builder = new SortKeyIndexBuilder();
        builder.setColId(2);
        builder.setAsc(true);
        builder.setDt(DataTypes.LongType);
        builder.appendMockDataEntry(100L, 200L, DataTypes.LongType);
        SortKeyIndex index = builder.build();
        assertNotNull(index);
        assertTrue(index instanceof LongSortKeyIndex);
    }
}
