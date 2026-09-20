package com.alibaba.polardbx.optimizer.selectivity;

import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.google.common.collect.ImmutableList;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link AbstractSelectivityEstimator#findColumnMeta(TableMeta, int)}.
 */
public class AbstractSelectivityEstimatorTest {

    private TableMeta tableMeta;
    private ColumnMeta col0;
    private ColumnMeta col1;
    private ColumnMeta col2;

    @Before
    public void setUp() {
        tableMeta = mock(TableMeta.class);
        col0 = mock(ColumnMeta.class);
        col1 = mock(ColumnMeta.class);
        col2 = mock(ColumnMeta.class);
        List<ColumnMeta> columns = ImmutableList.of(col0, col1, col2);
        when(tableMeta.getAllColumns()).thenReturn(columns);
    }

    @Test
    public void testFindColumnMeta_validFirstIndex() {
        Assert.assertSame(col0, AbstractSelectivityEstimator.findColumnMeta(tableMeta, 0));
    }

    @Test
    public void testFindColumnMeta_validLastIndex() {
        Assert.assertSame(col2, AbstractSelectivityEstimator.findColumnMeta(tableMeta, 2));
    }

    @Test
    public void testFindColumnMeta_negativeIndexReturnsNull() {
        Assert.assertNull(AbstractSelectivityEstimator.findColumnMeta(tableMeta, -1));
    }

    @Test
    public void testFindColumnMeta_minIntIndexReturnsNull() {
        Assert.assertNull(AbstractSelectivityEstimator.findColumnMeta(tableMeta, Integer.MIN_VALUE));
    }

    @Test
    public void testFindColumnMeta_indexEqualsSizeReturnsNull() {
        // boundary: index == size
        Assert.assertNull(AbstractSelectivityEstimator.findColumnMeta(tableMeta, 3));
    }

    @Test
    public void testFindColumnMeta_indexGreaterThanSizeReturnsNull() {
        Assert.assertNull(AbstractSelectivityEstimator.findColumnMeta(tableMeta, 100));
    }

    @Test
    public void testFindColumnMeta_emptyColumnsReturnsNull() {
        TableMeta emptyTable = mock(TableMeta.class);
        when(emptyTable.getAllColumns()).thenReturn(Collections.emptyList());
        Assert.assertNull(AbstractSelectivityEstimator.findColumnMeta(emptyTable, 0));
        Assert.assertNull(AbstractSelectivityEstimator.findColumnMeta(emptyTable, -1));
        Assert.assertNull(AbstractSelectivityEstimator.findColumnMeta(emptyTable, 1));
    }

    @Test(expected = NullPointerException.class)
    public void testFindColumnMeta_nullTableMetaThrows() {
        // Method does not guard against null tableMeta; documents current behavior.
        AbstractSelectivityEstimator.findColumnMeta(null, 0);
    }
}
