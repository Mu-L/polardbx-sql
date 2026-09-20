package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.executor.operator.util.GlobalTopNThreshold;
import com.alibaba.polardbx.executor.operator.util.LongTopNHeap;
import com.alibaba.polardbx.executor.operator.util.TopNThresholdFilter;
import com.alibaba.polardbx.executor.operator.util.topnutils.LongIndexRow;
import org.apache.calcite.sql.SqlKind;
import org.junit.Assert;
import org.junit.Test;

public class TopNRFEvaluatorTest {
    @Test
    public void testTopNThresholdFilterAsc() {
        final int topSize = 10000;
        final GlobalTopNThreshold globalTopNThreshold = new LongTopNHeap.LongGlobalTopNThresholdImpl(topSize, true);

        TopNThresholdFilter filter = new TopNThresholdFilter(globalTopNThreshold, SqlKind.LESS_THAN);

        // not initialized.
        Assert.assertTrue(filter.mightContainLong(1L));

        // update -100.
        globalTopNThreshold.updateIndexRow(new LongIndexRow(-1, -1, -100L, false));
        Assert.assertFalse(filter.mightContainLong(1L));
        Assert.assertTrue(filter.mightContainLong(0L));
        Assert.assertTrue(filter.mightContainLong(-1000L));

        // update -200.
        globalTopNThreshold.updateIndexRow(new LongIndexRow(-1, -1, -200L, false));
        Assert.assertFalse(filter.mightContainLong(1L));
        Assert.assertTrue(filter.mightContainLong(0L));
        Assert.assertFalse(filter.mightContainLong(-100L));
        Assert.assertFalse(filter.mightContainLong(-199L));
        Assert.assertTrue(filter.mightContainLong(-200L));
        Assert.assertTrue(filter.mightContainLong(-1000L));

        // update null.
        globalTopNThreshold.updateIndexRow(new LongIndexRow(-1, -1, -1, true));
        Assert.assertFalse(filter.mightContainLong(1L));
        Assert.assertTrue(filter.mightContainLong(0L));
        Assert.assertFalse(filter.mightContainLong(-100L));
        Assert.assertFalse(filter.mightContainLong(-199L));
        Assert.assertFalse(filter.mightContainLong(-200L));
        Assert.assertFalse(filter.mightContainLong(-1000L));
    }

    @Test
    public void testTopNThresholdFilterDesc() {
        final int topSize = 10000;
        final GlobalTopNThreshold globalTopNThreshold = new LongTopNHeap.LongGlobalTopNThresholdImpl(topSize, false);

        TopNThresholdFilter filter = new TopNThresholdFilter(globalTopNThreshold, SqlKind.GREATER_THAN);

        // not initialized.
        Assert.assertTrue(filter.mightContainLong(100L));

        // update null
        globalTopNThreshold.updateIndexRow(new LongIndexRow(-1, -1, -1, true));
        Assert.assertTrue(filter.mightContainLong(1L));
        Assert.assertTrue(filter.mightContainLong(0L));
        Assert.assertTrue(filter.mightContainLong(100L));
        Assert.assertTrue(filter.mightContainLong(199L));
        Assert.assertTrue(filter.mightContainLong(200L));
        Assert.assertTrue(filter.mightContainLong(1000L));

        // update 100.
        globalTopNThreshold.updateIndexRow(new LongIndexRow(-1, -1, 100L, false));
        Assert.assertFalse(filter.mightContainLong(1L));
        Assert.assertTrue(filter.mightContainLong(0L));
        Assert.assertTrue(filter.mightContainLong(1000L));

        // update 200.
        globalTopNThreshold.updateIndexRow(new LongIndexRow(-1, -1, 200L, false));
        Assert.assertFalse(filter.mightContainLong(1L));
        Assert.assertTrue(filter.mightContainLong(0L));
        Assert.assertFalse(filter.mightContainLong(100L));
        Assert.assertFalse(filter.mightContainLong(199L));
        Assert.assertTrue(filter.mightContainLong(200L));
        Assert.assertTrue(filter.mightContainLong(1000L));

        // update null and will not be updated.
        globalTopNThreshold.updateIndexRow(new LongIndexRow(-1, -1, -1, true));
        Assert.assertFalse(filter.mightContainLong(1L));
        Assert.assertTrue(filter.mightContainLong(0L));
        Assert.assertFalse(filter.mightContainLong(100L));
        Assert.assertFalse(filter.mightContainLong(199L));
        Assert.assertTrue(filter.mightContainLong(200L));
        Assert.assertTrue(filter.mightContainLong(1000L));
    }
}
