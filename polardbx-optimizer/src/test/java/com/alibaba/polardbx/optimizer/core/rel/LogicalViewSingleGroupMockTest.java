package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.optimizer.BaseRuleTest;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.Arrays;

import static org.mockito.Mockito.*;

/**
 * Advanced unit tests for isSingleGroup and isSingleGroupForExecutor methods in LogicalView
 * using Mockito to simulate different states
 *
 * @author fangwu
 */
public class LogicalViewSingleGroupMockTest extends BaseRuleTest {

    @Test
    public void testIsSingleGroupForExecutorWithDifferentValues() throws Exception {
        // Create a LogicalView instance
        LogicalTableScan scan = LogicalTableScan.create(relOptCluster,
            schema.getTableForMember(Arrays.asList("optest", "emp")));

        // Mock the LogicalView to control the isSingleGroup method behavior
        LogicalView logicalView = Mockito.spy(LogicalView.create(scan, scan.getTable()));

        // Test when isSingleGroup returns false
        Mockito.doReturn(false).when(logicalView).isSingleGroup(false);
        Assert.assertFalse("isSingleGroup should be false", logicalView.isSingleGroup());
        Assert.assertFalse("isSingleGroupForExecutor should be false", logicalView.isSingleGroupForExecutor());

        // Test when isSingleGroup returns true
        Mockito.doReturn(true).when(logicalView).isSingleGroup(false);
        Assert.assertTrue("isSingleGroup should be true", logicalView.isSingleGroup());
        Assert.assertTrue("isSingleGroupForExecutor should be true", logicalView.isSingleGroupForExecutor());
    }

    @Test
    public void testOSSTableScanIsSingleGroupForExecutorWithDifferentValues() throws Exception {
        // Create a LogicalView instance
        LogicalTableScan scan = LogicalTableScan.create(relOptCluster,
            schema.getTableForMember(Arrays.asList("optest", "emp")));

        // Mock the LogicalView to control the isSingleGroup method behavior
        LogicalView logicalView = Mockito.spy(LogicalView.create(scan, scan.getTable()));

        // Create an OSSTableScan from the LogicalView
        OSSTableScan ossTableScan = Mockito.spy(new OSSTableScan(logicalView));

        // Test when isSingleGroup returns false
        Mockito.doReturn(false).when(ossTableScan).isSingleGroup(false);
        Assert.assertFalse("isSingleGroup should be false in OSSTableScan", ossTableScan.isSingleGroup());
        Assert.assertFalse("isSingleGroupForExecutor should be false in OSSTableScan",
            ossTableScan.isSingleGroupForExecutor());

        // Test when isSingleGroup returns true
        Mockito.doReturn(true).when(ossTableScan).isSingleGroup(false);
        Assert.assertTrue("isSingleGroup should be true in OSSTableScan", ossTableScan.isSingleGroup());
        Assert.assertTrue("isSingleGroupForExecutor should be true in OSSTableScan",
            ossTableScan.isSingleGroupForExecutor());
    }
}