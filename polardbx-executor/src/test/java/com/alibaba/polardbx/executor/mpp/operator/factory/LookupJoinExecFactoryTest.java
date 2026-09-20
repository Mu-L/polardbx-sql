package com.alibaba.polardbx.executor.mpp.operator.factory;

import com.alibaba.polardbx.common.utils.ExecutorMode;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BKAJoin;
import com.alibaba.polardbx.optimizer.core.rel.Gather;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.LookupInfo;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.junit.Assert;
import org.junit.Test;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LookupJoinExecFactoryTest {

    private static final int SINGLE_PARALLELISM = 1;
    private static final int MULTIPLE_PARALLELISM = 2;
    private static final int HIGH_PARALLELISM = 4;

    @Test
    public void testExistLookupLocalIndex_NotBKAJoin() {
        // 测试非 BKAJoin 的情况
        Join join = mock(Join.class);
        ExecutionContext ec = mock(ExecutionContext.class);
        when(ec.getExecuteMode()).thenReturn(ExecutorMode.NONE);

        boolean result = LookupJoinExecFactory.isAdaptiveLookupOptimizationReady(join, ec, SINGLE_PARALLELISM);
        Assert.assertFalse("非 BKAJoin 应该返回 false", result);
    }

    @Test
    public void testIsAdaptiveLookupOptimizationReady_HighParallelism() {
        // 测试并行度大于1的情况，应该返回false
        BKAJoin bkaJoin = mock(BKAJoin.class);
        ExecutionContext ec = mock(ExecutionContext.class);
        when(ec.getExecuteMode()).thenReturn(ExecutorMode.NONE);

        boolean result = LookupJoinExecFactory.isAdaptiveLookupOptimizationReady(bkaJoin, ec, MULTIPLE_PARALLELISM);
        Assert.assertFalse("并行度大于1应该返回 false", result);
    }

    @Test
    public void testIsAdaptiveLookupOptimizationReady_HighParallelismWithOptimalConditions() {
        // 测试并行度大于1的情况，即使其他条件都满足，也应该返回false
        BKAJoin bkaJoin = mock(BKAJoin.class);
        LogicalView logicalView = mock(LogicalView.class);
        LookupInfo lookupInfo = mock(LookupInfo.class);
        ExecutionContext ec = mock(ExecutionContext.class);
        when(ec.getExecuteMode()).thenReturn(ExecutorMode.NONE);

        // 设置所有条件都满足的情况
        when(bkaJoin.isAdaptiveLookupOptimizationReady()).thenReturn(null); // 缓存为空
        when(bkaJoin.isHasSwitched()).thenReturn(true);
        when(bkaJoin.getInner()).thenReturn(logicalView);
        when(logicalView.getLookupInfo()).thenReturn(lookupInfo);
        when(lookupInfo.isPrimaryHasGsiLocalIndex()).thenReturn(true);

        boolean result = LookupJoinExecFactory.isAdaptiveLookupOptimizationReady(bkaJoin, ec, MULTIPLE_PARALLELISM);
        Assert.assertFalse("即使其他条件都满足，并行度大于1也应该返回 false", result);
    }

    @Test
    public void testIsAdaptiveLookupOptimizationReady_VeryHighParallelism() {
        // 测试更高并行度的情况
        BKAJoin bkaJoin = mock(BKAJoin.class);
        LogicalView logicalView = mock(LogicalView.class);
        LookupInfo lookupInfo = mock(LookupInfo.class);
        ExecutionContext ec = mock(ExecutionContext.class);
        when(ec.getExecuteMode()).thenReturn(ExecutorMode.NONE);

        // 设置所有条件都满足的情况
        when(bkaJoin.isAdaptiveLookupOptimizationReady()).thenReturn(null);
        when(bkaJoin.isHasSwitched()).thenReturn(true);
        when(bkaJoin.getInner()).thenReturn(logicalView);
        when(logicalView.getLookupInfo()).thenReturn(lookupInfo);
        when(lookupInfo.isPrimaryHasGsiLocalIndex()).thenReturn(true);

        boolean result = LookupJoinExecFactory.isAdaptiveLookupOptimizationReady(bkaJoin, ec, HIGH_PARALLELISM);
        Assert.assertFalse("并行度为4时也应该返回 false", result);
    }

    @Test
    public void testIsAdaptiveLookupOptimizationReady_HighParallelismWithGather() {
        // 测试并行度大于1且使用Gather的情况
        BKAJoin bkaJoin = mock(BKAJoin.class);
        Gather gather = mock(Gather.class);
        LogicalView logicalView = mock(LogicalView.class);
        LookupInfo lookupInfo = mock(LookupInfo.class);
        ExecutionContext ec = mock(ExecutionContext.class);
        when(ec.getExecuteMode()).thenReturn(ExecutorMode.NONE);

        // 设置所有条件都满足的情况
        when(bkaJoin.isAdaptiveLookupOptimizationReady()).thenReturn(null);
        when(bkaJoin.isHasSwitched()).thenReturn(true);
        when(bkaJoin.getInner()).thenReturn(gather);
        when(gather.getInput()).thenReturn(logicalView);
        when(logicalView.getLookupInfo()).thenReturn(lookupInfo);
        when(lookupInfo.isPrimaryHasGsiLocalIndex()).thenReturn(true);

        boolean result = LookupJoinExecFactory.isAdaptiveLookupOptimizationReady(bkaJoin, ec, MULTIPLE_PARALLELISM);
        Assert.assertFalse("Gather 场景下并行度大于1也应该返回 false", result);
    }

    @Test
    public void testIsAdaptiveLookupOptimizationReady_HighParallelismWithCachedTrue() {
        // 测试并行度大于1但缓存为true的情况，并行度检查在缓存检查之前
        BKAJoin bkaJoin = mock(BKAJoin.class);
        ExecutionContext ec = mock(ExecutionContext.class);
        when(ec.getExecuteMode()).thenReturn(ExecutorMode.NONE);
        when(bkaJoin.isAdaptiveLookupOptimizationReady()).thenReturn(true); // 缓存为true

        boolean result = LookupJoinExecFactory.isAdaptiveLookupOptimizationReady(bkaJoin, ec, MULTIPLE_PARALLELISM);
        // 根据实际实现，并行度检查在缓存检查之前，所以即使缓存为true，并行度大于1也应该返回false
        Assert.assertFalse("并行度检查在缓存检查之前，并行度大于1应该返回 false", result);
    }

    @Test
    public void testIsAdaptiveLookupOptimizationReady_HighParallelismInMppMode() {
        // 测试并行度大于1且在MPP模式的情况
        BKAJoin bkaJoin = mock(BKAJoin.class);
        LogicalView logicalView = mock(LogicalView.class);
        LookupInfo lookupInfo = mock(LookupInfo.class);
        ExecutionContext ec = mock(ExecutionContext.class);
        when(ec.getExecuteMode()).thenReturn(ExecutorMode.MPP);

        // 设置其他条件
        when(bkaJoin.isHasSwitched()).thenReturn(true);
        when(bkaJoin.getInner()).thenReturn(logicalView);
        when(logicalView.getLookupInfo()).thenReturn(lookupInfo);
        when(lookupInfo.isPrimaryHasGsiLocalIndex()).thenReturn(true);

        boolean result = LookupJoinExecFactory.isAdaptiveLookupOptimizationReady(bkaJoin, ec, MULTIPLE_PARALLELISM);
        Assert.assertFalse("MPP模式下并行度大于1也应该返回 false", result);
    }

    @Test
    public void testExistLookupLocalIndex_BKAJoinNotSwitched() {
        // 测试 BKAJoin 但未切换的情况
        BKAJoin bkaJoin = mock(BKAJoin.class);
        ExecutionContext ec = mock(ExecutionContext.class);
        when(ec.getExecuteMode()).thenReturn(ExecutorMode.NONE);
        when(bkaJoin.isHasSwitched()).thenReturn(false);
        when(bkaJoin.isAdaptiveLookupOptimizationReady()).thenReturn(null); // 缓存为空

        boolean result = LookupJoinExecFactory.isAdaptiveLookupOptimizationReady(bkaJoin, ec, SINGLE_PARALLELISM);
        Assert.assertFalse("BKAJoin 未切换应该返回 false", result);
    }

    @Test
    public void testIsAdaptiveLookupOptimizationReady_CachedResult() {
        // 测试缓存结果的情况
        BKAJoin bkaJoin = mock(BKAJoin.class);
        ExecutionContext ec = mock(ExecutionContext.class);
        when(ec.getExecuteMode()).thenReturn(ExecutorMode.NONE);
        when(bkaJoin.isAdaptiveLookupOptimizationReady()).thenReturn(true); // 有缓存结果

        boolean result = LookupJoinExecFactory.isAdaptiveLookupOptimizationReady(bkaJoin, ec, SINGLE_PARALLELISM);
        Assert.assertTrue("应该返回缓存的结果 true", result);
    }

    @Test
    public void testExistLookupLocalIndex_InnerNotLogicalViewOrGather() {
        // 测试 inner 不是 LogicalView 或 Gather 的情况
        BKAJoin bkaJoin = mock(BKAJoin.class);
        RelNode inner = mock(RelNode.class);
        ExecutionContext ec = mock(ExecutionContext.class);
        when(ec.getExecuteMode()).thenReturn(ExecutorMode.NONE);

        when(bkaJoin.isAdaptiveLookupOptimizationReady()).thenReturn(null); // 缓存为空
        when(bkaJoin.isHasSwitched()).thenReturn(true);
        when(bkaJoin.getInner()).thenReturn(inner);
        when(bkaJoin.setAdaptiveLookupOptimizationReady(false)).thenReturn(false);

        boolean result = LookupJoinExecFactory.isAdaptiveLookupOptimizationReady(bkaJoin, ec, SINGLE_PARALLELISM);
        Assert.assertFalse("inner 不是 LogicalView 或 Gather 应该返回 false", result);
    }

    @Test
    public void testExistLookupLocalIndex_LogicalViewWithoutLookupInfo() {
        // 测试 LogicalView 但没有 LookupInfo 的情况
        BKAJoin bkaJoin = mock(BKAJoin.class);
        LogicalView logicalView = mock(LogicalView.class);
        ExecutionContext ec = mock(ExecutionContext.class);
        when(ec.getExecuteMode()).thenReturn(ExecutorMode.NONE);

        when(bkaJoin.isAdaptiveLookupOptimizationReady()).thenReturn(null); // 缓存为空
        when(bkaJoin.isHasSwitched()).thenReturn(true);
        when(bkaJoin.getInner()).thenReturn(logicalView);
        when(logicalView.getLookupInfo()).thenReturn(null);
        when(bkaJoin.setAdaptiveLookupOptimizationReady(false)).thenReturn(false);

        boolean result = LookupJoinExecFactory.isAdaptiveLookupOptimizationReady(bkaJoin, ec, SINGLE_PARALLELISM);
        Assert.assertFalse("LogicalView 没有 LookupInfo 应该返回 false", result);
    }

    @Test
    public void testExistLookupLocalIndex_LogicalViewWithLookupInfoButNoPrimaryGsiLocalIndex() {
        // 测试 LogicalView 有 LookupInfo 但没有 PrimaryGsiLocalIndex 的情况
        BKAJoin bkaJoin = mock(BKAJoin.class);
        LogicalView logicalView = mock(LogicalView.class);
        LookupInfo lookupInfo = mock(LookupInfo.class);
        ExecutionContext ec = mock(ExecutionContext.class);
        when(ec.getExecuteMode()).thenReturn(ExecutorMode.NONE);

        when(bkaJoin.isAdaptiveLookupOptimizationReady()).thenReturn(null); // 缓存为空
        when(bkaJoin.isHasSwitched()).thenReturn(true);
        when(bkaJoin.getInner()).thenReturn(logicalView);
        when(logicalView.getLookupInfo()).thenReturn(lookupInfo);
        when(lookupInfo.isPrimaryHasGsiLocalIndex()).thenReturn(false);
        when(bkaJoin.setAdaptiveLookupOptimizationReady(false)).thenReturn(false);

        boolean result = LookupJoinExecFactory.isAdaptiveLookupOptimizationReady(bkaJoin, ec, SINGLE_PARALLELISM);
        Assert.assertFalse("LookupInfo 没有 PrimaryGsiLocalIndex 应该返回 false", result);
    }

    @Test
    public void testExistLookupLocalIndex_LogicalViewWithPrimaryGsiLocalIndex() {
        // 测试 LogicalView 有 PrimaryGsiLocalIndex 的情况
        BKAJoin bkaJoin = mock(BKAJoin.class);
        LogicalView logicalView = mock(LogicalView.class);
        LookupInfo lookupInfo = mock(LookupInfo.class);
        ExecutionContext ec = mock(ExecutionContext.class);
        when(ec.getExecuteMode()).thenReturn(ExecutorMode.NONE);

        when(bkaJoin.isAdaptiveLookupOptimizationReady()).thenReturn(null); // 缓存为空
        when(bkaJoin.isHasSwitched()).thenReturn(true);
        when(bkaJoin.getInner()).thenReturn(logicalView);
        when(logicalView.getLookupInfo()).thenReturn(lookupInfo);
        when(lookupInfo.isPrimaryHasGsiLocalIndex()).thenReturn(true);
        when(bkaJoin.setAdaptiveLookupOptimizationReady(true)).thenReturn(true);

        boolean result = LookupJoinExecFactory.isAdaptiveLookupOptimizationReady(bkaJoin, ec, SINGLE_PARALLELISM);
        Assert.assertTrue("LogicalView 有 PrimaryGsiLocalIndex 应该返回 true", result);
    }

    @Test
    public void testExistLookupLocalIndex_GatherWithLogicalViewWithoutLookupInfo() {
        // 测试 Gather 包含 LogicalView 但没有 LookupInfo 的情况
        BKAJoin bkaJoin = mock(BKAJoin.class);
        Gather gather = mock(Gather.class);
        LogicalView logicalView = mock(LogicalView.class);
        ExecutionContext ec = mock(ExecutionContext.class);
        when(ec.getExecuteMode()).thenReturn(ExecutorMode.NONE);

        when(bkaJoin.isAdaptiveLookupOptimizationReady()).thenReturn(null); // 缓存为空
        when(bkaJoin.isHasSwitched()).thenReturn(true);
        when(bkaJoin.getInner()).thenReturn(gather);
        when(gather.getInput()).thenReturn(logicalView);
        when(logicalView.getLookupInfo()).thenReturn(null);
        when(bkaJoin.setAdaptiveLookupOptimizationReady(false)).thenReturn(false);

        boolean result = LookupJoinExecFactory.isAdaptiveLookupOptimizationReady(bkaJoin, ec, SINGLE_PARALLELISM);
        Assert.assertFalse("Gather 中的 LogicalView 没有 LookupInfo 应该返回 false", result);
    }

    @Test
    public void testExistLookupLocalIndex_GatherWithLogicalViewWithPrimaryGsiLocalIndex() {
        // 测试 Gather 包含 LogicalView 且有 PrimaryGsiLocalIndex 的情况
        BKAJoin bkaJoin = mock(BKAJoin.class);
        Gather gather = mock(Gather.class);
        LogicalView logicalView = mock(LogicalView.class);
        LookupInfo lookupInfo = mock(LookupInfo.class);
        ExecutionContext ec = mock(ExecutionContext.class);
        when(ec.getExecuteMode()).thenReturn(ExecutorMode.NONE);

        when(bkaJoin.isAdaptiveLookupOptimizationReady()).thenReturn(null); // 缓存为空
        when(bkaJoin.isHasSwitched()).thenReturn(true);
        when(bkaJoin.getInner()).thenReturn(gather);
        when(gather.getInput()).thenReturn(logicalView);
        when(logicalView.getLookupInfo()).thenReturn(lookupInfo);
        when(lookupInfo.isPrimaryHasGsiLocalIndex()).thenReturn(true);
        when(bkaJoin.setAdaptiveLookupOptimizationReady(true)).thenReturn(true);

        boolean result = LookupJoinExecFactory.isAdaptiveLookupOptimizationReady(bkaJoin, ec, SINGLE_PARALLELISM);
        Assert.assertTrue("Gather 中的 LogicalView 有 PrimaryGsiLocalIndex 应该返回 true", result);
    }

    @Test
    public void testExistLookupLocalIndex_MppMode() {
        // 测试 MPP 模式的情况，应该返回 false
        BKAJoin bkaJoin = mock(BKAJoin.class);
        LogicalView logicalView = mock(LogicalView.class);
        LookupInfo lookupInfo = mock(LookupInfo.class);
        ExecutionContext ec = mock(ExecutionContext.class);
        when(ec.getExecuteMode()).thenReturn(ExecutorMode.MPP); // MPP 模式

        when(bkaJoin.isHasSwitched()).thenReturn(true);
        when(bkaJoin.getInner()).thenReturn(logicalView);
        when(logicalView.getLookupInfo()).thenReturn(lookupInfo);
        when(lookupInfo.isPrimaryHasGsiLocalIndex()).thenReturn(true);

        boolean result = LookupJoinExecFactory.isAdaptiveLookupOptimizationReady(bkaJoin, ec, SINGLE_PARALLELISM);
        Assert.assertFalse("MPP 模式应该返回 false", result);
    }

}