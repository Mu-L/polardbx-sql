package com.alibaba.polardbx.optimizer.htaprouting;

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.LogicalRelocate;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for PlanType.determinePlanType covering LogicalRelocate and LogicalModify branches
 * introduced in the "last write wins" feature.
 */
public class PlanTypeDeterminePlanTypeTest {

    private PlannerContext mockPlannerContext() {
        PlannerContext pc = mock(PlannerContext.class);
        when(pc.getHtapTrace()).thenReturn(Optional.of(new HtapTrace()));
        return pc;
    }

    @Test
    public void testDeterminePlanType_LogicalInsert_returnsROW() {
        LogicalInsert input = mock(LogicalInsert.class);
        PlannerContext pc = mockPlannerContext();

        PlanType result = PlanType.determinePlanType(input, OptimizerType.SMP, pc);
        Assert.assertEquals(PlanType.ROW, result);
    }

    @Test
    public void testDeterminePlanType_LogicalRelocate_returnsROW() {
        LogicalRelocate input = mock(LogicalRelocate.class);
        PlannerContext pc = mockPlannerContext();

        PlanType result = PlanType.determinePlanType(input, OptimizerType.SMP, pc);
        Assert.assertEquals(PlanType.ROW, result);
    }

    @Test
    public void testDeterminePlanType_LogicalModify_returnsROW() {
        LogicalModify input = mock(LogicalModify.class);
        PlannerContext pc = mockPlannerContext();

        PlanType result = PlanType.determinePlanType(input, OptimizerType.SMP, pc);
        Assert.assertEquals(PlanType.ROW, result);
    }

    @Test
    public void testDeterminePlanType_LogicalRelocate_withColumnarOptimizer_returnsROW() {
        LogicalRelocate input = mock(LogicalRelocate.class);
        PlannerContext pc = mockPlannerContext();

        // Even with COLUMNAR optimizer, LogicalRelocate should return ROW
        PlanType result = PlanType.determinePlanType(input, OptimizerType.COLUMNAR, pc);
        Assert.assertEquals(PlanType.ROW, result);
    }

    @Test
    public void testDeterminePlanType_LogicalModify_withColumnarOptimizer_returnsROW() {
        LogicalModify input = mock(LogicalModify.class);
        PlannerContext pc = mockPlannerContext();

        // Even with COLUMNAR optimizer, LogicalModify should return ROW
        PlanType result = PlanType.determinePlanType(input, OptimizerType.COLUMNAR, pc);
        Assert.assertEquals(PlanType.ROW, result);
    }
}
