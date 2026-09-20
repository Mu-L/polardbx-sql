package com.alibaba.polardbx.optimizer.htaprouting;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.Sets;
import org.junit.Test;

import java.util.Optional;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HtapTraceTest {

    @Test
    public void testTrace() {
        Optional<HtapTrace> trace = Optional.of(new HtapTrace());
        HtapTrace.addTrace(trace, "hello");
        Assert.assertEqual("hello\n", trace.get().printDetails().toLowerCase());

        trace = Optional.of(new HtapTrace());
        HtapTrace.traceCandidateOptimizerTypes(trace, Sets.newHashSet(OptimizerType.MPP, OptimizerType.SMP), "test");
        Assert.assertEqual("smp,mpp", trace.get().printCandidateOptimizerTypes().toLowerCase());
        Assert.assertEqual("candidate optimizer types: {SMP,MPP}, caused by test\n",
            trace.get().printDetails().toLowerCase());

        trace = Optional.of(new HtapTrace());
        HtapTrace.addTrace(trace, "");
        Assert.assertTrue(trace.get().printDetails().isEmpty());
    }

    @Test
    public void testTraceFromPlanCache() {
        Optional<HtapTrace> trace = Optional.of(new HtapTrace());
        ExecutionContext ec = new ExecutionContext("db");
        ec.setHtapTrace(trace.get());
        PlannerContext mockedPc = mock(PlannerContext.class);
        when(mockedPc.getRoutingType()).thenReturn(RoutingType.ROW);
        when(mockedPc.getWorkloadType()).thenReturn(WorkloadType.AP);
        when(mockedPc.getOptimizerType()).thenReturn(OptimizerType.MPP);
        when(mockedPc.getPlanType()).thenReturn(PlanType.COLUMNAR);
        HtapTrace.traceFromPlanCache(ec, mockedPc);

        Assert.assertEqual("row", trace.get().printRoutType().toLowerCase());
        Assert.assertEqual("null", trace.get().printCandidateOptimizerTypes().toLowerCase());
        Assert.assertEqual("mpp", trace.get().printOptimizerType().toLowerCase());
        Assert.assertEqual("ap", trace.get().printWorkLoadType().toLowerCase());
        Assert.assertEqual("columnar", trace.get().printPlanType().toLowerCase());
    }

    @Test
    public void testTraceFromSpm() {
        Optional<HtapTrace> trace = Optional.of(new HtapTrace());
        ExecutionContext ec = new ExecutionContext("db");
        ec.setHtapTrace(trace.get());
        PlannerContext mockedPc = mock(PlannerContext.class);
        when(mockedPc.getWorkloadType()).thenReturn(WorkloadType.AP);
        when(mockedPc.getPlanType()).thenReturn(PlanType.COLUMNAR);
        HtapTrace.traceFromSpm(ec, mockedPc);

        Assert.assertEqual("null", trace.get().printRoutType().toLowerCase());
        Assert.assertEqual("null", trace.get().printCandidateOptimizerTypes().toLowerCase());
        Assert.assertEqual("null", trace.get().printOptimizerType().toLowerCase());
        Assert.assertEqual("ap", trace.get().printWorkLoadType().toLowerCase());
        Assert.assertEqual("columnar", trace.get().printPlanType().toLowerCase());
    }

}
