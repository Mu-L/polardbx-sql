package com.alibaba.polardbx.executor.explain;

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.utils.ExplainExecutorUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.utils.ExplainResult;
import org.apache.calcite.util.trace.CalcitePlanOptimizerTrace;
import org.apache.calcite.util.trace.OptimizerPhase;
import org.apache.calcite.util.trace.PlanOptimizerTracer;
import org.apache.calcite.util.trace.PlanOptimizerTracer.PhaseSnapshot;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

/**
 * Verifies the textual output produced by {@code EXPLAIN OPTIMIZER} and
 * {@code EXPLAIN OPTIMIZER DETAIL}.
 * <p>
 * The test bypasses the optimizer pipeline by directly injecting a curated
 * sequence of {@link PhaseSnapshot} (with nested children) and
 * {@link PlanOptimizerTracer.RuleSnapshot} instances into the tracer, then
 * invokes the private {@code buildExplainOptimizerCursor} routine via
 * reflection and inspects the rendered rows.
 */
public class ExplainOptimizerTest {

    private ExecutionContext executionContext;
    private CalcitePlanOptimizerTrace trace;
    private PlanOptimizerTracer tracer;
    private ExecutionPlan executionPlan;

    @Before
    public void setUp() throws Exception {
        executionContext = new ExecutionContext();
        trace = new CalcitePlanOptimizerTrace();
        executionContext.setCalcitePlanOptimizerTrace(trace);
        tracer = trace.getOptimizerTracer();

        // ast == null skips the DDL branch in buildExplainOptimizerCursor.
        executionPlan = new ExecutionPlan(null, null, null);

        // Phase 1 (top level) – SQL Rewrite, with two rules.
        PhaseSnapshot phase1 = newPhaseSnapshot(OptimizerPhase.SQL_REWRITE, false, null, "1");
        addRuleSnapshot(phase1, "FilterMergeRule",
            "LogicalFilter\n  LogicalProject\n    LogicalTableScan");
        addRuleSnapshot(phase1, "FilterProjectTransposeRule",
            "LogicalFilter\n  LogicalTableScan");
        // Same rule fired twice – the rule-count summary must reflect this.
        addRuleSnapshot(phase1, "FilterMergeRule",
            "LogicalFilter\n  LogicalTableScan");
        completePhase(phase1, "LogicalProject\n  LogicalFilter\n    LogicalTableScan");

        // Phase 2 (top level) – Plan Enumerate (CBO), no rules.
        PhaseSnapshot phase2 = newPhaseSnapshot(OptimizerPhase.PLAN_ENUMERATE, false, null, "2");
        completePhase(phase2, "PhysicalProject\n  PhysicalTableScan");

        // Phase 2.1 (nested) – Sub-query CBO triggered inside Phase 2.
        PhaseSnapshot phase2_1 = newPhaseSnapshot(OptimizerPhase.SUBQUERY_CBO, false, phase2, "2.1");
        addRuleSnapshot(phase2_1, "JoinReorderRule",
            "LogicalJoin\n  LogicalTableScan\n  LogicalTableScan");
        completePhase(phase2_1, "PhysicalJoin\n  PhysicalTableScan\n  PhysicalTableScan");

        // Phase 3 (skipped) – e.g. a path that the optimizer evaluated but did not run.
        PhaseSnapshot phase3 = newSkippedPhaseSnapshot(OptimizerPhase.MPP, null, "3");

        injectPhaseSnapshots(tracer, phase1, phase2, phase2_1, phase3);
    }

    @Test
    public void testExplainOptimizer() throws Exception {
        executionContext.setExplain(makeExplainResult(ExplainResult.ExplainMode.OPTIMIZER));

        List<String> rows = readAllRows(invokeBuildCursor(executionContext, executionPlan));
        String output = String.join("\n", rows);

        // Phase summary header is always printed.
        Assert.assertTrue("missing phase summary header: " + output,
            output.contains("Phase Summary:"));

        // Each phase appears in the summary with its sequence number and display name.
        Assert.assertTrue(output.contains("1 " + OptimizerPhase.SQL_REWRITE.getDisplayName()));
        Assert.assertTrue(output.contains("2 " + OptimizerPhase.PLAN_ENUMERATE.getDisplayName()));
        Assert.assertTrue(output.contains("2.1 " + OptimizerPhase.SUBQUERY_CBO.getDisplayName()));
        Assert.assertTrue(output.contains("3 " + OptimizerPhase.MPP.getDisplayName() + "  [SKIPPED]"));

        // Final per-phase plan rendering must show the AFTER-phase plan strings.
        Assert.assertTrue("expected SQL_REWRITE after-plan in output",
            output.contains("LogicalProject"));
        Assert.assertTrue("expected PLAN_ENUMERATE after-plan in output",
            output.contains("PhysicalProject"));
        Assert.assertTrue("expected SUBQUERY_CBO after-plan in output",
            output.contains("PhysicalJoin"));

        // EXPLAIN OPTIMIZER (without DETAIL) must NOT emit the rule-level sections.
        Assert.assertFalse("rule counts should be hidden in EXPLAIN OPTIMIZER mode",
            output.contains("Rule counts:"));
        Assert.assertFalse("rules-applied section should be hidden in EXPLAIN OPTIMIZER mode",
            output.contains("Rules applied:"));
        Assert.assertFalse("individual rule names should be hidden in EXPLAIN OPTIMIZER mode",
            output.contains("=> [1] FilterMergeRule"));
    }

    @Test
    public void testExplainOptimizerDetail() throws Exception {
        executionContext.setExplain(makeExplainResult(ExplainResult.ExplainMode.OPTIMIZER_DETAIL));

        List<String> rows = readAllRows(invokeBuildCursor(executionContext, executionPlan));
        String output = String.join("\n", rows);

        // The summary block must still be present.
        Assert.assertTrue(output.contains("Phase Summary:"));
        Assert.assertTrue(output.contains("1 " + OptimizerPhase.SQL_REWRITE.getDisplayName()));
        Assert.assertTrue(output.contains("3 " + OptimizerPhase.MPP.getDisplayName() + "  [SKIPPED]"));

        // DETAIL mode must render the rule-count table.
        Assert.assertTrue("rule counts should be shown in EXPLAIN OPTIMIZER DETAIL mode",
            output.contains("Rule counts:"));
        // FilterMergeRule fired twice -> the count must read 2 in the table.
        Assert.assertTrue("FilterMergeRule should appear with count 2",
            output.contains("FilterMergeRule: 2"));
        Assert.assertTrue("FilterProjectTransposeRule should appear with count 1",
            output.contains("FilterProjectTransposeRule: 1"));

        // DETAIL mode must render the per-rule plan-before-rule snapshots in order.
        Assert.assertTrue("rules-applied section should be shown",
            output.contains("Rules applied:"));
        Assert.assertTrue("first rule label should be present",
            output.contains("=> [1] FilterMergeRule"));
        Assert.assertTrue("second rule label should be present",
            output.contains("=> [2] FilterProjectTransposeRule"));
        Assert.assertTrue("third rule label should be present",
            output.contains("=> [3] FilterMergeRule"));

        // The plan-before for the nested SUBQUERY_CBO rule must also be emitted.
        Assert.assertTrue("nested SUBQUERY_CBO rule label should be present",
            output.contains("=> [1] JoinReorderRule"));

        // Skipped phases never carry rules / plans even in DETAIL mode.
        Assert.assertFalse("skipped phase must not emit rule snapshots",
            output.contains("[SKIPPED]\n  => "));
    }

    // ── reflection helpers ──────────────────────────────────────────────────

    private static PhaseSnapshot newPhaseSnapshot(OptimizerPhase phase, boolean skipped,
                                                  PhaseSnapshot parent, String sequenceNumber)
        throws Exception {
        Constructor<PhaseSnapshot> ctor = PhaseSnapshot.class.getDeclaredConstructor(
            OptimizerPhase.class, boolean.class, PhaseSnapshot.class, String.class);
        ctor.setAccessible(true);
        return ctor.newInstance(phase, skipped, parent, sequenceNumber);
    }

    private static PhaseSnapshot newSkippedPhaseSnapshot(OptimizerPhase phase,
                                                         PhaseSnapshot parent,
                                                         String sequenceNumber) throws Exception {
        return newPhaseSnapshot(phase, true, parent, sequenceNumber);
    }

    private static void addRuleSnapshot(PhaseSnapshot snapshot, String ruleName,
                                        String planBefore) throws Exception {
        Method m = PhaseSnapshot.class.getDeclaredMethod("addRuleSnapshot",
            String.class, String.class);
        m.setAccessible(true);
        m.invoke(snapshot, ruleName, planBefore);
    }

    private static void completePhase(PhaseSnapshot snapshot, String afterPlan) throws Exception {
        Method m = PhaseSnapshot.class.getDeclaredMethod("complete", String.class);
        m.setAccessible(true);
        m.invoke(snapshot, afterPlan);
    }

    @SuppressWarnings("unchecked")
    private static void injectPhaseSnapshots(PlanOptimizerTracer tracer,
                                             PhaseSnapshot... snapshots) throws Exception {
        Field f = PlanOptimizerTracer.class.getDeclaredField("phaseSnapshots");
        f.setAccessible(true);
        List<PhaseSnapshot> list = (List<PhaseSnapshot>) f.get(tracer);
        list.clear();
        for (PhaseSnapshot s : snapshots) {
            list.add(s);
        }
    }

    private static ResultCursor invokeBuildCursor(ExecutionContext ec, ExecutionPlan ep)
        throws Exception {
        Method m = ExplainExecutorUtil.class.getDeclaredMethod(
            "buildExplainOptimizerCursor", ExecutionContext.class, ExecutionPlan.class);
        m.setAccessible(true);
        return (ResultCursor) m.invoke(null, ec, ep);
    }

    private static List<String> readAllRows(ResultCursor cursor) {
        List<String> rows = new ArrayList<>();
        Row row = cursor.next();
        while (row != null) {
            rows.add(row.getString(0));
            row = cursor.next();
        }
        return rows;
    }

    private static ExplainResult makeExplainResult(ExplainResult.ExplainMode mode) {
        ExplainResult er = new ExplainResult();
        er.explainMode = mode;
        return er;
    }
}
