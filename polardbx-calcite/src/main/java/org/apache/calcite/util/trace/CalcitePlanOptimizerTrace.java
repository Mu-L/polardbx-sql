/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.util.trace;

import org.apache.calcite.plan.Context;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.PlannerContextWithParam;

/**
 * Entry point for optimizer tracing, held on {@code ExecutionContext}.
 * When {@link #isOpen()} returns {@code true}, phase and rule snapshots
 * are recorded inside {@link PlanOptimizerTracer}.
 *
 * <p>Phase-level snapshots are recorded explicitly by each {@code Planner}
 * optimize method via {@link #beginPhaseSnapshot} / {@link #addSkippedPhase}.
 * Nested phases (e.g. CTE consumer re-optimization) are tracked inside
 * {@link PlanOptimizerTracer}'s phase stack.
 */
public class CalcitePlanOptimizerTrace {

    public static SqlExplainLevel DEFAULT_LEVEL = SqlExplainLevel.EXPPLAN_ATTRIBUTES;

    private boolean open;
    /** When true, HepPlanner rule-level snapshots are also recorded. */
    private boolean detailMode;
    private SqlExplainLevel sqlExplainLevel;
    private final PlanOptimizerTracer optimizerTracer;

    public CalcitePlanOptimizerTrace() {
        this.open = false;
        this.detailMode = false;
        this.sqlExplainLevel = SqlExplainLevel.EXPPLAN_ATTRIBUTES;
        this.optimizerTracer = new PlanOptimizerTracer();
    }

    // ── Open / close ──────────────────────────────────────────────────────────

    public boolean isOpen() {
        return open;
    }

    public void setOpen(boolean b) {
        this.open = b;
    }

    // ── Detail mode ───────────────────────────────────────────────────────────

    /** Returns whether rule-level snapshots should be recorded. */
    public boolean isDetailMode() {
        return detailMode;
    }

    public void setDetailMode(boolean b) {
        this.detailMode = b;
    }

    // ── ExplainLevel ──────────────────────────────────────────────────────────

    public SqlExplainLevel getSqlExplainLevel() {
        return sqlExplainLevel;
    }

    public void setSqlExplainLevel(SqlExplainLevel v) {
        this.sqlExplainLevel = v;
    }

    // ── Tracer accessor ───────────────────────────────────────────────────────

    public PlanOptimizerTracer getOptimizerTracer() {
        return optimizerTracer;
    }

    // ── Phase-level API ───────────────────────────────────────────────────────

    /**
     * Step 1 of the two-step phase-recording protocol.
     * Creates an empty phase slot so that any HepPlanner rule fires that
     * happen afterwards are attributed to this phase.
     * Must be called <em>before</em> the optimizer runs.
     */
    public void beginPhaseSnapshot(OptimizerPhase phase) {
        if (open) {
            optimizerTracer.beginPhaseSnapshot(phase);
        }
    }

    /**
     * Step 2 of the two-step phase-recording protocol.
     * Fills in the post-phase plan display on the last phase snapshot so that
     * rule snapshots are again attributed to the enclosing phase.
     * Must be called <em>after</em> the optimizer has finished.
     * No-op when tracing is not open.
     */
    public void endPhaseSnapshot(RelNode plan, PlannerContextWithParam context) {
        if (open) {
            optimizerTracer.endPhaseSnapshot(plan, context, getSqlExplainLevel());
        }
    }

    /**
     * Convenience wrapper that calls {@link #beginPhaseSnapshot} then
     * {@link #endPhaseSnapshot} in one shot.
     * Use only when there is no optimizer between the two steps (e.g. recording
     * the initial plan before any optimization).
     *
     * @deprecated Prefer the two-step pattern:
     *             {@code beginPhaseSnapshot(phase)} → optimizer → {@code endPhaseSnapshot(plan, ctx)}.
     */
    @Deprecated
    public void addPhaseSnapshot(OptimizerPhase phase, RelNode plan,
                                  PlannerContextWithParam context) {
        beginPhaseSnapshot(phase);
        endPhaseSnapshot(plan, context);
    }

    /**
     * Marks a phase as skipped (condition not met – no plan transformation
     * was attempted).  Only recorded when {@link #isDetailMode() detail mode}
     * is on; in non-detail mode the call is a no-op so that skipped phases
     * neither appear in the trace output nor consume a sequence number.
     */
    public void addSkippedPhase(OptimizerPhase phase) {
        if (open && detailMode) {
            optimizerTracer.addSkippedPhase(phase);
        }
    }

    // ── Rule-level API (called from HepPlanner) ───────────────────────────────

    /**
     * Saves the current root plan before a rule fires so that it can be
     * attached to the resulting rule snapshot.
     * No-op when detail mode is off.
     */
    public void savePlanDisplayIfNecessary(RelNode rootPlan, Context context) {
        if (open && detailMode) {
            optimizerTracer.savePlanDisplayIfNecessary(rootPlan, context, getSqlExplainLevel());
        }
    }

    /**
     * Records a rule-level snapshot after a rule fires.
     * No-op when detail mode is off.
     */
    public void traceIt(RelOptRuleCall ruleCall) {
        if (open && detailMode) {
            optimizerTracer.addRuleSnapshot(ruleCall.getRule().toString());
        }
    }

    /**
     * Increments the rule invocation count on the current phase snapshot.
     * Unlike {@link #traceIt} this always records the count (not gated by
     * detail mode) because the overhead is negligible.
     */
    public void countRule(RelOptRuleCall ruleCall) {
        if (open) {
            optimizerTracer.incrementRuleCount(ruleCall.getRule().toString());
        }
    }

    public void clean() {
        this.open = false;
        this.detailMode = false;
        optimizerTracer.clean();
    }

    // ── Static convenience helpers (used by HepPlanner / VolcanoPlanner) ─────

    public static void savePlanDisplayIfNecessaryFromContext(RelNode rootPlan, Context context) {
        if (context instanceof PlannerContextWithParam) {
            ((PlannerContextWithParam) context).getCalcitePlanOptimizerTrace()
                .ifPresent(x -> x.savePlanDisplayIfNecessary(rootPlan, context));
        }
    }

    public static void traceItFromContext(RelOptRuleCall ruleCall, Context context) {
        if (context instanceof PlannerContextWithParam) {
            ((PlannerContextWithParam) context).getCalcitePlanOptimizerTrace()
                .ifPresent(x -> x.traceIt(ruleCall));
        }
    }

    public static void countRuleFromContext(RelOptRuleCall ruleCall, Context context) {
        if (context instanceof PlannerContextWithParam) {
            ((PlannerContextWithParam) context).getCalcitePlanOptimizerTrace()
                .ifPresent(x -> x.countRule(ruleCall));
        }
    }

}
