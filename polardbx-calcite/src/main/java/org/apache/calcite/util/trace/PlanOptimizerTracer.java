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
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.sql.SqlExplainLevel;
import org.apache.calcite.util.PlannerContextWithParam;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Collects optimizer snapshots at two granularities:
 * <ul>
 *   <li>{@link PhaseSnapshot} – one entry per major optimization phase.</li>
 *   <li>{@link RuleSnapshot} – one entry per rule application inside a
 *       HepPlanner run (rule-level, fine-grained, only recorded when
 *       detail mode is active).</li>
 * </ul>
 *
 * <h3>Correct recording sequence for each phase</h3>
 * <ol>
 *   <li>Call {@link #beginPhaseSnapshot(OptimizerPhase)} <em>before</em> running
 *       the optimizer.  This creates an empty snapshot slot, pushes it onto
 *       the phase stack, and assigns a hierarchical sequence number so that
 *       nested phases (e.g. CTE consumer re-optimization) are rendered as
 *       2, 2.1, 2.1.1, etc.</li>
 *   <li>Run the optimizer ({@code HepPlanner.findBestExp()} etc.).  Each rule
 *       that fires calls {@link #addRuleSnapshot(String)} which appends to the
 *       snapshot on top of the stack.</li>
 *   <li>Call {@link #endPhaseSnapshot(RelNode, PlannerContextWithParam, SqlExplainLevel)}
 *       with the output plan to fill in the final plan display and pop the stack.</li>
 * </ol>
 */
public class PlanOptimizerTracer {

    private static final String INDENT_UNIT = "  ";

    // ── Phase-level snapshots (coarse-grained) ───────────────────────────────

    /**
     * A snapshot for one optimizer phase.
     * {@code planDisplay} is filled in <em>after</em> the phase completes via
     * {@link #endPhaseSnapshot}.
     * Rule-level snapshots fired during this phase are appended via
     * {@link #addRuleSnapshot(String)}.
     */
    public static class PhaseSnapshot {
        private final OptimizerPhase phase;
        private final PhaseSnapshot parent;
        private final String sequenceNumber;
        private final int depth;
        private final boolean skipped;
        private final long startTimeMs;
        private final List<RuleSnapshot> ruleSnapshots = new ArrayList<>();
        private final Map<String, Integer> ruleCounts = new HashMap<>();

        private int childCount;
        /** Serialised plan string; filled after the phase ends, or {@code null} when skipped. */
        private String planDisplay;
        /** Wall-clock duration (millis) set by {@link #endPhaseSnapshot}. */
        private long durationMs;

        PhaseSnapshot(OptimizerPhase phase, boolean skipped,
                      PhaseSnapshot parent, String sequenceNumber) {
            this.phase = phase;
            this.skipped = skipped;
            this.parent = parent;
            this.sequenceNumber = sequenceNumber;
            this.depth = parent == null ? 0 : parent.depth + 1;
            this.startTimeMs = System.currentTimeMillis();
        }

        public OptimizerPhase getPhase() {
            return phase;
        }

        public PhaseSnapshot getParent() {
            return parent;
        }

        public String getSequenceNumber() {
            return sequenceNumber;
        }

        /**
         * Returns the indentation prefix derived from the nesting depth,
         * e.g. "" for top-level, "  " for depth-1, "    " for depth-2.
         */
        public String getIndent() {
            if (depth == 0) {
                return "";
            }
            StringBuilder sb = new StringBuilder(depth * INDENT_UNIT.length());
            for (int i = 0; i < depth; i++) {
                sb.append(INDENT_UNIT);
            }
            return sb.toString();
        }

        public String getPlanDisplay() {
            return planDisplay;
        }

        public boolean isSkipped() {
            return skipped;
        }

        public List<RuleSnapshot> getRuleSnapshots() {
            return ruleSnapshots;
        }

        public Map<String, Integer> getRuleCounts() {
            return ruleCounts;
        }

        public long getDurationMs() {
            return durationMs;
        }

        /** Appends a rule snapshot and increments the per-rule fire count. */
        void addRuleSnapshot(String ruleName, String beforePlanDisplay) {
            ruleSnapshots.add(new RuleSnapshot(ruleName, beforePlanDisplay));
            ruleCounts.merge(ruleName, 1, Integer::sum);
        }

        /** Marks completion: fills plan display and computes duration. */
        void complete(String planDisplay) {
            this.planDisplay = planDisplay;
            this.durationMs = System.currentTimeMillis() - startTimeMs;
        }
    }

    // ── Rule-level snapshots (fine-grained, detail mode only) ────────────────

    /**
     * A snapshot taken <em>before</em> a single HepPlanner rule fires,
     * paired with the rule name that is about to transform it.
     */
    public static class RuleSnapshot {
        private final String ruleName;
        private final String planDisplay;

        public RuleSnapshot(String ruleName, String planDisplay) {
            this.ruleName = ruleName;
            this.planDisplay = planDisplay;
        }

        public String getRuleName() {
            return ruleName;
        }

        public String getPlanDisplay() {
            return planDisplay;
        }
    }

    // ── State ─────────────────────────────────────────────────────────────────

    private final Deque<PhaseSnapshot> phaseStack;
    private final List<PhaseSnapshot> phaseSnapshots;
    private int topLevelCount;

    /**
     * Used by HepPlanner to hold the plan string right before a rule fires,
     * so that it can be attached to the resulting rule snapshot.
     * Only populated when detail mode is enabled.
     */
    private String pendingRulePlanDisplay;

    private Map<RelNode, RuntimeStatisticsSketch> runtimeStatistics;

    // ── Constructor ───────────────────────────────────────────────────────────

    /**
     * Default constructor. Creates an empty tracer with no phase snapshots
     * and no runtime statistics. The tracer is in a usable state immediately;
     * callers drive it via {@link #beginPhaseSnapshot} / {@link #endPhaseSnapshot}.
     */
    public PlanOptimizerTracer() {
        this.phaseStack = new ArrayDeque<>();
        this.phaseSnapshots = new ArrayList<>();
        this.topLevelCount = 0;
        this.pendingRulePlanDisplay = null;
        this.runtimeStatistics = null;
    }

    // ── Phase-level API ───────────────────────────────────────────────────────

    /**
     * Step 1: creates an empty {@link PhaseSnapshot} slot for {@code phase},
     * records the start time, assigns a hierarchical sequence number, and
     * pushes it onto the phase stack.
     * Must be called <em>before</em> running the optimizer so that any rule
     * snapshots fired during the run are attributed to this phase.
     */
    public void beginPhaseSnapshot(OptimizerPhase phase) {
        PhaseSnapshot parent = phaseStack.peek();
        PhaseSnapshot snapshot = new PhaseSnapshot(phase, false, parent, nextSequenceNumber(parent));
        phaseSnapshots.add(snapshot);
        phaseStack.push(snapshot);
    }

    /**
     * Step 3: fills in the final plan display for the phase on top of the stack,
     * records the duration since {@link #beginPhaseSnapshot}, and pops the stack
     * so that rule snapshots are again attributed to the enclosing phase.
     * Must be called <em>after</em> the optimizer finishes.
     */
    public void endPhaseSnapshot(RelNode plan, PlannerContextWithParam context,
                                 SqlExplainLevel sqlExplainLevel) {
        PhaseSnapshot snapshot = phaseStack.poll();
        if (snapshot == null) {
            return;
        }
        snapshot.complete(buildPlanString(plan, (Context) context, sqlExplainLevel));
    }

    /**
     * Records that a phase was evaluated but skipped (condition not met).
     * No plan display is needed.  Skipped phases share the parent context
     * for sequence numbering but are not pushed onto the stack.
     */
    public void addSkippedPhase(OptimizerPhase phase) {
        PhaseSnapshot parent = phaseStack.peek();
        phaseSnapshots.add(new PhaseSnapshot(phase, true, parent, nextSequenceNumber(parent)));
    }

    public List<PhaseSnapshot> getPhaseSnapshots() {
        return phaseSnapshots;
    }

    /**
     * Allocates the next hierarchical sequence number under {@code parent}
     * (or at the top level when {@code parent} is null), bumping the
     * appropriate counter.
     */
    private String nextSequenceNumber(PhaseSnapshot parent) {
        if (parent == null) {
            return String.valueOf(++topLevelCount);
        }
        return parent.sequenceNumber + "." + (++parent.childCount);
    }

    // ── Rule-level API ────────────────────────────────────────────────────────

    /**
     * Called by HepPlanner before firing a rule to save the current root plan.
     * Only does work when detail mode is active (caller must guard with
     * {@link CalcitePlanOptimizerTrace#isDetailMode()}).
     */
    public void savePlanDisplayIfNecessary(RelNode rootPlan, Context context,
                                           SqlExplainLevel sqlExplainLevel) {
        pendingRulePlanDisplay = buildPlanString(rootPlan, context, sqlExplainLevel);
    }

    /**
     * Called by HepPlanner after a rule fires successfully.
     * Appends the rule snapshot to the phase on top of the stack.
     * If the stack is empty the call is silently ignored.
     */
    public void addRuleSnapshot(String ruleName) {
        PhaseSnapshot current = phaseStack.peek();
        if (current == null) {
            return;
        }
        current.addRuleSnapshot(ruleName, pendingRulePlanDisplay);
    }

    /**
     * Increments the rule count on the phase on top of the stack
     * without recording a full rule snapshot (no plan display).
     * Used by VolcanoPlanner / TopDownRuleDriver where plan-display-per-rule
     * is too expensive, but rule invocation counts are still useful.
     */
    public void incrementRuleCount(String ruleName) {
        PhaseSnapshot current = phaseStack.peek();
        if (current == null) {
            return;
        }
        current.ruleCounts.merge(ruleName, 1, Integer::sum);
    }

    private String buildPlanString(RelNode rootPlan, Context context,
                                   SqlExplainLevel sqlExplainLevel) {
        RelDrdsWriter relWriter = new RelDrdsWriter(sqlExplainLevel);
        if (context instanceof PlannerContextWithParam) {
            final PlannerContextWithParam ctxWithParam = (PlannerContextWithParam) context;
            relWriter = new RelDrdsWriter(null, sqlExplainLevel,
                ctxWithParam.getParams().getCurrentParameter(), ctxWithParam.getEvalFunc(),
                ctxWithParam.getExecContext());
        }
        rootPlan.explainForDisplay(relWriter);
        return relWriter.asString();
    }

    // ── Lifecycle & runtime statistics ────────────────────────────────────────

    public void clean() {
        phaseSnapshots.clear();
        phaseStack.clear();
        topLevelCount = 0;
        pendingRulePlanDisplay = null;
        runtimeStatistics = null;
    }

    public Map<RelNode, RuntimeStatisticsSketch> getRuntimeStatistics() {
        return runtimeStatistics;
    }

    public void setRuntimeStatistics(Map<RelNode, RuntimeStatisticsSketch> runtimeStatistics) {
        this.runtimeStatistics = runtimeStatistics;
    }
}
