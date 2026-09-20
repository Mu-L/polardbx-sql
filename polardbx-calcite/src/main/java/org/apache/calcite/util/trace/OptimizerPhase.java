package org.apache.calcite.util.trace;

/**
 * Represents a named phase in the query optimizer pipeline.
 */
public enum OptimizerPhase {

    // ── Entry point ─────────────────────────────────────────────────────────
    START("Start"),

    // ── Phase 1: Sub-query unnesting ─────────────────────────────────────────
    SUBQUERY_UNNEST("SubQuery Unnest"),
    SUBQUERY_TO_CORRELATE("SubQuery → Correlate"),
    CORRELATE_REMOVE("Correlate Remove"),

    // ── Phase 2: CTE processing ──────────────────────────────────────────────
    CTE_INLINE("CTE Inline"),
    CTE_OPTIMIZE("CTE Optimize"),

    // ── Phase 3: SQL rewrite (RBO) ───────────────────────────────────────────
    SQL_REWRITE("SQL Rewrite (RBO)"),

    // ── Phase 4: Plan enumeration (CBO) ─────────────────────────────────────
    PLAN_ENUMERATE("Plan Enumerate (CBO)"),

    // Nested CBO triggered by SubQueryPlanEnumerator for each sub-query plan
    // (always nested under PLAN_ENUMERATE).
    SUBQUERY_CBO("SubQuery CBO"),

    // ── Rule-triggered sub-plan re-optimization ──────────────────────────────
    // Phases below are entered from RelOptRule.onMatch and may be nested under
    // any RBO/CBO phase depending on which planner fires the rule:
    //   - PUSH_CORRELATE        : PushCorrelateRule → optimizeBySqlWriter
    //   - OPTIMIZE_CTE_CONSUMER : OptimizeCTEConsumerRule (in CTE_OPTIMIZE)
    //                              → optimizeBySqlWriter
    PUSH_CORRELATE("Push Correlate"),
    OPTIMIZE_CTE_CONSUMER("Optimize CTE Consumer"),

    // ── Phase 5: Physical optimization ───────────────────────────────────────
    RBO_AFTER_CBO("RBO After CBO"),

    // SMP path
    SMP("SMP"),

    // MPP path
    MPP("MPP"),
    MPP_RBO_AFTER_CBO("MPP RBO After CBO"),

    // Columnar path
    COLUMNAR_RBO("Columnar RBO"),
    COLUMNAR_INDEX_SELECTION("Columnar Index Selection"),
    COLUMNAR_POST_RBO("Columnar Post RBO"),
    COLUMNAR_CBO("Columnar CBO"),
    COLUMNAR_CBO_TIMEOUT("Columnar CBO (Timeout Retry)"),
    COLUMNAR_RBO_AFTER_CBO("Columnar RBO After CBO"),

    // ── Phase 6: Final RBO ───────────────────────────────────────────────────
    FINAL_RBO("Final RBO"),
    FINAL_RBO_PHYSICAL_SQL_BEFORE("Physical SQL (before)"),
    FINAL_RBO_PHYSICAL_SQL_AFTER("Physical SQL (after)"),

    // ── Exit point ───────────────────────────────────────────────────────────
    END("End");

    // ── Fields ───────────────────────────────────────────────────────────────

    private final String displayName;

    OptimizerPhase(String displayName) {
        this.displayName = displayName;
    }

    // ── Accessors ─────────────────────────────────────────────────────────────

    public String getDisplayName() {
        return displayName;
    }
}
