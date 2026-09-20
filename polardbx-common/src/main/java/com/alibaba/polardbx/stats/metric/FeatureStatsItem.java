package com.alibaba.polardbx.stats.metric;

/**
 * @author fangwu
 */
public enum FeatureStatsItem {
    NEW_BASELINE_NUM("long"),
    FIX_PLAN_NUM("long"),
    DATETIME_COMPENSATION_NUM("long"),
    HOT_EVOLVE_PLAN_NUM("long"),
    SAMPLE_TASK_SUCC("long"),
    SAMPLE_TASK_FAIL("long"),
    HLL_TASK_SUCC("long"),
    HLL_TASK_FAIL("long"),

    VEC_EXPRESSION_HIT_TIMES("long"),
    UNVEC_EXPRESSION_HIT_TIMES("long"),
    EXPAND_IN("long"),
    GSI_LOOKUP_TIMES("long"),
    GSI_LOOKUP_FALLBACK_MAINTABLE_TIMES("long");

    String type;
    static long hash = 0L;

    FeatureStatsItem(String type) {
        this.type = type;
    }

    public static int longStatsSize() {
        int i = 0;
        for (FeatureStatsItem item : values()) {
            if (item.type.equals("long")) {
                i++;
            }
        }
        return i;
    }

    public static long classVersion() {
        if (hash != 0L) {
            return hash;
        }
        for (FeatureStatsItem item : values()) {
            hash += item.name().hashCode();
            hash += item.type.hashCode();
        }
        return hash;
    }
}