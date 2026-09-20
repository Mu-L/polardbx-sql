package com.alibaba.polardbx.common;

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.stats.metric.FeatureStats;

import java.util.concurrent.ConcurrentHashMap;

import static com.alibaba.polardbx.stats.metric.FeatureStatsItem.UNVEC_EXPRESSION_HIT_TIMES;
import static com.alibaba.polardbx.stats.metric.FeatureStatsItem.VEC_EXPRESSION_HIT_TIMES;

public class ExprStatistic {
    private static final ConcurrentHashMap<String, Long> exprExecTimes = new ConcurrentHashMap<>();

    public static void updateExprStatistic(String key, boolean isVec) {
        if (key == null) {
            return;
        }
        FeatureStats stats = FeatureStats.getInstance();
        if (isVec) {
            stats.increment(VEC_EXPRESSION_HIT_TIMES);
            return;
        }
        stats.increment(UNVEC_EXPRESSION_HIT_TIMES);
        exprExecTimes.compute(key, (k, v) -> {
            if (v == null) {
                return 0L;
            }
            long after = v + 1;
            if (after >= DynamicConfig.getInstance().getExpressionStatsThreshold()) {
                EventLogger.log(EventType.EXPRESSION_STATS, k + " execTimes " + after);
                return 0L;
            }
            return after;
        });
    }
}
