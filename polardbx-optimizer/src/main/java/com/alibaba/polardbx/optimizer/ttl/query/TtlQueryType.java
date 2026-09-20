package com.alibaba.polardbx.optimizer.ttl.query;

import java.util.Collection;

/**
 * @author pangzhaoxing
 */
public enum TtlQueryType {
    HOT_ONLY, COLD_ONLY, HOT_AND_COLD, HOT_COMMON;

    public static boolean needTtlPruner(TtlQueryType ttlQueryType) {
        return ttlQueryType == HOT_AND_COLD;
    }

    public static boolean needHybridSchedule(TtlQueryType ttlQueryType) {
        return ttlQueryType == HOT_AND_COLD || ttlQueryType == COLD_ONLY;
    }

    public static boolean supportTransaction(TtlQueryType ttlQueryType) {
        return ttlQueryType == HOT_ONLY || ttlQueryType == HOT_COMMON;
    }

    public static TtlQueryType mergeTtlQueryType(Collection<TtlQueryType> ttlQueryTypes) {
        TtlQueryType mergeType = null;
        for (TtlQueryType type : ttlQueryTypes) {
            if (mergeType == null) {
                mergeType = type;
            } else {
                if (type == HOT_AND_COLD) {
                    mergeType = HOT_AND_COLD;
                } else {
                    if (type != mergeType) {
                        mergeType = HOT_AND_COLD;
                    }
                }
            }
        }
        return mergeType;
    }

}
