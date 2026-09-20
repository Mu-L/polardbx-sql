package com.alibaba.polardbx.gms.node;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import lombok.AllArgsConstructor;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.gms.node.CCLDetectManager.DEFAULT_ROOT_COLUMN;
import static com.alibaba.polardbx.gms.node.CCLDetectManager.UNDETERMINED_COLUMN;

/**
 * @author liugaoji
 */
public class CCLDetectUtils {
    protected static final Logger logger = LoggerFactory.getLogger(CCLDetectUtils.class);

    @AllArgsConstructor
    public static class DubiousItem {
        Long id;
        // sql
        String info;
        Long time;

        @Override
        public String toString() {
            return id + ";" + info + " " + time;
        }
    }

    // Utils
    public static String parseTemplateIdAndRootColumn(String sqlWithHint) {
        String hintPrefix = "/*DRDS /";
        String hintSuffix = "/ */";
        if (sqlWithHint == null || !sqlWithHint.startsWith(hintPrefix) || !sqlWithHint.contains(hintSuffix)) {
            return null;
        }
        int hintEndIndex = sqlWithHint.indexOf(hintSuffix);
        // Example hintContent: "127.0.0.1/195881395b000000/0//d4959a5f28/"
        String[] parts = sqlWithHint.substring(hintPrefix.length(), hintEndIndex).trim().split("/");
        if (parts.length > 9) {
            return parts[9];
        } else {
            return null;
        }
    }

    // Utils
    public static boolean isOutTrxSql(String sqlWithHint) {
        String hintPrefix = "/*DRDS /";
        String hintSuffix = "/ */";
        if (sqlWithHint == null || !sqlWithHint.startsWith(hintPrefix) || !sqlWithHint.contains(hintSuffix)) {
            return false;
        }
        int hintEndIndex = sqlWithHint.indexOf(hintSuffix);
        // Example hintContent: "127.0.0.1/195881395b000000/0//d4959a5f28/"
        String[] parts = sqlWithHint.substring(hintPrefix.length(), hintEndIndex).trim().split("/");
        if (parts.length > 1) {
            return parts[1].split("-").length == 1;
        } else {
            return false;
        }
    }

    // Utils
    public static boolean isRootColumn(String columnName, String partKeyName) {
        try {
            String rootColumn = DynamicConfig.getInstance().getCclDetectRootColumn();
            if (columnName.equalsIgnoreCase(rootColumn)
                || (partKeyName != null && rootColumn.equalsIgnoreCase(DEFAULT_ROOT_COLUMN)
                && columnName.equalsIgnoreCase(partKeyName))) {
                return true;
            }
        } catch (Exception e) {
            logger.error("CCL_DETECT check is RootColumn error " + e.getMessage());
        }
        return false;
    }

    // check DN has out trx sql
    public static boolean hasOutTrx(List<DubiousItem> infos, Map<String, Pair<Long, Integer>> interceptTime,
                                    Map<String, List<DubiousItem>> interceptInfos, boolean isTimeInMillis) {
        Set<String> outTrxKey = new HashSet<>();
        Map<String, Long> maxExecTime = new HashMap<>();
        infos.forEach(e -> {
            // only generate rule for specific query
            String key = CCLDetectUtils.parseTemplateIdAndRootColumn(e.info);

            if (key == null || key.isEmpty()) {
                return;
            }

            logger.warn(String.format("CCL_DETECT hasOutTrx key %s info %s", key, e.info));
            if (CCLDetectUtils.isOutTrxSql(e.info)) {
                outTrxKey.add(key);
            }

            Long time = e.time;
            Pair<Long, Integer> old = interceptTime.get(key);
            maxExecTime.compute(key, (k, v) -> v == null ? time : Math.max(v, time));
            if (old == null) {
                interceptTime.put(key, Pair.of(time, 1));
            } else {
                interceptTime.put(key, Pair.of(old.getKey() + time, old.getValue() + 1));
            }

            interceptInfos.computeIfAbsent(key, k -> new ArrayList<>()).add(e);
        });

        logger.warn(String.format("CCL_DETECT outTrxKey Size %d %s", outTrxKey.size(),
            outTrxKey.stream().limit(10).collect(java.util.stream.Collectors.toList())));

        logger.warn(String.format("CCL_DETECT interceptTime Size %d %s", interceptTime.size(),
            interceptTime.keySet().stream().limit(10).collect(java.util.stream.Collectors.toList())));
        long slowThreshold = (long) DynamicConfig.getInstance().getCclDetectSlowThreshold();
        long slowThresholdInTimeUnit = isTimeInMillis ? slowThreshold * 1000L : slowThreshold;
        boolean hasSlowOutTrx = outTrxKey.stream()
            .map(maxExecTime::get)
            .anyMatch(v -> v > slowThresholdInTimeUnit);

        return hasSlowOutTrx;
    }
}
