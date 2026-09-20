package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.sync.MetricSyncAllAction;
import com.alibaba.polardbx.gms.sync.GmsSyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.view.InformationSchemaMetric;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.alibaba.polardbx.stats.metric.FeatureStats;
import com.alibaba.polardbx.stats.metric.FeatureStatsItem;
import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

/**
 * @author fangwu
 */
public class InformationSchemaMetricHandler extends BaseVirtualViewSubClassHandler {

    public InformationSchemaMetricHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaMetric;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        return buildArrayResultCursor(cursor);
    }

    public static ArrayResultCursor buildArrayResultCursor(ArrayResultCursor cursor) {
        List<List<Map<String, Object>>> metrics =
            GmsSyncManagerHelper.sync(new MetricSyncAllAction(), "polardbx", SyncScope.CURRENT_ONLY);
        if (metrics == null || metrics.isEmpty()) {
            return cursor;
        }
        for (List<Map<String, Object>> nodeRows : metrics) {
            if (nodeRows == null) {
                continue;
            }
            for (Map<String, Object> row : nodeRows) {
                final String host = DataTypes.StringType.convertFrom(row.get(MetricSyncAllAction.HOST));
                final String instId = DataTypes.StringType.convertFrom(row.get(MetricSyncAllAction.INST));
                String feat = DataTypes.StringType.convertFrom(row.get(MetricSyncAllAction.METRIC_FEAT_KEY));
                String real = DataTypes.StringType.convertFrom(row.get(MetricSyncAllAction.METRIC_REAL_KEY));
                FeatureStats featureStats = FeatureStats.deserialize(feat);
                for (FeatureStatsItem item : FeatureStatsItem.values()) {
                    cursor.addRow(new Object[] {
                        host,
                        instId,
                        item.name(),
                        "feat",
                        featureStats.getLong(item)
                    });
                }

                for (Map.Entry<String, String> entry : decodeReal(real).entrySet()) {
                    cursor.addRow(new Object[] {
                        host,
                        instId,
                        entry.getKey(),
                        "real",
                        entry.getValue()
                    });
                }

            }
        }
        return cursor;
    }

    /*
     * Change context:
     * - Before: decodeReal() used jdk.nashorn.internal.runtime.QuotedStringTokenizer, an internal class of the
     *   jdk.scripting.nashorn module removed since JDK 15, causing NoClassDefFoundError on JDK 15+/21.
     *   Its quote-handling was never exercised: the real metric string produced by RealStatsLog.statLog()
     *   is strictly TYPE:number,TYPE:number,... with no quoted or comma-containing values.
     * - Path impact: only the information_schema.metric virtual view decode path is affected; feat metric
     *   decoding (FeatureStats.deserialize) and MetricSyncAllAction serialization are unchanged.
     * - Capability regression: None. java.util.StringTokenizer skips empty tokens on consecutive separators
     *   exactly like QuotedStringTokenizer does, so parsing results are identical for all real data.
     */
    public static Map<String, String> decodeReal(String real) {
        Map<String, String> map = Maps.newHashMap();
        StringTokenizer tokenizer = new StringTokenizer(real, ",");

        while (tokenizer.hasMoreTokens()) {
            String kv = tokenizer.nextToken();
            String[] kvPair = kv.split(":", 2);
            map.put(kvPair[0], kvPair[1]);
        }

        return map;
    }

}

