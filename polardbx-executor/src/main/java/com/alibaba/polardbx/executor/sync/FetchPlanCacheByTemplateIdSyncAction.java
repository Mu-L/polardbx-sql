package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.gms.sync.IGmsSyncAction;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;

import java.util.Map;
import java.util.Set;

/**
 * Sync action to fetch parameterized SQL from PlanCache by template IDs
 *
 * @author liugaoji
 */
public class FetchPlanCacheByTemplateIdSyncAction implements IGmsSyncAction {

    private Set<String> templateIds;

    public FetchPlanCacheByTemplateIdSyncAction() {
    }

    public FetchPlanCacheByTemplateIdSyncAction(Set<String> templateIds) {
        this.templateIds = templateIds;
    }

    public Set<String> getTemplateIds() {
        return templateIds;
    }

    public void setTemplateIds(Set<String> templateIds) {
        this.templateIds = templateIds;
    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor result = new ArrayResultCursor("PLAN_CACHE_BY_TEMPLATE_ID");
        result.addColumn("COMPUTE_NODE", DataTypes.StringType);
        result.addColumn("TEMPLATE_ID", DataTypes.StringType);
        result.addColumn("PARAMETERIZED_SQL", DataTypes.StringType);

        if (templateIds == null || templateIds.isEmpty()) {
            return result;
        }

        // Enumerate all cache keys in PlanCache
        for (Map.Entry<PlanCache.CacheKey, ExecutionPlan> entry : PlanCache.getInstance().getCache().asMap()
            .entrySet()) {
            PlanCache.CacheKey cacheKey = entry.getKey();

            // Skip CDC database
            if (SystemDbHelper.CDC_DB_NAME.equalsIgnoreCase(cacheKey.getSchema())) {
                continue;
            }

            // Check if templateId matches
            String templateId = cacheKey.getTemplateId();
            if (templateIds.contains(templateId)) {
                String parameterizedSql = cacheKey.getParameterizedSql();

                result.addRow(new Object[] {
                    TddlNode.getHost() + ":" + TddlNode.getPort(),
                    templateId,
                    parameterizedSql
                });
            }
        }

        return result;
    }
}