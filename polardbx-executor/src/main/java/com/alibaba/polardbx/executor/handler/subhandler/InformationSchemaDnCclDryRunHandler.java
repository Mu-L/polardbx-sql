package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.sync.FetchCCLDetectKeySyncAction;
import com.alibaba.polardbx.executor.sync.FetchPlanCacheByTemplateIdSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.node.CCLDetectDnActor;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaDnCclDryRun;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author liugaoji
 */
public class InformationSchemaDnCclDryRunHandler extends BaseVirtualViewSubClassHandler {

    public InformationSchemaDnCclDryRunHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    public static final String CREATE_CN_CCL_DETECT_RULE =
        "call polardbx.add_dn_ccl_rule('%s', 'SELECT', '', '', %s, '%s')";

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {

        // Step 1: Fetch CCL detect keys from all CN nodes using sync action
        FetchCCLDetectKeySyncAction cclDetectSyncAction = new FetchCCLDetectKeySyncAction();
        List<List<Map<String, Object>>> cclResults = SyncManagerHelper.syncIgnoreExceptions(
            cclDetectSyncAction, SystemDbHelper.DEFAULT_DB_NAME, SyncScope.ALL);

        // Step 2: Process CCL detect key results
        Map<String, CCLDetectKeyInfo> cclDetectKeyMap = processCclDetectKeyResults(cclResults);

        // Step 3: Extract all templateIds for PlanCache lookup
        Set<String> templateIds = extractTemplateIds(cclDetectKeyMap);

        // Step 4: Sync to all CN nodes to fetch parameterized SQL by templateIds
        Map<String, String> templateIdToSqlMap = new HashMap<>();
        if (!templateIds.isEmpty()) {
            FetchPlanCacheByTemplateIdSyncAction planCacheSyncAction =
                new FetchPlanCacheByTemplateIdSyncAction(templateIds);
            List<List<Map<String, Object>>> planCacheResults = SyncManagerHelper.syncIgnoreExceptions(
                planCacheSyncAction, SystemDbHelper.DEFAULT_DB_NAME, SyncScope.ALL);

            // Step 5: Process results and build templateId to parameterizedSql mapping
            templateIdToSqlMap = processPlanCacheResults(planCacheResults);
        }

        // Step 6: Output results with parameterizedSql
        buildResultRows(cclDetectKeyMap, templateIdToSqlMap, cursor);

        return cursor;
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaDnCclDryRun;
    }

    /**
     * Process CCL detect key results and build CCL detect key mapping
     *
     * @param cclResults CCL detect key results from sync action
     * @return Map of CCL detect key information
     */
    public Map<String, CCLDetectKeyInfo> processCclDetectKeyResults(List<List<Map<String, Object>>> cclResults) {
        Map<String, CCLDetectKeyInfo> cclDetectKeyMap = new HashMap<>();

        for (List<Map<String, Object>> nodeRows : cclResults) {
            if (nodeRows == null) {
                continue;
            }
            for (Map<String, Object> row : nodeRows) {
                String instId = (String) row.get("INST_ID");
                String templateId = (String) row.get("TEMPLATE_ID");
                Long concurrencyCount = (Long) row.get("CONCURRENCY_COUNT");
                if (instId != null && templateId != null && concurrencyCount != null) {
                    String key = instId + ";" + templateId;
                    cclDetectKeyMap.put(key, new CCLDetectKeyInfo(instId, templateId, concurrencyCount));
                }
            }
        }

        return cclDetectKeyMap;
    }

    /**
     * Extract template IDs from CCL detect key mapping
     *
     * @param cclDetectKeyMap CCL detect key mapping
     * @return Set of template IDs
     */
    public Set<String> extractTemplateIds(Map<String, CCLDetectKeyInfo> cclDetectKeyMap) {
        Set<String> templateIds = new HashSet<>();
        for (CCLDetectKeyInfo keyInfo : cclDetectKeyMap.values()) {
            templateIds.add(keyInfo.templateId);
        }
        return templateIds;
    }

    /**
     * Process plan cache results and build templateId to parameterizedSql mapping
     *
     * @param planCacheResults Plan cache results from sync action
     * @return Map of templateId to parameterizedSql
     */
    public Map<String, String> processPlanCacheResults(List<List<Map<String, Object>>> planCacheResults) {
        Map<String, String> templateIdToSqlMap = new HashMap<>();

        for (List<Map<String, Object>> nodeRows : planCacheResults) {
            if (nodeRows == null) {
                continue;
            }
            for (Map<String, Object> row : nodeRows) {
                String templateId = (String) row.get("TEMPLATE_ID");
                String parameterizedSql = (String) row.get("PARAMETERIZED_SQL");
                if (templateId != null && parameterizedSql != null) {
                    // Use the mapping for deduplication
                    templateIdToSqlMap.put(templateId, parameterizedSql);
                }
            }
        }

        return templateIdToSqlMap;
    }

    /**
     * Build result rows and add to cursor
     *
     * @param cclDetectKeyMap CCL detect key mapping
     * @param templateIdToSqlMap Template ID to SQL mapping
     * @param cursor Result cursor to add rows
     */
    public void buildResultRows(Map<String, CCLDetectKeyInfo> cclDetectKeyMap,
                                Map<String, String> templateIdToSqlMap,
                                ArrayResultCursor cursor) {
        for (CCLDetectKeyInfo keyInfo : cclDetectKeyMap.values()) {
            String parameterizedSql = templateIdToSqlMap.get(keyInfo.templateId);
            if (parameterizedSql == null) {
                parameterizedSql = "";
            }

            cursor.addRow(new Object[] {
                keyInfo.instId,
                keyInfo.templateId,
                String.format(CREATE_CN_CCL_DETECT_RULE, keyInfo.instId, keyInfo.concurrencyCount, keyInfo.templateId),
                parameterizedSql
            });
        }
    }

    /**
     * Helper class to store CCL detect key information
     */
    public static class CCLDetectKeyInfo {
        final String instId;
        final String templateId;
        final Long concurrencyCount;

        CCLDetectKeyInfo(String instId, String templateId, Long concurrencyCount) {
            this.instId = instId;
            this.templateId = templateId;
            this.concurrencyCount = concurrencyCount;
        }
    }
}