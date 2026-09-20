package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.FetchPlanCacheByTemplateIdSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.metadb.ccl.DnCclRecord;
import com.alibaba.polardbx.gms.node.CCLDetectDnActor;
import com.alibaba.polardbx.gms.node.CCLDetectManager;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.RelNode;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * @author liugaoji
 */
public class LogicalShowDnCclHandler extends HandlerCommon {
    private static final Logger logger = LoggerFactory.getLogger(LogicalShowDnCclHandler.class);

    public LogicalShowDnCclHandler(IRepository repo) {
        super(repo);
    }

    public Cursor handle() {
        List<DnCclRecord> allRecords = Lists.newArrayList();

        try {
            //storageStatusMap only storageIds of master && htap-learner.
            Iterator<StorageInstHaContext> iterator = CCLDetectManager.getDnMasterIterator();

            while (iterator.hasNext()) {
                StorageInstHaContext instHaContext = iterator.next();
                List<DnCclRecord> part = CCLDetectDnActor.getDnCclRules(() ->
                        DbTopologyManager.getConnectionForStorage(instHaContext)
                    , instHaContext.getInstId(), instHaContext.getStorageInstId());
                if (part != null) {
                    allRecords.addAll(part);
                }
            }
        } catch (Throwable t) {
            logger.error("CCL_DETECT check slave delay error!", t);
        }

        // Fetch SQL information for keywords (templateIds)
        Map<String, String> templateIdToSqlMap = fetchSqlByTemplateIds(allRecords);

        ArrayResultCursor arrayResultCursor = buildResult(allRecords, templateIdToSqlMap);
        return arrayResultCursor;
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        return handle();
    }

    private ArrayResultCursor buildResult(List<DnCclRecord> dnCclRecords, Map<String, String> templateIdToSqlMap) {
        ArrayResultCursor result = new ArrayResultCursor("DN_CCL");
        result.addColumn("STORAGE_INST_ID", DataTypes.StringType);
        result.addColumn("INST_ID", DataTypes.StringType);
        result.addColumn("ID", DataTypes.StringType);
        result.addColumn("TYPE", DataTypes.StringType);
        result.addColumn("SCHEMA", DataTypes.StringType);
        result.addColumn("TABLE", DataTypes.StringType);
        result.addColumn("STATE", DataTypes.StringType);
        result.addColumn("ORDER", DataTypes.StringType);
        result.addColumn("CONCURRENCY_COUNT", DataTypes.StringType);
        result.addColumn("MATCHED", DataTypes.StringType);
        result.addColumn("RUNNING", DataTypes.StringType);
        result.addColumn("WAITTING", DataTypes.StringType);
        result.addColumn("KEYWORDS", DataTypes.StringType);
        result.addColumn("SQL", DataTypes.StringType);
        result.initMeta();

        for (DnCclRecord dnCclRecord : dnCclRecords) {
            String sql = templateIdToSqlMap.get(dnCclRecord.keywords);
            if (sql == null) {
                sql = "";
            }

            result.addRow(new Object[] {
                dnCclRecord.storageId, dnCclRecord.inst, dnCclRecord.id, dnCclRecord.type, dnCclRecord.schema,
                dnCclRecord.table, dnCclRecord.state, dnCclRecord.order, dnCclRecord.concurrencyCount,
                dnCclRecord.matched, dnCclRecord.running, dnCclRecord.waiting, dnCclRecord.keywords, sql});
        }

        return result;
    }

    /**
     * Fetch SQL information by template IDs using sync action
     *
     * @param dnCclRecords DN CCL records
     * @return Map of templateId to parameterizedSql
     */
    private Map<String, String> fetchSqlByTemplateIds(List<DnCclRecord> dnCclRecords) {
        // Extract template IDs (keywords) from DN CCL records
        Set<String> templateIds = new HashSet<>();
        for (DnCclRecord record : dnCclRecords) {
            if (record.keywords != null && !record.keywords.isEmpty()) {
                templateIds.add(record.keywords);
            }
        }

        Map<String, String> templateIdToSqlMap = new HashMap<>();
        if (!templateIds.isEmpty()) {
            try {
                // Sync to all CN nodes to fetch parameterized SQL by templateIds
                FetchPlanCacheByTemplateIdSyncAction planCacheSyncAction =
                    new FetchPlanCacheByTemplateIdSyncAction(templateIds);
                List<List<Map<String, Object>>> planCacheResults = SyncManagerHelper.syncIgnoreExceptions(
                    planCacheSyncAction, SystemDbHelper.DEFAULT_DB_NAME, SyncScope.ALL);

                // Process results and build templateId to parameterizedSql mapping
                templateIdToSqlMap = processPlanCacheResults(planCacheResults);
            } catch (Throwable t) {
                logger.error("Failed to fetch SQL by template IDs", t);
            }
        }

        return templateIdToSqlMap;
    }

    /**
     * Process plan cache results and build templateId to parameterizedSql mapping
     *
     * @param planCacheResults Plan cache results from sync action
     * @return Map of templateId to parameterizedSql
     */
    private Map<String, String> processPlanCacheResults(List<List<Map<String, Object>>> planCacheResults) {
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

}
