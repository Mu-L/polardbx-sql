package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.sync.FetchIndexUsageSyncAction;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.view.InformationSchemaLogicalIndexUsage;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Handler for the {@code information_schema.logical_index_usage} view.
 * <p>
 * Queries all DN nodes via {@link FetchIndexUsageSyncAction} and aggregates
 * physical index usage statistics into logical table statistics. Key features:
 * <ul>
 *   <li>Physical schema/table names are mapped to logical names using
 *       {@link PhysicalNameExtractor}.</li>
 *   <li>GSI tables are identified via {@link SchemaManager} metadata; their
 *       {@code GSI_NAME} column is populated and {@code OBJECT_NAME} is set to
 *       the parent logical table name.</li>
 *   <li>The aggregated row includes {@code PARTITION_COUNT} (number of physical
 *       shards that contributed) and {@code AVG_USAGE} (average per shard).</li>
 * </ul>
 */
public class InformationSchemaLogicalIndexUsageHandler extends BaseVirtualViewSubClassHandler {

    private static final Logger logger =
        LoggerFactory.getLogger(InformationSchemaLogicalIndexUsageHandler.class);

    public InformationSchemaLogicalIndexUsageHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaLogicalIndexUsage;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        // Collect equality filters
        Set<String> equalSchemaNames = getEqualSchemaNames(virtualView, executionContext);
        Set<String> equalTableNames = getEqualTableNames(virtualView, executionContext);
        Set<String> equalIndexNames = getEqualIndexNames(virtualView, executionContext);

        // Build SQL — INDEX_NAME filter can be pushed down
        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT OBJECT_SCHEMA, OBJECT_NAME, INDEX_NAME, COUNT_STAR, ");
        sqlBuilder.append("COUNT_FETCH, SUM_TIMER_WAIT, MAX_TIMER_WAIT ");
        sqlBuilder.append("FROM performance_schema.table_io_waits_summary_by_index_usage ");
        sqlBuilder.append("WHERE INDEX_NAME IS NOT NULL ");
        sqlBuilder.append("AND INDEX_NAME != 'PRIMARY' ");
        sqlBuilder.append("AND OBJECT_SCHEMA != 'mysql'");

        if (equalIndexNames != null && !equalIndexNames.isEmpty()) {
            sqlBuilder.append(" AND INDEX_NAME IN (");
            boolean first = true;
            for (String index : equalIndexNames) {
                if (!first) {
                    sqlBuilder.append(",");
                }
                sqlBuilder.append("'").append(index.replace("'", "''")).append("'");
                first = false;
            }
            sqlBuilder.append(")");
        }

        String sql = sqlBuilder.toString();
        String currentSchema = executionContext.getSchemaName();

        // Execute DN queries with timeout
        FetchIndexUsageSyncAction fetchAction =
            new FetchIndexUsageSyncAction(currentSchema, sql, executionContext);
        ResultCursor resultCursor = fetchAction.sync();

        // Fail fast on timeout — thrown outside try-catch to avoid double error-code wrapping
        if (fetchAction.isTimeout()) {
            throw new RuntimeException(fetchAction.getWarningMessage());
        }

        // Process results only after confirming no timeout
        try {
            PhysicalToLogicalTableMapping nameMapping = PhysicalToLogicalTableMapping.buildForAllSchemas();

            // Per-schema GSI mapping cache: avoids rebuilding for each row
            // Key: logical schema name  →  (gsiTableName → parentTableName)
            Map<String, Map<String, String>> schemaGsiMappingCache = new HashMap<>();

            // Aggregate by logical (schema, parentTable, gsiName, indexName)
            Map<String, AggregatedIndexUsage> aggregatedMap = new HashMap<>();

            Row row;
            while ((row = resultCursor.next()) != null) {
                String physicalSchema = DataTypes.StringType.convertFrom(row.getObject(0));
                String physicalTableName = DataTypes.StringType.convertFrom(row.getObject(1));
                String indexName = DataTypes.StringType.convertFrom(row.getObject(2));
                Long countStar = DataTypes.LongType.convertFrom(row.getObject(3));
                Long countFetch = DataTypes.LongType.convertFrom(row.getObject(4));
                Long sumTimerWait = DataTypes.LongType.convertFrom(row.getObject(5));
                Long maxTimerWait = DataTypes.LongType.convertFrom(row.getObject(6));

                if (countStar == null) {
                    countStar = 0L;
                }
                if (countFetch == null) {
                    countFetch = 0L;
                }
                if (sumTimerWait == null) {
                    sumTimerWait = 0L;
                }
                if (maxTimerWait == null) {
                    maxTimerWait = 0L;
                }

                // Translate physical names to logical names via CN metadata mapping (with fallback)
                String logicalSchema = nameMapping.getLogicalSchemaName(physicalSchema);
                String logicalTableName = nameMapping.getLogicalTableName(physicalTableName);

                // Identify whether the logical table is a GSI
                Map<String, String> gsiMapping = schemaGsiMappingCache.computeIfAbsent(
                    logicalSchema, this::buildGsiMapping);

                String gsiName = "";
                String objectName = logicalTableName;
                if (gsiMapping.containsKey(logicalTableName.toLowerCase())) {
                    // This physical table belongs to a GSI: record the GSI name and
                    // point OBJECT_NAME to the parent primary table
                    gsiName = logicalTableName;
                    objectName = gsiMapping.get(logicalTableName.toLowerCase());
                }

                // Aggregation key uniquely identifies one output row
                String key = logicalSchema + "." + objectName + "." + gsiName + "." + indexName;

                AggregatedIndexUsage usage = aggregatedMap.get(key);
                if (usage == null) {
                    usage = new AggregatedIndexUsage(logicalSchema, objectName, gsiName, indexName,
                        0L, 0, 0L, 0L, 0L);
                    aggregatedMap.put(key, usage);
                }
                usage.totalUsage += countStar;
                usage.partitionCount++;
                usage.totalCountFetch += countFetch;
                usage.totalSumTimerWait += sumTimerWait;
                if (maxTimerWait > usage.maxTimerWait) {
                    usage.maxTimerWait = maxTimerWait;
                }
            }

            // Emit aggregated rows, applying post-filters for schema/table names
            for (AggregatedIndexUsage usage : aggregatedMap.values()) {
                if (equalSchemaNames != null && !equalSchemaNames.isEmpty()
                    && !equalSchemaNames.contains(usage.objectSchema)) {
                    continue;
                }
                if (equalTableNames != null && !equalTableNames.isEmpty()
                    && !equalTableNames.contains(usage.objectName)) {
                    continue;
                }

                long avgUsage = usage.partitionCount > 0
                    ? usage.totalUsage / usage.partitionCount : 0L;

                Object[] rowData = new Object[10];
                rowData[0] = usage.objectSchema;
                rowData[1] = usage.objectName;
                rowData[2] = usage.gsiName;
                rowData[3] = usage.indexName;
                rowData[4] = usage.totalUsage;
                rowData[5] = (long) usage.partitionCount;
                rowData[6] = avgUsage;
                rowData[7] = usage.totalCountFetch;
                rowData[8] = usage.totalSumTimerWait;
                rowData[9] = usage.maxTimerWait;

                cursor.addRow(rowData);
            }
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_DATA_OUTPUT,
                "Failed to process logical index usage results: " + e.getMessage());
        }

        return cursor;
    }

    /**
     * Build a mapping of {@code gsiTableName (lower-case) -> parentTableName} for the
     * given logical schema by inspecting {@link SchemaManager} metadata.
     * <p>
     * Returns an empty map if the schema is unknown or metadata cannot be accessed
     * (graceful degradation — {@code GSI_NAME} will be left blank for all rows).
     */
    Map<String, String> buildGsiMapping(String logicalSchema) {
        Map<String, String> gsiToParent = new HashMap<>();
        try {
            OptimizerContext oc = OptimizerContext.getContext(logicalSchema);
            if (oc == null) {
                return gsiToParent;
            }
            SchemaManager sm = oc.getLatestSchemaManager();
            for (TableMeta tm : sm.getAllTables()) {
                if (tm.isGsi()
                    && tm.getGsiTableMetaBean() != null
                    && tm.getGsiTableMetaBean().gsiMetaBean != null) {
                    gsiToParent.put(
                        tm.getTableName().toLowerCase(),
                        tm.getGsiTableMetaBean().gsiMetaBean.tableName);
                }
            }
        } catch (Exception e) {
            logger.warn("Failed to build GSI mapping for schema: " + logicalSchema, e);
        }
        return gsiToParent;
    }

    // ==================== Filter helpers ====================

    Set<String> getEqualSchemaNames(VirtualView virtualView, ExecutionContext executionContext) {
        Map<Integer, com.alibaba.polardbx.common.jdbc.ParameterContext> params =
            executionContext.getParamMap();
        if (params == null) {
            return Collections.emptySet();
        }
        return virtualView.getEqualsFilterValues(InformationSchemaLogicalIndexUsage.getObjectSchemaIndex(), params);
    }

    Set<String> getEqualTableNames(VirtualView virtualView, ExecutionContext executionContext) {
        Map<Integer, com.alibaba.polardbx.common.jdbc.ParameterContext> params =
            executionContext.getParamMap();
        if (params == null) {
            return Collections.emptySet();
        }
        return virtualView.getEqualsFilterValues(InformationSchemaLogicalIndexUsage.getObjectNameIndex(), params);
    }

    Set<String> getEqualGsiNames(VirtualView virtualView, ExecutionContext executionContext) {
        Map<Integer, com.alibaba.polardbx.common.jdbc.ParameterContext> params =
            executionContext.getParamMap();
        if (params == null) {
            return Collections.emptySet();
        }
        return virtualView.getEqualsFilterValues(InformationSchemaLogicalIndexUsage.getGsiNameIndex(), params);
    }

    Set<String> getEqualIndexNames(VirtualView virtualView, ExecutionContext executionContext) {
        Map<Integer, com.alibaba.polardbx.common.jdbc.ParameterContext> params =
            executionContext.getParamMap();
        if (params == null) {
            return Collections.emptySet();
        }
        return virtualView.getEqualsFilterValues(InformationSchemaLogicalIndexUsage.getIndexNameIndex(), params);
    }

    // ==================== Aggregation data class ====================

    private static class AggregatedIndexUsage {
        String objectSchema;
        String objectName;
        String gsiName;
        String indexName;
        Long totalUsage;
        int partitionCount;
        Long totalCountFetch;
        Long totalSumTimerWait;
        Long maxTimerWait;

        AggregatedIndexUsage(String objectSchema, String objectName, String gsiName,
                             String indexName, Long totalUsage, int partitionCount,
                             Long totalCountFetch, Long totalSumTimerWait, Long maxTimerWait) {
            this.objectSchema = objectSchema;
            this.objectName = objectName;
            this.gsiName = gsiName;
            this.indexName = indexName;
            this.totalUsage = totalUsage;
            this.partitionCount = partitionCount;
            this.totalCountFetch = totalCountFetch;
            this.totalSumTimerWait = totalSumTimerWait;
            this.maxTimerWait = maxTimerWait;
        }
    }
}
