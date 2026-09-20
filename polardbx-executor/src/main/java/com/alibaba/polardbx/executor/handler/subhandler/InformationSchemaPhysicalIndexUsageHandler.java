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
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.view.InformationSchemaPhysicalIndexUsage;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Handler for the {@code information_schema.physical_index_usage} view.
 * <p>
 * Retrieves raw index usage statistics from all DN nodes via
 * {@link FetchIndexUsageSyncAction} and exposes them as one row per physical
 * table partition.  Two convenience columns are appended to each row:
 * <ul>
 *   <li>{@code LOGICAL_SCHEMA} — derived from the physical schema name via
 *       {@link PhysicalNameExtractor#extractLogicalSchemaName}.</li>
 *   <li>{@code LOGICAL_TABLE}  — derived from the physical table name via
 *       {@link PhysicalNameExtractor#extractLogicalTableName}.</li>
 * </ul>
 * Post-filtering on {@code OBJECT_SCHEMA} and {@code OBJECT_NAME} is applied
 * using the physical names exactly as returned by the DN.
 */
public class InformationSchemaPhysicalIndexUsageHandler extends BaseVirtualViewSubClassHandler {

    private static final Logger logger =
        LoggerFactory.getLogger(InformationSchemaPhysicalIndexUsageHandler.class);

    public InformationSchemaPhysicalIndexUsageHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaPhysicalIndexUsage;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        Set<String> equalSchemaNames = getEqualSchemaNames(virtualView, executionContext);
        Set<String> equalTableNames = getEqualTableNames(virtualView, executionContext);

        StringBuilder sqlBuilder = new StringBuilder();
        sqlBuilder.append("SELECT OBJECT_SCHEMA, OBJECT_NAME, INDEX_NAME, COUNT_STAR, ");
        sqlBuilder.append("COUNT_FETCH, SUM_TIMER_WAIT, MAX_TIMER_WAIT ");
        sqlBuilder.append("FROM performance_schema.table_io_waits_summary_by_index_usage ");
        sqlBuilder.append("WHERE INDEX_NAME IS NOT NULL ");
        sqlBuilder.append("AND INDEX_NAME != 'PRIMARY' ");
        sqlBuilder.append("AND OBJECT_SCHEMA != 'mysql'");

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

            Row row;
            while ((row = resultCursor.next()) != null) {
                String objectSchema = DataTypes.StringType.convertFrom(row.getObject(0));
                String objectName = DataTypes.StringType.convertFrom(row.getObject(1));
                String indexName = DataTypes.StringType.convertFrom(row.getObject(2));
                Object countStar = row.getObject(3);
                Object countFetch = row.getObject(4);
                Object sumTimerWait = row.getObject(5);
                Object maxTimerWait = row.getObject(6);

                if (equalSchemaNames != null && !equalSchemaNames.isEmpty()
                    && !equalSchemaNames.contains(objectSchema)) {
                    continue;
                }
                if (equalTableNames != null && !equalTableNames.isEmpty()
                    && !equalTableNames.contains(objectName)) {
                    continue;
                }

                String logicalSchema = nameMapping.getLogicalSchemaName(objectSchema);
                String logicalTable = nameMapping.getLogicalTableName(objectName);

                Object[] rowData = new Object[9];
                rowData[0] = objectSchema;
                rowData[1] = objectName;
                rowData[2] = indexName;
                rowData[3] = countStar;
                rowData[4] = logicalSchema;
                rowData[5] = logicalTable;
                rowData[6] = countFetch;
                rowData[7] = sumTimerWait;
                rowData[8] = maxTimerWait;

                cursor.addRow(rowData);
            }
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_DATA_OUTPUT,
                "Failed to process physical index usage results: " + e.getMessage());
        }

        return cursor;
    }

    // ==================== Filter helpers ====================

    Set<String> getEqualSchemaNames(VirtualView virtualView, ExecutionContext executionContext) {
        Map<Integer, com.alibaba.polardbx.common.jdbc.ParameterContext> params =
            executionContext.getParamMap();
        if (params == null) {
            return Collections.emptySet();
        }
        return virtualView.getEqualsFilterValues(InformationSchemaPhysicalIndexUsage.getObjectSchemaIndex(), params);
    }

    Set<String> getEqualTableNames(VirtualView virtualView, ExecutionContext executionContext) {
        Map<Integer, com.alibaba.polardbx.common.jdbc.ParameterContext> params =
            executionContext.getParamMap();
        if (params == null) {
            return Collections.emptySet();
        }
        return virtualView.getEqualsFilterValues(InformationSchemaPhysicalIndexUsage.getObjectNameIndex(), params);
    }
}
