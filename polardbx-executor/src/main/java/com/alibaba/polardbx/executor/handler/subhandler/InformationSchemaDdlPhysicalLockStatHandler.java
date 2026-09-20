package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.gms.metadb.misc.DdlPhysicalLockStatAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlPhysicalLockStatRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaDdlPhysicalLockStat;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import org.apache.commons.collections.CollectionUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Handler for Information Schema DDL Physical Lock Stat view.
 * This handler queries the ddl_physical_lock_stat table and returns
 * statistics about physical table locks during DDL operations.
 * Supports filter pushdown for JOB_ID and TABLE_SCHEMA conditions.
 *
 * @author luoyanxin
 */
public class InformationSchemaDdlPhysicalLockStatHandler extends BaseVirtualViewSubClassHandler {

    static final Logger LOGGER = LoggerFactory.getLogger(InformationSchemaDdlPhysicalLockStatHandler.class);

    public InformationSchemaDdlPhysicalLockStatHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaDdlPhysicalLockStat;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        try (Connection metadbConn = MetaDbUtil.getConnection()) {
            DdlPhysicalLockStatAccessor accessor = new DdlPhysicalLockStatAccessor();
            accessor.setConnection(metadbConn);

            // Extract filter conditions from the query
            Map<Integer, ParameterContext> params = executionContext.getParams().getCurrentParameter();
            final int jobIdIndex = InformationSchemaDdlPhysicalLockStat.getJobIdIndex();
            final int tableSchemaIndex = InformationSchemaDdlPhysicalLockStat.getTableSchemaIndex();
            final int tableNameIndex = InformationSchemaDdlPhysicalLockStat.getTableNameIndex();

            Set<String> jobIds = virtualView.getEqualsFilterValues(jobIdIndex, params);
            Set<String> tableSchemas = virtualView.getEqualsFilterValues(tableSchemaIndex, params);
            Set<String> tableNames = virtualView.getEqualsFilterValues(tableNameIndex, params);

            List<DdlPhysicalLockStatRecord> records = new ArrayList<>();

            // Push down filter conditions to database query
            if (CollectionUtils.isNotEmpty(jobIds)) {
                // If JOB_ID filter is present, query by job_id
                for (String jobIdStr : jobIds) {
                    long jobId = Long.parseLong(jobIdStr);
                    if (CollectionUtils.isNotEmpty(tableSchemas) && CollectionUtils.isNotEmpty(tableNames)) {
                        // JOB_ID + TABLE_SCHEMA + TABLE_NAME filters
                        for (String tableSchema : tableSchemas) {
                            for (String tableName : tableNames) {
                                List<DdlPhysicalLockStatRecord> recordList =
                                    accessor.queryByJobIdAndTable(jobId, tableSchema, tableName);
                                if (CollectionUtils.isNotEmpty(recordList)) {
                                    records.addAll(recordList);
                                }
                            }
                        }
                    } else {
                        // Only JOB_ID filter
                        List<DdlPhysicalLockStatRecord> recordList = accessor.queryByJobId(jobId);
                        if (CollectionUtils.isNotEmpty(recordList)) {
                            records.addAll(recordList);
                        }
                    }
                }
            } else if (CollectionUtils.isNotEmpty(tableSchemas)) {
                // TABLE_SCHEMA filter (without JOB_ID)
                for (String tableSchema : tableSchemas) {
                    if (CollectionUtils.isNotEmpty(tableNames)) {
                        // TABLE_SCHEMA + TABLE_NAME filters
                        for (String tableName : tableNames) {
                            List<DdlPhysicalLockStatRecord> recordList =
                                accessor.queryBySchemaAndTable(tableSchema, tableName);
                            if (CollectionUtils.isNotEmpty(recordList)) {
                                records.addAll(recordList);
                            }
                        }
                    } else {
                        // Only TABLE_SCHEMA filter
                        List<DdlPhysicalLockStatRecord> recordList = accessor.queryBySchema(tableSchema);
                        if (CollectionUtils.isNotEmpty(recordList)) {
                            records.addAll(recordList);
                        }
                    }
                }
            } else {
                // No filters or only TABLE_NAME (cannot push down TABLE_NAME alone), query all
                records = accessor.queryAll();
            }

            // Add filtered records to result cursor
            for (DdlPhysicalLockStatRecord record : GeneralUtil.emptyIfNull(records)) {
                // Apply additional in-memory filtering for TABLE_NAME if needed
                // (when TABLE_NAME is specified without TABLE_SCHEMA, we query all and filter in memory)
                if (CollectionUtils.isEmpty(tableSchemas) && CollectionUtils.isNotEmpty(tableNames) &&
                    !tableNames.contains(record.getTableName().toLowerCase())) {
                    continue;
                }

                addRow(cursor, record);
            }

        } catch (SQLException e) {
            LOGGER.error("Failed to query ddl_physical_lock_stat table", e);
        }

        return cursor;
    }

    private static void addRow(ArrayResultCursor cursor, DdlPhysicalLockStatRecord record) {
        cursor.addRow(new Object[] {
            record.getId(),
            record.getJobId(),
            record.getDdlType(),
            record.getTableSchema(),
            record.getTableName(),
            record.getPhysicalDb(),
            record.getPhysicalTable(),
            record.getLockDurationMs(),
            record.getRowCount(),
            record.getStartTime(),
            record.getCurLockStartTime(),
            record.getEndTime(),
            record.getState() == DdlPhysicalLockStatRecord.STATE_LOCKED ? "LOCKED" : "UNLOCKED"
        });
    }
}
