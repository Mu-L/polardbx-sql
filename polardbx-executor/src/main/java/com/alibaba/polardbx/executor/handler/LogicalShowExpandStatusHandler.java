package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.misc.ExpandPartitionTaskRecord;
import com.alibaba.polardbx.gms.metadb.misc.ExpandPartitionTasksAccessor;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShowExpandStatus;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Handler for: SHOW EXPAND STATUS [FOR table]
 */
public class LogicalShowExpandStatusHandler extends HandlerCommon {

    public LogicalShowExpandStatusHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        final LogicalShow show = (LogicalShow) logicalPlan;
        final SqlShowExpandStatus showExpandStatus = (SqlShowExpandStatus) show.getNativeSqlNode();

        ArrayResultCursor result = new ArrayResultCursor("EXPAND_STATUS");
        result.addColumn("SCHEMA_NAME", DataTypes.StringType);
        result.addColumn("TABLE_NAME", DataTypes.StringType);
        result.addColumn("STATUS", DataTypes.StringType);
        result.addColumn("INITIAL_PARTS", DataTypes.LongType);
        result.addColumn("TARGET_PARTS", DataTypes.LongType);
        result.addColumn("COMPLETED", DataTypes.LongType);
        result.addColumn("PENDING", DataTypes.LongType);
        result.addColumn("DDL_JOB_ID", DataTypes.LongType);
        result.addColumn("GMT_CREATED", DataTypes.StringType);
        result.addColumn("GMT_MODIFIED", DataTypes.StringType);
        result.initMeta();

        String schemaName = show.getSchemaName();
        if (TStringUtil.isEmpty(schemaName)) {
            schemaName = executionContext.getSchemaName();
        }

        SqlNode tableNameNode = showExpandStatus.getTableName();
        String tableName = null;
        if (tableNameNode != null) {
            if (tableNameNode instanceof SqlIdentifier) {
                SqlIdentifier id = (SqlIdentifier) tableNameNode;
                if (id.names.size() == 2) {
                    String schemaFromId = id.names.get(0);
                    if (TStringUtil.isNotEmpty(schemaFromId)) {
                        schemaName = schemaFromId;
                    }
                    tableName = id.names.get(1);
                } else {
                    tableName = id.getLastName();
                }
            } else {
                tableName = RelUtils.lastStringValue(tableNameNode);
            }
        }

        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            ExpandPartitionTasksAccessor accessor = new ExpandPartitionTasksAccessor();
            accessor.setConnection(conn);

            List<ExpandPartitionTaskRecord> records;
            if (tableName != null) {
                records = accessor.queryBySchemaTable(schemaName, tableName);
            } else {
                records = accessor.queryAll();
            }

            for (ExpandPartitionTaskRecord rec : records) {
                if (!TStringUtil.isEmpty(schemaName) && !schemaName.equalsIgnoreCase(rec.schemaName)) {
                    continue;
                }
                String statusStr;
                switch (rec.status) {
                case ExpandPartitionTaskRecord.STATUS_RUNNING:
                    statusStr = "RUNNING";
                    break;
                case ExpandPartitionTaskRecord.STATUS_COMPLETED:
                    statusStr = "COMPLETED";
                    break;
                case ExpandPartitionTaskRecord.STATUS_CANCELLED:
                    statusStr = "CANCELLED";
                    break;
                case ExpandPartitionTaskRecord.STATUS_FAILED:
                    statusStr = "FAILED";
                    break;
                case ExpandPartitionTaskRecord.STATUS_PAUSED:
                    statusStr = "PAUSED";
                    break;
                default:
                    statusStr = "UNKNOWN";
                    break;
                }

                result.addRow(new Object[] {
                    rec.schemaName,
                    rec.tableName,
                    statusStr,
                    rec.initialPartitionCount,
                    rec.targetPartitionCount,
                    rec.completedPartitions,
                    rec.pendingPartitions,
                    rec.ddlJobId,
                    rec.gmtCreated == null ? null : rec.gmtCreated.toString(),
                    rec.gmtModified == null ? null : rec.gmtModified.toString()});
            }
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                "SHOW EXPAND STATUS", "expand_partition_tasks", e.getMessage());
        }

        return result;
    }
}
