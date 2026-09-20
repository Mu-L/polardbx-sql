package com.alibaba.polardbx.executor.ddl.job.task.mce;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.metadb.table.ExternalizedColumnInfo;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMceState;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;

/**
 * Validates an MCE internalize (externalized column -> plain column) target before any physical
 * or metadata mutation. Mirrors {@link MceExternalizeTargetValidator} for the reverse direction.
 * <p>
 * The target must be a terminal externalized column: the loaded ColumnMeta carries the
 * externalized flag, its MCE state derives to {@code EXTERNALIZED}, and the single MetaDB columns
 * record is the physical addr column whose {@code ext_type:} comment defines the original logical
 * type. A column still mid-migration (forward or reverse job in flight) is rejected here and falls
 * back to the generic MODIFY rejection.
 */
public final class MceInternalizeTargetValidator {

    private MceInternalizeTargetValidator() {
    }

    public static void validate(TableMeta tableMeta, String schemaName, String tableName,
                                String columnName, String originalType) {
        if (tableMeta == null) {
            throw ddlError(schemaName, tableName, "table metadata is unavailable");
        }

        ColumnMeta target = tableMeta.getColumnIgnoreCase(columnName);
        if (target == null) {
            throw ddlError(schemaName, tableName,
                String.format("target column '%s' does not exist", columnName));
        }
        if (!target.isExternalizedColumn()
            || tableMeta.getColumnMceState(columnName) != ColumnMceState.EXTERNALIZED) {
            throw ddlError(schemaName, tableName,
                String.format("target column '%s' is not a terminal externalized column", columnName));
        }
        String physicalAddrName = target.getMappingName();
        if (physicalAddrName == null
            || !physicalAddrName.equalsIgnoreCase(ExternalizedColumnInfo.toAddrColumnName(columnName))) {
            throw ddlError(schemaName, tableName,
                String.format("target column '%s' has unexpected physical addr mapping '%s'",
                    columnName, physicalAddrName));
        }
        if (!ExternalizedColumnInfo.isSupportedType(originalType)) {
            throw ddlError(schemaName, tableName,
                String.format("target column '%s' has unsupported original type '%s'", columnName, originalType));
        }
        String storedType = resolveStoredOriginalType(schemaName, tableName, physicalAddrName);
        if (storedType == null || !storedType.equalsIgnoreCase(originalType)) {
            throw ddlError(schemaName, tableName,
                String.format("declared type '%s' does not match the stored ext_type '%s' of column '%s'",
                    originalType, storedType, columnName));
        }
        if (tableMeta.withCci()) {
            throw ddlError(schemaName, tableName, "table has an existing CCI");
        }
        // A terminal externalized table may legally carry GSIs, but internalize only adds the
        // restored content column to primary physical tables: GSI physical tables would miss the
        // column during the dual-write window. Require dropping GSIs first.
        if (tableMeta.withGsiExcludingPureCci()) {
            throw ddlError(schemaName, tableName,
                "table has global secondary indexes; drop them before internalizing");
        }
        // Broadcast tables cannot express the single-primary-route staging protocol; externalized
        // lifecycles reject them in every direction.
        if (isBroadcast(tableMeta, schemaName, tableName)) {
            throw ddlError(schemaName, tableName, "broadcast tables are not supported");
        }
        McePrimaryKeyValidator.requirePrimaryKey(tableMeta, schemaName, tableName);
    }

    /**
     * Broadcast detection shared by the MCE direction validators and the ALTER ADD EXTERNALIZE
     * entry: partitionInfo covers auto-mode databases, the rule manager covers DRDS-mode ones.
     */
    public static boolean isBroadcast(TableMeta tableMeta, String schemaName, String tableName) {
        if (tableMeta.getPartitionInfo() != null && tableMeta.getPartitionInfo().isBroadcastTable()) {
            return true;
        }
        OptimizerContext context = OptimizerContext.getContext(schemaName);
        return context != null && context.getRuleManager() != null
            && context.getRuleManager().isBroadCast(tableName);
    }

    /**
     * Read the authoritative {@code ext_type:} original type from the physical addr column's
     * MetaDB comment. Fail-close when the addr record is missing or carries no ext_type marker.
     */
    public static String resolveStoredOriginalType(String schemaName, String tableName, String physicalAddrName) {
        try (Connection connection = MetaDbDataSource.getInstance().getConnection()) {
            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(connection);
            List<ColumnsRecord> addrRecords =
                tableInfoManager.queryOneColumn(schemaName, tableName, physicalAddrName);
            if (addrRecords.size() != 1) {
                return null;
            }
            return ExternalizedColumnInfo.extractOriginalType(addrRecords.get(0).columnComment);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("Failed to read ext_type of addr column '%s' for table %s.%s: %s",
                    physicalAddrName, schemaName, tableName, e.getMessage()), e);
        }
    }

    /**
     * Read the user comment preserved in the addr column's {@code COMMENT_B64} segment, or null.
     */
    public static String resolveStoredUserComment(String schemaName, String tableName, String physicalAddrName) {
        try (Connection connection = MetaDbDataSource.getInstance().getConnection()) {
            TableInfoManager tableInfoManager = new TableInfoManager();
            tableInfoManager.setConnection(connection);
            List<ColumnsRecord> addrRecords =
                tableInfoManager.queryOneColumn(schemaName, tableName, physicalAddrName);
            if (addrRecords.size() != 1) {
                return null;
            }
            return ExternalizedColumnInfo.extractUserComment(addrRecords.get(0).columnComment);
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("Failed to read user comment of addr column '%s' for table %s.%s: %s",
                    physicalAddrName, schemaName, tableName, e.getMessage()), e);
        }
    }

    private static TddlRuntimeException ddlError(String schemaName, String tableName, String detail) {
        return new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
            String.format("Cannot internalize %s.%s: %s", schemaName, tableName, detail));
    }
}
