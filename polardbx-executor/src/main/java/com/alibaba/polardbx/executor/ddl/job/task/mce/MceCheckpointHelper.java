package com.alibaba.polardbx.executor.ddl.job.task.mce;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.ddl.mce.MceColumnStateDelegate;
import com.alibaba.polardbx.gms.metadb.misc.MceColumnStateAccessor;
import com.alibaba.polardbx.gms.metadb.misc.MceColumnStateRecord;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

final class MceCheckpointHelper {

    private MceCheckpointHelper() {
    }

    static MceColumnStateRecord loadOrCreate(long jobId, long taskId, String schemaName, String tableName,
                                             String columnName, String addrColumnName, String physicalDb,
                                             String physicalTable, String partitionName, int state, String extra) {
        return new MceColumnStateDelegate<MceColumnStateRecord>(new MceColumnStateAccessor()) {
            @Override
            protected MceColumnStateRecord invoke() {
                List<MceColumnStateRecord> records = mceColumnStateAccessor.queryByCheckpointKey(jobId, taskId,
                    columnName, partitionName);
                if (!records.isEmpty()) {
                    if (records.size() != 1) {
                        throw error(schemaName, tableName, columnName,
                            "duplicate checkpoint rows: " + records.size());
                    }
                    MceColumnStateRecord existing = records.get(0);
                    validateCheckpoint(existing, jobId, taskId, schemaName, tableName, columnName, addrColumnName,
                        physicalDb, physicalTable, partitionName, state, extra);
                    return existing;
                }
                MceColumnStateRecord record = new MceColumnStateRecord();
                record.setJobId(jobId);
                record.setTaskId(taskId);
                record.setTableSchema(schemaName);
                record.setTableName(tableName);
                record.setColumnName(columnName);
                record.setAddrColumnName(addrColumnName);
                record.setState(state);
                record.setStatus(MceColumnStateRecord.STATUS_INIT);
                record.setPhysicalDb(physicalDb == null ? "" : physicalDb);
                record.setPhysicalTable(physicalTable == null ? "" : physicalTable);
                record.setPartitionName(partitionName == null ? "" : partitionName);
                record.setExtra(extra);
                requireAffectedOne(schemaName, tableName, columnName, "insert checkpoint",
                    mceColumnStateAccessor.insert(Collections.singletonList(record)));
                return record;
            }
        }.execute();
    }

    static void initMaxPk(MceColumnStateRecord record, String maxPk, String pkType, String extra) {
        new MceColumnStateDelegate<Void>(new MceColumnStateAccessor()) {
            @Override
            protected Void invoke() {
                int affected = mceColumnStateAccessor.updateCheckpointInit(record.getJobId(), record.getTaskId(),
                    record.getTableSchema(), record.getTableName(), record.getColumnName(), record.getPhysicalDb(),
                    record.getPhysicalTable(), record.getPartitionName(), maxPk, pkType,
                    MceColumnStateRecord.STATUS_RUNNING, extra);
                requireAffectedOne(record.getTableSchema(), record.getTableName(), record.getColumnName(),
                    "initialize checkpoint", affected);
                return null;
            }
        }.execute();
        record.setMaxPk(maxPk);
        record.setPkType(pkType);
        record.setStatus(MceColumnStateRecord.STATUS_RUNNING);
        record.setExtra(extra);
    }

    static void saveProgress(MceColumnStateRecord record, String lastPk, long processedRows) {
        new MceColumnStateDelegate<Void>(new MceColumnStateAccessor()) {
            @Override
            protected Void invoke() {
                int affected = mceColumnStateAccessor.updateCheckpoint(record.getJobId(), record.getTaskId(),
                    record.getTableSchema(), record.getTableName(), record.getColumnName(), record.getPhysicalDb(),
                    record.getPhysicalTable(), record.getPartitionName(), lastPk, processedRows,
                    MceColumnStateRecord.STATUS_RUNNING);
                requireAffectedOne(record.getTableSchema(), record.getTableName(), record.getColumnName(),
                    "save checkpoint", affected);
                return null;
            }
        }.execute();
        record.setLastPk(lastPk);
        record.setProcessedRows(processedRows);
        record.setStatus(MceColumnStateRecord.STATUS_RUNNING);
    }

    static void markSuccess(MceColumnStateRecord record) {
        new MceColumnStateDelegate<Void>(new MceColumnStateAccessor()) {
            @Override
            protected Void invoke() {
                int affected = mceColumnStateAccessor.updateCheckpointStatus(record.getJobId(), record.getTaskId(),
                    record.getTableSchema(), record.getTableName(), record.getColumnName(), record.getPhysicalDb(),
                    record.getPhysicalTable(), record.getPartitionName(), MceColumnStateRecord.STATUS_SUCCESS, null);
                requireAffectedOne(record.getTableSchema(), record.getTableName(), record.getColumnName(),
                    "complete checkpoint", affected);
                return null;
            }
        }.execute();
        record.setStatus(MceColumnStateRecord.STATUS_SUCCESS);
    }

    private static void validateCheckpoint(MceColumnStateRecord record, long jobId, long taskId, String schemaName,
                                           String tableName, String columnName, String addrColumnName,
                                           String physicalDb, String physicalTable, String partitionName,
                                           int state, String extra) {
        if (record.getJobId() != jobId || record.getTaskId() != taskId
            || !equalsIgnoreCase(record.getTableSchema(), schemaName)
            || !equalsIgnoreCase(record.getTableName(), tableName)
            || !equalsIgnoreCase(record.getColumnName(), columnName)
            || !equalsIgnoreCase(record.getAddrColumnName(), addrColumnName)
            || !Objects.equals(emptyIfNull(record.getPhysicalDb()), emptyIfNull(physicalDb))
            || !Objects.equals(emptyIfNull(record.getPhysicalTable()), emptyIfNull(physicalTable))
            || !Objects.equals(emptyIfNull(record.getPartitionName()), emptyIfNull(partitionName))
            || record.getState() != state
            || record.getStatus() < MceColumnStateRecord.STATUS_INIT
            || record.getStatus() > MceColumnStateRecord.STATUS_SUCCESS
            || !Objects.equals(record.getExtra(), extra)) {
            throw error(schemaName, tableName, columnName, "checkpoint identity/state mismatch");
        }
    }

    private static void requireAffectedOne(String schemaName, String tableName, String columnName,
                                           String action, int affected) {
        if (affected != 1) {
            throw error(schemaName, tableName, columnName, action + " affected " + affected + " rows");
        }
    }

    private static String emptyIfNull(String value) {
        return value == null ? "" : value;
    }

    private static boolean equalsIgnoreCase(String left, String right) {
        return left != null && right != null && left.equalsIgnoreCase(right);
    }

    private static TddlRuntimeException error(String schemaName, String tableName, String columnName, String reason) {
        return new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
            "[MCE] invalid checkpoint for " + schemaName + "." + tableName + "." + columnName + ": " + reason);
    }
}
