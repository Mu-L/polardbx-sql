package com.alibaba.polardbx.executor.ddl.job.task.mce;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.metadb.misc.MceColumnStateAccessor;
import com.alibaba.polardbx.gms.metadb.misc.MceColumnStateRecord;

import java.util.List;

final class MceControlStateHelper {

    private MceControlStateHelper() {
    }

    static void insertOrValidate(MceColumnStateAccessor accessor, MceColumnStateRecord expected) {
        if (expected.getState() != MceColumnStateRecord.STATE_NONE) {
            throw error("new control row must start from NONE", expected.getTableSchema(), expected.getTableName(),
                expected.getColumnName());
        }
        insertOrValidateExpectedState(accessor, expected);
    }

    /**
     * Reverse MCE creates the control row together with the READ_ADDR two-record layout: reads
     * still use the existing addr while content backfill restores plaintext. No other initial
     * state is valid for internalize.
     */
    static void insertOrValidateReadAddr(MceColumnStateAccessor accessor, MceColumnStateRecord expected) {
        if (expected.getState() != MceColumnStateRecord.STATE_READ_ADDR) {
            throw error("internalize control row must start from READ_ADDR", expected.getTableSchema(),
                expected.getTableName(), expected.getColumnName());
        }
        insertOrValidateExpectedState(accessor, expected);
    }

    private static void insertOrValidateExpectedState(MceColumnStateAccessor accessor, MceColumnStateRecord expected) {
        validateIdentity(expected, expected.getJobId(), expected.getTableSchema(), expected.getTableName(),
            expected.getColumnName(), expected.getAddrColumnName());
        List<MceColumnStateRecord> records = accessor.queryControlRowsByColumnForUpdate(
            expected.getTableSchema(), expected.getTableName(), expected.getColumnName());
        if (records.isEmpty()) {
            requireAffectedOne("insert", accessor.insert(java.util.Collections.singletonList(expected)), expected);
            return;
        }
        MceColumnStateRecord existing = requireSingle(records, expected.getTableSchema(), expected.getTableName(),
            expected.getColumnName());
        validateIdentity(existing, expected.getJobId(), expected.getTableSchema(), expected.getTableName(),
            expected.getColumnName(), expected.getAddrColumnName());
        if (existing.getState() != expected.getState()) {
            throw error("existing control state=" + existing.getState() + ", expected=" + expected.getState(),
                expected.getTableSchema(), expected.getTableName(), expected.getColumnName());
        }
    }

    static void transition(MceColumnStateAccessor accessor, long jobId, String schemaName, String tableName,
                           String columnName, String addrColumnName, int expectedState, int newState) {
        MceColumnStateRecord record = lockAndRequire(accessor, jobId, schemaName, tableName, columnName,
            addrColumnName);
        if (record.getState() == newState) {
            transitionCheckpointStatesForCompatibility(accessor, jobId, schemaName, tableName, columnName,
                addrColumnName, expectedState, newState);
            return;
        }
        if (record.getState() != expectedState) {
            throw error("unexpected control state=" + record.getState() + ", expected=" + expectedState,
                schemaName, tableName, columnName);
        }
        int affected = accessor.compareAndSetControlState(record.getId(), expectedState, newState);
        requireAffectedOne("transition " + expectedState + "->" + newState, affected, record);
        transitionCheckpointStatesForCompatibility(accessor, jobId, schemaName, tableName, columnName,
            addrColumnName, expectedState, newState);
    }

    static void requireState(MceColumnStateAccessor accessor, long jobId, String schemaName, String tableName,
                             String columnName, String addrColumnName, int expectedState) {
        MceColumnStateRecord record = lockAndRequire(accessor, jobId, schemaName, tableName, columnName,
            addrColumnName);
        if (record.getState() != expectedState) {
            throw error("unexpected control state=" + record.getState() + ", expected=" + expectedState,
                schemaName, tableName, columnName);
        }
    }

    static void delete(MceColumnStateAccessor accessor, long jobId, String schemaName, String tableName,
                       String columnName, String addrColumnName, int expectedState) {
        MceColumnStateRecord record = lockAndRequire(accessor, jobId, schemaName, tableName, columnName,
            addrColumnName);
        if (record.getState() != expectedState) {
            throw error("unexpected control state=" + record.getState() + ", expected=" + expectedState,
                schemaName, tableName, columnName);
        }
        requireAffectedOne("delete", accessor.deleteControlState(record.getId(), expectedState), record);
    }

    static void deleteIfPresent(MceColumnStateAccessor accessor, long jobId, String schemaName, String tableName,
                                String columnName, String addrColumnName, int expectedState) {
        List<MceColumnStateRecord> records =
            accessor.queryControlRowsByColumnForUpdate(schemaName, tableName, columnName);
        if (records == null || records.isEmpty()) {
            return;
        }
        MceColumnStateRecord record = requireSingle(records, schemaName, tableName, columnName);
        validateIdentity(record, jobId, schemaName, tableName, columnName, addrColumnName);
        if (record.getState() != expectedState) {
            throw error("unexpected control state=" + record.getState() + ", expected=" + expectedState,
                schemaName, tableName, columnName);
        }
        requireAffectedOne("delete", accessor.deleteControlState(record.getId(), expectedState), record);
    }

    private static MceColumnStateRecord lockAndRequire(MceColumnStateAccessor accessor, long jobId,
                                                       String schemaName, String tableName, String columnName,
                                                       String addrColumnName) {
        MceColumnStateRecord record = requireSingle(
            accessor.queryControlRowsByColumnForUpdate(schemaName, tableName, columnName),
            schemaName, tableName, columnName);
        validateIdentity(record, jobId, schemaName, tableName, columnName, addrColumnName);
        return record;
    }

    /**
     * Old CN versions load checkpoint rows as column states. Keep their shadow state aligned with
     * the control row during a rolling upgrade; new CN versions never use checkpoints for resolution.
     */
    private static void transitionCheckpointStatesForCompatibility(MceColumnStateAccessor accessor, long jobId,
                                                                   String schemaName, String tableName,
                                                                   String columnName, String addrColumnName,
                                                                   int expectedState, int newState) {
        List<MceColumnStateRecord> checkpoints = accessor.queryCheckpointsByJobAndColumnForUpdate(
            jobId, schemaName, tableName, columnName);
        if (checkpoints == null || checkpoints.isEmpty()) {
            return;
        }

        int expectedCount = 0;
        for (MceColumnStateRecord checkpoint : checkpoints) {
            if (checkpoint == null
                || checkpoint.getJobId() != jobId
                || !equalsIgnoreCase(checkpoint.getTableSchema(), schemaName)
                || !equalsIgnoreCase(checkpoint.getTableName(), tableName)
                || !equalsIgnoreCase(checkpoint.getColumnName(), columnName)
                || !equalsIgnoreCase(checkpoint.getAddrColumnName(), addrColumnName)
                || isBlank(checkpoint.getPhysicalDb())
                || isBlank(checkpoint.getPhysicalTable())
                || isBlank(checkpoint.getPartitionName())
                || checkpoint.getStatus() < MceColumnStateRecord.STATUS_INIT
                || checkpoint.getStatus() > MceColumnStateRecord.STATUS_FAILED
                || (newState > expectedState && checkpoint.getStatus() != MceColumnStateRecord.STATUS_SUCCESS)
                || (checkpoint.getState() != expectedState && checkpoint.getState() != newState)) {
                throw error("checkpoint identity/state mismatch during " + expectedState + "->" + newState,
                    schemaName, tableName, columnName);
            }
            if (checkpoint.getState() == expectedState) {
                expectedCount++;
            }
        }

        if (expectedCount > 0) {
            int affected = accessor.compareAndSetCheckpointStates(jobId, schemaName, tableName, columnName,
                expectedState, newState);
            if (affected != expectedCount) {
                throw error("checkpoint transition " + expectedState + "->" + newState + " affected "
                    + affected + " rows, expected=" + expectedCount, schemaName, tableName, columnName);
            }
        }
    }

    private static MceColumnStateRecord requireSingle(List<MceColumnStateRecord> records, String schemaName,
                                                      String tableName, String columnName) {
        int size = records == null ? 0 : records.size();
        if (size != 1) {
            throw error("expected exactly one control row, actual=" + size, schemaName, tableName, columnName);
        }
        return records.get(0);
    }

    private static void validateIdentity(MceColumnStateRecord record, long jobId, String schemaName,
                                         String tableName, String columnName, String addrColumnName) {
        if (record.getJobId() != jobId
            || !equalsIgnoreCase(record.getTableSchema(), schemaName)
            || !equalsIgnoreCase(record.getTableName(), tableName)
            || !equalsIgnoreCase(record.getColumnName(), columnName)
            || !equalsIgnoreCase(record.getAddrColumnName(), addrColumnName)
            || !isEmpty(record.getPhysicalDb())
            || !isEmpty(record.getPhysicalTable())
            || !isEmpty(record.getPartitionName())
            || record.getStatus() != MceColumnStateRecord.STATUS_RUNNING) {
            throw error("control row identity mismatch", schemaName, tableName, columnName);
        }
    }

    private static void requireAffectedOne(String action, int affected, MceColumnStateRecord record) {
        requireAffectedOne(action, affected, record.getTableSchema(), record.getTableName(), record.getColumnName());
    }

    private static void requireAffectedOne(String action, int affected, String schemaName, String tableName,
                                           String columnName) {
        if (affected != 1) {
            throw error(action + " affected " + affected + " rows", schemaName, tableName, columnName);
        }
    }

    private static boolean equalsIgnoreCase(String left, String right) {
        return left != null && right != null && left.equalsIgnoreCase(right);
    }

    private static boolean isEmpty(String value) {
        return value == null || value.isEmpty();
    }

    private static boolean isBlank(String value) {
        return value == null || value.trim().isEmpty();
    }

    private static TddlRuntimeException error(String reason, String schemaName, String tableName, String columnName) {
        return new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
            "[MCE] invalid control state for " + schemaName + "." + tableName + "." + columnName + ": " + reason);
    }
}
