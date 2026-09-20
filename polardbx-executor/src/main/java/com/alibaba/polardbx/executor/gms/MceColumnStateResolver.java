package com.alibaba.polardbx.executor.gms;

import com.alibaba.polardbx.gms.metadb.misc.MceColumnStateRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnStatus;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.metadb.table.ExternalizedColumnInfo;
import com.alibaba.polardbx.optimizer.config.table.ColumnMceState;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Resolves persisted MCE control rows against the raw columns metadata before publishing them to TableMeta.
 */
final class MceColumnStateResolver {

    private MceColumnStateResolver() {
    }

    static Result resolve(String schemaName, String tableName, List<MceColumnStateRecord> records,
                          List<ColumnsRecord> rawColumns) {
        Map<String, Candidate> candidates = findCandidates(rawColumns);
        Map<String, List<MceColumnStateRecord>> controls = new LinkedHashMap<>();
        boolean hasUnattributedInvalidControl = false;

        for (MceColumnStateRecord record : safeList(records)) {
            if (!isControl(record)) {
                continue;
            }
            if (record == null || StringUtils.isBlank(record.getColumnName())) {
                hasUnattributedInvalidControl = true;
                continue;
            }
            controls.computeIfAbsent(normalize(record.getColumnName()), key -> new ArrayList<>()).add(record);
        }

        Map<String, ColumnMceState> states = new HashMap<>();
        Map<String, String> addrColumns = new HashMap<>();
        Map<String, String> errors = new HashMap<>();

        if (hasUnattributedInvalidControl) {
            markCandidatesInvalid(candidates, errors, "INVALID_RECORD_FIELD: empty column_name");
        }

        for (Map.Entry<String, List<MceColumnStateRecord>> entry : controls.entrySet()) {
            String columnName = entry.getKey();
            List<MceColumnStateRecord> columnRecords = entry.getValue();
            if (columnRecords.size() != 1) {
                markInvalid(columnName, errors,
                    "DUPLICATE_CONTROL_ROW: count=" + columnRecords.size());
                continue;
            }

            MceColumnStateRecord record = columnRecords.get(0);
            Candidate candidate = candidates.get(columnName);
            String validationError = validateRecord(schemaName, tableName, record, candidate);
            if (validationError != null) {
                markInvalid(columnName, errors, validationError);
                continue;
            }

            ColumnMceState state;
            try {
                state = ColumnMceState.of(record.getState());
            } catch (IllegalArgumentException e) {
                markInvalid(columnName, errors,
                    "INVALID_STATE_VALUE: " + record.getState());
                continue;
            }

            String layoutError = validateLayout(state, candidate);
            if (layoutError != null) {
                markInvalid(columnName, errors, layoutError);
                continue;
            }
            states.put(columnName, state);
            addrColumns.put(columnName, record.getAddrColumnName());
        }

        for (String candidateColumn : candidates.keySet()) {
            if (!controls.containsKey(candidateColumn) && !errors.containsKey(candidateColumn)
                && candidates.get(candidateColumn).requiresControl()) {
                markInvalid(candidateColumn, errors, "MISSING_CONTROL_ROW");
            }
        }

        if (!errors.isEmpty()) {
            throw new ValidationException(schemaName, tableName, errors);
        }
        return new Result(states, addrColumns);
    }

    private static String validateRecord(String schemaName, String tableName, MceColumnStateRecord record,
                                         Candidate candidate) {
        if (StringUtils.isBlank(record.getTableSchema()) || StringUtils.isBlank(record.getTableName())
            || StringUtils.isBlank(record.getColumnName()) || StringUtils.isBlank(record.getAddrColumnName())) {
            return "INVALID_RECORD_FIELD: blank identity field";
        }
        if (!record.getTableSchema().equalsIgnoreCase(schemaName)
            || !record.getTableName().equalsIgnoreCase(tableName)) {
            return "INVALID_RECORD_FIELD: schema/table mismatch";
        }
        String expectedAddr = ExternalizedColumnInfo.toAddrColumnName(record.getColumnName());
        if (!expectedAddr.equalsIgnoreCase(record.getAddrColumnName())) {
            return "ADDR_NAME_MISMATCH: expected=" + expectedAddr + ", actual=" + record.getAddrColumnName();
        }
        if (record.getStatus() != MceColumnStateRecord.STATUS_RUNNING) {
            return "INVALID_RECORD_FIELD: status=" + record.getStatus();
        }
        if (candidate == null) {
            return "STATE_COLUMN_LAYOUT_MISMATCH: content/addr pair not found";
        }
        return null;
    }

    private static String validateLayout(ColumnMceState state, Candidate candidate) {
        ColumnsRecord content = candidate.content;
        ColumnsRecord addr = candidate.addr;
        if (content.status != ColumnStatus.PUBLIC.getValue()) {
            return "STATE_COLUMN_LAYOUT_MISMATCH: content status=" + content.status;
        }
        if (!equalsIgnoreCase(addr.columnMappingName, content.columnName)) {
            return "ADDR_MAPPING_MISMATCH: addr mapping=" + addr.columnMappingName;
        }
        if (!"varchar".equalsIgnoreCase(addr.dataType)
            || addr.characterMaximumLength != ExternalizedColumnInfo.ADDR_VARCHAR_LENGTH) {
            return "STATE_COLUMN_LAYOUT_MISMATCH: invalid addr definition";
        }
        String originalType = ExternalizedColumnInfo.extractOriginalType(addr.columnComment);
        if (!ExternalizedColumnInfo.isSupportedType(originalType)) {
            return "STATE_COLUMN_LAYOUT_MISMATCH: unsupported original type=" + originalType;
        }
        if (!equalsIgnoreCase(originalType, content.dataType)) {
            return "STATE_COLUMN_LAYOUT_MISMATCH: original type=" + originalType
                + ", content type=" + content.dataType;
        }

        switch (state) {
        case NONE:
            if (content.isExternalizedColumn() || addr.isExternalizedColumn()
                || StringUtils.isNotEmpty(content.columnMappingName)
                || addr.status != ColumnStatus.ABSENT.getValue()) {
                return "STATE_COLUMN_LAYOUT_MISMATCH: NONE";
            }
            break;
        case DUAL_WRITE:
            if (content.isExternalizedColumn() || addr.isExternalizedColumn()
                || StringUtils.isNotEmpty(content.columnMappingName)
                || addr.status != ColumnStatus.WRITE_ONLY.getValue()) {
                return "STATE_COLUMN_LAYOUT_MISMATCH: DUAL_WRITE";
            }
            break;
        case READ_ADDR:
        case EXTERNALIZED:
            if (!content.isExternalizedColumn() || addr.isExternalizedColumn()
                || addr.status != ColumnStatus.WRITE_ONLY.getValue()
                || !equalsIgnoreCase(content.columnMappingName, addr.columnName)) {
                return "STATE_COLUMN_LAYOUT_MISMATCH: " + state;
            }
            break;
        default:
            return "INVALID_STATE_VALUE: " + state;
        }
        return null;
    }

    private static Map<String, Candidate> findCandidates(List<ColumnsRecord> rawColumns) {
        Map<String, ColumnsRecord> columns = new HashMap<>();
        for (ColumnsRecord record : safeList(rawColumns)) {
            if (record != null && StringUtils.isNotBlank(record.columnName)) {
                columns.put(normalize(record.columnName), record);
            }
        }

        Map<String, Candidate> candidates = new HashMap<>();
        for (ColumnsRecord addr : columns.values()) {
            if (!ExternalizedColumnInfo.isAddrColumn(addr.columnName)) {
                continue;
            }
            String contentName = normalize(ExternalizedColumnInfo.toLogicalColumnName(addr.columnName));
            ColumnsRecord content = columns.get(contentName);
            if (content != null
                && equalsIgnoreCase(addr.columnMappingName, content.columnName)
                && addr.status != ColumnStatus.PUBLIC.getValue()) {
                candidates.put(contentName, new Candidate(content, addr));
            }
        }
        return candidates;
    }

    private static boolean isControl(MceColumnStateRecord record) {
        return record == null || (StringUtils.isEmpty(record.getPhysicalDb())
            && StringUtils.isEmpty(record.getPhysicalTable())
            && StringUtils.isEmpty(record.getPartitionName()));
    }

    private static void markCandidatesInvalid(Map<String, Candidate> candidates,
                                              Map<String, String> errors, String reason) {
        for (String columnName : candidates.keySet()) {
            markInvalid(columnName, errors, reason);
        }
    }

    private static void markInvalid(String columnName, Map<String, String> errors, String reason) {
        errors.put(columnName, abbreviate(reason));
    }

    private static String abbreviate(String reason) {
        return StringUtils.abbreviate(reason == null ? "UNKNOWN" : reason, 512);
    }

    private static boolean equalsIgnoreCase(String left, String right) {
        return left != null && right != null && left.equalsIgnoreCase(right);
    }

    private static String normalize(String value) {
        return value.toLowerCase(Locale.ROOT);
    }

    private static <T> List<T> safeList(List<T> values) {
        return values == null ? Collections.emptyList() : values;
    }

    static final class Result {
        private final Map<String, ColumnMceState> states;
        private final Map<String, String> addrColumns;

        private Result(Map<String, ColumnMceState> states, Map<String, String> addrColumns) {
            this.states = Collections.unmodifiableMap(new HashMap<>(states));
            this.addrColumns = Collections.unmodifiableMap(new HashMap<>(addrColumns));
        }

        Map<String, ColumnMceState> getStates() {
            return states;
        }

        Map<String, String> getAddrColumns() {
            return addrColumns;
        }

    }

    static final class ValidationException extends RuntimeException {
        private ValidationException(String schemaName, String tableName, Map<String, String> errors) {
            super("Invalid MCE column state metadata for " + schemaName + "." + tableName + ": " + errors);
        }
    }

    private static final class Candidate {
        private final ColumnsRecord content;
        private final ColumnsRecord addr;

        private Candidate(ColumnsRecord content, ColumnsRecord addr) {
            this.content = content;
            this.addr = addr;
        }

        private boolean requiresControl() {
            return content.isExternalizedColumn()
                || addr.isExternalizedColumn()
                || addr.status != ColumnStatus.ABSENT.getValue();
        }
    }
}
