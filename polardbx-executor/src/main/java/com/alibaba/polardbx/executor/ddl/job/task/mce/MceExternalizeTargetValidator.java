package com.alibaba.polardbx.executor.ddl.job.task.mce;

import com.alibaba.polardbx.common.ddl.foreignkey.ForeignKeyData;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.metadb.limit.Limits;
import com.alibaba.polardbx.gms.metadb.table.ExternalizedColumnInfo;
import com.alibaba.polardbx.optimizer.config.table.ColumnMceState;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GeneratedColumnUtil;
import com.alibaba.polardbx.optimizer.config.table.IndexColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.rel.dml.ExternalizedDmlRewriter;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * Validates an MCE target before any physical or metadata mutation.
 */
public final class MceExternalizeTargetValidator {

    private MceExternalizeTargetValidator() {
    }

    public static void validate(TableMeta tableMeta, String schemaName, String tableName,
                                String columnName, String originalType) {
        if (tableMeta == null) {
            throw ddlError(schemaName, tableName, "table metadata is unavailable");
        }
        if (tableMeta.withCci()) {
            throw ddlError(schemaName, tableName, "table has an existing CCI");
        }
        // Broadcast tables cannot express the single-primary-route staging protocol; reject them
        // here like the CREATE TABLE and internalize entries do.
        if (MceInternalizeTargetValidator.isBroadcast(tableMeta, schemaName, tableName)) {
            throw ddlError(schemaName, tableName, "broadcast tables are not supported");
        }
        ExternalizedDmlRewriter.validateNoAppendRenameConflict(tableMeta);

        ColumnMeta target = tableMeta.getColumnIgnoreCase(columnName);
        if (target == null) {
            throw ddlError(schemaName, tableName,
                String.format("target column '%s' does not exist", columnName));
        }
        if (!ExternalizedColumnInfo.isSupportedType(originalType)) {
            throw ddlError(schemaName, tableName,
                String.format("target column '%s' has unsupported EXTERNALIZE type '%s'", columnName, originalType));
        }
        if (target.isLogicalGeneratedColumn() || target.isGeneratedColumn()) {
            throw ddlError(schemaName, tableName,
                String.format("target column '%s' is a generated column", columnName));
        }
        validateSourceDefaultSemantics(target, schemaName, tableName, columnName);

        String addressColumnName = ExternalizedColumnInfo.toAddrColumnName(columnName);
        if (addressColumnName.length() > Limits.MAX_LENGTH_OF_COLUMN_NAME) {
            throw ddlError(schemaName, tableName,
                String.format("derived address column name '%s' exceeds max length %d",
                    addressColumnName, Limits.MAX_LENGTH_OF_COLUMN_NAME));
        }

        ColumnMceState state = tableMeta.getColumnMceState(columnName);
        if (state != ColumnMceState.NONE || target.isExternalizedColumn()
            || tableMeta.getMceAddrColumnName(columnName) != null) {
            throw ddlError(schemaName, tableName,
                String.format("target column '%s' is already externalized or in MCE lifecycle", columnName));
        }

        validateNotForeignKeyColumn(tableMeta, schemaName, tableName, columnName);
        validateNotIndexed(tableMeta, schemaName, tableName, columnName);
        validateNoGeneratedColumnDependency(tableMeta, schemaName, tableName, columnName);
        McePrimaryKeyValidator.requirePrimaryKey(tableMeta, schemaName, tableName);
    }

    private static void validateNotForeignKeyColumn(TableMeta tableMeta, String schemaName, String tableName,
                                                    String columnName) {
        Map<String, ForeignKeyData> foreignKeys = tableMeta.getForeignKeys();
        if (foreignKeys != null) {
            for (ForeignKeyData foreignKey : foreignKeys.values()) {
                if (foreignKey != null && containsIgnoreCase(foreignKey.columns, columnName)) {
                    throw ddlError(schemaName, tableName,
                        String.format("target column '%s' is foreign key child column in constraint '%s'",
                            columnName, foreignKey.constraint));
                }
            }
        }

        Map<String, ForeignKeyData> referencedForeignKeys = tableMeta.getReferencedForeignKeys();
        if (referencedForeignKeys != null) {
            for (ForeignKeyData foreignKey : referencedForeignKeys.values()) {
                if (foreignKey != null && containsIgnoreCase(foreignKey.refColumns, columnName)) {
                    throw ddlError(schemaName, tableName,
                        String.format("target column '%s' is foreign key parent column in constraint '%s'",
                            columnName, foreignKey.constraint));
                }
            }
        }
    }

    private static void validateNoGeneratedColumnDependency(TableMeta tableMeta, String schemaName, String tableName,
                                                            String columnName) {
        Map<String, List<String>> logicalDependencies =
            GeneratedColumnUtil.getAllLogicalReferencedColumnByRef(tableMeta);
        Map<String, List<String>> physicalDependencies =
            GeneratedColumnUtil.getAllReferencedColumnByRef(tableMeta);
        if (logicalDependencies == null || physicalDependencies == null) {
            throw ddlError(schemaName, tableName, "generated-column dependency metadata is unavailable");
        }

        Map<String, List<String>> generatedColumnsByReference = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        mergeGeneratedColumnDependencies(generatedColumnsByReference, logicalDependencies);
        mergeGeneratedColumnDependencies(generatedColumnsByReference, physicalDependencies);

        Set<String> dependentGeneratedColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        Set<String> visitedReferences = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        Deque<String> references = new ArrayDeque<>();
        references.add(columnName);
        while (!references.isEmpty()) {
            String reference = references.removeFirst();
            if (!visitedReferences.add(reference)) {
                continue;
            }
            List<String> directDependents = generatedColumnsByReference.get(reference);
            if (directDependents == null) {
                continue;
            }
            for (String dependent : directDependents) {
                if (dependent != null && dependentGeneratedColumns.add(dependent)) {
                    references.addLast(dependent);
                }
            }
        }

        if (!dependentGeneratedColumns.isEmpty()) {
            throw ddlError(schemaName, tableName,
                String.format("target column '%s' is referenced by generated column(s) %s",
                    columnName, dependentGeneratedColumns));
        }
    }

    private static void mergeGeneratedColumnDependencies(Map<String, List<String>> target,
                                                         Map<String, List<String>> source) {
        source.forEach((reference, generatedColumns) -> {
            if (reference == null || generatedColumns == null) {
                return;
            }
            target.computeIfAbsent(reference, ignored -> new ArrayList<>()).addAll(generatedColumns);
        });
    }

    private static void validateSourceDefaultSemantics(ColumnMeta target, String schemaName, String tableName,
                                                       String columnName) {
        if (target.getField() == null) {
            throw ddlError(schemaName, tableName,
                String.format("target column '%s' field metadata is unavailable", columnName));
        }
        if (!target.getField().isNullable() || target.getField().getDefault() != null || target.isDefaultExpr()) {
            throw ddlError(schemaName, tableName,
                String.format("target column '%s' requires a nullable source column without a declared default; "
                    + "current MCE cannot preserve NOT NULL/default coercion for content and addr", columnName));
        }
    }

    private static void validateNotIndexed(TableMeta tableMeta, String schemaName, String tableName,
                                           String columnName) {
        List<IndexMeta> indexes = tableMeta.getIndexes();
        if (indexes == null) {
            return;
        }
        Map<String, List<String>> generatedColumnsByReference =
            GeneratedColumnUtil.getAllReferencedColumnByRef(tableMeta);
        if (generatedColumnsByReference == null) {
            throw ddlError(schemaName, tableName, "generated-column dependency metadata is unavailable");
        }
        List<String> dependentGeneratedColumns = generatedColumnsByReference.get(columnName);
        for (IndexMeta index : indexes) {
            if (index == null) {
                continue;
            }
            if (index.isFunctionIndex()) {
                throw ddlError(schemaName, tableName,
                    String.format("table has local function index '%s'; its target-column dependency "
                        + "cannot be proven safe for MCE", index.getPhysicalIndexName()));
            }
            if (index.getKeyColumnsExt() == null) {
                continue;
            }
            for (IndexColumnMeta keyColumn : index.getKeyColumnsExt()) {
                if (keyColumn == null || !keyColumn.hasColumn()) {
                    continue;
                }
                if (columnName.equalsIgnoreCase(keyColumn.getName())) {
                    throw ddlError(schemaName, tableName,
                        String.format("target column '%s' is an indexed column in local index '%s'",
                            columnName, index.getPhysicalIndexName()));
                }
                if (containsIgnoreCase(dependentGeneratedColumns, keyColumn.getName())) {
                    throw ddlError(schemaName, tableName,
                        String.format("target column '%s' is referenced by indexed generated column '%s' "
                                + "in local index '%s'",
                            columnName, keyColumn.getName(), index.getPhysicalIndexName()));
                }
            }
        }
    }

    private static boolean containsIgnoreCase(List<String> values, String value) {
        if (values == null || value == null) {
            return false;
        }
        return values.stream().anyMatch(item -> value.equalsIgnoreCase(item));
    }

    private static TddlRuntimeException ddlError(String schemaName, String tableName, String detail) {
        return new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
            String.format("Cannot externalize %s.%s: %s", schemaName, tableName, detail));
    }
}
