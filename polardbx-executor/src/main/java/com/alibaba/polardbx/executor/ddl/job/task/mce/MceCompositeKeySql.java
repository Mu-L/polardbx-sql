package com.alibaba.polardbx.executor.ddl.job.task.mce;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.executor.ddl.omc.OmcCompositeKeyRangeUtils;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Shared typed composite-key SQL primitives for MCE backfill and verification.
 */
final class MceCompositeKeySql {

    private static final int MAX_PREPARED_STATEMENT_PARAMETERS = 65535;

    private MceCompositeKeySql() {
    }

    static PreparedPredicate buildRangePredicate(List<String> primaryKeys, List<ParameterContext> lowerExclusive,
                                                 List<ParameterContext> upperInclusive) {
        validatePrimaryKeys(primaryKeys);
        if (lowerExclusive == null && upperInclusive == null) {
            throw new IllegalArgumentException("At least one composite-key range bound is required");
        }
        validateTupleArity(primaryKeys, lowerExclusive, "lower bound");
        validateTupleArity(primaryKeys, upperInclusive, "upper bound");

        List<ParameterContext> bounds = new ArrayList<>(primaryKeys.size() * 2);
        List<String> predicates = new ArrayList<>(2);
        if (lowerExclusive != null) {
            predicates.add("(" + OmcCompositeKeyRangeUtils.buildQuotedCondition(
                primaryKeys, SqlStdOperatorTable.GREATER_THAN) + ")");
            bounds.addAll(lowerExclusive);
        }
        if (upperInclusive != null) {
            predicates.add("(" + OmcCompositeKeyRangeUtils.buildQuotedCondition(
                primaryKeys, SqlStdOperatorTable.LESS_THAN_OR_EQUAL) + ")");
            bounds.addAll(upperInclusive);
        }
        Map<Integer, ParameterContext> params = OmcCompositeKeyRangeUtils.buildParameterContexts(
            bounds, lowerExclusive != null, upperInclusive != null);
        return new PreparedPredicate(String.join(" AND ", predicates), params);
    }

    static PreparedSql buildCasUpdate(String physicalDb, String physicalTable, List<String> primaryKeys,
                                      String addrColumn, List<String> autoUpdateColumns, List<CasRow> rows) {
        validatePrimaryKeys(primaryKeys);
        requireIdentifier(addrColumn, "addr column");
        if (rows == null || rows.isEmpty()) {
            throw new IllegalArgumentException("CAS rows must not be empty");
        }
        int maxRows = limitCasBatchRows(Integer.MAX_VALUE, primaryKeys.size());
        if (rows.size() > maxRows) {
            throw new IllegalArgumentException("CAS row count " + rows.size()
                + " exceeds prepared-statement parameter limit for primary-key arity " + primaryKeys.size());
        }
        for (CasRow row : rows) {
            if (row == null) {
                throw new IllegalArgumentException("CAS row must not be null");
            }
            if (row.primaryKey == null) {
                throw new IllegalArgumentException("CAS primary key must not be null");
            }
            validateTupleArity(primaryKeys, row.primaryKey, "CAS primary key");
            validateTupleValues(row.primaryKey, "CAS primary key");
        }

        StringBuilder sql = new StringBuilder();
        Map<Integer, ParameterContext> params = new LinkedHashMap<>();
        int nextIndex = 1;
        sql.append("UPDATE ").append(qualifiedTable(physicalDb, physicalTable))
            .append(" SET ").append(quote(addrColumn)).append(" = CASE");
        for (CasRow row : rows) {
            sql.append(" WHEN ").append(exactKeyPredicate(primaryKeys)).append(" THEN ?");
            nextIndex = addTupleParams(params, nextIndex, row.primaryKey);
            params.put(nextIndex, row.addr == null
                ? new ParameterContext(ParameterMethod.setNull1, new Object[] {nextIndex, null})
                : new ParameterContext(ParameterMethod.setString, new Object[] {nextIndex, row.addr}));
            nextIndex++;
        }
        sql.append(" ELSE ").append(quote(addrColumn)).append(" END");
        appendSelfAssignments(sql, autoUpdateColumns);
        sql.append(" WHERE ").append(quote(addrColumn)).append(" = '' AND (");
        for (int rowIndex = 0; rowIndex < rows.size(); rowIndex++) {
            if (rowIndex > 0) {
                sql.append(" OR ");
            }
            sql.append(exactKeyPredicate(primaryKeys));
            nextIndex = addTupleParams(params, nextIndex, rows.get(rowIndex).primaryKey);
        }
        sql.append(")");
        return new PreparedSql(sql.toString(), params);
    }

    static void appendSelfAssignments(StringBuilder sql, List<String> columns) {
        if (columns == null) {
            throw new IllegalArgumentException("Self-assignment columns must not be null");
        }
        for (String column : columns) {
            sql.append(", ").append(quote(column)).append(" = ").append(quote(column));
        }
    }

    static int limitCasBatchRows(int requestedRows, int primaryKeyArity) {
        if (requestedRows <= 0) {
            throw new IllegalArgumentException("MCE batch row count must be positive");
        }
        if (primaryKeyArity <= 0) {
            throw new IllegalArgumentException("MCE primary-key arity must be positive");
        }
        long paramsPerRow = 2L * primaryKeyArity + 1L;
        int maxRows = (int) (MAX_PREPARED_STATEMENT_PARAMETERS / paramsPerRow);
        if (maxRows <= 0) {
            throw new IllegalArgumentException("MCE primary-key arity " + primaryKeyArity
                + " exceeds prepared-statement parameter limit");
        }
        return Math.min(requestedRows, maxRows);
    }

    static List<ParameterContext> extractTuple(Map<Integer, ParameterContext> row, int firstColumn,
                                               int primaryKeyArity, String context) {
        if (row == null || firstColumn <= 0 || primaryKeyArity <= 0) {
            throw new IllegalArgumentException(context + " has invalid tuple coordinates");
        }
        List<ParameterContext> tuple = new ArrayList<>(primaryKeyArity);
        for (int keyIndex = 0; keyIndex < primaryKeyArity; keyIndex++) {
            ParameterContext parameter = row.get(firstColumn + keyIndex);
            if (!isValidNonNullParameter(parameter)) {
                throw new IllegalArgumentException(context + " is missing non-null primary-key component "
                    + keyIndex);
            }
            tuple.add(withIndex(parameter, keyIndex + 1));
        }
        return tuple;
    }

    static Object value(ParameterContext parameter) {
        return parameter == null || parameter.getArgs() == null || parameter.getArgs().length < 2
            ? null : parameter.getArgs()[1];
    }

    static String qualifiedTable(String physicalDb, String physicalTable) {
        return quote(requireIdentifier(physicalDb, "physical database"))
            + "." + quote(requireIdentifier(physicalTable, "physical table"));
    }

    static String quote(String identifier) {
        return "`" + requireIdentifier(identifier, "identifier").replace("`", "``") + "`";
    }

    static String quotedColumns(List<String> columns) {
        validatePrimaryKeys(columns);
        List<String> quoted = new ArrayList<>(columns.size());
        for (String column : columns) {
            quoted.add(quote(column));
        }
        return String.join(", ", quoted);
    }

    static String orderBy(List<String> primaryKeys, boolean descending) {
        validatePrimaryKeys(primaryKeys);
        List<String> expressions = new ArrayList<>(primaryKeys.size());
        for (String primaryKey : primaryKeys) {
            expressions.add(quote(primaryKey) + (descending ? " DESC" : " ASC"));
        }
        return String.join(", ", expressions);
    }

    private static String exactKeyPredicate(List<String> primaryKeys) {
        List<String> predicates = new ArrayList<>(primaryKeys.size());
        for (String primaryKey : primaryKeys) {
            predicates.add(quote(primaryKey) + " <=> ?");
        }
        return "(" + String.join(" AND ", predicates) + ")";
    }

    private static int addTupleParams(Map<Integer, ParameterContext> target, int nextIndex,
                                      List<ParameterContext> tuple) {
        for (ParameterContext parameter : tuple) {
            target.put(nextIndex, withIndex(parameter, nextIndex));
            nextIndex++;
        }
        return nextIndex;
    }

    private static ParameterContext withIndex(ParameterContext parameter, int index) {
        Object[] args = parameter.getArgs().clone();
        args[0] = index;
        return new ParameterContext(parameter.getParameterMethod(), args);
    }

    private static void validatePrimaryKeys(List<String> primaryKeys) {
        if (primaryKeys == null || primaryKeys.isEmpty()) {
            throw new IllegalArgumentException("MCE primary-key columns must not be empty");
        }
        for (String primaryKey : primaryKeys) {
            requireIdentifier(primaryKey, "primary-key column");
        }
    }

    private static void validateTupleArity(List<String> primaryKeys, List<ParameterContext> tuple, String context) {
        if (tuple != null && tuple.size() != primaryKeys.size()) {
            throw new IllegalArgumentException(context + " arity " + tuple.size()
                + " does not match primary-key arity " + primaryKeys.size());
        }
    }

    private static void validateTupleValues(List<ParameterContext> tuple, String context) {
        for (int keyIndex = 0; keyIndex < tuple.size(); keyIndex++) {
            if (!isValidNonNullParameter(tuple.get(keyIndex))) {
                throw new IllegalArgumentException(context + " has invalid component " + keyIndex);
            }
        }
    }

    private static boolean isValidNonNullParameter(ParameterContext parameter) {
        return parameter != null && parameter.getParameterMethod() != null && parameter.getArgs() != null
            && parameter.getArgs().length >= 2 && parameter.getArgs()[1] != null;
    }

    private static String requireIdentifier(String identifier, String description) {
        if (identifier == null || identifier.isEmpty()) {
            throw new IllegalArgumentException(description + " must not be empty");
        }
        return identifier;
    }

    static class PreparedSql {
        private final String sql;
        private final Map<Integer, ParameterContext> params;

        PreparedSql(String sql, Map<Integer, ParameterContext> params) {
            this.sql = sql;
            this.params = Collections.unmodifiableMap(new LinkedHashMap<>(params));
        }

        String getSql() {
            return sql;
        }

        Map<Integer, ParameterContext> getParams() {
            return params;
        }
    }

    static final class PreparedPredicate extends PreparedSql {
        PreparedPredicate(String sql, Map<Integer, ParameterContext> params) {
            super(sql, params);
        }
    }

    static final class CasRow {
        private final List<ParameterContext> primaryKey;
        private final String addr;

        CasRow(List<ParameterContext> primaryKey, String addr) {
            this.primaryKey = primaryKey == null ? null : Collections.unmodifiableList(new ArrayList<>(primaryKey));
            this.addr = addr;
        }
    }
}
