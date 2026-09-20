package com.alibaba.polardbx.executor.ddl.omc;

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Builds DNF range conditions and their parameters for lexicographically ordered composite keys.
 */
public final class OmcCompositeKeyRangeUtils {

    private OmcCompositeKeyRangeUtils() {
    }

    /**
     * Builds a DNF range condition from SQL column expressions supplied by the caller.
     *
     * <p>This method deliberately does not quote or otherwise rewrite the expressions. It preserves the historical
     * raw-expression behavior for callers that already provide SQL.
     */
    public static String buildRawCondition(List<String> columnExpressions, SqlOperator operator) {
        validateColumns(columnExpressions);

        final SqlOperator nonFinalOperator;
        if (SqlStdOperatorTable.LESS_THAN == operator || SqlStdOperatorTable.LESS_THAN_OR_EQUAL == operator) {
            nonFinalOperator = SqlStdOperatorTable.LESS_THAN;
        } else if (SqlStdOperatorTable.GREATER_THAN == operator
            || SqlStdOperatorTable.GREATER_THAN_OR_EQUAL == operator) {
            nonFinalOperator = SqlStdOperatorTable.GREATER_THAN;
        } else {
            throw new TddlNestableRuntimeException(
                MessageFormat.format("buildCondition not support SqlOperator {0}", String.valueOf(operator)));
        }

        StringBuilder condition = new StringBuilder();
        for (int orIndex = 0; orIndex < columnExpressions.size(); ++orIndex) {
            if (orIndex > 0) {
                condition.append(" OR ");
            }
            condition.append("(");
            for (int andIndex = 0; andIndex <= orIndex; ++andIndex) {
                if (andIndex > 0) {
                    condition.append(" AND ");
                }
                SqlOperator currentOperator = andIndex < orIndex ? SqlStdOperatorTable.EQUALS
                    : (orIndex != columnExpressions.size() - 1 ? nonFinalOperator : operator);
                condition.append(columnExpressions.get(andIndex))
                    .append(" ")
                    .append(currentOperator.getName())
                    .append(" ?");
            }
            condition.append(")");
        }
        return condition.toString();
    }

    /**
     * Builds a DNF range condition from unquoted column names.
     */
    public static String buildQuotedCondition(List<String> columnNames, SqlOperator operator) {
        validateColumns(columnNames);
        return buildRawCondition(columnNames.stream()
            .map(SqlIdentifier::surroundWithBacktick)
            .collect(Collectors.toList()), operator);
    }

    /**
     * Expands one or two composite-key tuples into the triangular parameter order required by the DNF condition.
     * When both bounds are present, {@code params} must contain the lower tuple followed by the upper tuple.
     */
    public static Map<Integer, ParameterContext> buildParameterContexts(List<ParameterContext> params,
                                                                        boolean withLowerBound,
                                                                        boolean withUpperBound) {
        if (!withLowerBound && !withUpperBound) {
            throw new IllegalArgumentException("At least one composite-key bound is required");
        }
        if (params == null || params.isEmpty()) {
            throw new IllegalArgumentException("Composite-key bound parameters must not be empty");
        }

        int boundCount = (withLowerBound ? 1 : 0) + (withUpperBound ? 1 : 0);
        if (params.size() % boundCount != 0) {
            throw new IllegalArgumentException("Composite-key bound arity mismatch: parameter count " + params.size()
                + " is not divisible by bound count " + boundCount);
        }
        int keyArity = params.size() / boundCount;
        if (keyArity <= 0) {
            throw new IllegalArgumentException("Composite-key bound arity must be positive");
        }
        for (ParameterContext parameterContext : params) {
            if (parameterContext == null || parameterContext.getArgs() == null
                || parameterContext.getArgs().length < 2) {
                throw new IllegalArgumentException("Composite-key bound contains an invalid parameter");
            }
        }

        Map<Integer, ParameterContext> planParams = new HashMap<>();
        int nextParamIndex = 1;
        if (withLowerBound) {
            nextParamIndex = expandTuple(params, 0, keyArity, nextParamIndex, planParams);
        }
        if (withUpperBound) {
            int tupleOffset = withLowerBound ? keyArity : 0;
            expandTuple(params, tupleOffset, keyArity, nextParamIndex, planParams);
        }
        return planParams;
    }

    private static int expandTuple(List<ParameterContext> params, int tupleOffset, int keyArity, int nextParamIndex,
                                   Map<Integer, ParameterContext> planParams) {
        for (int prefixEnd = 0; prefixEnd < keyArity; ++prefixEnd) {
            for (int keyIndex = 0; keyIndex <= prefixEnd; ++keyIndex) {
                ParameterContext source = params.get(tupleOffset + keyIndex);
                planParams.put(nextParamIndex,
                    new ParameterContext(source.getParameterMethod(),
                        new Object[] {nextParamIndex, source.getArgs()[1]}));
                nextParamIndex++;
            }
        }
        return nextParamIndex;
    }

    private static void validateColumns(List<String> columns) {
        if (columns == null || columns.isEmpty()) {
            throw new IllegalArgumentException("Composite-key column list must not be empty");
        }
        if (columns.stream().anyMatch(column -> column == null || column.isEmpty())) {
            throw new IllegalArgumentException("Composite-key column must not be empty");
        }
    }
}
