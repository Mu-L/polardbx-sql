package com.alibaba.polardbx.optimizer.core.planner.rule.holisticUnnest;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.LogicVisitor;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlQuantifyOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.calcite.util.Util.last;

public abstract class HolSubqueryRemoveRule extends RelOptRule {
    public static final HolSubqueryRemoveRule PROJECT =
        new HolSubqueryRemoveRule(
            operand(Project.class, null, RexUtil.SubQueryFinder.PROJECT_PREDICATE, any()),
            RelFactories.LOGICAL_BUILDER, "COLSubQueryRemoveRule:Project") {

            public void onMatch(RelOptRuleCall call) {
                matchProject(this, call);
            }
        };

    public static final HolSubqueryRemoveRule FILTER =
        new HolSubqueryRemoveRule(
            operand(Filter.class, null, RexUtil.SubQueryFinder.FILTER_PREDICATE, any()),
            RelFactories.LOGICAL_BUILDER, "COLSubQueryRemoveRule:Filter") {

            public void onMatch(RelOptRuleCall call) {
                matchFilter(this, call);
            }
        };

    public static final HolSubqueryRemoveRule JOIN =
        new HolSubqueryRemoveRule(
            operand(Join.class, null, RexUtil.SubQueryFinder.JOIN_PREDICATE, any()),
            RelFactories.LOGICAL_BUILDER, "COLSubQueryRemoveRule:Join") {

            public void onMatch(RelOptRuleCall call) {
                matchJoin(this, call);
            }
        };

    public HolSubqueryRemoveRule(RelOptRuleOperand operand, RelBuilderFactory relBuilderFactory, String description) {
        super(operand, relBuilderFactory, description);
    }

    protected RexNode apply(RexSubQuery e, Set<CorrelationId> variablesSet,
                            RelOptUtil.Logic logic,
                            RelBuilder builder, int inputCount, int offset, int subQueryIndex) {
        switch (e.getKind()) {
        case SCALAR_QUERY:
            return rewriteScalarQuery(e, variablesSet, builder, inputCount, offset);
        case EXISTS:
            return rewriteExists(e, variablesSet, logic, builder);
        case NOT_EXISTS:
            return rewriteNotExists(e, variablesSet, logic, builder);
        case SOME:
            return rewriteSome(e, variablesSet, logic, builder, subQueryIndex);
        case ALL:
            return rewriteAll(e, variablesSet, logic, builder, subQueryIndex);
        case IN:
            return rewriteIn(e, variablesSet, logic, builder, subQueryIndex);
        case NOT_IN:
            return rewriteNotIn(e, variablesSet, logic, builder, subQueryIndex);
        default:
            throw new AssertionError(e.getKind());
        }
    }

    /**
     * Rewrites a scalar sub-query into an
     * {@link org.apache.calcite.rel.core.Aggregate}.
     *
     * @param e Scalar sub-query to rewrite
     * @param variablesSet A set of variables used by a relational
     * expression of the specified RexSubQuery
     * @param builder Builder
     * @param offset Offset to shift {@link RexInputRef}
     * @return Expression that may be used to replace the RexSubQuery
     */
    private static RexNode rewriteScalarQuery(RexSubQuery e, Set<CorrelationId> variablesSet,
                                              RelBuilder builder, int inputCount, int offset) {
        builder.push(e.rel);
        final RelMetadataQuery mq = e.rel.getCluster().getMetadataQuery();
        final Boolean unique =
            mq.areColumnsUnique(builder.peek(), ImmutableBitSet.of());
        if (unique == null || !unique) {
            builder.aggregate(builder.groupKey(),
                builder.aggregateCall(SqlStdOperatorTable.SINGLE_VALUE, false, false, null, null,
                    builder.field(0)));
        }
        builder.columnarJoin(JoinRelType.LEFT, builder.literal(true), variablesSet);
        return field(builder, inputCount, offset);
    }

    private static void validateQuantifiedSubQueryArity(RexSubQuery e) {
        final int nOperands = e.getOperands().size();
        final int nSubQueryFields = e.rel.getRowType().getFieldCount();
        if (nOperands != 1 || nSubQueryFields != 1) {
            throw new TddlRuntimeException(ErrorCode.ERR_MULTI_COLUMN_QUANTIFIED_COMPARISON,
                e.op.getName(), String.valueOf(nOperands), String.valueOf(nSubQueryFields));
        }
    }

    /**
     * Rewrites a SOME sub-query into a {@link Join}.
     * <p>
     * For comparisons other than <>, the rewrite uses MIN/MAX of the inner column m to
     * check whether <em>any</em> row in B satisfies {@code a op b}. Under TRUE logic
     * with a non-nullable inner column, a correlate SEMI JOIN is produced; otherwise a
     * tri-valued CASE is generated over a scalar aggregation.
     *
     * @param e SOME sub-query to rewrite
     * @param variablesSet Correlation variables referenced by {@code e.rel}
     * @param logic Expected logic context (TRUE / TRUE_FALSE / TRUE_FALSE_UNKNOWN)
     * @param builder Builder
     * @param subQueryIndex sub-query index in multiple sub-queries
     * @return Expression that may be used to replace the RexSubQuery
     */
    private static RexNode rewriteSome(RexSubQuery e, Set<CorrelationId> variablesSet,
                                       RelOptUtil.Logic logic, RelBuilder builder, int subQueryIndex) {
        validateQuantifiedSubQueryArity(e);
        final SqlQuantifyOperator op = (SqlQuantifyOperator) e.op;
        switch (op.comparisonKind) {
        case GREATER_THAN_OR_EQUAL:
        case LESS_THAN_OR_EQUAL:
        case LESS_THAN:
        case GREATER_THAN:
        case NOT_EQUALS:
            break;
        default:
            // "= SOME" should have been rewritten into IN.
            throw new AssertionError("unexpected " + op);
        }

        final RexNode caseRexNode;
        List<RexNode> caseWhen = Lists.newArrayList();
        final RexNode literalFalse = builder.literal(false);
        final RexNode literalTrue = builder.literal(true);
        final RexLiteral literalUnknown = builder.getRexBuilder().makeNullLiteral(literalFalse.getType());

        final SqlAggFunction minMax = op.comparisonKind == SqlKind.GREATER_THAN
            || op.comparisonKind == SqlKind.GREATER_THAN_OR_EQUAL
            ? SqlStdOperatorTable.MIN : SqlStdOperatorTable.MAX;

        final String qAlias = qAlias(subQueryIndex);
        builder.push(e.rel);
        int mField, cField, dField, lField, uField;
        switch (op.comparisonKind) {
        case GREATER_THAN_OR_EQUAL:
        case LESS_THAN_OR_EQUAL:
        case LESS_THAN:
        case GREATER_THAN:
            // Two-value logic fast path: when logic is TRUE and b is NOT NULL, the tri-valued
            // CASE degenerates to a semi-join on (a op m).
            if (logic == RelOptUtil.Logic.TRUE && !builder.field(0).getType().isNullable()) {
                builder.aggregate(builder.groupKey(),
                    builder.aggregateCall(minMax, false, false, null, "m", builder.field(0)));
                builder.as(qAlias);
                mField = builder.peek().getRowType().getFieldCount() - 1;
                builder.correlateSemiJoin(JoinRelType.SEMI,
                    builder.call(RexUtil.op(op.comparisonKind), e.operands.get(0), builder.field(2, 1, mField)),
                    variablesSet);
                return literalTrue;
            }

            builder.aggregate(builder.groupKey(),
                builder.aggregateCall(minMax, false, false, null, "m", builder.field(0)),
                builder.count(false, "c"),             // Total rows in B
                builder.count(false, "d", builder.field(0))); // Non-NULL rows in B
            builder.as(qAlias);
            builder.columnarJoin(JoinRelType.LEFT, literalTrue, variablesSet);
            mField = builder.peek().getRowType().getFieldCount() - 3;
            cField = builder.peek().getRowType().getFieldCount() - 2;
            dField = builder.peek().getRowType().getFieldCount() - 1;

            // B is empty -> SOME is false.
            caseWhen.add(builder.or(
                builder.isNull(builder.field(cField)),
                builder.equals(builder.field(cField), builder.literal(0))));
            caseWhen.add(literalFalse);
            // a IS NULL -> unknown.
            if (e.operands.get(0).getType().isNullable()) {
                caseWhen.add(builder.isNull(e.operands.get(0)));
                caseWhen.add(literalUnknown);
            }
            // a op m holds on the extremum of B -> true (at least one row satisfies).
            caseWhen.add(builder.call(RexUtil.op(op.comparisonKind), e.operands.get(0), builder.field(mField)));
            caseWhen.add(literalTrue);
            // Otherwise, if B contains NULLs (c > d) the outcome is unknown; else false.
            caseWhen.add(builder.greaterThan(builder.field(cField), builder.field(dField)));
            caseWhen.add(literalUnknown);
            caseWhen.add(literalFalse);
            caseRexNode = builder.call(SqlStdOperatorTable.CASE, caseWhen);
            return builder.getRexBuilder().makeCastForConvertlet(e.getType(), caseRexNode);
        case NOT_EQUALS:
            // Two-value logic fast path: when logic is TRUE and b is NOT NULL, checking
            // a<>min(b) OR a<>max(b) is equivalent to "some row differs from a".
            if (logic == RelOptUtil.Logic.TRUE && !builder.field(0).getType().isNullable()) {
                builder.aggregate(builder.groupKey(),
                    builder.min("l", builder.field(0)),
                    builder.max("u", builder.field(0)));
                builder.as(qAlias);
                lField = builder.peek().getRowType().getFieldCount() - 2;
                uField = builder.peek().getRowType().getFieldCount() - 1;
                builder.correlateSemiJoin(JoinRelType.SEMI,
                    builder.or(
                        builder.call(RexUtil.op(op.comparisonKind), e.operands.get(0), builder.field(2, 1, lField)),
                        builder.call(RexUtil.op(op.comparisonKind), e.operands.get(0), builder.field(2, 1, uField))
                    ),
                    variablesSet);
                return literalTrue;
            }

            builder.aggregate(builder.groupKey(),
                builder.count(false, "c"),                        // Total rows in B
                builder.count(false, "d", builder.field(0)),  // Non-NULL rows in B
                builder.min("l", builder.field(0)),                // min(b) ignoring NULL
                builder.max("u", builder.field(0)));               // max(b) ignoring NULL
            builder.as(qAlias);
            builder.columnarJoin(JoinRelType.LEFT, literalTrue, variablesSet);
            cField = builder.peek().getRowType().getFieldCount() - 4;
            dField = builder.peek().getRowType().getFieldCount() - 3;
            lField = builder.peek().getRowType().getFieldCount() - 2;
            uField = builder.peek().getRowType().getFieldCount() - 1;

            // B is empty -> SOME is false.
            caseWhen.add(builder.or(
                builder.isNull(builder.field(cField)),
                builder.equals(builder.field(cField), builder.literal(0))));
            caseWhen.add(literalFalse);
            // a IS NULL -> unknown.
            if (e.operands.get(0).getType().isNullable()) {
                caseWhen.add(builder.isNull(e.operands.get(0)));
                caseWhen.add(literalUnknown);
            }
            // There is a non-null row differing from a (l != a or u != a) -> true.
            caseWhen.add(builder.and(
                builder.isNotNull(builder.field(lField)),
                builder.or(
                    builder.notEquals(e.operands.get(0), builder.field(lField)),
                    builder.notEquals(e.operands.get(0), builder.field(uField)))));
            caseWhen.add(literalTrue);
            // All non-null rows equal a and no NULL rows exist -> false.
            caseWhen.add(builder.and(
                builder.equals(builder.field(cField), builder.field(dField)),
                builder.equals(e.operands.get(0), builder.field(lField))));
            caseWhen.add(literalFalse);
            // Otherwise (all rows NULL, or NULL disturbance) -> unknown.
            caseWhen.add(literalUnknown);
            caseRexNode = builder.call(SqlStdOperatorTable.CASE, caseWhen);
            return builder.getRexBuilder().makeCastForConvertlet(e.getType(), caseRexNode);
        default:
            throw new AssertionError("not possible - per above check");
        }
    }

    /**
     * Rewrites an ALL sub-query into a {@link Join}.
     * <p>
     * For ordinal comparisons the rewrite checks {@code a op MAX(b)} / {@code a op MIN(b)};
     * for EQUALS it collapses to {@code MIN(b) = MAX(b) = a} when no NULL disturbance is
     * possible. Under TRUE logic with a non-nullable inner column, a correlate SEMI JOIN is
     * produced; otherwise a tri-valued CASE is generated over a scalar aggregation.
     *
     * @param e ALL sub-query to rewrite
     * @param variablesSet Correlation variables referenced by {@code e.rel}
     * @param logic Expected logic context (TRUE / TRUE_FALSE / TRUE_FALSE_UNKNOWN)
     * @param builder Builder
     * @param subQueryIndex sub-query index in multiple sub-queries
     * @return Expression that may be used to replace the RexSubQuery
     */
    private static RexNode rewriteAll(RexSubQuery e, Set<CorrelationId> variablesSet,
                                      RelOptUtil.Logic logic, RelBuilder builder, int subQueryIndex) {
        validateQuantifiedSubQueryArity(e);
        final SqlQuantifyOperator op = (SqlQuantifyOperator) e.op;
        switch (op.comparisonKind) {
        case GREATER_THAN_OR_EQUAL:
        case LESS_THAN_OR_EQUAL:
        case LESS_THAN:
        case GREATER_THAN:
        case EQUALS:
            break;
        default:
            // "<> ALL" should have been rewritten into IN/NOT IN.
            throw new AssertionError("unexpected " + op);
        }

        final RexNode caseRexNode;
        List<RexNode> caseWhen = Lists.newArrayList();
        final RexNode literalFalse = builder.literal(false);
        final RexNode literalTrue = builder.literal(true);
        final RexLiteral literalUnknown = builder.getRexBuilder().makeNullLiteral(literalFalse.getType());

        final SqlAggFunction minMax = op.comparisonKind == SqlKind.GREATER_THAN
            || op.comparisonKind == SqlKind.GREATER_THAN_OR_EQUAL
            ? SqlStdOperatorTable.MAX  // For > and >=, need to check the maximum value
            : SqlStdOperatorTable.MIN; // For < and <=, need to check the minimum value

        final String qAlias = qAlias(subQueryIndex);
        builder.push(e.rel);
        int mField, cField, dField, lField, uField;
        switch (op.comparisonKind) {
        case GREATER_THAN_OR_EQUAL:
        case LESS_THAN_OR_EQUAL:
        case LESS_THAN:
        case GREATER_THAN:
            // Two-value logic fast path: under TRUE logic with a NOT NULL inner column,
            // ALL holds iff B is empty OR a op extremum(B).
            if (logic == RelOptUtil.Logic.TRUE && !builder.field(0).getType().isNullable()) {
                builder.aggregate(builder.groupKey(),
                    builder.aggregateCall(minMax, false, false, null, "m", builder.field(0)),
                    builder.count(false, "c"));
                builder.as(qAlias);
                mField = builder.peek().getRowType().getFieldCount() - 2;
                cField = builder.peek().getRowType().getFieldCount() - 1;
                builder.correlateSemiJoin(JoinRelType.SEMI,
                    builder.or(
                        builder.equals(builder.field(2, 1, cField), builder.literal(0)),
                        builder.call(RexUtil.op(op.comparisonKind), e.operands.get(0), builder.field(2, 1, mField))),
                    variablesSet);
                return literalTrue;
            }

            builder.aggregate(builder.groupKey(),
                builder.aggregateCall(minMax, false, false, null, "m", builder.field(0)),
                builder.count(false, "c"),             // Total rows in B
                builder.count(false, "d", builder.field(0))); // Non-NULL rows in B
            builder.as(qAlias);
            builder.columnarJoin(JoinRelType.LEFT, literalTrue, variablesSet);
            mField = builder.peek().getRowType().getFieldCount() - 3;
            cField = builder.peek().getRowType().getFieldCount() - 2;
            dField = builder.peek().getRowType().getFieldCount() - 1;

            // B is empty -> ALL is true (vacuous truth).
            caseWhen.add(builder.or(
                builder.isNull(builder.field(cField)),
                builder.equals(builder.field(cField), builder.literal(0))));
            caseWhen.add(literalTrue);
            // a IS NULL -> unknown.
            if (e.operands.get(0).getType().isNullable()) {
                caseWhen.add(builder.isNull(e.operands.get(0)));
                caseWhen.add(literalUnknown);
            }
            // No NULL in B (c = d) and a op extremum(B) holds -> true.
            caseWhen.add(builder.and(
                builder.equals(builder.field(cField), builder.field(dField)),
                builder.call(RexUtil.op(op.comparisonKind), e.operands.get(0), builder.field(mField))));
            caseWhen.add(literalTrue);
            // All rows are NULL (m IS NULL) or a op extremum(B) still holds under NULL disturbance -> unknown.
            // Merging "m IS NULL" with "IS_TRUE(a op m)" is safe: when the latter is true, m must be non-null.
            caseWhen.add(builder.or(
                builder.isNull(builder.field(mField)),
                builder.call(RexUtil.op(op.comparisonKind), e.operands.get(0), builder.field(mField))));
            caseWhen.add(literalUnknown);
            // Otherwise -> false.
            caseWhen.add(literalFalse);
            caseRexNode = builder.call(SqlStdOperatorTable.CASE, caseWhen);
            return builder.getRexBuilder().makeCastForConvertlet(e.getType(), caseRexNode);
        case EQUALS:
            // Two-value logic fast path: with b NOT NULL, COUNT(DISTINCT b) = 1 is
            // equivalent to MIN(b) = MAX(b); combined with a = MIN(b) this is sufficient
            // (and necessary) for ALL-EQUALS, plus the B-empty branch.
            if (logic == RelOptUtil.Logic.TRUE && !builder.field(0).getType().isNullable()) {
                builder.aggregate(builder.groupKey(),
                    builder.min("l", builder.field(0)),
                    builder.max("u", builder.field(0)),
                    builder.count(false, "c"));
                builder.as(qAlias);
                lField = builder.peek().getRowType().getFieldCount() - 3;
                uField = builder.peek().getRowType().getFieldCount() - 2;
                cField = builder.peek().getRowType().getFieldCount() - 1;
                builder.correlateSemiJoin(JoinRelType.SEMI,
                    builder.or(
                        builder.equals(builder.field(2, 1, cField), builder.literal(0)),
                        builder.and(
                            builder.equals(builder.field(2, 1, lField), builder.field(2, 1, uField)),
                            builder.equals(e.operands.get(0), builder.field(2, 1, lField)))),
                    variablesSet);
                return literalTrue;
            }

            builder.aggregate(builder.groupKey(),
                builder.count(false, "c"),        // Total number of elements in B
                builder.count(false, "d", builder.field(0)),  // Number of non-NULL elements in B
                builder.min("l", builder.field(0)),           // Minimum value in B (ignoring NULL)
                builder.max("u", builder.field(0)));          // Maximum value in B (ignoring NULL)
            builder.as(qAlias);
            builder.columnarJoin(JoinRelType.LEFT, literalTrue, variablesSet);
            cField = builder.peek().getRowType().getFieldCount() - 4;
            dField = builder.peek().getRowType().getFieldCount() - 3;
            lField = builder.peek().getRowType().getFieldCount() - 2;
            uField = builder.peek().getRowType().getFieldCount() - 1;

            // When B is empty, ALL is true
            caseWhen.add(builder.or(
                builder.isNull(builder.field(cField)),
                builder.equals(builder.field(cField), builder.literal(0))));
            caseWhen.add(literalTrue);
            // When a is NULL, result is unknown
            if (e.operands.get(0).getType().isNullable()) {
                caseWhen.add(builder.isNull(e.operands.get(0)));
                caseWhen.add(literalUnknown);
            }
            // When all non-null values in B equal a and no NULLs exist, result is true
            caseWhen.add(builder.and(
                builder.equals(builder.field(cField), builder.field(dField)),
                builder.equals(builder.field(lField), builder.field(uField)),
                builder.equals(e.operands.get(0), builder.field(lField))));
            caseWhen.add(literalTrue);
            // When all non-null values in B equal a but NULLs present, result is unknown
            caseWhen.add(builder.and(
                builder.equals(builder.field(lField), builder.field(uField)),
                builder.equals(e.operands.get(0), builder.field(lField))));
            caseWhen.add(literalUnknown);
            // When non-null values exist that differ from a, result is false
            caseWhen.add(builder.greaterThan(builder.field(dField), builder.literal(0)));
            caseWhen.add(literalFalse);
            // Otherwise (all values are NULL), result is unknown
            caseWhen.add(literalUnknown);
            caseRexNode = builder.call(SqlStdOperatorTable.CASE, caseWhen);
            return builder.getRexBuilder().makeCastForConvertlet(e.getType(), caseRexNode);
        default:
            throw new AssertionError("not possible - per above check");
        }
    }

    /**
     * Rewrites an EXISTS RexSubQuery into a {@link Join}.
     *
     * @param e EXISTS sub-query to rewrite
     * @param variablesSet A set of variables used by a relational
     * expression of the specified RexSubQuery
     * @param logic Logic for evaluating
     * @param builder Builder
     * @return Expression that may be used to replace the RexSubQuery
     */
    private static RexNode rewriteExists(RexSubQuery e, Set<CorrelationId> variablesSet,
                                         RelOptUtil.Logic logic, RelBuilder builder) {
        builder.push(e.rel);
        if (logic == RelOptUtil.Logic.TRUE) {
            builder.correlateSemiJoin(JoinRelType.SEMI, builder.literal(true), variablesSet);
            return builder.literal(true);
        }
        builder.project(builder.alias(builder.literal(true), "i"));
        builder.distinct();
        builder.as("dt");
        builder.columnarJoin(JoinRelType.LEFT, builder.literal(true), variablesSet);
        return builder.isNotNull(last(builder.fields()));
    }

    /**
     * Rewrites a NOT EXISTS RexSubQuery into a {@link Join}.
     * <p>
     * Mirrors {@link #rewriteExists}: under TRUE logic it emits an ANTI correlate semi-join;
     * otherwise it left-joins against a DISTINCT TRUE marker and tests for IS NULL.
     *
     * @param e NOT EXISTS sub-query to rewrite
     * @param variablesSet Correlation variables referenced by {@code e.rel}
     * @param logic Expected logic context
     * @param builder Builder
     * @return Expression that may be used to replace the RexSubQuery
     */
    private static RexNode rewriteNotExists(RexSubQuery e, Set<CorrelationId> variablesSet,
                                            RelOptUtil.Logic logic, RelBuilder builder) {
        builder.push(e.rel);
        if (logic == RelOptUtil.Logic.TRUE) {
            builder.correlateSemiJoin(JoinRelType.ANTI, builder.literal(true), variablesSet);
            return builder.literal(true);
        }
        builder.project(builder.alias(builder.literal(true), "i"));
        builder.distinct();
        builder.as("dt");
        builder.columnarJoin(JoinRelType.LEFT, builder.literal(true), variablesSet);
        return builder.isNull(last(builder.fields()));
    }

    /**
     * Rewrites an IN RexSubQuery into a {@link Join}.
     *
     * @param e IN sub-query to rewrite
     * @param variablesSet A set of variables used by a relational
     * expression of the specified RexSubQuery
     * @param logic Logic for evaluating
     * @param builder Builder
     * @param subQueryIndex sub-query index in multiple sub-queries
     * @return Expression that may be used to replace the RexSubQuery
     */
    private static RexNode rewriteIn(RexSubQuery e, Set<CorrelationId> variablesSet,
                                     RelOptUtil.Logic logic, RelBuilder builder, int subQueryIndex) {
        RelDataType leftRowType = builder.peek().getRowType();
        RexBuilder rexBuilder = builder.getRexBuilder();
        builder.push(e.rel);
        if (logic == RelOptUtil.Logic.TRUE) {
            List<RexNode> joinConditions = new ArrayList<>();
            for (int i = 0; i < e.getOperands().size(); i++) {
                RexNode operand = e.getOperands().get(i);
                RexNode field = builder.field(2, 1, i);
                joinConditions.add(builder.equals(operand, field));
            }
            builder.correlateSemiJoin(JoinRelType.SEMI, builder.and(joinConditions), variablesSet);
            return builder.literal(true);
        }
        final RexNode literalTrue = builder.literal(true);
        final RexNode literalFalse = builder.literal(false);
        final RexLiteral literalUnknown = builder.getRexBuilder().makeNullLiteral(literalFalse.getType());

        CorrelationId correlationId = null;
        if (e.getOperands().stream().anyMatch(operand -> !RelOptUtil.InputFinder.bits(operand).isEmpty())) {
            if (CollectionUtils.isEmpty(variablesSet)) {
                correlationId = e.rel.getCluster().createCorrel();
                variablesSet = Sets.newHashSet(correlationId);
            } else {
                correlationId = variablesSet.iterator().next();
            }
        }

        String qAlias = qAlias(subQueryIndex);

        if (logic == RelOptUtil.Logic.UNKNOWN_AS_FALSE) {
            // 0. filter e.getOperands() respectively equal to e.rel columns
            List<RexNode> filterConditions = new ArrayList<>();
            for (int i = 0; i < e.getOperands().size(); i++) {
                RexNode operand = e.getOperands().get(i);
                operand = operand.accept(new ReplaceInputRefShuttle(rexBuilder, leftRowType, correlationId));
                RexNode field = builder.field(i);
                filterConditions.add(builder.equals(operand, field));
            }
            builder.filter(builder.and(filterConditions));
            // 1. project first column f1
            builder.project(builder.alias(builder.field(0), "f1"));
            // 2. distinct f1
            builder.distinct();
            // 3. Create left type correlate
            builder.columnarJoin(JoinRelType.LEFT, literalTrue, variablesSet);
            // 4. first column is not null is true
            int f1Field = builder.peek().getRowType().getFieldCount() - 1;
            RexNode resultProjection = builder.isNotNull(builder.field(f1Field));
            return rexBuilder.makeCastForConvertlet(e.getType(), resultProjection);
        }

        // Tri-valued logic:
        // strict_case  = CASE WHEN AND_i ( op_i = f_i )                                   THEN 1 ELSE 0 END
        // possible_case = CASE WHEN AND_i ( op_i = f_i OR f_i IS NULL OR op_i IS NULL )   THEN 1 ELSE 0 END
        final RexNode literalOne = builder.literal(1);
        final RexNode literalZero = builder.literal(0);

        List<RexNode> strictConds = new ArrayList<>();
        List<RexNode> possibleConds = new ArrayList<>();
        for (int i = 0; i < e.getOperands().size(); i++) {
            RexNode operand = e.getOperands().get(i);
            operand = operand.accept(new ReplaceInputRefShuttle(rexBuilder, leftRowType, correlationId));
            RexNode field = builder.field(i);
            // Strict equality: wrap with IS TRUE so that NULL operand/field collapses UNKNOWN to FALSE.
            strictConds.add(builder.call(SqlStdOperatorTable.IS_TRUE,
                builder.equals(operand, field)));
            // Possible equality: op = f, OR either side IS NULL.
            List<RexNode> conds = Lists.newArrayList(builder.equals(operand, field));
            if (field.getType().isNullable()) {
                conds.add(builder.isNull(field));
            }
            if (operand.getType().isNullable()) {
                conds.add(builder.isNull(operand));
            }
            possibleConds.add(builder.or(conds));
        }

        // 1. Project the two 0/1 indicator columns only.
        RexNode strictCase = builder.call(SqlStdOperatorTable.CASE,
            builder.and(strictConds), literalOne, literalZero);
        RexNode possibleCase = builder.call(SqlStdOperatorTable.CASE,
            builder.and(possibleConds), literalOne, literalZero);
        builder.project(ImmutableList.of(
            builder.alias(strictCase, "s_case"),
            builder.alias(possibleCase, "p_case")));

        // 2. Empty groupKey aggregation: count(*) + max(strict) + max(possible).
        builder.aggregate(builder.groupKey(),
            builder.count(false, "c"),
            builder.aggregateCall(SqlStdOperatorTable.MAX, false, false, null, "strict",
                builder.field(0)),
            builder.aggregateCall(SqlStdOperatorTable.MAX, false, false, null, "possible",
                builder.field(1)));
        builder.as(qAlias);

        // 3. Correlate LEFT JOIN to bind outer row.
        builder.columnarJoin(JoinRelType.LEFT, literalTrue, variablesSet);
        int cField = builder.peek().getRowType().getFieldCount() - 3;
        int strictField = builder.peek().getRowType().getFieldCount() - 2;
        int possibleField = builder.peek().getRowType().getFieldCount() - 1;

        // 4. Simplified tri-valued CASE.
        List<RexNode> caseWhen = new ArrayList<>();
        // c IS NULL OR c = 0 => false (B empty)
        caseWhen.add(builder.or(
            builder.isNull(builder.field(cField)),
            builder.equals(builder.field(cField), builder.literal(0))));
        caseWhen.add(literalFalse);
        // strict = 1 => true (exact match exists)
        caseWhen.add(builder.equals(builder.field(strictField), literalOne));
        caseWhen.add(literalTrue);
        // possible = 1 => unknown (potential match with NULL disturbance)
        caseWhen.add(builder.equals(builder.field(possibleField), literalOne));
        caseWhen.add(literalUnknown);
        // otherwise => false
        caseWhen.add(literalFalse);

        RexNode resultProjection = builder.call(SqlStdOperatorTable.CASE, caseWhen);
        return rexBuilder.makeCastForConvertlet(e.getType(), resultProjection);
    }

    /**
     * Rewrites a NOT IN RexSubQuery into a {@link Join}.
     * <p>
     * Tri-valued dual of {@link #rewriteIn}: under TRUE logic with all operands NOT NULL
     * it emits an ANTI correlate semi-join; otherwise it builds scalar strict/possible
     * indicators and maps them through a simplified CASE.
     *
     * @param e NOT IN sub-query to rewrite
     * @param variablesSet Correlation variables referenced by {@code e.rel}
     * @param logic Expected logic context
     * @param builder Builder
     * @param subQueryIndex sub-query index in multiple sub-queries
     * @return Expression that may be used to replace the RexSubQuery
     */
    private static RexNode rewriteNotIn(RexSubQuery e, Set<CorrelationId> variablesSet,
                                        RelOptUtil.Logic logic, RelBuilder builder, int subQueryIndex) {
        RelDataType leftRowType = builder.peek().getRowType();
        RexBuilder rexBuilder = builder.getRexBuilder();
        builder.push(e.rel);

        boolean anyENullable = false;
        for (int i = 0; i < e.getOperands().size(); i++) {
            anyENullable |= builder.field(i).getType().isNullable();
        }
        if (logic == RelOptUtil.Logic.TRUE && !anyENullable
            && e.getOperands().stream().noneMatch(o -> o.getType().isNullable())) {
            List<RexNode> joinConditions = new ArrayList<>();
            for (int i = 0; i < e.getOperands().size(); i++) {
                RexNode operand = e.getOperands().get(i);
                RexNode field = builder.field(2, 1, i);
                joinConditions.add(builder.equals(operand, field));
            }

            builder.correlateSemiJoin(JoinRelType.ANTI, builder.and(joinConditions), variablesSet);
            return builder.literal(true);
        }
        final RexNode literalTrue = builder.literal(true);
        final RexNode literalFalse = builder.literal(false);
        final RexLiteral literalUnknown =
            builder.getRexBuilder().makeNullLiteral(literalFalse.getType());

        CorrelationId correlationId = null;
        if (e.getOperands().stream().anyMatch(operand -> !RelOptUtil.InputFinder.bits(operand).isEmpty())) {
            if (CollectionUtils.isEmpty(variablesSet)) {
                correlationId = e.rel.getCluster().createCorrel();
                variablesSet = Sets.newHashSet(correlationId);
            } else {
                correlationId = variablesSet.iterator().next();
            }
        }
        String qAlias = qAlias(subQueryIndex);
        // Tri-valued logic: scalar aggregation
        final RexNode literalOne = builder.literal(1);
        final RexNode literalZero = builder.literal(0);

        List<RexNode> strictConds = new ArrayList<>();
        List<RexNode> possibleConds = new ArrayList<>();
        for (int i = 0; i < e.getOperands().size(); i++) {
            RexNode operand = e.getOperands().get(i);
            operand = operand.accept(new ReplaceInputRefShuttle(rexBuilder, leftRowType, correlationId));
            RexNode field = builder.field(i);
            // Strict equality: wrap with IS TRUE so UNKNOWN collapses to FALSE.
            strictConds.add(builder.call(SqlStdOperatorTable.IS_TRUE,
                builder.equals(operand, field)));
            // Possible equality: op = f, OR either side IS NULL.
            List<RexNode> conds = Lists.newArrayList(builder.equals(operand, field));
            if (field.getType().isNullable()) {
                conds.add(builder.isNull(field));
            }
            if (operand.getType().isNullable()) {
                conds.add(builder.isNull(operand));
            }
            possibleConds.add(builder.or(conds));
        }

        // 1. Project two 0/1 indicator columns only.
        RexNode strictCase = builder.call(SqlStdOperatorTable.CASE,
            builder.and(strictConds), literalOne, literalZero);
        RexNode possibleCase = builder.call(SqlStdOperatorTable.CASE,
            builder.and(possibleConds), literalOne, literalZero);
        builder.project(ImmutableList.of(
            builder.alias(strictCase, "s_case"),
            builder.alias(possibleCase, "p_case")));

        // 2. Empty groupKey aggregation: count(*) + max(strict) + max(possible).
        builder.aggregate(builder.groupKey(),
            builder.count(false, "c"),
            builder.aggregateCall(SqlStdOperatorTable.MAX, false, false, null, "strict",
                builder.field(0)),
            builder.aggregateCall(SqlStdOperatorTable.MAX, false, false, null, "possible",
                builder.field(1)));
        builder.as(qAlias);

        // 3. Correlate LEFT JOIN to bind outer row.
        builder.columnarJoin(JoinRelType.LEFT, literalTrue, variablesSet);
        int cField = builder.peek().getRowType().getFieldCount() - 3;
        int strictField = builder.peek().getRowType().getFieldCount() - 2;
        int possibleField = builder.peek().getRowType().getFieldCount() - 1;

        // 4. Simplified tri-valued CASE (dual of IN).
        List<RexNode> caseWhen = new ArrayList<>();
        // c IS NULL OR c = 0 => true (NOT IN empty set is true)
        caseWhen.add(builder.or(
            builder.isNull(builder.field(cField)),
            builder.equals(builder.field(cField), builder.literal(0))));
        caseWhen.add(literalTrue);
        // strict = 1 => false (exact match exists)
        caseWhen.add(builder.equals(builder.field(strictField), literalOne));
        caseWhen.add(literalFalse);
        // possible = 1 => unknown (potential match with NULL disturbance)
        caseWhen.add(builder.equals(builder.field(possibleField), literalOne));
        caseWhen.add(literalUnknown);
        // otherwise => true
        caseWhen.add(literalTrue);

        RexNode resultProjection = builder.call(SqlStdOperatorTable.CASE, caseWhen);
        return rexBuilder.makeCastForConvertlet(e.getType(), resultProjection);
    }

    /**
     * Builds the sub-query alias used to scope the inner aggregation, e.g. {@code q},
     * {@code q1}, {@code q2}... when the outer relation contains multiple sub-queries.
     */
    private static String qAlias(int subQueryIndex) {
        return subQueryIndex == 0 ? "q" : "q" + subQueryIndex;
    }

    /**
     * Returns a reference to a particular field, by offset, across several
     * inputs on a {@link RelBuilder}'s stack.
     */
    private static RexInputRef field(RelBuilder builder, int inputCount, int offset) {
        for (int inputOrdinal = 0; ; ) {
            final RelNode r = builder.peek(inputCount, inputOrdinal);
            if (offset < r.getRowType().getFieldCount()) {
                return builder.field(inputCount, inputOrdinal, offset);
            }
            ++inputOrdinal;
            offset -= r.getRowType().getFieldCount();
        }
    }

    /**
     * Returns a list of expressions that project the first {@code fieldCount}
     * fields of the top input on a {@link RelBuilder}'s stack.
     */
    private static List<RexNode> fields(RelBuilder builder, int fieldCount) {
        final List<RexNode> projects = new ArrayList<>();
        for (int i = 0; i < fieldCount; i++) {
            projects.add(builder.field(i));
        }
        return projects;
    }

    private static void matchProject(HolSubqueryRemoveRule rule,
                                     RelOptRuleCall call) {
        final Project project = call.rel(0);
        final RelBuilder builder = call.builder();
        final RexSubQuery e =
            requireNonNull(RexUtil.SubQueryFinder.find(project.getProjects()));
        final RelOptUtil.Logic logic =
            LogicVisitor.find(RelOptUtil.Logic.TRUE_FALSE_UNKNOWN, project.getProjects(), e);
        builder.push(project.getInput());
        final int fieldCount = builder.peek().getRowType().getFieldCount();
        final Set<CorrelationId> variablesSet = RelOptUtil.getVariablesUsed(e.rel);
        variablesSet.retainAll(project.getVariablesSet());
        final RexNode target =
            rule.apply(e, variablesSet, logic, builder, 1, fieldCount, 0);
        final RexShuttle shuttle = new HolSubqueryRemoveRule.ReplaceSubQueryShuttle(e, target);
        List<RexNode> newProjects = shuttle.apply(project.getProjects());
        Set<CorrelationId> remainingVars = Sets.newHashSet();
        for (RexNode newProject : newProjects) {
            remainingVars.addAll(collectRemainingCorrelVars(newProject, project.getVariablesSet()));
        }
        builder.project(newProjects, project.getRowType().getFieldNames(), remainingVars);
        call.transformTo(builder.build());
    }

    private static void matchFilter(HolSubqueryRemoveRule rule,
                                    RelOptRuleCall call) {
        final Filter filter = call.rel(0);
        final Set<CorrelationId> filterVariablesSet = filter.getVariablesSet();
        final RelBuilder builder = call.builder();
        builder.push(filter.getInput());
        int count = 0;
        RexNode c = filter.getCondition();
        while (true) {
            final RexSubQuery e = RexUtil.SubQueryFinder.find(c);
            if (e == null) {
                break;
            }
            count++;
            final RelOptUtil.Logic logic =
                LogicVisitor.find(RelOptUtil.Logic.TRUE, ImmutableList.of(c), e);
            final Set<CorrelationId> variablesSet = RelOptUtil.getVariablesUsed(e.rel);
            variablesSet.retainAll(filterVariablesSet);
            final RexNode target =
                rule.apply(e, variablesSet, logic,
                    builder, 1, builder.peek().getRowType().getFieldCount(), count);
            final RexShuttle shuttle = new HolSubqueryRemoveRule.ReplaceSubQueryShuttle(e, target);
            c = c.accept(shuttle);
        }
        builder.filter(c);
        builder.project(fields(builder, filter.getRowType().getFieldCount()));
        call.transformTo(builder.build());
    }

    private static void matchJoin(HolSubqueryRemoveRule rule, RelOptRuleCall call) {
        final Join join = call.rel(0);
        final RelBuilder builder = call.builder();
        final RexSubQuery e = requireNonNull(RexUtil.SubQueryFinder.find(join.getCondition()));

        final int nFieldsLeft = join.getLeft().getRowType().getFieldCount();
        final int nFieldsRight = join.getRight().getRowType().getFieldCount();

        // Columns (direct + correl-referenced) consumed by the sub-query.
        ImmutableBitSet inputSet = RelOptUtil.InputFinder.bits(e.getOperands(), null);
        final Set<CorrelationId> variablesSet = RelOptUtil.getVariablesUsed(e.rel);
        variablesSet.retainAll(join.getVariablesSet());
        for (CorrelationId id : variablesSet) {
            inputSet = ImmutableBitSet.union(ImmutableList.of(
                RelOptUtil.correlationColumns(id, e.rel), inputSet));
        }

        boolean hitsLeft = inputSet.intersects(ImmutableBitSet.range(0, nFieldsLeft));
        boolean hitsRight =
            inputSet.intersects(ImmutableBitSet.range(nFieldsLeft, nFieldsLeft + nFieldsRight));
        // Independent sub-queries fold into the left branch.
        if (!hitsLeft && !hitsRight) {
            hitsLeft = true;
        }

        final RelOptUtil.Logic logic = adjustLogicForOuterJoin(
            LogicVisitor.find(RelOptUtil.Logic.TRUE, ImmutableList.of(join.getCondition()), e),
            join, hitsLeft, hitsRight);

        if (hitsLeft && !hitsRight) {
            rewriteLeftOnly(rule, call, join, builder, e, logic, variablesSet, nFieldsLeft);
            return;
        }
        if (hitsRight && !hitsLeft) {
            rewriteRightOnly(rule, call, join, builder, e, logic, variablesSet,
                nFieldsLeft, nFieldsRight);
            return;
        }
        rewriteInnerBothSides(call, join, builder);
    }

    /**
     * Rewriting under {@link RelOptUtil.Logic#TRUE} row-filters the side the sub-query references,
     * which is only sound if the join is free to drop those rows. When the referenced side is the
     * one an outer join must preserve and NULL-pad, downgrade to UNKNOWN_AS_FALSE so every rewrite
     * below takes its NULL-preserving branch instead.
     */
    private static RelOptUtil.Logic adjustLogicForOuterJoin(RelOptUtil.Logic logic, Join join,
                                                            boolean hitsLeft, boolean hitsRight) {
        final JoinRelType joinType = join.getJoinType();
        final boolean hitsPreservedSide =
            (hitsLeft && joinType.generatesNullsOnRight())
                || (hitsRight && joinType.generatesNullsOnLeft());
        return hitsPreservedSide && logic == RelOptUtil.Logic.TRUE
            ? RelOptUtil.Logic.UNKNOWN_AS_FALSE : logic;
    }

    /**
     * Left-only: rewrite against L, shift right-side refs by addedOnLeft, then project the
     * middle rewrite-appended columns away.
     */
    private static void rewriteLeftOnly(HolSubqueryRemoveRule rule, RelOptRuleCall call, Join join,
                                        RelBuilder builder, RexSubQuery e, RelOptUtil.Logic logic,
                                        Set<CorrelationId> variablesSet, int nFieldsLeft) {
        builder.push(join.getLeft());
        final int sizeBefore = builder.peek().getRowType().getFieldCount();
        final RexNode target = rule.apply(e, variablesSet, logic, builder, 1, nFieldsLeft, 0);
        final int addedOnLeft = builder.peek().getRowType().getFieldCount() - sizeBefore;
        final RelNode newLeft = builder.build();

        // Shift ORIGINAL condition so right-side refs (both InputRef and RexFieldAccess under
        // CorrelVariable) are pushed right by addedOnLeft. Must run BEFORE ReplaceSubQueryShuttle
        // because `target` is already authored in newLeft coords and must not be shifted again.
        final RexBuilder rexBuilder = builder.getRexBuilder();
        final RexNode shiftedCondition;
        if (addedOnLeft > 0) {
            // Rebind $cor to the NEW join row (newLeft + right) so later field index shifts
            // reference the correct schema.
            final RelDataType newJoinRowType =
                joinedRowType(rexBuilder, newLeft.getRowType(), join.getRight().getRowType());
            shiftedCondition = join.getCondition().accept(new ShiftCorrelRefShuttle(
                rexBuilder, newJoinRowType, variablesSet, nFieldsLeft, addedOnLeft));
        } else {
            shiftedCondition = join.getCondition();
        }
        final RexNode newCond =
            new ReplaceSubQueryShuttle(e, target).apply(shiftedCondition);

        final Set<CorrelationId> newJoinVariables =
            Sets.newHashSet(collectRemainingCorrelVars(newCond, join.getVariablesSet()));

        final RelNode newJoin = RelFactories.DEFAULT_JOIN_FACTORY.createJoin(
            newLeft, join.getRight(), newCond, newJoinVariables,
            join.getJoinType(), join.isSemiJoinDone());
        builder.push(newJoin);

        // Strip the middle appended columns (sitting between newLeft's original part and right).
        final int totalFields = builder.peek().getRowType().getFieldCount();
        builder.project(builder.fields(
            ImmutableBitSet.range(0, nFieldsLeft)
                .union(ImmutableBitSet.range(nFieldsLeft + addedOnLeft, totalFields))
                .asList()));
        call.transformTo(builder.build());
    }

    /**
     * Right-only: mirror of left-only. Rebind $cor from merged(L+R) coords to R-local coords,
     * rewrite against R to get R', then Join(L, R') and strip trailing added columns (SEMI/ANTI
     * already emit only L).
     */
    private static void rewriteRightOnly(HolSubqueryRemoveRule rule, RelOptRuleCall call, Join join,
                                         RelBuilder builder, RexSubQuery e, RelOptUtil.Logic logic,
                                         Set<CorrelationId> variablesSet,
                                         int nFieldsLeft, int nFieldsRight) {
        final RexBuilder rexBuilder = builder.getRexBuilder();
        final RelDataType rightRowType = join.getRight().getRowType();

        // Rebind each correlation variable to R coords inside the sub-query rel and operands.
        RelNode rewrittenSubRel = e.rel;
        List<RexNode> rewrittenOperands = e.getOperands();
        final Map<Integer, Integer> indexMap = new HashMap<>();
        for (int i = nFieldsLeft; i < nFieldsLeft + nFieldsRight; i++) {
            indexMap.put(i, i - nFieldsLeft);
        }
        for (CorrelationId id : variablesSet) {
            final RexCorrelVariable newCv =
                (RexCorrelVariable) rexBuilder.makeCorrel(rightRowType, id);
            final RelOptUtil.RexFieldAccessReplacer replacer =
                new RelOptUtil.RexFieldAccessReplacer(id, newCv, rexBuilder, indexMap);
            rewrittenSubRel = rewrittenSubRel.accept(new RelOptUtil.RelNodesExprsHandler(replacer));
            final List<RexNode> next = new ArrayList<>(rewrittenOperands.size());
            for (RexNode op : rewrittenOperands) {
                next.add(op.accept(replacer));
            }
            rewrittenOperands = next;
        }
        // Plain InputRefs in e.operands were in join-merged coords; move to R-local coords.
        final List<RexNode> shiftedOperands = new ArrayList<>(rewrittenOperands.size());
        for (RexNode op : rewrittenOperands) {
            shiftedOperands.add(RexUtil.shift(op, nFieldsLeft, -nFieldsLeft));
        }
        final RexSubQuery eShifted = new RexSubQuery(
            e.getType(), e.getOperator(), ImmutableList.copyOf(shiftedOperands), rewrittenSubRel);

        builder.push(join.getRight());
        final int sizeBefore = builder.peek().getRowType().getFieldCount();
        final RexNode targetInRight =
            rule.apply(eShifted, variablesSet, logic, builder, 1, nFieldsRight, 0);
        final int addedOnRight = builder.peek().getRowType().getFieldCount() - sizeBefore;
        final RelNode rightRewritten = builder.build();

        // In Join(L, R'), right-frame refs shift by nFieldsLeft; key off original `e`.
        final RexNode target = RexUtil.shift(targetInRight, 0, nFieldsLeft);
        final RexNode newCond = new ReplaceSubQueryShuttle(e, target).apply(join.getCondition());

        final RelNode newJoin = RelFactories.DEFAULT_JOIN_FACTORY.createJoin(
            join.getLeft(), rightRewritten, newCond,
            collectRemainingCorrelVars(newCond, join.getVariablesSet()),
            join.getJoinType(), join.isSemiJoinDone());
        builder.push(newJoin);

        if (addedOnRight > 0
            && join.getJoinType() != JoinRelType.SEMI
            && join.getJoinType() != JoinRelType.ANTI) {
            builder.project(builder.fields(
                ImmutableBitSet.range(0, nFieldsLeft + nFieldsRight).asList()));
        }
        call.transformTo(builder.build());
    }

    /**
     * Fallback when the sub-query straddles both sides: only INNER degenerates to
     * CrossProduct + Filter (handed off to FILTER in the next Hep iteration). Non-INNER has
     * no lossless degeneration due to NULL-padding.
     */
    private static void rewriteInnerBothSides(RelOptRuleCall call, Join join, RelBuilder builder) {
        if (join.getJoinType() != JoinRelType.INNER) {
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER,
                "HolSubqueryRemoveRule: join sub-query referencing both sides is not supported "
                    + "for joinType=" + join.getJoinType());
        }
        builder.push(join.getLeft());
        builder.push(join.getRight());
        builder.join(JoinRelType.INNER, builder.literal(true));
        builder.filter(join.getVariablesSet(), join.getCondition());
        call.transformTo(builder.build());
    }

    /**
     * Builds the concatenated row type of {@code leftType} followed by {@code rightType}.
     */
    private static RelDataType joinedRowType(RexBuilder rexBuilder,
                                             RelDataType leftType, RelDataType rightType) {
        return rexBuilder.getTypeFactory().builder()
            .addAll(leftType.getFieldList())
            .addAll(rightType.getFieldList())
            .build();
    }

    /**
     * Collects all correlation variables that are still referenced by remaining
     * {@link RexSubQuery} nodes in the given condition, intersected with the declared set.
     *
     * <p>This is used when reconstructing a Join after unnesting one sub-query: the new Join
     * must still declare the correlation variables needed by any remaining (not-yet-unnested)
     * sub-queries in the condition.
     *
     * @param condition The RexNode condition to scan for remaining RexSubQuery nodes
     * @param declared The set of correlation variables declared by the original Join
     * @return The subset of declared correlation variables referenced by remaining subqueries;
     * empty set if condition contains no RexSubQuery or declared is empty
     */
    static Set<CorrelationId> collectRemainingCorrelVars(RexNode condition, Set<CorrelationId> declared) {
        if (declared == null || declared.isEmpty()) {
            return ImmutableSet.of();
        }
        final Set<CorrelationId> result = Sets.newHashSet();
        condition.accept(new RexShuttle() {
            @Override
            public RexNode visitSubQuery(RexSubQuery subQuery) {
                result.addAll(RelOptUtil.getVariablesUsed(subQuery.rel));
                return super.visitSubQuery(subQuery);
            }
        });
        result.retainAll(declared);
        return result;
    }

    private static class ReplaceInputRefShuttle extends RexShuttle {
        private final RexBuilder rexBuilder;
        final RelDataType rowType;
        final CorrelationId correlationId;

        ReplaceInputRefShuttle(RexBuilder rexBuilder, RelDataType rowType, CorrelationId correlationId) {
            this.rexBuilder = rexBuilder;
            this.rowType = rowType;
            this.correlationId = correlationId;
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            RexNode v = rexBuilder.makeCorrel(rowType, correlationId);
            return rexBuilder.makeFieldAccess(v, inputRef.getIndex());
        }
    }

    /**
     * Shuttle that shifts refs pointing beyond the original left width by a fixed offset.
     * Handles two cases in a single pass:
     * 1. {@link RexInputRef} whose index {@code >= threshold} gets {@code index + shift}.
     * 2. {@link RexFieldAccess} under a {@link RexCorrelVariable} whose CorrelationId is
     * in {@code targetIds} and whose field index {@code >= threshold} gets its
     * CorrelVariable rebound to {@code newRowType} and field index {@code += shift}.
     *
     * <p>Refs with index below {@code threshold} (i.e. still within the original left side)
     * are left untouched.
     */
    private static class ShiftCorrelRefShuttle extends RexShuttle {
        private final RexBuilder rexBuilder;
        private final RelDataType newRowType;
        private final Set<Integer> targetIds;
        private final int threshold;
        private final int shift;
        private boolean insideSubQuery = false;

        ShiftCorrelRefShuttle(RexBuilder rexBuilder, RelDataType newRowType,
                              Set<CorrelationId> variablesSet, int threshold, int shift) {
            this.rexBuilder = rexBuilder;
            this.newRowType = newRowType;
            this.targetIds = variablesSet.stream().map(CorrelationId::getId).collect(Collectors.toSet());
            this.threshold = threshold;
            this.shift = shift;
        }

        @Override
        public RexNode visitSubQuery(RexSubQuery subQuery) {
            // Once we step into a sub-query, RexInputRef refers to the sub-query's
            // own RelNode coordinate space and must NOT be shifted by the outer
            // Join-side offset. Outer correlated refs (RexFieldAccess on a
            // RexCorrelVariable) are still rebased.

            // Rewrite operands (outer correlated refs, etc.).
            final boolean saved = insideSubQuery;
            RelNode newSub;
            try {
                insideSubQuery = true;
                newSub = subQuery.rel.accept(new RelOptUtil.RelNodesExprsHandler(this));
            } finally {
                insideSubQuery = saved;
            }
            boolean[] update = {newSub != subQuery.rel};
            List<RexNode> clonedOperands = visitList(subQuery.operands, update);
            if (update[0]) {
                return new RexSubQuery(subQuery.getType(), subQuery.getOperator(),
                    ImmutableList.copyOf(clonedOperands), newSub).setHasOptimized(subQuery.isHasOptimized());
            } else {
                return subQuery;
            }
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            if (insideSubQuery) {
                return super.visitInputRef(inputRef);
            }
            int idx = inputRef.getIndex();
            if (idx >= threshold) {
                return rexBuilder.makeInputRef(inputRef.getType(), idx + shift);
            }
            return super.visitInputRef(inputRef);
        }

        @Override
        public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
            RexNode refExpr = fieldAccess.getReferenceExpr();
            if (refExpr instanceof RexCorrelVariable
                && targetIds.contains(((RexCorrelVariable) refExpr).getId().getId())) {
                int idx = fieldAccess.getField().getIndex();
                if (idx >= threshold) {
                    CorrelationId id = ((RexCorrelVariable) refExpr).getId();
                    RexNode newCv = rexBuilder.makeCorrel(newRowType, id);
                    return rexBuilder.makeFieldAccess(newCv, idx + shift);
                }
            }
            return super.visitFieldAccess(fieldAccess);
        }
    }

    /**
     * Shuttle that replaces occurrences of a given
     * {@link org.apache.calcite.rex.RexSubQuery} with a replacement
     * expression.
     */
    private static class ReplaceSubQueryShuttle extends RexShuttle {
        private final RexSubQuery subQuery;
        private final RexNode replacement;

        ReplaceSubQueryShuttle(RexSubQuery subQuery, RexNode replacement) {
            this.subQuery = subQuery;
            this.replacement = replacement;
        }

        @Override
        public RexNode visitSubQuery(RexSubQuery subQuery) {
            return subQuery.equals(this.subQuery) ? replacement : subQuery;
        }
    }

}