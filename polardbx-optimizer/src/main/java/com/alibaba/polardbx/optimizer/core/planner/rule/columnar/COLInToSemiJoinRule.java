package com.alibaba.polardbx.optimizer.core.planner.rule.columnar;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.utils.DrdsRexFolder;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.DynamicValues;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.tools.RelBuilder;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

import static com.alibaba.polardbx.optimizer.utils.OptimizerUtils.getParametersForOptimizer;
import static com.alibaba.polardbx.optimizer.utils.OptimizerUtils.getParametersMapForOptimizer;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.AND;

public class COLInToSemiJoinRule extends RelOptRule {

    public static final COLInToSemiJoinRule INSTANCE = new COLInToSemiJoinRule();

    public COLInToSemiJoinRule() {
        super(operand(LogicalFilter.class,
            operand(OSSTableScan.class, null, none())), "COLInToSemiJoinRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        if (RexUtil.hasSubQuery(filter.getCondition())) {
            return false;
        }
        List<RexNode> conds = RelOptUtil.conjunctions(filter.getCondition());
        PlannerContext pc = PlannerContext.getPlannerContext(filter.getCluster());
        long threshold = pc.getParamManager().getLong(ConnectionParams.COL_IN_SEMIJOIN_THRESHOLD);
        for (RexNode cond : conds) {
            if (shouldUseSemiJoin(cond, pc, threshold)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        List<RexNode> conds = RelOptUtil.conjunctions(filter.getCondition());
        List<RexNode> restConds = Lists.newArrayList();
        List<RexNode> semiJoinConds = Lists.newArrayList();
        PlannerContext pc = PlannerContext.getPlannerContext(filter.getCluster());
        long threshold = pc.getParamManager().getLong(ConnectionParams.COL_IN_SEMIJOIN_THRESHOLD);
        for (RexNode cond : conds) {
            if (shouldUseSemiJoin(cond, pc, threshold)) {
                semiJoinConds.add(cond);
            } else {
                restConds.add(cond);
            }
        }
        if (CollectionUtils.isEmpty(semiJoinConds)) {
            return;
        }
        RelBuilder rb = call.builder();
        RexBuilder rexBuilder = call.builder().getRexBuilder();

        RelNode left = filter.getInput();
        if (!CollectionUtils.isEmpty(restConds)) {
            left = filter.copy(filter.getTraitSet(), filter.getInput(),
                restConds.size() == 1 ? restConds.get(0) : rexBuilder.makeCall(AND, restConds));
        }

        for (RexNode cond : semiJoinConds) {
            final ImmutableList.Builder<ImmutableList<RexNode>> tupleList = ImmutableList.builder();
            RexNode leftRex = ((RexCall) cond).getOperands().get(0);
            RexNode rightRex = ((RexCall) cond).getOperands().get(1);

            // build dynamic values
            List<RexNode> rows = ((RexCall) rightRex).getOperands();
            RelDataType rowType = null;
            if (rows.size() > 1) {
                // not raw string
                for (RexNode inner : rows) {
                    if (inner instanceof RexCall && inner.getKind() == SqlKind.ROW) {
                        tupleList.add(ImmutableList.<RexNode>builder().addAll(
                            ((RexCall) inner).getOperands()).build());
                    } else {
                        tupleList.add(ImmutableList.of(inner));
                    }
                    rowType = inner.getType();
                }
            } else {
                // raw string
                tupleList.add(ImmutableList.of(rows.get(0)));
                RawString rawString = getRawString(pc, (RexDynamicParam) rows.get(0));
                List<SqlTypeName> newTypeNames = getRawStringSqlType(rawString.getObjList().get(0));
                final RelDataTypeFactory.Builder builder = filter
                    .getCluster()
                    .getTypeFactory()
                    .builder();
                for (int i = 0; i < newTypeNames.size(); i++) {
                    builder.add("$f" + i, newTypeNames.get(i));
                }
                rowType = builder.build();
            }
            RelDataType rightRowType = SqlTypeUtil.promoteToRowType(
                rexBuilder.getTypeFactory(), rowType, "");
            ImmutableList<ImmutableList<RexNode>> tuples = tupleList.build();
            DynamicValues dynamicValues = DynamicValues.create(
                filter.getCluster(), filter.getCluster().traitSet(), rightRowType, tuples,
                LogicalDynamicValues.detectRawStringMode(
                    getParametersMapForOptimizer(filter), rightRowType, tuples));

            // build semi join condition
            List<RexNode> current = new ArrayList<>();
            if (leftRex instanceof RexInputRef) {
                current.add(rb.equals(leftRex,
                    RexUtil.shift(
                        RexInputRef.of(0, dynamicValues.getRowType()),
                        left.getRowType().getFieldCount())));
            } else {
                for (int i = 0; i < ((RexCall) leftRex).getOperands().size(); i++) {
                    current.add(rb.equals(((RexCall) leftRex).getOperands().get(i),
                        RexUtil.shift(RexInputRef.of(i, dynamicValues.getRowType()),
                            left.getRowType().getFieldCount())));
                }
            }

            // build semi join
            rb.push(left);
            rb.push(dynamicValues);
            left = rb.logicalSemiJoin(current,
                SqlStdOperatorTable.EQUALS,
                cond.getKind() == SqlKind.IN ? JoinRelType.SEMI : JoinRelType.ANTI,
                ImmutableList.of(),
                ImmutableSet.of(),
                new SqlNodeList(SqlParserPos.ZERO),
                "filter").build();
        }
        call.transformTo(left);
    }

    public static boolean shouldUseSemiJoin(RexNode cond, PlannerContext pc, long threshold) {
        if ((cond instanceof RexCall) &&
            (cond.getKind() == SqlKind.IN || cond.getKind() == SqlKind.NOT_IN)) {
            RexCall rexcall = (RexCall) cond;
            if (rexcall.getOperands().size() != 2) {
                return false;
            }
            RexNode leftRex = rexcall.getOperands().get(0);
            RexNode rightRex = rexcall.getOperands().get(1);
            List<RexNode> rows = null;
            // (x, y) in ((?,?))
            if (leftRex.getKind() == SqlKind.ROW && rightRex.getKind() == SqlKind.ROW
                && leftRex instanceof RexCall && rightRex instanceof RexCall) {
                if (!pc.getParamManager().getBoolean(ConnectionParams.ENABLE_COL_MULTI_IN_SEMIJOIN)) {
                    return false;
                }
                // left must be input reference only
                for (RexNode ref : ((RexCall) leftRex).getOperands()) {
                    if (!(ref instanceof RexInputRef)) {
                        return false;
                    }
                    // don't support string type
                    if ((!pc.getParamManager().getBoolean(ConnectionParams.ENABLE_COL_IN_SEMIJOIN_STRING))
                        && DataTypeUtil.isStringType(DataTypeUtil.calciteToDrdsType(ref.getType()))) {
                        return false;
                    }
                }
                rows = ((RexCall) rightRex).getOperands();
            }
            // x in (?)
            if (leftRex instanceof RexInputRef
                && (pc.getParamManager().getBoolean(ConnectionParams.ENABLE_COL_IN_SEMIJOIN_STRING)
                || !DataTypeUtil.isStringType(DataTypeUtil.calciteToDrdsType(leftRex.getType())))
                && rightRex.getKind() == SqlKind.ROW) {
                rows = ((RexCall) rightRex).getOperands();
            }

            if (CollectionUtils.isEmpty(rows)) {
                return false;
            }
            // check
            if (rows.size() > 1) {
                // not raw string
                if (DrdsRexFolder.fold(rightRex, pc) == null) {
                    return false;
                }
                // check row type
                RelDataType rowType = null;
                for (RexNode inner : rows) {
                    if (rowType != null && !rowType.equalsSansFieldNames(inner.getType())) {
                        return false;
                    }
                    rowType = inner.getType();
                }
                // check threshold
                return rows.size() >= threshold;
            } else if (rows.size() == 1) {
                // raw string
                if (!(rows.get(0) instanceof RexDynamicParam)) {
                    return false;
                }
                RawString value = getRawString(pc, (RexDynamicParam) rows.get(0));
                if (value == null) {
                    return false;
                }
                if (value.size() == 0) {
                    return false;
                }
                // check row type
                List<SqlTypeName> typeNames = null;
                for (Object object : value.getObjList()) {
                    List<SqlTypeName> newTypeNames = getRawStringSqlType(object);
                    if (newTypeNames == null) {
                        return false;
                    }
                    if (typeNames != null && !typeNames.equals(newTypeNames)) {
                        return false;
                    }
                    typeNames = newTypeNames;
                }
                // check threshold
                return value.size() >= threshold;
            }
        }
        return false;
    }

    public static RawString getRawString(PlannerContext pc, RexDynamicParam rexDynamicParam) {
        Parameters parameters = getParametersForOptimizer(pc);
        if (parameters == null) {
            return null;
        }
        int index = rexDynamicParam.getIndex();
        if (index < 0) {
            return null;
        }
        ParameterContext parameterContext = parameters.getCurrentParameter().get(index + 1);
        if (parameterContext == null) {
            return null;
        }
        Object value = parameterContext.getValue();
        return value instanceof RawString ? (RawString) value : null;
    }

    private static List<SqlTypeName> getRawStringSqlType(Object object) {
        List<SqlTypeName> newTypeNames = Lists.newArrayList();
        if (object instanceof List) {
            for (Object listItem : (List<?>) object) {
                if (!(listItem instanceof String || listItem instanceof Number)) {
                    return null;
                }
                newTypeNames.add(DataTypeUtil.typeNameOfParam(listItem));
            }
        } else if (object instanceof String || object instanceof Number) {
            newTypeNames.add(DataTypeUtil.typeNameOfParam(object));
        } else {
            // unknown mode.
            return null;
        }
        return newTypeNames;
    }
}