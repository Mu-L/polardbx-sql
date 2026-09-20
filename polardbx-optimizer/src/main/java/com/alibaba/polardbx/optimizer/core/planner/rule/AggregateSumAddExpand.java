package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.TddlRelDataTypeSystemImpl;
import com.alibaba.polardbx.optimizer.core.TddlTypeFactoryImpl;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlCountAggFunction;
import org.apache.calcite.sql.fun.SqlSumAggFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.Pair;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author liugaoji
 * SELECT SUM(id), SUM(id+1), SUM(id+2) => SELECT SUM(id), SUM(id) + 1 * count(), SUM(id) + 2 * count()
 */
public class AggregateSumAddExpand extends RelOptRule {

    public static final AggregateSumAddExpand INSTANCE = new AggregateSumAddExpand(LogicalAggregate.class);
    //to avoid extra impact, we enable this RBO rule only when agg call number >= AGGCALL_THRESHOLD
    private static final int AGGCALL_THRESHOLD = 90;

    public AggregateSumAddExpand(Class<? extends Aggregate> aggregateClass) {
        super(
            operand(aggregateClass,
                operand(LogicalProject.class, any())),
            "AggregateSumAddExpand"
        );
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        return PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_SIMPLIFY_GROUP_BY_RULE);
    }

    public void onMatch(RelOptRuleCall call) {
        final LogicalAggregate aggregate = call.rel(0);
        final LogicalProject input = call.rel(1);
        RelBuilder relBuilder = call.builder();

        // check: 1. must be no group by 2. all the exps are sum aggcall
        if (!aggregate.getGroupSet().isEmpty()
            || (aggregate.getAggCallList().size() != input.getChildExps().size())
            || (aggregate.getAggCallList().size() < AGGCALL_THRESHOLD)
            || (!(input.getProjects().get(0) instanceof RexInputRef))) {
            return;
        }
        // 3. first agg call must be RexInputRef 4. the rest call are pointed to first agg call
        RexInputRef originCall = (RexInputRef) input.getProjects().get(0);
        for (int i = 1; i < input.getProjects().size(); i++) {
            // must like RexCall(+($20, ?0))
            if (!(input.getProjects().get(i) instanceof RexCall)) {
                return;
            }
            RexCall rexCall = (RexCall) input.getProjects().get(i);
            if ((!(rexCall.getOperands().get(0) instanceof RexInputRef))
                || !((RexInputRef) rexCall.getOperands().get(0)).getName().equals(originCall.getName())) {
                return;
            }
        }

        for (AggregateCall aggregateCall : aggregate.getAggCallList()) {
            if ((!(aggregateCall.getAggregation() instanceof SqlSumAggFunction))
                || (aggregateCall.getArgList().size() != 1)
                || (aggregateCall.getArgList().get(0) == null)) {
                return;
            }
        }

        // step0: capture the column info like sum(id + k)
        List<Integer> toRemoveId = new ArrayList<>();
        List<RexDynamicParam> toRemoveValue = new ArrayList<>();
        List<Integer> matchedRef = new ArrayList<>();
        List<AggregateCall> aggregateCalls = new ArrayList<>();
        List<RexInputRef> check = new ArrayList<>();
        for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
            AggregateCall aggregateCall = aggregate.getAggCallList().get(i);
            // sum(id)
            if (input.getChildExps().get(i) instanceof RexInputRef) {
                check.add((RexInputRef) input.getChildExps().get(i));
                // sum(id + x)
            } else if (input.getChildExps().get(i) instanceof RexCall) {
                RexCall exp = (RexCall) input.getChildExps().get(i);
                //1. only 2 operands. op 1 = inputRef 3. op2 = RexDynamic
                if (exp.getOperands().size() != 2
                    || !(exp.getOperands().get(0) instanceof RexInputRef)
                    || !(exp.getOperands().get(1) instanceof RexDynamicParam)
                    || !(exp.getOperands().get(1).getType().getSqlTypeName().equals(SqlTypeName.BIGINT))) {
                    return;
                }
                RexInputRef index = (RexInputRef) exp.getOperands().get(0);
                // aggCall like sum(id + constant) and sum(id) has evaluated
                if (check.contains(index)) {
                    RexDynamicParam dynamicParam = (RexDynamicParam) exp.getOperands().get(1);
                    // group(1) indicate the id
                    matchedRef.add(check.indexOf(index));
                    toRemoveId.add(i);
                    toRemoveValue.add(dynamicParam);
                    continue;
                }
            }
            aggregateCalls.add(aggregateCall);
        }
        relBuilder.push(input);
        //step1: delete removed group call
        TddlTypeFactoryImpl tddlTypeFactory = new TddlTypeFactoryImpl(TddlRelDataTypeSystemImpl.getInstance());
        aggregateCalls.add(AggregateCall.create(new SqlCountAggFunction("COUNT"), false, new ArrayList<>(), -1,
            tddlTypeFactory.createSqlType(
                SqlTypeName.BIGINT),
            "added_count"));
        relBuilder.aggregate(relBuilder.groupKey(), aggregateCalls);
        LogicalAggregate newAgg = (LogicalAggregate) relBuilder.build();
        relBuilder.push(newAgg);
        //step2: modify the project expr
        //sum(id + k) = sum(id) + k * count
        List<Pair<RexNode, String>> projects = new ArrayList<>();
        RexInputRef right = relBuilder.getRexBuilder().makeInputRef(newAgg, newAgg.getAggCallList().size() - 1);
        int index = 0, notRemoveIndex = 0;
        for (int i = 0; i < aggregate.getAggCallList().size(); i++) {
            if (!toRemoveId.contains(i)) {
                projects.add(Pair.of(relBuilder.getRexBuilder().makeInputRef(newAgg, notRemoveIndex++), ""));
            } else {
                Integer matchedIndex = matchedRef.get(index);
                RexBuilder rexBuilder = new RexBuilder(tddlTypeFactory);
                //id
                RexInputRef left = relBuilder.getRexBuilder().makeInputRef(newAgg, matchedIndex);
                //x * count()
                RexDynamicParam rexDynamicParam = toRemoveValue.get(index);
                RexNode multiply = rexBuilder.makeCall(right.getType(),
                    TddlOperatorTable.MULTIPLY, new ArrayList<>(Arrays.asList(right, rexDynamicParam)));
                //id + (x * count())
                RexNode add = rexBuilder.makeCall(newAgg.getRowType().getFieldList().get(matchedIndex).getType(),
                    TddlOperatorTable.PLUS, new ArrayList<>(Arrays.asList(left, multiply)));
                projects.add(
                    Pair.of(add, ""));
                index++;
            }
        }
        relBuilder.project(Pair.left(projects), Pair.right(projects));
        call.transformTo(relBuilder.build());
    }
}
