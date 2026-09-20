package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.rule.cte.CTEUtil;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.PushUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.optimizer.utils.TableTopologyUtil;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.logical.LogicalCorrelate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalValues;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.trace.OptimizerPhase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static com.alibaba.polardbx.common.properties.ConnectionParams.PUSH_CORRELATE_MATERIALIZED_LIMIT;
import static org.apache.calcite.sql.SqlKind.EQUALS;
import static org.apache.calcite.sql.SqlKind.INSERT;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.IN;
import static org.apache.calcite.sql.fun.SqlStdOperatorTable.ROW;

/**
 * @author fangwu
 */
public class PushCorrelateRule extends RelOptRule {
    private boolean pushToValues = false;

    public PushCorrelateRule(RelOptRuleOperand operand, String description, boolean pushToValues) {
        super(operand, "PushCorrelateRule:" + description);
        this.pushToValues = pushToValues;
    }

    public static final PushCorrelateRule INSTANCE = new PushCorrelateRule(
        operand(Correlate.class, some(operand(LogicalView.class, none()), operand(RelNode.class, any()))),
        "INSTANCE", false);

    public static final PushCorrelateRule INSTANCE_VALUES = new PushCorrelateRule(
        operand(Correlate.class, some(operand(LogicalValues.class, none()), operand(RelNode.class, any()))),
        "INSTANCE", true);

    @Override
    public boolean matches(RelOptRuleCall call) {
        if (!pushToValues) {
            final LogicalView leftView = (LogicalView) call.rels[1];
            if (leftView instanceof OSSTableScan) {
                return false;
            }
        }

        if (!PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_PUSH_CORRELATE)) {
            return false;
        }
        return true;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalCorrelate logicalCorrelate = (LogicalCorrelate) call.rels[0];
        RelNode rightPlan = call.rels[2];
        if (RelOptUtil.findCorrelates(rightPlan).size() > 0) {
            return;
        }
        if (RelOptUtil.anyCte(rightPlan)) {
            return;
        }
        if (CTEUtil.findCte(rightPlan)) {
            return;
        }
        if (pushToValues) {
            if (!PlannerContext.getPlannerContext(logicalCorrelate).getExecutionContext().getParamManager()
                .getBoolean(ConnectionParams.ENABLE_TRANS_CORRELATE_TO_VALUES)) {
                return;
            }
            if (PlannerContext.getPlannerContext(logicalCorrelate).getSqlKind() == INSERT) {
                return;
            }
            handlePushToLogicalValues(call, logicalCorrelate, rightPlan);
        } else {
            handlePushToLogicalView(call, logicalCorrelate, rightPlan);
        }
    }

    private void handlePushToLogicalValues(RelOptRuleCall call, LogicalCorrelate logicalCorrelate, RelNode rightPlan) {
        final LogicalValues leftValues = (LogicalValues) call.rels[1];
        if (leftValues.getRowType().getFieldList().size() == 1 &&
            leftValues.getRowType().getFieldList().get(0).getName().equals("ZERO") &&
            leftValues.getTuples().size() == 1) {

            int rightFieldCount = rightPlan.getRowType().getFieldCount();
            List<RexNode> projects = new ArrayList<>();
            List<String> names = new ArrayList<>();
            projects.add(leftValues.getTuples().get(0).get(0));
            names.add("ZERO");
            for (int i = 0; i < rightFieldCount; i++) {
                projects.add(call.builder().getRexBuilder().makeInputRef(rightPlan, i));
                names.add(rightPlan.getRowType().getFieldList().get(i).getName());
            }
            call.transformTo(LogicalProject.create(rightPlan, projects, names));
        }
    }

    private void handlePushToLogicalView(RelOptRuleCall call, LogicalCorrelate logicalCorrelate, RelNode rightPlan) {
        final LogicalView leftView = (LogicalView) call.rels[1];
        RelBuilder relBuilder = call.builder();

        // single table pushdown
        boolean allSingleTable = false;
        Set<RelOptTable> tables = RelOptUtil.findTables(leftView.getPushedRelNode());
        tables.addAll(RelOptUtil.findTables(rightPlan));
        RelNode subqueryRel = rightPlan;

        // judge if one materialized apply plan should be built
        boolean materializedPath = false;

        if (!TableTopologyUtil.isAllSingleTableInSamePhysicalDB(tables)) {
            // meaning has correlate columns
            if (RelOptUtil.getVariablesUsed(rightPlan).size() > 0) {
                return;
            }

            // for IN subquery
            ParamManager paramManager = PlannerContext.getPlannerContext(rightPlan).getParamManager();
            int limit = paramManager.getInt(PUSH_CORRELATE_MATERIALIZED_LIMIT);
            if (logicalCorrelate.getJoinType() == SemiJoinType.SEMI) {
                if (logicalCorrelate.getLeftConditions() == null ||
                    logicalCorrelate.getLeftConditions().size() != 1 ||
                    logicalCorrelate.getOpKind() != EQUALS ||
                    logicalCorrelate.getRight().getRowType().getFieldList().get(0).getType().getSqlTypeName()
                        == SqlTypeName.FLOAT ||
                    subqueryRel.estimateRowCount(subqueryRel.getCluster().getMetadataQuery()) > limit) {
                    return;
                }
                materializedPath = true;
                // optimize path for in subquery
            } else if (logicalCorrelate.getJoinType() == SemiJoinType.ANTI) {
                // not support pushing down anti subquery
                return;
            }
        } else {
            if (PlannerContext.getPlannerContext(call).getParamManager()
                .getBoolean(ConnectionParams.ENABLE_CHECK_PUSH_CORRELATE)) {
                if (!PushUtil.canPushTree(rightPlan)) {
                    return;
                }
            }
            allSingleTable = true;
        }

        rightPlan = RelUtils.removeHepRelVertex(rightPlan);
        LogicalView newLogicalView = leftView.copy(leftView.getTraitSet().replace(Convention.NONE));
        relBuilder.push(newLogicalView);
        List<RexNode> projects = (List<RexNode>) relBuilder.getRexBuilder().identityProjects(leftView.getRowType());
        final ImmutableList.Builder<RexNode> builder = ImmutableList.builder();
        final PlannerContext rightCtx = PlannerContext.getPlannerContext(rightPlan);
        rightCtx.optimizerTrace(x -> x.beginPhaseSnapshot(OptimizerPhase.PUSH_CORRELATE));
        try {
            rightPlan = Planner.getInstance().optimizeBySqlWriter(rightPlan, rightCtx);
        } finally {
            final RelNode endPlan = rightPlan;
            rightCtx.optimizerTrace(x -> x.endPhaseSnapshot(endPlan, rightCtx));
        }
        RexDynamicParam rexDynamicParam =
            relBuilder.getRexBuilder()
                .makeDynamicParam(logicalCorrelate.getJoinType() == SemiJoinType.LEFT ?
                        relBuilder.getTypeFactory()
                            .createTypeWithNullability(rightPlan.getRowType().getFieldList().get(0).getType(), true) :
                        logicalCorrelate.getCluster().getTypeFactory().createSqlType(SqlTypeName.BIGINT), -2,
                    rightPlan);

        rexDynamicParam.setSemiType(logicalCorrelate.getJoinType());

        if (allSingleTable) {
            // LeftCondition and opKind need to be separeted when transforming correlate to subquery
            // This reversion need these two attributes in order to let physical sql working.
            rexDynamicParam.setLeftCondition(logicalCorrelate.getLeftConditions());
            rexDynamicParam.setSubqueryKind(logicalCorrelate.getOpKind());
        }
        builder.addAll(projects);
        // `not all single table` case do not support correlate columns

        if (materializedPath) {
            // Materialized optimize option for IN subquery
            RexBuilder rexBuilder = relBuilder.getRexBuilder();
            rexDynamicParam =
                (RexDynamicParam) rexBuilder
                    .makeDynamicParam(rexDynamicParam.getRel().getRowType().getFieldList().get(0).getType(),
                        rexDynamicParam.getIndex(), rexDynamicParam.getRel()
                        , rexDynamicParam.getSubqueryOperands(), rexDynamicParam.getSubqueryOp(),
                        rexDynamicParam.getSubqueryKind());
            rexDynamicParam.setSemiType(SemiJoinType.LEFT);
            rexDynamicParam.setMaxOnerow(false);
            builder.add(rexBuilder.makeCall(IN, logicalCorrelate.getLeftConditions().get(0),
                rexBuilder.makeCall(ROW, rexDynamicParam)));
        } else {
            builder.add(rexDynamicParam);
        }

        List<RexNode> newProjects = builder.build();

        if (allSingleTable) {
            call.transformTo(relBuilder
                .project(newProjects, Collections.emptyList(), ImmutableSet.of(logicalCorrelate.getCorrelationId()))
                .build());
        } else {
            call.transformTo(relBuilder
                .project(newProjects, Collections.emptyList())
                .build());
        }
    }
}
