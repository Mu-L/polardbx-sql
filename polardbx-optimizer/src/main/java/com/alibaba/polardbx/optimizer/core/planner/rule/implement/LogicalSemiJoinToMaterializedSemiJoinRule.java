/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.optimizer.core.planner.rule.implement;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.planner.rule.JoinConditionSimplifyRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalProject;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptRuleOperand;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;
import java.util.Map;

public abstract class LogicalSemiJoinToMaterializedSemiJoinRule extends RelOptRule {
    protected Convention outConvention = DrdsConvention.INSTANCE;

    protected LogicalSemiJoinToMaterializedSemiJoinRule(RelOptRuleOperand operand, String desc) {
        super(operand, "LogicalSemiJoinToMaterializedSemiJoinRule:" + desc);
    }

    @Override
    public Convention getOutConvention() {
        return outConvention;
    }

    private boolean enable(PlannerContext plannerContext) {
        return plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_MATERIALIZED_SEMI_JOIN);
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        if (!RelOptUtil.NO_COLLATION_AND_DISTRIBUTION.test(call.rel(0))) {
            return false;
        }
        if (call.rel(1) instanceof OSSTableScan) {
            return false;
        }
        return enable(PlannerContext.getPlannerContext(call));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalSemiJoin semiJoin = call.rel(0);
        final LogicalView logicalView = call.rel(1);
        if (logicalView instanceof OSSTableScan) {
            return;
        }
        RelNode right = semiJoin.getRight();

        RexNode newCondition =
            JoinConditionSimplifyRule.simplifyCondition(semiJoin.getCondition(), semiJoin.getCluster().getRexBuilder());

        if (!RexUtils
            .isBatchKeysAccessCondition(semiJoin, newCondition,
                semiJoin.getLeft().getRowType().getFieldCount(),
                RexUtils.RestrictType.LEFT,
                LogicalSemiJoinToMaterializedSemiJoinRule::typeCheck, true)) {
            return;
        }
        if (!canMaterializedSemiJoin(semiJoin)) {
            return;
        }

        RelTraitSet inputTraitSet = right.getCluster().getPlanner().emptyTraitSet().replace(outConvention);
        LogicalView left = logicalView.copy(inputTraitSet);
        right = convert(right, inputTraitSet);

        ImmutableBitSet inputBitSet = RelOptUtil.InputFinder.bits(newCondition);
        List<RexNode> projects = Lists.newArrayList();
        Map<Integer, Integer> mappings = Maps.newHashMap();
        int leftTypeCount = left.getRowType().getFieldCount();
        final RelDataTypeFactory.Builder typeBuilder = right.getCluster().getTypeFactory().builder();
        List<RelDataTypeField> inputRowTypeList = right.getRowType().getFieldList();
        int cnt = leftTypeCount;
        for (int i : inputBitSet) {
            if (i < leftTypeCount) {
                continue;
            }
            mappings.put(i, cnt++);
            projects.add(RexInputRef.of(i - leftTypeCount, inputRowTypeList));
            typeBuilder.add(inputRowTypeList.get(i - leftTypeCount));
        }
        if (projects.size() < inputRowTypeList.size()) {
            right = new PhysicalProject(right.getCluster(), inputTraitSet, right, projects, typeBuilder.build());
        }

        newCondition = RexUtil.shift(newCondition, mappings);

        ImmutableBitSet rightBitSet = ImmutableBitSet.range(0, right.getRowType().getFieldCount());
        Boolean rightInputUnique = semiJoin.getCluster().getMetadataQuery().areColumnsUnique(right, rightBitSet);
        boolean distinctInput = rightInputUnique == null ? true : !rightInputUnique;

        createMaterializedSemiJoin(
            call,
            semiJoin,
            left,
            right,
            newCondition,
            distinctInput);
    }

    protected abstract void createMaterializedSemiJoin(
        RelOptRuleCall call,
        LogicalSemiJoin join,
        LogicalView left,
        RelNode right,
        RexNode newCondition,
        boolean distinctInput);

    public static boolean typeCheck(Pair<RelDataType, RelDataType> relDataTypePair) {
        RelDataType relDataType1 = relDataTypePair.getKey();
        RelDataType relDataType2 = relDataTypePair.getValue();
        DataType dt1 = DataTypeUtil.calciteToDrdsType(relDataType1);
        DataType dt2 = DataTypeUtil.calciteToDrdsType(relDataType2);

        if (dt1 == null || dt2 == null) {
            return false;
        }

        if (DataTypeUtil.isFloatSqlType(dt2)) {
            // float number can not be materialized
            return false;
        } else if (DataTypeUtil.isNumberSqlType(dt1)) {
            if (!DataTypeUtil.isStringSqlType(dt2) && !DataTypeUtil.isNumberSqlType(dt2)) {
                // if dt2 neither of number or string may mis-match with dt1
                return false;
            } else {
                return true;
            }
        } else if (DataTypeUtil.equalsSemantically(dt1, DataTypes.BooleanType)) {
            // BooleanType may come from tinyint(1) which will mis-match
            if (!DataTypeUtil.isStringSqlType(dt2)
                && !DataTypeUtil.isNumberSqlType(dt2)
                && !DataTypeUtil.equalsSemantically(dt2, DataTypes.BooleanType)) {
                return false;
            } else {
                return true;
            }
        } else if (DataTypeUtil.equalsSemantically(dt1, dt2)) {
            return true;
        } else {
            return false;
        }
    }

    public static boolean canMaterializedSemiJoin(LogicalSemiJoin join) {
        if (join.getJoinType() != JoinRelType.SEMI && join.getJoinType() != JoinRelType.ANTI) {
            return false;
        }

        if (join.getJoinType() == JoinRelType.ANTI && join.getOperands().isEmpty()) {
            return false;
        }

        RelNode left = join.getLeft();
        if (left instanceof RelSubset) {
            left = ((RelSubset) left).getOriginal();
        }

        if (CBOUtil.checkBkaJoinForLogicalView(left)) {
            return true;
        }

        return false;
    }
}
