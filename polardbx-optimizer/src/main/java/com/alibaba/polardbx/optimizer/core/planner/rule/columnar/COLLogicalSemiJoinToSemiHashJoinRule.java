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

package com.alibaba.polardbx.optimizer.core.planner.rule.columnar;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalSemiJoinToSemiHashJoinRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.mpp.RuleUtils;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.HashAgg;
import com.alibaba.polardbx.optimizer.core.rel.HashWindow;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalFilter;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalProject;
import com.alibaba.polardbx.optimizer.core.rel.SemiHashJoin;
import com.alibaba.polardbx.optimizer.hint.operator.HintType;
import com.alibaba.polardbx.optimizer.hint.util.CheckJoinHint;
import com.alibaba.polardbx.optimizer.utils.CalciteUtils;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.SemiJoin;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.rules.ProjectRemoveRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.IntPair;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class COLLogicalSemiJoinToSemiHashJoinRule extends LogicalSemiJoinToSemiHashJoinRule {

    public static final LogicalSemiJoinToSemiHashJoinRule INSTANCE =
        new COLLogicalSemiJoinToSemiHashJoinRule("INSTANCE");

    public static final LogicalSemiJoinToSemiHashJoinRule OUTER_INSTANCE =
        new COLLogicalSemiJoinToSemiHashJoinRule(true, "OUTER_INSTANCE");

    COLLogicalSemiJoinToSemiHashJoinRule(String desc) {
        super("COL_" + desc);
        this.outConvention = CBOUtil.getColConvention();
    }

    COLLogicalSemiJoinToSemiHashJoinRule(boolean outDriver, String desc) {
        super(outDriver, "COL_" + desc);
        this.outConvention = CBOUtil.getColConvention();
    }

    @Override
    protected void createSemiHashJoin(RelOptRuleCall call,
                                      LogicalSemiJoin semiJoin,
                                      RelNode left,
                                      RelNode right,
                                      RexNode newCondition,
                                      CBOUtil.RexNodeHolder equalConditionHolder,
                                      CBOUtil.RexNodeHolder otherConditionHolder) {
        List<Pair<RelDistribution, Pair<RelNode, RelNode>>> implementationList = new ArrayList<>();
        JoinInfo joinInfo = JoinInfo.of(left, right, equalConditionHolder.getRexNode());
        List<Pair<List<Integer>, List<Integer>>> keyPairList = new ArrayList<>();
        for (IntPair pair : joinInfo.pairs()) {
            keyPairList.add(Pair.of(ImmutableIntList.of(pair.source), ImmutableIntList.of(pair.target)));
        }

        if (PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_PARTITION_WISE)
            && PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_PARTITION_WISE_JOIN)) {
            CBOUtil.columnarHashDistribution(keyPairList, semiJoin, left, right, null, implementationList);
        }
        // implementationList may be empty, in this case use none partition wise join
        if (CollectionUtils.isEmpty(implementationList)) {
            nonePartitionWiseJoin(semiJoin, left, right, joinInfo, implementationList);
        }

        if (PlannerContext.getPlannerContext(call).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_BROADCAST_JOIN)) {
            CBOUtil.columnarBroadcastDistribution(semiJoin, left, right, implementationList);
        }

        if (semiJoin.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE) != RelDistributions.SINGLETON) {
            reverseBroadcast(call,
                semiJoin,
                left,
                right,
                newCondition,
                equalConditionHolder,
                otherConditionHolder);
        }
        for (Pair<RelDistribution, Pair<RelNode, RelNode>> implementation : implementationList) {
            SemiHashJoin semiHashJoin = SemiHashJoin.create(
                semiJoin.getTraitSet().replace(outConvention).replace(implementation.getKey()),
                implementation.getValue().getKey(),
                implementation.getValue().getValue(),
                newCondition,
                semiJoin,
                equalConditionHolder.getRexNode(),
                otherConditionHolder.getRexNode(),
                outDriver);

            RelOptCost fixedCost = CheckJoinHint.check(semiJoin, HintType.CMD_SEMI_HASH_JOIN);
            if (fixedCost != null) {
                semiHashJoin.setFixedCost(fixedCost);
            }
            if (semiJoin.getTraitSet().getTrait(RelDistributionTraitDef.INSTANCE) == RelDistributions.SINGLETON) {
                call.transformTo(
                    convert(semiHashJoin, semiHashJoin.getTraitSet().replace(RelDistributions.SINGLETON)));
            } else {
                call.transformTo(semiHashJoin);
            }
        }
    }

    private void nonePartitionWiseJoin(SemiJoin semiJoin,
                                       RelNode left,
                                       RelNode right,
                                       JoinInfo joinInfo,
                                       List<Pair<RelDistribution, Pair<RelNode, RelNode>>> implementationList) {
        if (CollectionUtils.isEmpty(implementationList)) {
            RelDataType keyDataType = CalciteUtils.getJoinKeyDataType(
                semiJoin.getCluster().getTypeFactory(), semiJoin, joinInfo.leftKeys, joinInfo.rightKeys);
            RelNode hashLeft = RuleUtils.ensureKeyDataTypeDistribution(left, keyDataType, joinInfo.leftKeys);
            RelNode hashRight = RuleUtils.ensureKeyDataTypeDistribution(right, keyDataType, joinInfo.rightKeys);
            implementationList.add(Pair.of(hashLeft.getTraitSet().getDistribution(), Pair.of(hashLeft, hashRight)));
        }
    }

    /**
     * build broadcast_left semi join right
     */
    protected void reverseBroadcast(RelOptRuleCall call,
                                    LogicalSemiJoin semiJoin,
                                    RelNode left,
                                    RelNode right,
                                    RexNode newCondition,
                                    CBOUtil.RexNodeHolder equalConditionHolder,
                                    CBOUtil.RexNodeHolder otherConditionHolder) {
        if (!this.outDriver) {
            return;
        }
        ParamManager paramManager = PlannerContext.getPlannerContext(semiJoin).getParamManager();
        if (!paramManager.getBoolean(ConnectionParams.ENABLE_BROADCAST_JOIN)) {
            return;
        }
        if (!paramManager.getBoolean(ConnectionParams.ENABLE_REVERSE_BROADCAST_SEMI_HASH_JOIN)) {
            return;
        }
        if (semiJoin.getJoinType() != JoinRelType.SEMI) {
            return;
        }

        // cost based pruning
        RelMetadataQuery mq = semiJoin.getCluster().getMetadataQuery();
        RelOptCost shuffleCost =
            CBOUtil.mockColExchangeCost(mq, semiJoin, RelDistributions.hashOss(ImmutableList.of(1), 10))
                .plus(CBOUtil.mockColExchangeCost(mq, left, RelDistributions.hashOss(ImmutableList.of(1), 10)))
                .plus(CBOUtil.mockColExchangeCost(mq, right, RelDistributions.hashOss(ImmutableList.of(1), 10)));
        RelOptCost broadcastCost = CBOUtil.mockColExchangeCost(mq, left, RelDistributions.BROADCAST_DISTRIBUTED);
        if (shuffleCost.isLt(broadcastCost)) {
            return;
        }

        //SemiJoin(semi)
        //  Left
        //  Right
        RelNode broadcastLeft =
            RelOptRule.convert(left, left.getTraitSet().replace(RelDistributions.BROADCAST_DISTRIBUTED));
        RelNode newRight = RelOptRule.convert(right, right.getTraitSet().replace(RelDistributions.ANY));
        SemiHashJoin semiHashJoin = SemiHashJoin.create(
            semiJoin.getTraitSet().simplify().replace(outConvention).replace(RelDistributions.ANY),
            broadcastLeft,
            newRight,
            newCondition,
            semiJoin,
            equalConditionHolder.getRexNode(),
            otherConditionHolder.getRexNode(),
            outDriver);

        //left has global unique key
        if (reverseBroadcastUk(call, semiHashJoin, paramManager)) {
            return;
        }

        //left doesn't have global unique key
        reverseBroadcastNoneUk(call, semiHashJoin, paramManager);
    }

    /**
     * build broadcast_left semi join right while left has global unique key
     */
    protected boolean reverseBroadcastUk(RelOptRuleCall call,
                                         SemiHashJoin semiHashJoin,
                                         ParamManager paramManager) {
        //Project
        //  HashAgg(group="uk")
        //    Exchange(hash(any column of uk))
        //      SemiHashJoin(semi, outer)
        //        Exchange(broadcast)
        //          Left
        //        Right

        Set<ImmutableBitSet> leftUK =
            semiHashJoin.getCluster().getMetadataQuery().getUniqueKeys(semiHashJoin.getLeft());
        if (CollectionUtils.isNotEmpty(leftUK)) {
            ImmutableBitSet firstUK = leftUK.stream().findFirst().get();
            List<Pair<RelDistribution, RelDistribution>> implementationList = genImplementation(firstUK, semiHashJoin,
                paramManager.getBoolean(ConnectionParams.ENABLE_PARTITION_WISE)
                    && paramManager.getBoolean(ConnectionParams.ENABLE_PARTITION_WISE_AGG));

            Map<Integer, Integer> map = Maps.newHashMap();
            for (int i : firstUK) {
                map.put(i, map.size());
            }
            List<AggregateCall> aggCallList = Lists.newArrayList();
            for (int i = 0; i < semiHashJoin.getRowType().getFieldCount(); i++) {
                RelDataTypeField field = semiHashJoin.getRowType().getFieldList().get(i);
                if (firstUK.get(i)) {
                    continue;
                }
                aggCallList.add(AggregateCall.create(SqlStdOperatorTable.__FIRST_VALUE,
                    false,
                    false,
                    ImmutableIntList.of(i),
                    -1,
                    field.getType(),
                    field.getName()));
                map.put(i, map.size());
            }
            for (Pair<RelDistribution, RelDistribution> implementation : implementationList) {
                // hash agg
                HashAgg hashAgg = HashAgg.create(
                    call.getPlanner().emptyTraitSet().simplify().replace(outConvention)
                        .replace(implementation.getKey()),
                    convert(semiHashJoin, semiHashJoin.getTraitSet().replace(implementation.getValue())),
                    firstUK,
                    ImmutableList.of(firstUK),
                    aggCallList);

                // project
                List<RexNode> projects = Lists.newArrayList();
                final RelDataTypeFactory.Builder builder = semiHashJoin.getCluster().getTypeFactory().builder();
                List<RelDataTypeField> aggRowTypeList = hashAgg.getRowType().getFieldList();
                for (int i = 0; i < semiHashJoin.getRowType().getFieldCount(); i++) {
                    final RelDataTypeField relDataTypeField = aggRowTypeList.get(map.get(i));
                    projects.add(RexInputRef.of(map.get(i), hashAgg.getRowType()));
                    builder.add(relDataTypeField);
                }

                Mappings.TargetMapping mapping = Mappings.target(
                    map, semiHashJoin.getRowType().getFieldCount(), hashAgg.getRowType().getFieldCount());
                RelDistribution projectDis = hashAgg.getTraitSet().getDistribution().apply(mapping);
                PhysicalProject physicalProject = new PhysicalProject(
                    hashAgg.getCluster(), hashAgg.getTraitSet().replace(projectDis),
                    hashAgg, projects, builder.build());
                if (ProjectRemoveRule.isTrivial(physicalProject)) {
                    call.transformTo(hashAgg);
                } else {
                    call.transformTo(physicalProject);
                }
            }
            return true;
        }
        return false;
    }

    /**
     * build broadcast_left semi join right while left doesn't have global unique key
     */
    protected void reverseBroadcastNoneUk(RelOptRuleCall call,
                                          SemiHashJoin semiHashJoin,
                                          ParamManager paramManager) {
        //Project(all columns except node_id)
        //  Filter(node_id=min_node_id)
        //    HashWindow(min(node_id) partition by all columns except node_id) as min_node_id
        //       Exchange(hash(any column except node_id))
        //         Project(*, node_id)
        //          SemiHashJoin(semi, outer)
        //            Exchange(broadcast)
        //              Left
        //            Right
        int originCols = semiHashJoin.getRowType().getFieldCount();
        RexBuilder rexBuilder = semiHashJoin.getCluster().getRexBuilder();

        // Project(*, node_id)
        List<RexNode> preProjects = Lists.newArrayList();
        final RelDataTypeFactory.Builder preTypeBuilder = semiHashJoin.getCluster().getTypeFactory().builder();
        List<RelDataTypeField> inputRowTypeList = semiHashJoin.getRowType().getFieldList();
        for (int i = 0; i < originCols; i++) {
            preProjects.add(RexInputRef.of(i, inputRowTypeList));
            preTypeBuilder.add(inputRowTypeList.get(i));
        }
        RexCall nodeId = (RexCall) rexBuilder.makeCall(TddlOperatorTable.NODE_ID);
        preProjects.add(nodeId);
        preTypeBuilder.add("$f" + originCols, nodeId.getType());
        PhysicalProject physicalProject = new PhysicalProject(
            semiHashJoin.getCluster(), semiHashJoin.getTraitSet(), semiHashJoin, preProjects, preTypeBuilder.build());

        ImmutableBitSet groupSet = ImmutableBitSet.range(originCols);
        List<Pair<RelDistribution, RelDistribution>> implementationList = genImplementation(groupSet, semiHashJoin,
            paramManager.getBoolean(ConnectionParams.ENABLE_PARTITION_WISE)
                && paramManager.getBoolean(ConnectionParams.ENABLE_PARTITION_WISE_WINDOW));

        for (Pair<RelDistribution, RelDistribution> implementation : implementationList) {
            // HashWindow(min(node_id) partition by all columns except node_id) as min_node_id
            RelDataTypeField field = physicalProject.getRowType().getFieldList().get(originCols);
            // build agg
            AggregateCall aggCall = AggregateCall.create(SqlStdOperatorTable.MIN,
                false,
                false,
                ImmutableIntList.of(originCols),
                -1,
                field.getType(),
                field.getName());
            // build partition by
            List<RexNode> partitionKeys = Lists.newArrayList();
            for (int i = 0; i < originCols; i++) {
                partitionKeys.add(RexInputRef.of(i, physicalProject.getRowType()));
            }
            // build over
            RexOver over = (RexOver) rexBuilder.makeOver(aggCall.type, aggCall.getAggregation(),
                ImmutableList.of(RexInputRef.of(originCols, physicalProject.getRowType())),
                partitionKeys,
                RexWindowBound.UNBOUNDED_PRECEDING,
                RexWindowBound.UNBOUNDED_FOLLOWING,
                false
            );
            final Window.RexWinAggCall winAggCall =
                new Window.RexWinAggCall(
                    over.getAggOperator(),
                    over.getType(),
                    over.getOperands(),
                    0,
                    over.isDistinct());

            // build window group
            Window.Group windowGroup =
                new Window.Group(groupSet, over.getWindow().isRows(), RexWindowBound.UNBOUNDED_PRECEDING,
                    RexWindowBound.UNBOUNDED_PRECEDING, RelCollations.of(), ImmutableList.of(winAggCall));

            final List<Map.Entry<String, RelDataType>> fieldList =
                new ArrayList<>(physicalProject.getRowType().getFieldList());
            fieldList.add(
                org.apache.calcite.util.Pair.of("min_node_id_$" + (originCols + 1), winAggCall.getType()));
            final RelDataType windowRowType =
                physicalProject.getCluster().getTypeFactory().createStructType(fieldList);

            HashWindow hashWindow = HashWindow.create(
                call.getPlanner().emptyTraitSet().simplify().replace(outConvention).replace(implementation.getKey()),
                convert(physicalProject, physicalProject.getTraitSet().simplify().replace(implementation.getValue())),
                ImmutableList.of(),
                ImmutableList.of(windowGroup),
                windowRowType);

            // Filter(node_id=min_node_id)
            PhysicalFilter physicalFilter = new PhysicalFilter(
                hashWindow.getCluster(),
                hashWindow.getTraitSet().simplify(),
                hashWindow,
                rexBuilder.makeCall(TddlOperatorTable.EQUALS,
                    RexInputRef.of(originCols, hashWindow.getRowType()),
                    RexInputRef.of(originCols + 1, hashWindow.getRowType())),
                ImmutableSet.of());

            // Project(all columns except node_id)
            List<RexNode> finalProjects = Lists.newArrayList();
            final RelDataTypeFactory.Builder finalTypeBuilder = semiHashJoin.getCluster().getTypeFactory().builder();
            for (int i = 0; i < originCols; i++) {
                finalProjects.add(RexInputRef.of(i, physicalFilter.getRowType()));
                finalTypeBuilder.add(physicalFilter.getRowType().getFieldList().get(i));
            }
            PhysicalProject finalProject = new PhysicalProject(
                physicalFilter.getCluster(), physicalFilter.getTraitSet(), physicalFilter, finalProjects,
                finalTypeBuilder.build());
            call.transformTo(finalProject);
        }
    }

    protected List<Pair<RelDistribution, RelDistribution>> genImplementation(ImmutableBitSet groupSet,
                                                                             RelNode input,
                                                                             boolean enablePartitionWise) {
        List<Pair<RelDistribution, RelDistribution>> implementationList = new ArrayList<>();
        int maxShard = PlannerContext.getPlannerContext(input).getColumnarMaxShardCnt();
        // partition wise first
        if (enablePartitionWise) {
            int inputLoc = -1;
            for (int i = 0; i < groupSet.cardinality(); i++) {
                inputLoc = groupSet.nextSetBit(inputLoc + 1);
                // ignore small group column
                if (CBOUtil.groupSmall(input, ImmutableList.of(inputLoc))) {
                    continue;
                }
                RelDistribution aggDistribution = RelDistributions.hashOss(ImmutableList.of(i), maxShard);
                RelDistribution inputDistribution = RelDistributions.hashOss(ImmutableList.of(inputLoc), maxShard);
                implementationList.add(Pair.of(aggDistribution, inputDistribution));
            }
        }
        if (implementationList.isEmpty()) {
            RelDistribution aggDistribution =
                RelDistributions.hash(ImmutableIntList.identity(groupSet.cardinality()));
            RelDistribution inputDistribution = RelDistributions.hash(groupSet.toList());
            implementationList.add(Pair.of(aggDistribution, inputDistribution));
        }
        return implementationList;
    }
}
