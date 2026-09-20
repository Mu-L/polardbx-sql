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

package com.alibaba.polardbx.optimizer.core.join;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.optimizer.core.rel.MaterializedSemiJoin;
import com.alibaba.polardbx.optimizer.sharding.advisor.UnionFind;
import com.google.common.collect.Maps;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class LookupPredicateBuilder {

    private final Join join;

    private final LookupPredicate predicate;

    private final Map<String, Integer> rexNodeMap;

    // list is ordered as 'outer' -- 'inner' -- 'param'
    UnionFind unionFind;

    public LookupPredicateBuilder(Join join, List<String> lvOriginNames) {
        this.join = join;
        boolean notIn = (join.getJoinType() == JoinRelType.ANTI);
        this.predicate = new LookupPredicate(notIn, lvOriginNames);
        this.rexNodeMap = Maps.newHashMap();
    }

    public LookupPredicate build(List<LookupEquiJoinKey> joinKeys) {
        buildUnionFind();
        // try predicate pruning first
        if (unionFind != null) {
            buildInside(joinKeys);
            if (predicate.size() > 0) {
                return predicate;
            }
        }

        // predicate pruning failed, fall back to normal case
        unionFind = null;
        buildInside(joinKeys);
        return predicate;
    }

    private void buildInside(List<LookupEquiJoinKey> joinKeys) {
        Set<Integer> lookupColumnSet = new HashSet<>();
        for (LookupEquiJoinKey key : joinKeys) {
            if (key.isNullSafeEqual()) {
                RelDataType relDataTypeInner =
                    join.getInner().getRowType().getFieldList().get(key.getInnerIndex()).getType();
                RelDataType relDataTypeOuter =
                    join.getOuter().getRowType().getFieldList().get(key.getOuterIndex()).getType();
                if (relDataTypeInner.isNullable() || relDataTypeOuter.isNullable()) {
                    continue; // '<=>' semantics can not be represented as IN expression unless both join key are not nullable
                }
            }
            if (!key.isCanFindOriginalColumn()) {
                continue; // can not be represented as IN expression, because column origin is null
            }
            if (unionFind != null) {
                if (unionFind.find(key.getInnerIndex() + join.getOuter().getRowType().getFieldCount())
                    == unionFind.find(key.getOuterIndex())) {
                    continue;
                }
            }
            int targetIndex;
            if (join instanceof MaterializedSemiJoin) {
                // lookup on outer side, note that duplicate column retains
                String column = getJoinKeyColumnName(key);
                targetIndex = key.getInnerIndex();
                predicate.addEqualPredicate(
                    new SqlIdentifier(column, SqlParserPos.ZERO), targetIndex, key.getUnifiedType());
            } else {
                // lookup on inner side
                if (!lookupColumnSet.add(key.getInnerIndex())) {
                    continue;
                }
                String column = getJoinKeyColumnName(key);
                targetIndex = key.getOuterIndex();
                predicate.addEqualPredicate(
                    new SqlIdentifier(column, SqlParserPos.ZERO), targetIndex, key.getUnifiedType());
            }
        }
    }

    public static String getJoinKeyColumnName(LookupEquiJoinKey joinKey) {
        return joinKey.getLookupColunmnName();
    }

    private void buildRexNodeMap(List<RexNode> inputs, int joinInputSize) {
        for (RexNode rexNode : inputs) {
            if (rexNode.isA(SqlKind.EQUALS) && rexNode instanceof RexCall
                && ((RexCall) rexNode).operands.size() == 2) {
                RexCall rexCall = (RexCall) rexNode;
                if (rexCall.getOperands().get(0) instanceof RexDynamicParam ||
                    rexCall.getOperands().get(0) instanceof RexLiteral) {
                    String flag = rexCall.getOperands().get(1).toString();
                    rexNodeMap.putIfAbsent(flag, joinInputSize + rexNodeMap.size());
                } else if (rexCall.getOperands().get(1) instanceof RexLiteral ||
                    rexCall.getOperands().get(1) instanceof RexDynamicParam) {
                    String flag = rexCall.getOperands().get(1).toString();
                    rexNodeMap.putIfAbsent(flag, joinInputSize + rexNodeMap.size());
                }
            }
        }
    }

    private Integer getUnionFindIndex(RexNode rexNode, int refOffset) {
        if (rexNode instanceof RexInputRef) {
            return ((RexInputRef) rexNode).getIndex() + refOffset;
        }
        if (rexNode instanceof RexDynamicParam || rexNode instanceof RexLiteral) {
            String flag = rexNode.toString();
            return rexNodeMap.get(flag);
        }
        return null;
    }

    private void buildUnionFind() {
        unionFind = null;
        if (!InstConfUtil.getBool(ConnectionParams.ENABLE_SIMPLIFY_LOOKUP_JOIN)) {
            return;
        }
        RelMetadataQuery mq = join.getCluster().getMetadataQuery();
        RelNode outer = join.getOuter();
        RelNode inner = join.getInner();
        if (outer instanceof RelSubset || inner == null) {
            return;
        }
        RelOptPredicateList outerPreds = mq.getPulledUpPredicates(outer);
        RelOptPredicateList innerPreds = mq.getPulledUpPredicates(inner);

        if (outerPreds.pulledUpPredicates.isEmpty() || innerPreds.pulledUpPredicates.isEmpty()) {
            return;
        }

        final int outerFieldCount = outer.getRowType().getFieldCount();
        final int innerFieldCount = inner.getRowType().getFieldCount();
        final int joinInputSize = outerFieldCount + innerFieldCount;
        buildRexNodeMap(outerPreds.pulledUpPredicates, joinInputSize);
        buildRexNodeMap(innerPreds.pulledUpPredicates, joinInputSize);
        unionFind = new UnionFind(joinInputSize + rexNodeMap.size());
        findRexLiterNode(outerPreds.pulledUpPredicates, 0);
        findRexLiterNode(innerPreds.pulledUpPredicates, outerFieldCount);
    }

    private void findRexLiterNode(List<RexNode> inputs, int refOffset) {
        for (RexNode rexNode : inputs) {
            if (rexNode.isA(SqlKind.EQUALS) && rexNode instanceof RexCall
                && ((RexCall) rexNode).operands.size() == 2) {
                RexCall rexCall = (RexCall) rexNode;
                Integer leftIndex = getUnionFindIndex(rexCall.getOperands().get(0), refOffset);
                Integer rightIndex = getUnionFindIndex(rexCall.getOperands().get(1), refOffset);
                if (leftIndex != null && rightIndex != null) {
                    unionFind.union(leftIndex, rightIndex);
                }
            }
        }
    }
}
