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

package com.alibaba.polardbx.optimizer.config.meta;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.ExternalTableScan;
import com.alibaba.polardbx.optimizer.core.rel.GroupTopN;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MysqlTableScan;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalCTEConsumer;
import com.alibaba.polardbx.optimizer.core.rel.Xplan.XPlanTableScan;
import com.alibaba.polardbx.optimizer.selectivity.JoinSelectivityEstimator;
import com.alibaba.polardbx.optimizer.selectivity.TableScanSelectivityEstimator;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.core.CTEAnchor;
import org.apache.calcite.rel.core.CTEProducer;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.TableLookup;
import org.apache.calcite.rel.logical.LogicalCTEConsumer;
import org.apache.calcite.rel.logical.LogicalExpand;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.RuntimeFilterBuilder;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdSelectivity;
import org.apache.calcite.rel.metadata.RelMdUtil;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.BuiltInMethod;

import java.util.ArrayList;
import java.util.List;

public class DrdsRelMdSelectivity extends RelMdSelectivity {
    /**
     * make sure you have overridden the SOURCE
     */
    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.SELECTIVITY.method, new DrdsRelMdSelectivity());

    private final static Logger logger = LoggerFactory.getLogger(DrdsRelMdSelectivity.class);

    public Double getSelectivity(Join rel, RelMetadataQuery mq, RexNode predicate) {
        if (predicate == null || predicate.isAlwaysTrue()) {
            return 1.0;
        } else {
            JoinSelectivityEstimator selectivityEstimator = new JoinSelectivityEstimator(rel, mq);
            Double selectivity = selectivityEstimator.evaluate(predicate);
            if (selectivity == null) {
                selectivity = RelMdUtil.guessSelectivity(predicate);
            }
            return selectivity;
        }
    }

    public Double getSelectivity(TableLookup rel, RelMetadataQuery mq, RexNode predicate) {
        return mq.getSelectivity(rel.getProject(), predicate);
    }

    public Double getSelectivity(LogicalView rel, RelMetadataQuery mq, RexNode predicate) {
        return rel.getSelectivity(mq, predicate);
    }

    public Double getSelectivity(LogicalTableScan rel, RelMetadataQuery mq, RexNode predicate) {
        if (predicate == null || predicate.isAlwaysTrue()) {
            return 1.0;
        } else {
            TableScanSelectivityEstimator tableScanSelectivityEstimator = new TableScanSelectivityEstimator(rel, mq);
            Double selectivity = tableScanSelectivityEstimator.evaluate(predicate);
            if (selectivity == null) {
                selectivity = RelMdUtil.guessSelectivity(predicate);
            }
            return selectivity;
        }
    }

    public Double getSelectivity(GroupTopN rel, RelMetadataQuery mq,
                                 RexNode predicate) {
        final List<RexNode> notPushable = new ArrayList<>();
        final List<RexNode> pushable = new ArrayList<>();
        RelOptUtil.splitFilters(
            rel.getGroupSet(),
            predicate,
            pushable,
            notPushable);
        final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
        RexNode childPred =
            RexUtil.composeConjunction(rexBuilder, pushable, true);

        Double selectivity = mq.getSelectivity(rel.getInput(), childPred);
        if (selectivity == null) {
            return null;
        } else {
            RexNode pred =
                RexUtil.composeConjunction(rexBuilder, notPushable, true);
            return selectivity * RelMdUtil.guessSelectivity(pred);
        }
    }

    public Double getSelectivity(CTEAnchor rel, RelMetadataQuery mq, RexNode predicate) {
        return mq.getSelectivity(rel.getRight(), predicate);
    }

    public Double getSelectivity(CTEProducer rel, RelMetadataQuery mq, RexNode predicate) {
        return mq.getSelectivity(rel.getInput(), predicate);
    }

    public Double getSelectivity(LogicalCTEConsumer rel, RelMetadataQuery mq, RexNode predicate) {
        return mq.getSelectivity(rel.getInnerRel(), predicate);
    }

    public Double getSelectivity(PhysicalCTEConsumer rel, RelMetadataQuery mq, RexNode predicate) {
        // Remap predicate through projects to producer's column space
        List<RexNode> projects = rel.getProjects();
        if (projects != null && !projects.isEmpty() && predicate != null && !predicate.isAlwaysTrue()) {
            predicate = predicate.accept(new RexShuttle() {
                @Override
                public RexNode visitInputRef(RexInputRef ref) {
                    if (ref.getIndex() < projects.size()) {
                        return projects.get(ref.getIndex());
                    }
                    return ref;
                }
            });
        }

        // When conditions exist, compute conditional selectivity:
        // P(predicate | conditions) = selectivity(predicate AND conditions) / selectivity(conditions)
        // This avoids double-counting with rowCount(consumer) which already factors in selectivity(conditions).
        List<RexNode> conditions = rel.getConditions();
        if (conditions != null && !conditions.isEmpty() && predicate != null && !predicate.isAlwaysTrue()) {
            RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
            RexNode conditionExpr = RexUtil.composeConjunction(rexBuilder, conditions, true);
            if (conditionExpr != null) {
                Double selectivityC = mq.getSelectivity(CBOUtil.getCteProducer(rel), conditionExpr);
                if (selectivityC != null && selectivityC > 0) {
                    List<RexNode> combined = new ArrayList<>();
                    combined.add(predicate);
                    combined.add(conditionExpr);
                    RexNode combinedPredicate = RexUtil.composeConjunction(rexBuilder, combined, true);
                    Double selectivityPC = mq.getSelectivity(CBOUtil.getCteProducer(rel), combinedPredicate);
                    if (selectivityPC != null) {
                        return Math.min(selectivityPC / selectivityC, 1.0);
                    }
                }
            }
        }

        return mq.getSelectivity(CBOUtil.getCteProducer(rel), predicate);
    }

    public static int getColumnIndex(TableMeta tableMeta, ColumnMeta columnMeta) {
        return tableMeta.getAllColumns().indexOf(columnMeta);
    }

    public static boolean isPrimaryKeyAutoIncrement(TableMeta tableMeta) {
        return tableMeta.isHasPrimaryKey()
            && tableMeta.getPrimaryIndex().getKeyColumns().size() == 1
            && tableMeta.getPrimaryIndex().getKeyColumns().get(0).getField().isAutoIncrement();
    }

    public Double getSelectivity(LogicalExpand rel, RelMetadataQuery mq, RexNode predicate) {
        return mq.getSelectivity(rel.getInput(), predicate);
    }

    public Double getSelectivity(MysqlTableScan rel, RelMetadataQuery mq, RexNode predicate) {
        return mq.getSelectivity(rel.getNodeForMetaQuery(), predicate);
    }

    public Double getSelectivity(ExternalTableScan rel, RelMetadataQuery mq, RexNode predicate) {
        return rel.getSelectivity(mq, predicate);
    }

    public Double getSelectivity(XPlanTableScan rel, RelMetadataQuery mq, RexNode predicate) {
        return mq.getSelectivity(rel.getNodeForMetaQuery(), predicate);
    }

    public Double getSelectivity(RuntimeFilterBuilder rel, RelMetadataQuery mq,
                                 RexNode predicate) {
        return mq.getSelectivity(rel.getInput(), predicate);
    }
//
//    public Double getSelectivity(PhysicalProject rel, RelMetadataQuery mq,
//                                 RexNode predicate) {
//        final List<RexNode> notPushable = new ArrayList<>();
//        final List<RexNode> pushable = new ArrayList<>();
//        RelOptUtil.splitFilters(
//            ImmutableBitSet.range(rel.getRowType().getFieldCount()),
//            predicate,
//            pushable,
//            notPushable);
//        final RexBuilder rexBuilder = rel.getCluster().getRexBuilder();
//        RexNode childPred =
//            RexUtil.composeConjunction(rexBuilder, pushable, true);
//
//        RexNode modifiedPred;
//        if (childPred == null) {
//            modifiedPred = null;
//        } else {
//            modifiedPred = RelOptUtil.pushPastProject(childPred, rel);
//        }
//        Double selectivity = mq.getSelectivity(rel.getInput(), modifiedPred);
//        if (selectivity == null) {
//            return null;
//        } else {
//            RexNode pred =
//                RexUtil.composeConjunction(rexBuilder, notPushable, true);
//            return selectivity * RelMdUtil.guessSelectivity(pred);
//        }
//    }
}
