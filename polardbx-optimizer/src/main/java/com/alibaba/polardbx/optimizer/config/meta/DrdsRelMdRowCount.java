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

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.ExternalTableScan;
import com.alibaba.polardbx.optimizer.core.rel.GroupTopN;
import com.alibaba.polardbx.optimizer.core.rel.HashAgg;
import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.MysqlTableScan;
import com.alibaba.polardbx.optimizer.core.rel.PhysicalCTEConsumer;
import com.alibaba.polardbx.optimizer.core.rel.Xplan.XPlanTableScan;
import com.alibaba.polardbx.optimizer.view.ViewPlan;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.CTEAnchor;
import org.apache.calcite.rel.core.CTEProducer;
import org.apache.calcite.rel.core.DynamicValues;
import org.apache.calcite.rel.core.GroupJoin;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableLookup;
import org.apache.calcite.rel.logical.LogicalCTEConsumer;
import org.apache.calcite.rel.logical.LogicalExpand;
import org.apache.calcite.rel.logical.RuntimeFilterBuilder;
import org.apache.calcite.rel.metadata.ReflectiveRelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMdRowCount;
import org.apache.calcite.rel.metadata.RelMetadataProvider;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.Map;
import java.util.Optional;

import static com.alibaba.polardbx.optimizer.utils.OptimizerUtils.getParametersMapForOptimizer;

public class DrdsRelMdRowCount extends RelMdRowCount {

    public static final RelMetadataProvider SOURCE =
        ReflectiveRelMetadataProvider.reflectiveSource(
            BuiltInMethod.ROW_COUNT.method, new DrdsRelMdRowCount());

    private final static Logger logger = LoggerFactory.getLogger(DrdsRelMdRowCount.class);

    public Double getRowCount(LogicalView rel, RelMetadataQuery mq) {
        return rel.getRowCount(mq);
    }

    public Double getRowCount(ViewPlan rel, RelMetadataQuery mq) {
        return mq.getRowCount(rel.getPlan());
    }

    public Double getRowCount(MysqlTableScan rel, RelMetadataQuery mq) {
        return mq.getRowCount(rel.getNodeForMetaQuery());
    }

    public Double getRowCount(CTEAnchor rel, RelMetadataQuery mq) {
        return mq.getRowCount(rel.getRight());
    }

    public Double getRowCount(CTEProducer rel, RelMetadataQuery mq) {
        return mq.getRowCount(rel.getInput());
    }

    public Double getRowCount(LogicalCTEConsumer rel, RelMetadataQuery mq) {
        return mq.getRowCount(rel.getInnerRel());
    }

    public Double getRowCount(PhysicalCTEConsumer rel, RelMetadataQuery mq) {
        Double rowCount = mq.getRowCount(CBOUtil.getCteProducer(rel));
        if (rowCount != null && !rel.getConditions().isEmpty()) {
            RexNode condition = RexUtil.composeConjunction(
                rel.getCluster().getRexBuilder(), rel.getConditions(), true);
            if (condition != null) {
                Double selectivity = mq.getSelectivity(CBOUtil.getCteProducer(rel), condition);
                if (selectivity != null) {
                    rowCount *= selectivity;
                }
            }
        }
        return rowCount;
    }

    public Double getRowCount(ExternalTableScan rel, RelMetadataQuery mq) {
        return rel.getRowCount(mq);
    }

    public Double getRowCount(XPlanTableScan rel, RelMetadataQuery mq) {
        return mq.getRowCount(rel.getNodeForMetaQuery());
    }

    public Double getRowCount(GroupTopN rel, RelMetadataQuery mq) {
        Double rowCount = mq.getRowCount(rel.getInput());
        if (rowCount == null) {
            return null;
        }
        ImmutableBitSet groupKey = rel.getGroupSet();

        int shard = 1;
        if ((rel.isPartial())) {
            if (CBOUtil.isColumnarOptimizer(rel)) {
                // partition wise
                shard = PlannerContext.getPlannerContext(rel).getColumnarMaxShardCnt();
            } else {
                shard = PlannerContext.getPlannerContext(rel).getParamManager().
                    getInt(ConnectionParams.PARTIAL_AGG_SHARD);
            }
        }
        // rowCount is the cardinality of the group by columns
        Double distinctRowCount =
            mq.getDistinctRowCount(rel.getInput(), groupKey, null);
        if (distinctRowCount == null || distinctRowCount > rowCount + 1D) {
            distinctRowCount = mq.getRowCount(rel.getInput()) / 10;
        }

        double rowPerGroup = rowCount / distinctRowCount / shard;
        Map<Integer, ParameterContext> params = getParametersMapForOptimizer(rel);
        long offset = 0;
        if (rel.getOffset() != null) {
            offset = CBOUtil.getRexParam(rel.getOffset(), params);
        }
        double result = Math.max(rowPerGroup - offset, 0D);
        if (rel.getFetch() != null) {
            long limit = Math.max(CBOUtil.getRexParam(rel.getFetch(), params), 1);
            result = Math.min((double) limit, result);
        }
        result = Math.max(result, 1D) * distinctRowCount * shard;
        result = Math.min(result, rowCount);
        return result;
    }

    @Override
    public Double getRowCount(Sort rel, RelMetadataQuery mq) {
        Double rowCount = mq.getRowCount(rel.getInput());
        if (rowCount == null) {
            return null;
        }

        Map<Integer, ParameterContext> params = getParametersMapForOptimizer(rel);

        long offset = 0;
        if (rel.offset != null) {
            offset = CBOUtil.getRexParam(rel.offset, params);
        }

        rowCount = Math.max(rowCount - offset, 0D);

        if (rel.fetch != null) {
            long limit = Math.max(CBOUtil.getRexParam(rel.fetch, params), 1);
            if (limit < rowCount) {
                return (double) limit;
            }
        }
        return rowCount;
    }

    public Double getRowCountColumnarPartialAgg(Aggregate rel, RelMetadataQuery mq) {
        // special path for columnar partial agg
        if (!(rel instanceof HashAgg)) {
            return null;
        }

        if (!(((HashAgg) rel).isPartial())) {
            return null;
        }

        int shard;
        if (CBOUtil.isColumnarOptimizer(rel)) {
            // partition wise
            shard = PlannerContext.getPlannerContext(rel).getColumnarMaxShardCnt();
        } else {
            shard = PlannerContext.getPlannerContext(rel).getParamManager().getInt(ConnectionParams.PARTIAL_AGG_SHARD);
        }

        ImmutableBitSet groupKey = rel.getGroupSet();
        Double distinctRowCount =
            mq.getDistinctRowCount(rel.getInput(), groupKey, null);
        if (distinctRowCount == null || distinctRowCount < 1) {
            return null;
        }
        double total = mq.getRowCount(rel.getInput());

        double bins = distinctRowCount * rel.getGroupSets().size();
        if (bins < 1) {
            return null;
        }
        long balls = (long) (total / shard);
        double base = 1 - 1D / bins;
        double pow = 1D;
        while (balls > 0) {
            if (balls % 2 == 1) {
                pow *= base;
            }
            base *= base;
            balls >>= 1;
        }
        return bins * (1 - pow) * shard;
    }

    @Override
    public Double getRowCount(Aggregate rel, RelMetadataQuery mq) {
        Double value = getRowCountColumnarPartialAgg(rel, mq);
        if (value != null) {
            return value;
        }
        ImmutableBitSet groupKey = rel.getGroupSet(); // .range(rel.getGroupCount());

        // rowCount is the cardinality of the group by columns
        Double distinctRowCount =
            mq.getDistinctRowCount(rel.getInput(), groupKey, null);
        double rowCount = mq.getRowCount(rel.getInput());
        if (distinctRowCount == null || distinctRowCount > rowCount + 1D) {
            distinctRowCount = mq.getRowCount(rel.getInput()) / 10;
        }

        // Grouping sets multiply
        distinctRowCount *= rel.getGroupSets().size();
        return distinctRowCount;
    }

    @Override
    public Double getRowCount(DynamicValues rel, RelMetadataQuery mq) {
        DynamicValues.RawStringMode mode = rel.getRawStringMode();
        if (mode == null) {
            // Unmarked nodes: DML direct VALUES sources (deliberately unmarked) and plans
            // deserialized from older versions. Re-detect so folded VALUES keep reporting the
            // RawString array length instead of falling back to the one-row default.
            mode = LogicalDynamicValues.detectRawStringMode(
                getParametersMapForOptimizer(rel), rel.getRowType(), rel.getTuples());
        }
        if (mode != null) {
            Double rowCount = getRawStringRowCount(rel);
            if (rowCount != null) {
                return rowCount;
            }
        }
        return super.getRowCount(rel, mq);
    }

    /**
     * In raw-string mode the row count is the length of the RawString array backing the first
     * column's dynamic param. The shape is extracted through
     * LogicalDynamicValues.extractLiteralDynamicParam; a non-matching shape (e.g. a marker
     * surviving a tuple-rewriting rule) reports null so the caller falls back to the default
     * row count instead of crashing the optimizer.
     */
    private Double getRawStringRowCount(DynamicValues rel) {
        if (rel.tuples.isEmpty() || rel.tuples.get(0).isEmpty()) {
            return null;
        }
        RexDynamicParam dynamicParam = LogicalDynamicValues.extractLiteralDynamicParam(rel.tuples.get(0).get(0));
        if (dynamicParam == null) {
            return null;
        }
        Map<Integer, ParameterContext> params = getParametersMapForOptimizer(rel);
        Object arg = Optional.ofNullable(params)
            .map(map -> map.get(dynamicParam.getIndex() + 1))
            .map(ParameterContext::getValue)
            .orElse(null);
        return arg instanceof RawString ? (double) ((RawString) arg).size() : null;
    }

    public Double getRowCount(GroupJoin rel, RelMetadataQuery mq) {
        ImmutableBitSet groupKey = rel.getGroupSet(); // .range(rel.getGroupCount());

        final int[] ints = groupKey.toArray();
        int min = ints[0];
        int max = ints[ints.length - 1];
        final long leftLength = rel.getLeft().getRowType().getFieldCount();
        // rowCount is the cardinality of the group by columns
        Double distinctRowCount = null;
        distinctRowCount =
            mq.getDistinctRowCount(rel.copyAsJoin(rel.getTraitSet(), rel.getCondition()), groupKey, null);
        final Join input = rel.copyAsJoin(rel.getTraitSet(), rel.getCondition());
        double rowCount = mq.getRowCount(input);
        if (distinctRowCount == null || distinctRowCount > rowCount) {
            distinctRowCount = rowCount / 10;
        }

        // Grouping sets multiply
        distinctRowCount *= rel.getGroupSets().size();
        return distinctRowCount;
    }

    public Double getRowCount(TableLookup rel, RelMetadataQuery mq) {
        if (rel.isRelPushedToPrimary()) {
            return mq.getRowCount(rel.getProject());
        } else {
            return mq.getRowCount(rel.getJoin().getLeft());
        }
    }

    public Double getRowCount(LogicalExpand rel, RelMetadataQuery mq) {
        return rel.estimateRowCount(mq);
    }

    public Double getRowCount(RuntimeFilterBuilder rel, RelMetadataQuery mq) {
        return mq.getRowCount(rel.getInput());
    }
}
