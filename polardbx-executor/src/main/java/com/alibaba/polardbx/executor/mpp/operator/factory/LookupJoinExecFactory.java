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

package com.alibaba.polardbx.executor.mpp.operator.factory;

import com.alibaba.polardbx.common.utils.ExecutorMode;
import com.alibaba.polardbx.executor.operator.Executor;
import com.alibaba.polardbx.executor.operator.LookupJoinExec;
import com.alibaba.polardbx.executor.operator.LookupJoinGsiExec;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinUtils;
import com.alibaba.polardbx.optimizer.core.rel.BKAJoin;
import com.alibaba.polardbx.optimizer.core.rel.Gather;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.SemiBKAJoin;
import com.alibaba.polardbx.optimizer.utils.RexUtils;
import com.alibaba.polardbx.stats.metric.FeatureStats;
import com.alibaba.polardbx.stats.metric.FeatureStatsItem;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.optimizer.core.join.EquiJoinUtils.existLookupGsiSide;

public class LookupJoinExecFactory extends ExecutorFactory {

    private Join join;
    private List<EquiJoinKey> allJoinKeys; // including null-safe equal (`<=>`)
    private boolean maxOneRow;

    public LookupJoinExecFactory(Join join, ExecutorFactory outerFactory, ExecutorFactory innerFactory) {
        if (join instanceof SemiBKAJoin) {
            this.maxOneRow = join.getJoinType() == JoinRelType.LEFT || join.getJoinType() == JoinRelType.INNER;
        } else {
            this.maxOneRow = false;
        }
        this.join = join;
        this.allJoinKeys = EquiJoinUtils.buildEquiJoinKeys(join, join.getOuter(), join.getInner(),
            (RexCall) join.getCondition(), join.getJoinType());
        addInput(innerFactory);
        addInput(outerFactory);
    }

    @Override
    public Executor createExecutor(ExecutionContext context, int index) {
        Executor ret;
        Executor inner;
        boolean allowMultiReadConn = ExecUtils.allowMultipleReadConns(context, null);

        Executor outer = getInputs().get(1).createExecutor(context, index);
        if (getInputs().get(1) instanceof LogicalViewExecutorFactory) {
            LogicalView outerLv = ((LogicalViewExecutorFactory) getInputs().get(1)).getLogicalView();
            allowMultiReadConn = allowMultiReadConn && ExecUtils.allowMultipleReadConns(context, outerLv);
        }

        int shardCount = -1;
        int parallelism = 1;
        if (getInputs().get(0) instanceof LogicalViewExecutorFactory) {
            LogicalViewExecutorFactory innerLvExecFactory = (LogicalViewExecutorFactory) getInputs().get(0);
            LogicalView innerLv = innerLvExecFactory.getLogicalView();
            allowMultiReadConn = allowMultiReadConn && ExecUtils.allowMultipleReadConns(context, innerLv);

            // 分库 -> List[List[一个物理 SQL 中的所有表]]]
            Map<String, List<List<String>>> targetTables = innerLv.getTargetTables(context);
            shardCount = targetTables.values().stream().mapToInt(List::size).sum();
            parallelism = innerLvExecFactory.getParallelism();
        }

        boolean isLookUpGsi = existLookupGsiSide(join);
        boolean isAdaptiveLookupOptimizationReady = isAdaptiveLookupOptimizationReady(join, context, parallelism);

        inner = getInputs().get(0).createExecutor(context, index);
        IExpression otherCondition = convertExpression(join.getCondition(), context);

        if (!isLookUpGsi) {
            FeatureStats.getInstance().increment(FeatureStatsItem.GSI_LOOKUP_TIMES);
            ret = new LookupJoinExec(outer, inner, join.getJoinType(), maxOneRow, allJoinKeys, allJoinKeys,
                otherCondition, context, shardCount, parallelism, allowMultiReadConn,
                isAdaptiveLookupOptimizationReady);
        } else {
            ret = new LookupJoinGsiExec(outer, inner, join.getJoinType(), maxOneRow, allJoinKeys, allJoinKeys,
                otherCondition, context, shardCount, parallelism, allowMultiReadConn);
        }

        registerRuntimeStat(ret, join, context);
        return ret;
    }

    public static boolean isAdaptiveLookupOptimizationReady(Join join, ExecutionContext ec, int parallelism) {
        // Early return for non-BKAJoin types
        if (!(join instanceof BKAJoin)) {
            return false;
        }
        if (parallelism > 1) {
            return false;
        }
        if (ec.getExecuteMode() == ExecutorMode.MPP) {
            return false;
        }

        BKAJoin bkaJoin = (BKAJoin) join;

        // If already computed, return cached result
        Boolean cachedResult = bkaJoin.isAdaptiveLookupOptimizationReady();
        if (cachedResult != null) {
            return cachedResult;
        }

        // Compute the result
        boolean isReady = computeAdaptiveLookupOptimizationReadiness(bkaJoin);

        // Cache and return the result
        return bkaJoin.setAdaptiveLookupOptimizationReady(isReady);
    }

    /**
     * Computes whether adaptive lookup optimization is ready for the given BKAJoin.
     *
     * @param bkaJoin the BKA join to check
     * @return true if adaptive lookup optimization is ready, false otherwise
     */
    private static boolean computeAdaptiveLookupOptimizationReadiness(BKAJoin bkaJoin) {
        // join must be switched, which meaning that all column join returned must be from main table relation
        if (!bkaJoin.isHasSwitched()) {
            return false;
        }

        // Extract LogicalView from inner relation
        LogicalView logicalView = extractLogicalView(bkaJoin.getInner());
        if (logicalView == null) {
            return false;
        }

        // Check if lookup info exists and has GSI local index
        return logicalView.getLookupInfo() != null
            && logicalView.getLookupInfo().isPrimaryHasGsiLocalIndex();
    }

    /**
     * Extracts LogicalView from RelNode, handling both direct LogicalView and Gather wrapping
     *
     * @param relNode the relation node to extract from
     * @return LogicalView if found, null otherwise
     */
    private static LogicalView extractLogicalView(RelNode relNode) {
        if (relNode instanceof LogicalView) {
            return (LogicalView) relNode;
        }

        if (relNode instanceof Gather) {
            RelNode input = ((Gather) relNode).getInput();
            return input instanceof LogicalView ? (LogicalView) input : null;
        }

        return null;
    }

    private IExpression convertExpression(RexNode rexNode, ExecutionContext context) {
        return RexUtils.buildRexNode(rexNode, context, new ArrayList<>());
    }
}
