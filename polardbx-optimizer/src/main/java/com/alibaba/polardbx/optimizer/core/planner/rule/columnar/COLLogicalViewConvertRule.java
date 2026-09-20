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
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.planner.rule.implement.LogicalViewConvertRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.utils.TableTopologyUtil;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.rel.RelDistribution;
import org.apache.calcite.rel.RelDistributions;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.util.ImmutableBitSet;

import java.util.List;

public class COLLogicalViewConvertRule extends LogicalViewConvertRule {
    public static final LogicalViewConvertRule INSTANCE = new COLLogicalViewConvertRule("INSTANCE");

    COLLogicalViewConvertRule(String desc) {
        super("COL_" + desc);
        this.outConvention = CBOUtil.getColConvention();
    }

    @Override
    protected void createLogicalview(RelOptRuleCall call, LogicalView logicalView) {
        if (!(logicalView instanceof OSSTableScan)) {
            LogicalView newLogicalView = logicalView.copy(logicalView.getTraitSet().simplify().replace(outConvention));
            newLogicalView.optimize();
            call.transformTo(newLogicalView);
            return;
        }

        Pair<Integer, Integer> pair = getShardColumn((OSSTableScan) logicalView);
        if (pair == null) {
            basicOssTableScan(call, logicalView);
            return;
        }
        int target = pair.getKey();
        int shard = pair.getValue();
        if (target < 0 || CBOUtil.groupSmall(logicalView, ImmutableBitSet.of(target))) {
            LogicalView newLogicalView =
                logicalView.copy(logicalView.getTraitSet().simplify().replace(outConvention));
            call.transformTo(convert(newLogicalView,
                newLogicalView.getTraitSet().simplify().replace(RelDistributions.RANDOM_DISTRIBUTED)));
            return;
        }
        RelDistribution relDistribution = RelDistributions.hashOss(Lists.newArrayList(target), shard);
        LogicalView newLogicalView = logicalView.copy(logicalView.getTraitSet().simplify().replace(outConvention)
            .replace(relDistribution));
        call.transformTo(newLogicalView);
    }

    public static Pair<Integer, Integer> getShardColumn(OSSTableScan ossTableScan) {
        TableMeta tm = CBOUtil.getTableMeta(ossTableScan.getTable());
        if (!TableTopologyUtil.isShard(tm, PlannerContext.getPlannerContext(ossTableScan))) {
            return null;
        }

        boolean enableMock = PlannerContext.getPlannerContext(ossTableScan).getParamManager()
            .getBoolean(ConnectionParams.ENABLE_OSS_MOCK_COLUMNAR);
        // only support partition by direct_hash
        if (!canPartitionWise(tm, enableMock)) {
            return null;
        }

        List<String> columns = tm.getPartitionInfo().getPartitionColumns();
        if (columns.isEmpty()) {
            return null;
        }
        String column = columns.get(0);
        if (StringUtils.isEmpty(column)) {
            return null;
        }
        int target = -1;
        for (int i = 0; i < ossTableScan.getRowType().getFieldList().size(); i++) {
            if (ossTableScan.getRowType().getFieldList().get(i).getName().equalsIgnoreCase(column)) {
                target = i;
                break;
            }
        }
        int shard = tm.getPartitionInfo().getPartitionBy().getPartitions().size();
        return Pair.of(target, shard);
    }

    public static boolean canPartitionWise(TableMeta tm, boolean enableMock) {
        PartitionByDefinition definition = tm.getPartitionInfo().getPartitionBy();
        if (enableMock) {
            // for oss table test, support key
            if (definition.getStrategy().isKey()) {
                return true;
            }
        }

        // must be partitioned by direct_hash. But for test-purpose, allow hash
        if (!(definition.getStrategy().isDirectHash() || definition.getStrategy().isHashed())) {
            return false;
        }
        List<String> columns = tm.getPartitionInfo().getPartitionColumns();
        //hash by multiple columns
        if (columns.size() != 1) {
            return false;
        }
        List<SqlNode> partitionExprList = definition.getPartitionExprList();
        if (partitionExprList.size() != 1) {
            return false;
        }
        return partitionExprList.get(0) instanceof SqlIdentifier;
    }

    protected void basicOssTableScan(RelOptRuleCall call, LogicalView logicalView) {
        LogicalView newLogicalView = logicalView.copy(logicalView.getTraitSet().simplify().replace(outConvention));
        call.transformTo(newLogicalView);
    }
}
