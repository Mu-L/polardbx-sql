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

package com.alibaba.polardbx.optimizer.core.rel.dml.writer;

import com.google.common.collect.Lists;
import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableInsertBuilder;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableInsertSharder;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableInsertSharder.PhyTableShardResult;
import com.alibaba.polardbx.optimizer.core.rel.SingleTableInsert;
import com.alibaba.polardbx.optimizer.core.rel.dml.DmlWriteContext;
import com.alibaba.polardbx.optimizer.core.rel.dml.PhysicalRoute;
import com.alibaba.polardbx.optimizer.core.rel.dml.RoutedInsertInput;
import com.alibaba.polardbx.optimizer.core.rel.dml.RoutedModifyInput;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.alibaba.polardbx.rule.TableRule;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Writer for INSERT or REPLACE on single table
 *
 * @author chenmo.cm
 */
public class SingleInsertWriter extends InsertWriter {

    protected final TableMeta tableMeta;
    protected final TableRule tableRule;

    public SingleInsertWriter(RelOptTable targetTable,
                              LogicalInsert insert,
                              TableMeta tableMeta,
                              TableRule tableRule) {
        super(targetTable, insert);
        this.tableMeta = tableMeta;
        this.tableRule = tableRule;
    }

    @Override
    public List<RelNode> getInput(ExecutionContext executionContext) {
        String schemaName = this.tableMeta.getSchemaName();
        if (!DbInfoManager.getInstance().isNewPartitionDb(schemaName)) {
            return getInputForShardingTables(executionContext);
        } else {
            return getInputForPartitioningTables(executionContext);
        }
    }

    public List<RelNode> getInputForPartitioningTables(ExecutionContext executionContext) {

        SingleTableInsert singleTableInsert = this.singleTableOperation;
        Map<Integer, ParameterContext> params = executionContext.getParams() == null ?
            null : executionContext.getParams().getCurrentParameter();
        ;
        Pair<String, String> phyDbAndTb = singleTableInsert.getPhyGroupAndPhyTablePair(params, executionContext);

        // Build plan
        final List<PhyTableShardResult> shardResults =
            Lists.newArrayList(new PhyTableShardResult(phyDbAndTb.getKey(), phyDbAndTb.getValue(), null));

        final DmlWriteContext writeContext = executionContext.getDmlWriteContext();
        if (writeContext != null) {
            final int rowCount = executionContext.getParams().isBatch()
                ? executionContext.getParams().getBatchParameters().size()
                : ((com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues) RelUtils.getRelInput(insert))
                .getTuples().size();
            writeContext.beforeInsertPlans(this, new RoutedInsertInput(
                    RoutedModifyInput.buildInsertRoutes(insert.getSchemaName(), shardResults, rowCount)),
                executionContext);
        }

        final PhyTableInsertSharder partitioner = new PhyTableInsertSharder(insert,
            executionContext.getParams(),
            SequenceAttribute.getAutoValueOnZero(executionContext.getSqlMode()));

        final PhyTableInsertBuilder phyPlanbuilder = new PhyTableInsertBuilder(partitioner.getSqlTemplate(),
            executionContext,
            insert,
            insert.getDbType(),
            insert.getSchemaName());

        final List<RelNode> result = phyPlanbuilder.build(shardResults);
        return writeContext == null ? result : writeContext.afterInsertPlans(this, result, executionContext);

    }

    public List<RelNode> getInputForShardingTables(ExecutionContext executionContext) {

        // Get group key
        final String physicalTableName = getPhysicalTableName();
        final String groupIndex = getGroupIndex();

        // Build plan
        final List<PhyTableShardResult> shardResults =
            Lists.newArrayList(new PhyTableShardResult(groupIndex, physicalTableName, null));

        final DmlWriteContext writeContext = executionContext.getDmlWriteContext();
        if (writeContext != null) {
            final int rowCount = executionContext.getParams().isBatch()
                ? executionContext.getParams().getBatchParameters().size()
                : ((com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues) RelUtils.getRelInput(insert))
                .getTuples().size();
            writeContext.beforeInsertPlans(this, new RoutedInsertInput(
                    RoutedModifyInput.buildInsertRoutes(insert.getSchemaName(), shardResults, rowCount)),
                executionContext);
        }

        final PhyTableInsertSharder partitioner = new PhyTableInsertSharder(insert,
            executionContext.getParams(),
            SequenceAttribute.getAutoValueOnZero(executionContext.getSqlMode()));

        final PhyTableInsertBuilder phyPlanbuilder = new PhyTableInsertBuilder(partitioner.getSqlTemplate(),
            executionContext,
            insert,
            insert.getDbType(),
            insert.getSchemaName());

        final List<RelNode> result = phyPlanbuilder.build(shardResults);
        return writeContext == null ? result : writeContext.afterInsertPlans(this, result, executionContext);
    }

    private String getPhysicalTableName() {
        if (tableRule != null) {
            return tableRule.getTbNamePattern();
        }
        //In Drds mode, system tables don't have tableRule
        return insert.getLogicalTableName();
    }

    private String getGroupIndex() {
        if (tableRule != null) {
            return tableRule.getDbNamePattern();
        }
        final TddlRuleManager rule = OptimizerContext.getContext(insert.getSchemaName()).getRuleManager();
        return rule.getDefaultDbIndex(insert.getLogicalTableName());
    }

}
