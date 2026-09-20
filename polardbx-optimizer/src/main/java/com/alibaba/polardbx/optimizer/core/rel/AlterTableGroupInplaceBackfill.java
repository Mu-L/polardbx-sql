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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.SqlConverter;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * 支持存储层下推的AlterTableGroup回填操作
 * 使用polardbx_hasher函数在存储层计算路由信息
 */
public class AlterTableGroupInplaceBackfill extends AbstractRelNode {
    /**
     * Creates an <code>AbstractRelNode</code>.
     */
    final String schemaName;
    final String logicalTableName;
    final Map<String, Set<String>> sourcePhyTables;
    final Map<String, Set<String>> targetPhyTables;
    final Map<String, Set<String>> srcTargetTableMap;
    final Map<String, List<Pair<Long, Long>>> targetTablePartitionBounds;
    final Map<String, List<Pair<Long, Long>>> sourceTablePartitionBounds;
    final List<List<String>> activePartitionKeys;
    final boolean useChangeSet;

    public AlterTableGroupInplaceBackfill(RelOptCluster cluster,
                                          RelTraitSet traitSet,
                                          String schemaName,
                                          String logicalTableName,
                                          Map<String, Set<String>> sourcePhyTables,
                                          Map<String, Set<String>> targetPhyTables,
                                          Map<String, Set<String>> srcTargetTableMap,
                                          Map<String, List<Pair<Long, Long>>> targetTablePartitionBounds,
                                          Map<String, List<Pair<Long, Long>>> sourceTablePartitionBounds,
                                          List<List<String>> activePartitionKeys,
                                          boolean useChangeSet) {
        super(cluster, traitSet);
        this.logicalTableName = logicalTableName;
        this.schemaName = schemaName;
        this.sourcePhyTables = sourcePhyTables;
        this.targetPhyTables = targetPhyTables;
        this.srcTargetTableMap = srcTargetTableMap;
        this.targetTablePartitionBounds = targetTablePartitionBounds;
        this.sourceTablePartitionBounds = sourceTablePartitionBounds;
        this.activePartitionKeys = activePartitionKeys;
        this.useChangeSet = useChangeSet;
    }

    public static AlterTableGroupInplaceBackfill createAlterTableGroupInplaceBackfill(String schemaName,
                                                                                      String logicalTableName,
                                                                                      ExecutionContext ec,
                                                                                      Map<String, Set<String>> sourcePhyTables,
                                                                                      Map<String, Set<String>> targetPhyTables,
                                                                                      Map<String, Set<String>> srcTargetTableMap,
                                                                                      Map<String, List<Pair<Long, Long>>> targetTablePartitionBounds,
                                                                                      Map<String, List<Pair<Long, Long>>> sourceTablePartitionBounds,
                                                                                      List<List<String>> activePartitionKeys,
                                                                                      boolean useChangeSet) {
        final RelOptCluster cluster = SqlConverter.getInstance(schemaName, ec).createRelOptCluster(null);
        RelTraitSet traitSet = RelTraitSet.createEmpty();
        return new AlterTableGroupInplaceBackfill(cluster, traitSet, schemaName, logicalTableName, sourcePhyTables,
            targetPhyTables, srcTargetTableMap, targetTablePartitionBounds, sourceTablePartitionBounds,
            activePartitionKeys, useChangeSet);
    }

    @Override
    public String getSchemaName() {
        return schemaName;
    }

    public String getLogicalTableName() {
        return logicalTableName;
    }

    public Map<String, Set<String>> getSourcePhyTables() {
        return sourcePhyTables;
    }

    public Map<String, Set<String>> getTargetPhyTables() {
        return targetPhyTables;
    }

    public boolean isUseChangeSet() {
        return useChangeSet;
    }

    public Map<String, Set<String>> getSrcTargetTableMap() {
        return srcTargetTableMap;
    }

    public Map<String, List<Pair<Long, Long>>> getTargetTablePartitionBounds() {
        return targetTablePartitionBounds;
    }

    public List<List<String>> getActivePartitionKeys() {
        return activePartitionKeys;
    }

    public Map<String, List<Pair<Long, Long>>> getSourceTablePartitionBounds() {
        return sourceTablePartitionBounds;
    }
}