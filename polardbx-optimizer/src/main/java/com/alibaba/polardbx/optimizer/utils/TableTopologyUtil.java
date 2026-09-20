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

package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.util.Util;

import java.util.List;
import java.util.Set;

public class TableTopologyUtil {

    public static boolean isBroadcast(TableMeta tableMeta) {
        return OptimizerContext.getContext(tableMeta.getSchemaName()).getRuleManager()
            .isBroadCast(tableMeta.getTableName());
    }

    public static boolean isSingle(TableMeta tableMeta) {
        return OptimizerContext.getContext(tableMeta.getSchemaName()).getRuleManager()
            .isTableInSingleDb(tableMeta.getTableName());
    }

    public static boolean isShard(TableMeta tableMeta) {
        return OptimizerContext.getContext(tableMeta.getSchemaName()).getRuleManager()
            .isShard(tableMeta.getTableName());
    }

    /**
     * 根据当前执行上下文，判断当前表是否是分片表
     */
    public static boolean isShard(TableMeta tableMeta, PlannerContext plannerContext) {
        if (plannerContext == null || PlannerContext.EMPTY_CONTEXT == plannerContext){
            return isShard(tableMeta);
        }
        return isShard(tableMeta, plannerContext.getExecutionContext());
    }

    /**
     * 根据当前执行上下文，判断当前表是否是分片表
     */
    public static boolean isShard(TableMeta tableMeta, ExecutionContext ec) {
        if (ec == null || ec.getSchemaManager() == null){
            return isShard(tableMeta);
        }
        return ec.getSchemaManager(tableMeta.getSchemaName()).getTddlRuleManager()
                .isShard(tableMeta.getTableName());
    }

    public static boolean isAllSingleTableInSamePhysicalDB(Set<RelOptTable> scans) {
        String currentSchema = null;
        int index = 0;
        for (RelOptTable scan : scans) {
            final List<String> qualifiedName = scan.getQualifiedName();
            final String schemaName = qualifiedName.size() == 2 ? qualifiedName.get(0) : null;

            if (index == 0) {
                currentSchema = schemaName;
            }
            if (currentSchema == null) {
                if (currentSchema != schemaName) {
                    return false;
                }
            } else if (!currentSchema.equalsIgnoreCase(schemaName)) {
                return false;
            }
            index++;
        }

        //currentSchema maybe null
        String schemaName = OptimizerContext.getContext(currentSchema).getSchemaName();

        boolean isNewPartDb = DbInfoManager.getInstance().isNewPartitionDb(schemaName);

        TddlRuleManager or = OptimizerContext.getContext(schemaName).getRuleManager();
        if (isNewPartDb) {
            //auto
            long tableGroupId = -1;
            for (RelOptTable scan : scans) {
                final List<String> qualifiedName = scan.getQualifiedName();
                final String tableName = Util.last(qualifiedName);
                if (!or.isTableInSingleDb(tableName)) {
                    return false;
                }

                PartitionInfo partitionInfo = or.getPartitionInfoManager().getPartitionInfo(tableName);
                if (partitionInfo == null) {
                    return false;
                }
                if (tableGroupId == -1) {
                    tableGroupId = partitionInfo.getTableGroupId();
                } else if (tableGroupId != partitionInfo.getTableGroupId()) {
                    return false;
                }
            }
        } else {
            //drds
            for (RelOptTable scan : scans) {
                final List<String> qualifiedName = scan.getQualifiedName();
                final String tableName = Util.last(qualifiedName);
                if (!or.isTableInSingleDb(tableName)) {
                    return false;
                }
            }
        }
        return true;
    }

    public static boolean supportPushTheSingleAutoTable(
        String leftTable, String rightTable, TddlRuleManager or) {
        PartitionInfo leftPartitionInfo = or.getPartitionInfoManager().getPartitionInfo(leftTable);
        PartitionInfo rightPartitionInfo = or.getPartitionInfoManager().getPartitionInfo(rightTable);
        if (leftPartitionInfo.getTableGroupId() != null && rightPartitionInfo.getTableGroupId() != null) {
            return leftPartitionInfo.isSingleTable() && rightPartitionInfo.isSingleTable() &&
                leftPartitionInfo.getTableGroupId().equals(rightPartitionInfo.getTableGroupId());

        }
        return false;
    }

    /**
     * push-down the single or broadcast table on drds mode.
     */
    public static boolean supportPushSingleOrBroadcastDrdsTable(
        String leftTable, String rightTable, TddlRuleManager or, JoinRelType joinType,
        LogicalView leftView, LogicalView rightView, ParamManager paramManager) {

        // 两个单表，可以下推
        if (or.isTableInSingleDb(leftTable) && or.isTableInSingleDb(rightTable)) {
            return true;
        }

        // 两个广播表
        if (or.isBroadCast(leftTable) && or.isBroadCast(rightTable)) {
            return true;
        }

        // 一个广播表，一个单表
        if (or.isTableInSingleDb(leftTable) && or.isBroadCast(rightTable)) {
            return true;
        }

        if (or.isBroadCast(leftTable) && or.isTableInSingleDb(rightTable)) {
            return true;
        }

        switch (joinType) {
        case INNER:
            if (or.isBroadCast(leftTable) || or.isBroadCast(rightTable)) {
                return true;
            }
            break;
        case LEFT:
        case ANTI:
        case SEMI:
        case LEFT_SEMI:
            if (or.isBroadCast(rightTable)) {
                return true;
            }
            if (paramManager.getBoolean(ConnectionParams.ENABLE_PUSH_SINGLE_GROUP_JOIN)) {
                if (or.isBroadCast(leftTable) && rightView.isSingleGroup()) {
                    return true;
                }
            }
            break;
        case RIGHT:
            if (or.isBroadCast(leftTable)) {
                return true;
            }
            if (paramManager.getBoolean(ConnectionParams.ENABLE_PUSH_SINGLE_GROUP_JOIN)) {
                if (or.isBroadCast(rightTable) && leftView.isSingleGroup()) {
                    return true;
                }
            }
            break;
        case FULL:
        default:
            return false;
        }

        return false;
    }
}