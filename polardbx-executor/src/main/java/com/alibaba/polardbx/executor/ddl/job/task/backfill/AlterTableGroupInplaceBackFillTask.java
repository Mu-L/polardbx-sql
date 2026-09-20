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

package com.alibaba.polardbx.executor.ddl.job.task.backfill;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.ddl.job.task.BaseBackfillTask;
import com.alibaba.polardbx.executor.ddl.job.task.RemoteExecutableDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.omc.InplaceBackfillUtils;
import com.alibaba.polardbx.executor.gsi.GsiBackfillManager;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.AlterTableGroupInplaceBackfill;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundSpec;
import com.alibaba.polardbx.optimizer.partition.boundspec.PartitionBoundVal;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.pruning.SearchDatumInfo;
import lombok.Getter;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/**
 * 支持存储层下推的AlterTableGroup回填任务
 * 使用polardbx_hasher函数在存储层计算路由信息，避免数据在CN节点传输
 */
@TaskName(name = "AlterTableGroupInplaceBackFillTask")
@Getter
public class AlterTableGroupInplaceBackFillTask extends BaseBackfillTask implements RemoteExecutableDdlTask {

    String logicalTableName;
    Map<String, Set<String>> sourcePhyTables;
    Map<String, Set<String>> targetPhyTables;
    Map<String, Set<String>> srcTargetTableMap;
    Map<String, List<Pair<Long, Long>>> targetTablePartitionBounds;
    Map<String, List<Pair<Long, Long>>> sourceTablePartitionBounds;
    List<List<String>> activePartitionKeys;
    Map<String, Set<String>> srcTargetPartitionMap;
    boolean firstPartitionLevelActiveForInplaceBackfill;
    boolean useChangeSet;
    Integer hotKeyNum;

    @JSONCreator
    public AlterTableGroupInplaceBackFillTask(String schemaName,
                                              String logicalTableName,
                                              Map<String, Set<String>> sourcePhyTables,
                                              Map<String, Set<String>> targetPhyTables,
                                              Map<String, Set<String>> srcTargetTableMap,
                                              Map<String, List<Pair<Long, Long>>> targetTablePartitionBounds,
                                              Map<String, List<Pair<Long, Long>>> sourceTablePartitionBounds,
                                              List<List<String>> activePartitionKeys,
                                              Map<String, Set<String>> srcTargetPartitionMap,
                                              Integer hotKeyNum,
                                              boolean firstPartitionLevelActiveForInplaceBackfill,
                                              boolean useChangeSet) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
        this.sourcePhyTables = sourcePhyTables;
        this.targetPhyTables = targetPhyTables;
        this.srcTargetTableMap = srcTargetTableMap;
        this.targetTablePartitionBounds = targetTablePartitionBounds;
        this.sourceTablePartitionBounds = sourceTablePartitionBounds;
        this.activePartitionKeys = activePartitionKeys;
        this.srcTargetPartitionMap = srcTargetPartitionMap;
        this.hotKeyNum = hotKeyNum;
        this.firstPartitionLevelActiveForInplaceBackfill = firstPartitionLevelActiveForInplaceBackfill;
        this.useChangeSet = useChangeSet;
        if (useChangeSet) {
            // onExceptionTryRollback, such as dn ha
            onExceptionTryRecoveryThenRollback();
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_NOT_SUPPORT, "Inplace backfill must enable changeSet feature");
        }
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        executeImpl(executionContext);
    }

    @Override
    protected void executeImpl(ExecutionContext executionContext) {
        updateTaskStateInNewTxn(DdlTaskState.DIRTY);
        executionContext = executionContext.copy();
        executionContext.setBackfillId(getTaskId());
        executionContext.setTaskId(getTaskId());
        executionContext.setSchemaName(schemaName);
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);

        prepareBackfillTask(executionContext);
        // 使用存储层下推的回填计划ø
        final RelNode executablePushDownBackfillPlan = AlterTableGroupInplaceBackfill
            .createAlterTableGroupInplaceBackfill(schemaName, logicalTableName, executionContext, sourcePhyTables,
                targetPhyTables, srcTargetTableMap, targetTablePartitionBounds, sourceTablePartitionBounds,
                activePartitionKeys, useChangeSet);
        ExecutorHelper.execute(executablePushDownBackfillPlan, executionContext);
    }

    @Override
    protected void rollbackImpl(ExecutionContext executionContext) {
        GsiBackfillManager gsiBackfillManager = new GsiBackfillManager(schemaName);
        gsiBackfillManager.deleteByBackfillId(getTaskId());
    }

    private void prepareBackfillTask(ExecutionContext ec) {
        if (GeneralUtil.isEmpty(targetTablePartitionBounds) || GeneralUtil.isEmpty(activePartitionKeys)
            || GeneralUtil.isEmpty(srcTargetTableMap)) {
            initializeCollections();
            InplaceBackfillUtils.prepareBackfillTask(schemaName, logicalTableName, srcTargetTableMap,
                srcTargetPartitionMap, sourceTablePartitionBounds, targetTablePartitionBounds, activePartitionKeys,
                hotKeyNum, firstPartitionLevelActiveForInplaceBackfill, ec);
        }
    }

    private void initializeCollections() {
        targetTablePartitionBounds = new TreeMap<>(String::compareToIgnoreCase);
        sourceTablePartitionBounds = new TreeMap<>(String::compareToIgnoreCase);
        activePartitionKeys = new ArrayList<>();
        srcTargetTableMap = new TreeMap<>(String::compareToIgnoreCase);
    }

    public static String getTaskName() {
        return "AlterTableGroupInplaceBackFillTask";
    }

    @Override
    public List<String> explainInfo(ExecutionContext ec) {
        String backfillTask = "INPLACE_BACKFILL_TASK(" + logicalTableName + ")";
        List<String> command = new ArrayList<>(1);
        command.add(backfillTask);
        return command;
    }
}