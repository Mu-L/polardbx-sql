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

package com.alibaba.polardbx.executor.ddl.job.task.tablegroup;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TableGroupSyncAction;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

@Getter
@TaskName(name = "ReloadTableMetaAfterChangeTableGroupTask")
public class ReloadTableMetaAfterChangeTableGroupTask extends BaseGmsTask {

    protected String targetTableGroup;

    @JSONCreator
    public ReloadTableMetaAfterChangeTableGroupTask(final String schemaName,
                                                    final String targetTableGroup) {
        super(schemaName, null);
        this.targetTableGroup = targetTableGroup;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        reloadTableGroup();
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        reloadTableGroup();
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
    }

    protected void reloadTableGroup() {

        DdlJobManager jobManager = new DdlJobManager();
        List<DdlTask> prevTasks = jobManager.getTasksFromMetaDB(getJobId(),
            (new AlterTableSetTableGroupChangeMetaOnlyTask(null, null, null, null, false, false, null,
                false)).getName());
        assert prevTasks.size() == 1;
        AlterTableSetTableGroupChangeMetaOnlyTask setTableGroupChangeMetaOnlyTask =
            (AlterTableSetTableGroupChangeMetaOnlyTask) prevTasks.get(0);
        //get the targetTableGroup from AlterTableSetTableGroupChangeMetaOnlyTask in the some job
        targetTableGroup = setTableGroupChangeMetaOnlyTask.getTargetTableGroup();
        String sourceTableGroup = setTableGroupChangeMetaOnlyTask.getCurTableGroup();

        syncTableGroup(sourceTableGroup);
    }

    private void syncTableGroup(String sourceTableGroup) {
        try {
            SyncManagerHelper.syncThrowExceptions(new TableGroupSyncAction(schemaName, targetTableGroup),
                SyncScope.ALL);
        } catch (Throwable t) {
            LOGGER.error(String.format(
                "error occurs while sync table group, schemaName:%s, tableGroupName:%s", schemaName, targetTableGroup));
            // Change-context: AONE-85096607
            // Before: the raw cause was wrapped without context, and when the cause carried no message
            // (e.g. an NPE from a missing OptimizerContext), clients only saw "Caused by: null".
            // Path impact: the DDL task failure now reports schema and both table groups involved
            // in the change, so the error is actionable without digging into CN logs.
            // Capability regression: none; only the failure message gains context.
            throw GeneralUtil.nestedException(
                String.format("failed to sync table group [%s] (changed from table group [%s]) in schema [%s]",
                    targetTableGroup, sourceTableGroup, schemaName), t);
        }
    }

}
