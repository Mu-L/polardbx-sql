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

package com.alibaba.polardbx.executor.ddl.job.factory.storagepool;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.balancer.action.ActionUtils;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DrainCDCTask;
import com.alibaba.polardbx.executor.ddl.job.task.storagepool.DrainStorageInfoTask;
import com.alibaba.polardbx.executor.ddl.job.task.storagepool.StorageInstValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.storagepool.StoragePoolTaskUtils;
import com.alibaba.polardbx.executor.ddl.job.task.storagepool.StoragePoolValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.BackgroupRebalanceTask;
import com.alibaba.polardbx.executor.ddl.job.validator.StoragePoolValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterStoragePoolPrepareData;
import com.alibaba.polardbx.optimizer.locality.StoragePoolInfo;
import com.alibaba.polardbx.optimizer.locality.StoragePoolManager;
import com.alibaba.polardbx.optimizer.locality.StoragePoolUtils;
import com.google.common.collect.Lists;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Set;

public class AlterStoragePoolDrainNodeJobFactory extends DdlJobFactory {
    private AlterStoragePoolPrepareData prepareData;
    private ExecutionContext executionContext;

    public AlterStoragePoolDrainNodeJobFactory(AlterStoragePoolPrepareData prepareData,
                                               ExecutionContext executionContext) {
        super();
        this.prepareData = prepareData;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {
        List<String> dnIds = prepareData.getDnIds();
        String instId = InstIdUtil.getMasterInstId();
        if (!prepareData.getStoragePoolName().equalsIgnoreCase(StoragePoolManager.RECYCLE_STORAGE_POOL_NAME)) {
            StoragePoolValidator.validateStoragePoolReady(instId, dnIds);
        }
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        String instId = InstIdUtil.getMasterInstId();
        Long planId = executionContext.getParamManager().getLong(ConnectionParams.DDL_PLAN_ID);
        //validate again.
        StoragePoolTaskUtils.validateStoragePoolNameExistsAndDnIdsInStoragePool(prepareData.getStoragePoolName(),
            prepareData.getDnIds());

        Boolean drainRecycleStoragePool =
            prepareData.getStoragePoolName().equalsIgnoreCase(StoragePoolManager.RECYCLE_STORAGE_POOL_NAME);
        StorageInstValidateTask
            storageInstValidateTask = new StorageInstValidateTask(prepareData.getSchemaName(), instId,
            prepareData.getDnIds(), false, false);
        StoragePoolValidateTask
            storagePoolValidateTask = new StoragePoolValidateTask(prepareData.getSchemaName(), instId,
            prepareData.getStoragePoolName(),
            prepareData.getDnIds());
        DrainStorageInfoTask
            drainStorageInfoTask =
            new DrainStorageInfoTask(prepareData.getSchemaName(), instId, prepareData.getDnIds(),
                prepareData.getStoragePoolName());
        String rebalanceSql =
            StoragePoolTaskUtils.constructRebalanceSqlForDrainNode(prepareData.getStoragePoolName(),
                prepareData.getDnIds(), planId);
        DdlTask rebalanceStoragePoolTask = new BackgroupRebalanceTask(SystemDbHelper.DEFAULT_DB_NAME, rebalanceSql);
        DdlTask drainCDCTask = null;
        if (drainRecycleStoragePool) {
            storageInstValidateTask = new StorageInstValidateTask(prepareData.getSchemaName(), instId,
                prepareData.getDnIds(), false, false, true);
            drainCDCTask = new DrainCDCTask(SystemDbHelper.DEFAULT_DB_NAME, prepareData.getDnIds());
        }
        ExecutableDdlJob ddlJob = new ExecutableDdlJob();
        List<DdlTask> ddlTasks =
            Lists.newArrayList(storageInstValidateTask, storagePoolValidateTask, drainStorageInfoTask,
                    rebalanceStoragePoolTask, drainCDCTask)
                .stream().filter(o -> o != null).collect(java.util.stream.Collectors.toList());
        ddlJob.addSequentialTasks(
            ddlTasks
        );
        return ddlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(StoragePoolUtils.LOCK_PREFIX, prepareData.getStoragePoolName()));
        resources.add(concatWithDot(StoragePoolUtils.LOCK_PREFIX, StoragePoolUtils.FULL_LOCK_NAME));
        resources.add(ActionUtils.genRebalanceTenantResourceName(prepareData.getStoragePoolName()));
    }

    @Override
    protected void sharedResources(Set<String> resources) {

    }

}
