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

package com.alibaba.polardbx.executor.ddl.job.task.storagepool;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.optimizer.locality.StoragePoolUtils;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.AlterStoragePoolSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.StorageInfoAccessor;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.locality.StoragePoolManager;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

import static com.alibaba.polardbx.gms.topology.StorageInfoRecord.STORAGE_STATUS_REMOVED;
import static com.alibaba.polardbx.optimizer.locality.StoragePoolManager.EMPTY_STORAGE_POOL;
import static com.alibaba.polardbx.optimizer.locality.StoragePoolManager.RECYCLE_STORAGE_POOL_NAME;

@Getter
@TaskName(name = "DrainStorageInfoTask")
public class DrainStorageInfoTask extends BaseStoragePoolInfoTask {

    @JSONCreator
    public DrainStorageInfoTask(String schemaName, String instId,
                                List<String> dnIds, String storagePoolName) {
        super(schemaName, instId, dnIds, "", storagePoolName);
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        // init base storage pool info
        initBaseStoragePoolInfoTask(metaDbConnection);

        // validate storage pool and dnIds
        StoragePoolTaskUtils.validateStoragePoolNameExistsAndDnIdsInStoragePool(storagePoolName, dnIds);

        // for __recycle__ storage pool, we remove this dn completely from instance, so update status directly.
        // for other storage pool, we only put this dn into __recycle storage pool, and update status in other task.
        if (storagePoolName.equalsIgnoreCase(StoragePoolManager.RECYCLE_STORAGE_POOL_NAME)) {
            // update storage status
            StoragePoolTaskUtils.updateStorageStatus(storageInfoAccessor, dnIds, STORAGE_STATUS_REMOVED);

            // update storage name
            StoragePoolTaskUtils.updateStoragePoolName(storageInfoAccessor, filteredStorageInfoRecords,
                EMPTY_STORAGE_POOL);

            // shrink storage pool
            storagePoolManager.shrinkStoragePoolSimply(metaDbConnection, storagePoolName, dnIdStr);
        } else {
            // update storage name
            StoragePoolTaskUtils.updateStoragePoolName(storageInfoAccessor, storageInfoRecords,
                RECYCLE_STORAGE_POOL_NAME);

            // shrink storage pool
            storagePoolManager.shrinkStoragePool(metaDbConnection, storagePoolName, dnIdStr, undeletableDnId);
        }

        MetaDbConfigManager.getInstance()
            .notify(MetaDbDataIdBuilder.getStorageInfoDataId(instId), metaDbConnection);

    }

    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        initBaseStoragePoolInfoTask(metaDbConnection);

        // firstly recover storage pool name.
        StoragePoolTaskUtils.updateStoragePoolName(storageInfoAccessor, filteredStorageInfoRecords, storagePoolName);

        // recover storage pool.
        storagePoolManager.appendStoragePool(metaDbConnection, storagePoolName, dnIdStr, undeletableDnId);

        // update but not neccassessary
        MetaDbConfigManager.getInstance()
            .notify(MetaDbDataIdBuilder.getStorageInfoDataId(instId), metaDbConnection);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        rollbackImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
        SyncManagerHelper.syncThrowExceptions(new AlterStoragePoolSyncAction("", ""), SyncScope.ALL);
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        SyncManagerHelper.syncThrowExceptions(new AlterStoragePoolSyncAction("", ""), SyncScope.ALL);
    }

}
