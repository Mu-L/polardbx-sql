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
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.AlterStoragePoolSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.StorageInfoAccessor;
import com.alibaba.polardbx.gms.topology.StorageInfoExtraFieldJSON;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.locality.StoragePoolManager;
import com.alibaba.polardbx.optimizer.locality.StoragePoolUtils;
import lombok.Getter;
import org.apache.commons.lang.StringUtils;

import java.sql.Connection;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.topology.StorageInfoRecord.STORAGE_STATUS_READY;
import static com.alibaba.polardbx.gms.topology.StorageInfoRecord.STORAGE_STATUS_REMOVED;
import static com.alibaba.polardbx.optimizer.locality.StoragePoolUtils.RECYCLE_STORAGE_POOL;

@Getter
@TaskName(name = "DeleteStorageInfoTask")
public class DeleteStorageInfoTask extends BaseStoragePoolInfoTask {

    @JSONCreator
    public DeleteStorageInfoTask(String schemaName, String instId, List<String> dnIds, String undeletableDnId,
                                 String storagePoolName) {
        super(schemaName, instId, dnIds, undeletableDnId, storagePoolName);
    }

    @Override
    public void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        updateSupportedCommands(true, false, metaDbConnection);

        // init storage pool
        initBaseStoragePoolInfoTask(metaDbConnection);

        // update storage pool name
        StoragePoolTaskUtils.updateStoragePoolName(storageInfoAccessor, filteredStorageInfoRecords,
            RECYCLE_STORAGE_POOL);

        // update storage status
        StoragePoolTaskUtils.updateStorageStatus(storageInfoAccessor, dnIds, STORAGE_STATUS_REMOVED);

        // remove storage pool
        storagePoolManager.mergeIntoStoragePool(metaDbConnection, storagePoolName, RECYCLE_STORAGE_POOL);

        MetaDbConfigManager.getInstance()
            .notify(MetaDbDataIdBuilder.getStorageInfoDataId(instId), metaDbConnection);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
//        rollbackImpl(metaDbConnection, executionContext);
//        initBaseStoragePoolInfoTask(metaDbConnection);
//        // update storage pool name
//        StoragePoolTaskUtils.updateStoragePoolName(storageInfoAccessor, filteredStorageInfoRecords, storagePoolName);
//
//        // add storage pool
//        storagePoolManager.addStoragePool(metaDbConnection, storagePoolName, dnIdStr, undeletableDnId);
//
//        // update status
//        StoragePoolTaskUtils.updateStorageStatus(storageInfoAccessor, dnIds, STORAGE_STATUS_READY);
//
//        // nodify storage info
//        MetaDbConfigManager.getInstance()
//            .notify(MetaDbDataIdBuilder.getStorageInfoDataId(instId), metaDbConnection);
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
        SyncManagerHelper.syncThrowExceptions(new AlterStoragePoolSyncAction("", ""),
            SyncScope.ALL);
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        SyncManagerHelper.syncThrowExceptions(new AlterStoragePoolSyncAction("", ""),
            SyncScope.ALL);
    }

}
