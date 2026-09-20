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
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.optimizer.locality.StoragePoolUtils;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.AlterStoragePoolSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.StorageInfoAccessor;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.locality.StoragePoolManager;
import lombok.Getter;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

@Getter
@TaskName(name = "AddStorageInfoTask")
// here is add meta to complex_task_outline table, no need to update tableVersion,
// so no need to extends from BaseGmsTask
public class AddStorageInfoTask extends BaseStoragePoolInfoTask {

    @JSONCreator
    public AddStorageInfoTask(String schemaName, String instId,
                              List<String> dnIds, String storagePoolName, String undeletableDnId) {
        super(schemaName, instId, dnIds, storagePoolName, undeletableDnId);
    }

    public AddStorageInfoTask(String schemaName, String instId, String storagePoolName) {
        super(schemaName, instId, new ArrayList<>(), storagePoolName, "");
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        // init base storage pool info
        initBaseStoragePoolInfoTask(metaDbConnection);

        // validate storage pool duplicate
        StoragePoolTaskUtils.validateStoragePoolNameDuplicate(storagePoolName);

        // which only make effect when the task has been finished, because it's in transaction
        updateSupportedCommands(true, false, metaDbConnection);

        // update added storage inst status
        StoragePoolTaskUtils.updateStorageStatus(storageInfoAccessor, dnIds, StorageInfoRecord.STORAGE_STATUS_READY);

        // update added storage inst pool name
        StoragePoolTaskUtils.updateStoragePoolName(storageInfoAccessor, filteredStorageInfoRecords, storagePoolName);

        // update storage pool info
        storagePoolManager.addStoragePool(metaDbConnection, storagePoolName, dnIdStr, undeletableDnId);

        // if there are no storage pool for default storage pool, we reset it.
        if (!storagePoolManager.storagePoolCacheByName.containsKey(StoragePoolUtils.DEFAULT_STORAGE_POOL)) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "the storage pool info has not been initialized, you can restart CN for initializing");
//            resetDefaultStoragePool(storagePoolManager, storageInfoAccessor, metaDbConnection);
        }

        MetaDbConfigManager.getInstance()
            .notify(MetaDbDataIdBuilder.getStorageInfoDataId(instId), metaDbConnection);

    }

//    void resetDefaultStoragePool(StoragePoolManager storagePoolManager, StorageInfoAccessor storageInfoAccessor,
//                                 Connection connection) {
//        // get other dn id.
//        Set<String> otherDnIds =
//            storageInfoRecords.stream().filter(o -> !dnIds.contains(o.storageInstId)).map(o -> o.storageInstId)
//                .collect(Collectors.toSet());
//        String otherDnIdStr = StoragePoolUtils.buildStringFromStorageInstList(otherDnIds);
//
//        // get other storage info records
//        List<StorageInfoRecord> otherStorageInfoRecords =
//            StoragePoolTaskUtils.getStorageInfoRecordsByDnIds(storageInfoAccessor, instId, otherDnIds);
//
//        // find first none default storage pool
//        String defaultUndeletableDnId =
//            StoragePoolTaskUtils.findUndeletableDnIds(otherDnIds, DbTopologyManager.singleGroupStorageInstList);
//
//        // update storage info
//        StoragePoolTaskUtils.updateStoragePoolName(storageInfoAccessor, otherStorageInfoRecords,
//            StoragePoolUtils.DEFAULT_STORAGE_POOL);
//
//        // update storage pool info
//        storagePoolManager.addStoragePool(connection, StoragePoolUtils.DEFAULT_STORAGE_POOL, otherDnIdStr,
//            defaultUndeletableDnId);
//
//    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
//        StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
//        storageInfoAccessor.setConnection(metaDbConnection);
//        StoragePoolManager storagePoolManager = StoragePoolManager.getInstance();
//        List<StorageInfoRecord> originalStorageInfoRecords =
//            StoragePoolTaskUtils.getStorageInfoRecordsByDnIds(storageInfoAccessor, instId, dnIds);
//        StoragePoolTaskUtils.updateStoragePoolName(storageInfoAccessor, originalStorageInfoRecords,
//            StoragePoolUtils.RECYCLE_STORAGE_POOL);
//        String dnIdStr = StoragePoolUtils.buildStringFromStorageInstList(dnIds);
//        storagePoolManager.deleteStoragePool(metaDbConnection, storagePoolName);
//        rollbackImpl(metaDbConnection, executionContext);
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
