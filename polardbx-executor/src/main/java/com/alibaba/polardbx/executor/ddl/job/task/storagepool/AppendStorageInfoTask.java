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
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.optimizer.locality.StoragePoolUtils.RECYCLE_STORAGE_POOL;
import static com.alibaba.polardbx.optimizer.locality.StoragePoolUtils.buildStringFromStorageInstList;

@Getter
@TaskName(name = "AppendStorageInfoTask")
public class AppendStorageInfoTask extends BaseStoragePoolInfoTask {

    Map<String, StorageInfoRecord> originalStorageInfoMap;

    @JSONCreator
    public AppendStorageInfoTask(String schemaName, String instId,
                                 Map<String, StorageInfoRecord> originalStorageInfoMap,
                                 List<String> dnIds, String undeletableDnId, String storagePoolName) {
        super(schemaName, instId, dnIds, undeletableDnId, storagePoolName);
        this.originalStorageInfoMap = originalStorageInfoMap;
    }

    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        initBaseStoragePoolInfoTask(metaDbConnection);

        // update storage inst as ready.
        StoragePoolTaskUtils.updateStorageStatus(storageInfoAccessor, dnIds, StorageInfoRecord.STORAGE_STATUS_READY);

        // update storage pool name
        StoragePoolTaskUtils.updateStoragePoolName(storageInfoAccessor, filteredStorageInfoRecords, storagePoolName);

        if (!storagePoolName.equalsIgnoreCase(RECYCLE_STORAGE_POOL)) {
            if (StringUtils.isEmpty(undeletableDnId)) {
                undeletableDnId = dnIds.get(0);
            }
            // shrink recycle storage pool because we put it into other storage pool.
            storagePoolManager.shrinkStoragePoolSimply(metaDbConnection, RECYCLE_STORAGE_POOL, dnIdStr);
        }
        // append storage pool
        storagePoolManager.appendStoragePool(metaDbConnection, storagePoolName, dnIdStr, undeletableDnId);

        // update op-version
        MetaDbConfigManager.getInstance()
            .notify(MetaDbDataIdBuilder.getStorageInfoDataId(instId), metaDbConnection);
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        executeImpl(metaDbConnection, executionContext);
    }

    void recoverStorageInfoByOriginalRecord(StorageInfoRecord newRecord, StorageInfoRecord oldRecord) {
        int oldStatus = oldRecord.status;
        storageInfoAccessor.updateStorageStatus(newRecord.storageInstId, oldStatus);
        storageInfoAccessor.updateStoragePoolName(newRecord.storageInstId, oldRecord.extras);
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        //        executeImpl(metaDbConnection, executionContext);
        initBaseStoragePoolInfoTask(metaDbConnection);

        for (StorageInfoRecord record : storageInfoRecords) {
            recoverStorageInfoByOriginalRecord(record, originalStorageInfoMap.get(record.storageInstId));
        }

        // shrink storage pool, all the dn will be returned to recycle.
        if (storagePoolManager.removeDnListFromOriginalStoragePoolInfo(dnIdStr, storagePoolName).isEmpty()) {
            undeletableDnId = "";
        }

        storagePoolManager.shrinkStoragePool(metaDbConnection, storagePoolName, dnIdStr, undeletableDnId);

        // update op-version
        MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getStorageInfoDataId(instId), metaDbConnection);
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
