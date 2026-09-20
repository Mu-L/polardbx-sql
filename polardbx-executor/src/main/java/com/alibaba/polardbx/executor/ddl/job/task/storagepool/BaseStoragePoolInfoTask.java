package com.alibaba.polardbx.executor.ddl.job.task.storagepool;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.gms.topology.StorageInfoAccessor;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.optimizer.locality.StoragePoolManager;
import com.alibaba.polardbx.optimizer.locality.StoragePoolUtils;
import lombok.Getter;

import java.sql.Connection;
import java.util.List;

@Getter
public class BaseStoragePoolInfoTask extends BaseDdlTask {
    String schemaName;
    String instId;
    List<String> dnIds;
    String undeletableDnId;
    String storagePoolName;

    @JSONCreator
    public BaseStoragePoolInfoTask(String schemaName, String instId, List<String> dnIds, String undeletableDnId,
                                   String storagePoolName) {
        super(schemaName);
        this.schemaName = schemaName;
        this.instId = instId;
        this.dnIds = dnIds;
        this.undeletableDnId = undeletableDnId;
        this.storagePoolName = storagePoolName;
    }

    transient StorageInfoAccessor storageInfoAccessor;
    transient StoragePoolManager storagePoolManager;
    transient List<StorageInfoRecord> storageInfoRecords;
    transient List<StorageInfoRecord> filteredStorageInfoRecords;
    transient String dnIdStr;

    void initBaseStoragePoolInfoTask(Connection metaDbConnection) {
        storageInfoAccessor = new StorageInfoAccessor();
        storageInfoAccessor.setConnection(metaDbConnection);

        storagePoolManager = StoragePoolManager.getInstance();

        storageInfoRecords = storageInfoAccessor.getStorageInfosByInstId(instId);
        filteredStorageInfoRecords =
            StoragePoolTaskUtils.getStorageInfoRecordsByDnIds(storageInfoAccessor, instId, dnIds);

        dnIdStr = StoragePoolUtils.buildStringFromStorageInstList(dnIds);
    }
}
