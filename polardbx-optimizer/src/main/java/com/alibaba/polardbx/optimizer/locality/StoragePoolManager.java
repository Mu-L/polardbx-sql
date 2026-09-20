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

package com.alibaba.polardbx.optimizer.locality;

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.listener.ConfigListener;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.locality.LocalityDesc;
import com.alibaba.polardbx.gms.locality.StoragePoolInfoAccessor;
import com.alibaba.polardbx.gms.locality.StoragePoolInfoRecord;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.gms.topology.ServerInstIdManager;
import com.alibaba.polardbx.gms.topology.StorageInfoAccessor;
import com.alibaba.polardbx.gms.topology.StorageInfoExtraFieldJSON;
import com.alibaba.polardbx.gms.topology.StorageInfoRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.gms.util.PartitionNameUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * @author yijin
 * @since 2023/01
 */
public class StoragePoolManager extends AbstractLifecycle {

    private static final Logger logger = LoggerFactory.getLogger(LocalityManager.class);
    private static Boolean mockMode = false;

    public static String DEFAULT_STORAGE_POOL_NAME = "_default";

    public static String RECYCLE_STORAGE_POOL_NAME = "_recycle";
    public static String ALL_STORAGE_POOL = "__all_storage_pool";
    public static String INIT_STORAGE_POOL = "init_storage_pool";
    public static String EMPTY_STORAGE_POOL = "";
    public static int LOCK_TIME_OUT = 10;
    private volatile Map<Long, StoragePoolInfo> storagePoolCache;

    public volatile Map<String, StoragePoolInfo> storagePoolCacheByName;

    public volatile Map<String, String> storagePoolMap;

    private static StoragePoolManager INSTANCE = new StoragePoolManager();

    private List<String> defaultStorageInstList;

    public static StoragePoolManager getInstance() {
        if (!INSTANCE.isInited()) {
            synchronized (INSTANCE) {
                if (!INSTANCE.isInited()) {
                    INSTANCE.init();
                }
            }
        }
        return INSTANCE;
    }

    private StoragePoolManager() {
    }

    public Boolean notExistStoragePoolName(String storagePool) {
        return !storagePoolCacheByName.containsKey(storagePool);
    }

    public List<String> getStorageInstListByName(String storagePool) {
        if (notExistStoragePoolName(storagePool)) {
            return new ArrayList<>();
        } else {
            return getStoragePoolInfo(storagePool).getDnLists();
        }
    }

    public Map<String, StorageInfoRecord> getStorageInfoMap(String storagePoolName) {
        List<String> storageInstList = getStorageInstListByName(storagePoolName);
        Map<String, StorageInfoRecord> storagePoolInfoMap =
            DbTopologyManager.getStorageInfoMap(InstIdUtil.getMasterInstId());
        Map<String, StorageInfoRecord> fileterStoragePoolInfoMap = new HashMap<>();
        for (String storageInst : storagePoolInfoMap.keySet()) {
            if (storageInstList.contains(storageInst)) {
                fileterStoragePoolInfoMap.put(storageInst, storagePoolInfoMap.get(storageInst));
            }
        }
        return fileterStoragePoolInfoMap;
    }

    public String getUndeletableStorageInstByName(String storagePool) {
        if (notExistStoragePoolName(storagePool)) {
            return "";
        } else {
            return getStoragePoolInfo(storagePool).getUndeletableDnId();
        }
    }

    public StoragePoolInfo getStoragePoolInfo(String storagePool) {
        return storagePoolCacheByName.get(storagePool);
    }

    public LocalityDesc getDefaultLocalityDesc() {
        return LocalityInfoUtils.parse("dn=" + StringUtils.join(defaultStorageInstList, ","));
    }

    public LocalityDesc getDefaultStoragePoolLocalityDesc() {
        return LocalityInfoUtils.parse(LocalityDesc.STORAGE_POOL_PREFIX + "'" + DEFAULT_STORAGE_POOL_NAME + "'");
    }

    public Boolean isTriggered() {
        Set<String> cachedStoragePoolName = new HashSet<>(storagePoolCacheByName.keySet());
        Boolean isRecycleStoragePoolEmpty =
            CollectionUtils.isEmpty(storagePoolCacheByName.get(RECYCLE_STORAGE_POOL_NAME).getDnLists());
        cachedStoragePoolName.remove(DEFAULT_STORAGE_POOL_NAME);
        cachedStoragePoolName.remove(RECYCLE_STORAGE_POOL_NAME);
        return !cachedStoragePoolName.isEmpty() || !isRecycleStoragePoolEmpty;
    }

    @Override
    protected void doInit() {
        super.doInit();
        logger.warn("init storage pool manager...");
        if (mockMode) {
            this.storagePoolCache = new HashMap<>();
            this.storagePoolCacheByName = new HashMap<>();
            return;
        }
        setupConfigListener();
    }

    private void setupConfigListener() {
        // While init, we would first get lock, then write record into storage_pool_info only
        // if storage_pool_info is empty.
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            StoragePoolInfoConfigListener listener = new StoragePoolInfoConfigListener();
            String dataId = MetaDbDataIdBuilder.getStoragePoolInfoDataId();

            MetaDbConfigManager.getInstance().register(dataId, conn);
            MetaDbConfigManager.getInstance().bindListener(dataId, listener);

            logger.warn("try to intialize storage pool...");

            Boolean storagePoolStorageInitRequired = true;
            Boolean locked = false;
            while (!locked && storagePoolStorageInitRequired) {
                locked = MetaDbUtil.tryGetLock(conn, INIT_STORAGE_POOL, LOCK_TIME_OUT);
                if (!locked) {
                    storagePoolStorageInitRequired = initializeDefaultAndRecycleStoragePool(false);
                }
            }
            if (locked && storagePoolStorageInitRequired) {
                initializeDefaultAndRecycleStoragePool(true);
            }
            if (locked) {
                MetaDbUtil.releaseLock(conn, INIT_STORAGE_POOL);
            }
            reloadStoragePoolInfoFromMetaDb();
        } catch (SQLException e) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                "setup storage pool config_listener failed");
        }
    }

    private void initializeDefaultAndRecycleStoragePoolImpl(Connection metaDbConn
        , String instId) throws SQLException {
        try {
            StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
            storageInfoAccessor.setConnection(metaDbConn);
            StoragePoolInfoAccessor accessor = new StoragePoolInfoAccessor();
            accessor.setConnection(metaDbConn);
            metaDbConn.setAutoCommit(false);

            // firstly get storage inst records
            List<StorageInfoRecord> storageInfoRecords =
                storageInfoAccessor.getStorageInfosByInstId(instId).stream()
                    .filter(o -> o.instKind == StorageInfoRecord.INST_KIND_MASTER)
                    .collect(Collectors.toList());
            Set<String> storageInstIds =
                storageInfoRecords.stream().map(o -> o.storageInstId).collect(Collectors.toSet());
            // find default dn id and undeletable dn id
            String defaultDnIds = StringUtils.join(storageInstIds, ",");
            List<String> undeletableDnIds =
                DbTopologyManager.getNonDeletableStorageInst(metaDbConn).stream()
                    .filter(o -> storageInstIds.contains(o))
                    .collect(Collectors.toList());
            String undeletableDnId;
            if (undeletableDnIds.isEmpty()) {
                if (storageInstIds.size() > 0) {
                    undeletableDnId = new ArrayList<>(storageInstIds).get(0);
                } else {
                    logger.warn(
                        "initialize storage pool failed ... because there are no avaliable storage insts");
                    return;
                }
            } else {
                undeletableDnId = undeletableDnIds.get(0);
            }
            EventLogger.log(EventType.STORAGE_POOL_INFO,
                String.format("initialize %s storage pool info with %s, %s", "default", defaultDnIds,
                    undeletableDnId));
            accessor.addNewStoragePoolInfo(DEFAULT_STORAGE_POOL_NAME, defaultDnIds, undeletableDnId);

            EventLogger.log(EventType.STORAGE_POOL_INFO,
                String.format("initialize %s storage pool info with %s, %s", "recycle", "",
                    ""));
            accessor.addNewStoragePoolInfo(RECYCLE_STORAGE_POOL_NAME, "", "");

            for (StorageInfoRecord storageInfoRecord : storageInfoRecords) {
                StorageInfoExtraFieldJSON extras =
                    Optional.ofNullable(storageInfoRecord.extras).orElse(new StorageInfoExtraFieldJSON());
                extras.setStoragePoolName(DEFAULT_STORAGE_POOL_NAME);
                storageInfoAccessor.updateStoragePoolName(storageInfoRecord.storageInstId, extras);
            }
            metaDbConn.commit();
        } catch (SQLException e) {
            logger.error("initialize storage pool config failed.", e);
            throw e;
        }
    }

    private Boolean initializeDefaultAndRecycleStoragePool(Boolean locked) {
        int iso = -1;
        ServerInstIdManager serverInstIdManager = ServerInstIdManager.getInstance();
        String instId = serverInstIdManager.getMasterInstId();
        Boolean initializedSuccess = false;
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            // 1. set iso to READ_COMMITED
            iso = conn.getTransactionIsolation();
            StoragePoolInfoAccessor accessor = new StoragePoolInfoAccessor();
            try {
                conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                accessor.setConnection(conn);

                // 2. get all storage pool info
                List<StoragePoolInfoRecord> records = accessor.getAllStoragePoolInfoRecord();
                if (!records.isEmpty()) {
                    return false;
                }
                if (!locked) {
                    return true;
                } else {
                    // 3. initialize storage pool info
                    initializeDefaultAndRecycleStoragePoolImpl(conn, instId);
                    MetaDbConfigManager.getInstance().notify(MetaDbDataIdBuilder.getStoragePoolInfoDataId(), conn);
                    initializedSuccess = true;
                }
            } finally {
                if (iso >= 0) {
                    conn.setTransactionIsolation(iso);
                }
            }
        } catch (SQLException ex) {
            throw new RuntimeException(ex);
        }
        return !initializedSuccess;
    }

    @Override
    protected void doDestroy() {
        super.doDestroy();
    }

    protected static class StoragePoolInfoConfigListener implements ConfigListener {
        @Override
        public void onHandleConfig(String dataId, long newOpVersion) {
            StoragePoolManager.getInstance().reloadStoragePoolInfoFromMetaDb();
        }
    }

    public List<String> getListAfterRemoveDnIdFromOginalStoragePoolInfo(String dnIds, String storagePoolName) {
        List<String> storageInstList = getStorageInstListByName(storagePoolName);
        storageInstList.removeAll(StoragePoolUtils.buildStorageInstListFromString(dnIds));
        return storageInstList;

    }

    public String removeDnListFromOriginalStoragePoolInfo(String dnIds, String storagePoolName) {
        List<String> storageInstList = getListAfterRemoveDnIdFromOginalStoragePoolInfo(dnIds, storagePoolName);
        return StoragePoolUtils.buildStringFromStorageInstList(storageInstList);
    }

    public String appendDnListFromOriginalStoragePoolInfo(String dnIds, String storagePoolName) {
        List<String> storageInstList = getStorageInstListByName(storagePoolName);
        storageInstList.addAll(StoragePoolUtils.buildStorageInstListFromString(dnIds));
        return StoragePoolUtils.buildStringFromStorageInstList(storageInstList);
    }

    public void addStoragePool(Connection metaDbConnection, String storagePoolName, String dnIds,
                               String undeletableDnId) {
        if (!storagePoolCacheByName.containsKey(storagePoolName)) {
            // store storage pool
            storeStoragePoolInfo(metaDbConnection, storagePoolName, dnIds, undeletableDnId);
            if (storagePoolCacheByName.containsKey(RECYCLE_STORAGE_POOL_NAME)) {
                // remove from recycle storage pool
                String targetDnIdStr = removeDnListFromOriginalStoragePoolInfo(dnIds, RECYCLE_STORAGE_POOL_NAME);
                updateStoragePoolInfo(metaDbConnection, RECYCLE_STORAGE_POOL_NAME, targetDnIdStr, "");
            }
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                String.format("duplicate storage pool name '%s' found! " + storagePoolName));
        }
    }

    public void deleteStoragePool(Connection metadbConnection, String storagePoolName) {
        if (storagePoolCacheByName.containsKey(storagePoolName)) {
            deleteStoragePoolInfo(metadbConnection, storagePoolName);
        }
    }

    public void mergeIntoStoragePool(Connection connection, String originalStoragePoolName,
                                     String targetStoragePoolName) {
        if (storagePoolCacheByName.containsKey(originalStoragePoolName)) {
            if (!storagePoolCacheByName.containsKey(targetStoragePoolName)) {
                updateStoragePoolInfoName(originalStoragePoolName, targetStoragePoolName);
            } else {
                StoragePoolInfo targetStoragePoolInfo = storagePoolCacheByName.get(targetStoragePoolName);
                StoragePoolInfo originalStoragePoolInfo = storagePoolCacheByName.get(originalStoragePoolName);
                String undeletableDnId = targetStoragePoolInfo.getUndeletableDnId();
                List<String> dnIds =
                    StoragePoolUtils.mergeDnIdList(originalStoragePoolInfo.getDnLists(),
                        targetStoragePoolInfo.getDnLists());
                String dnIdStr = StoragePoolUtils.buildStringFromStorageInstList(dnIds);
                updateStoragePoolInfo(connection, targetStoragePoolName, dnIdStr, undeletableDnId);
                deleteStoragePool(connection, originalStoragePoolName);
            }
        }
    }

    public void shrinkStoragePoolSimply(Connection connection, String storagePoolName, String dnIds) {
        if (storagePoolCacheByName.containsKey(storagePoolName)) {
            StoragePoolInfo storagePoolInfo = storagePoolCacheByName.get(storagePoolName);
            String afterShrinkDnIds = removeDnListFromOriginalStoragePoolInfo(dnIds, storagePoolName);
            updateStoragePoolInfo(connection, storagePoolName, afterShrinkDnIds, storagePoolInfo.getUndeletableDnId());
        }
    }

    public void autoExpandDefaultStoragePool(Connection connection) {
        if (storagePoolCacheByName.containsKey(DEFAULT_STORAGE_POOL_NAME)) {
            StoragePoolInfo storagePoolInfo = storagePoolCacheByName.get(DEFAULT_STORAGE_POOL_NAME);
            String undeletableDnId = storagePoolInfo.getUndeletableDnId();
            Set<String> fullDnSet = DbTopologyManager.getAllAliveStorageInsts(InstIdUtil.getMasterInstId());
            String afterExpandDnIds = StoragePoolUtils.buildStringFromStorageInstList(fullDnSet);
            updateStoragePoolInfo(connection, DEFAULT_STORAGE_POOL_NAME, afterExpandDnIds, undeletableDnId);
        }
    }

    public void appendStoragePool(Connection connection, String storagePoolName, String dnIds, String undeletableDnId) {
        if (storagePoolCacheByName.containsKey(storagePoolName)) {
            StoragePoolInfo storagePoolInfo = storagePoolCacheByName.get(storagePoolName);
            String afterAppendDnIds = appendDnListFromOriginalStoragePoolInfo(dnIds, storagePoolName);
            String afterAppendUndeletableDnId = storagePoolInfo.getUndeletableDnId();
            if (!StringUtils.isEmpty(afterAppendUndeletableDnId)) {
                undeletableDnId = afterAppendUndeletableDnId;
            }
            updateStoragePoolInfo(connection, storagePoolName, afterAppendDnIds, undeletableDnId);
//            reloadStoragePoolInfoFromMetaDb();
        }
    }

    public void shrinkStoragePool(Connection connection, String storagePoolName, String dnIds, String undeletableDnId) {
        if (storagePoolCacheByName.containsKey(storagePoolName)) {
            StoragePoolInfo storagePoolInfo = storagePoolCacheByName.get(storagePoolName);
            boolean recycleExists = !notExistStoragePoolName(RECYCLE_STORAGE_POOL_NAME);
            String afterShrinkDnIds = removeDnListFromOriginalStoragePoolInfo(dnIds, storagePoolName);
            String expandRecycleDnIds = appendDnListFromOriginalStoragePoolInfo(dnIds, RECYCLE_STORAGE_POOL_NAME);
            undeletableDnId = storagePoolInfo.getUndeletableDnId();
            List<String> dnList = getListAfterRemoveDnIdFromOginalStoragePoolInfo(dnIds, storagePoolName);
            if (!StringUtils.isEmpty(undeletableDnId) && !dnList.contains(undeletableDnId)) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    "we do not expect this operation, this is dangerous because there are no undeletable dn in storage pool");
            }
            if (StringUtils.isEmpty(afterShrinkDnIds)) {
                undeletableDnId = "";
            }
            updateStoragePoolInfo(connection, storagePoolName, afterShrinkDnIds, undeletableDnId);
            if (recycleExists) {
                updateStoragePoolInfo(connection, RECYCLE_STORAGE_POOL_NAME, expandRecycleDnIds, "");
            } else {
                storeStoragePoolInfo(connection, RECYCLE_STORAGE_POOL_NAME, expandRecycleDnIds, "");
            }
//            reloadStoragePoolInfoFromMetaDb();
        }
    }

    public void storeStoragePoolInfo(Connection connection, String storagePoolName, String dnIds,
                                     String undeletableDnId) {
        StoragePoolInfoAccessor accessor = new StoragePoolInfoAccessor();
        accessor.setConnection(connection);
        accessor.addNewStoragePoolInfo(storagePoolName, dnIds, undeletableDnId);
    }

    public void deleteStoragePoolInfo(Connection conn, String storagePoolName) {
        StoragePoolInfoAccessor accessor = new StoragePoolInfoAccessor();
        accessor.setConnection(conn);
        accessor.deleteStoragePoolInfo(storagePoolName);
    }

    public void truncateStoragePoolInfo() {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            StoragePoolInfoAccessor accessor = new StoragePoolInfoAccessor();
            accessor.setConnection(conn);
            accessor.truncateStoragePoolInfo(DEFAULT_STORAGE_POOL_NAME, RECYCLE_STORAGE_POOL_NAME);
        } catch (SQLException e) {
            MetaDbLogUtil.META_DB_LOG.error(e);
            throw GeneralUtil.nestedException(e);
        }

    }

    public void updateStoragePoolInfoName(String originalStoragePoolName, String targetStoragePoolName) {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            StoragePoolInfoAccessor accessor = new StoragePoolInfoAccessor();
            accessor.setConnection(conn);
            accessor.updateStoragePoolInfoName(originalStoragePoolName, targetStoragePoolName);
        } catch (SQLException e) {
            MetaDbLogUtil.META_DB_LOG.error(e);
            throw GeneralUtil.nestedException(e);
        }
    }

    public void updateStoragePoolInfo(Connection conn, String storagePoolName, String dnIds, String undeletableDnId) {
        StoragePoolInfoAccessor accessor = new StoragePoolInfoAccessor();
        accessor.setConnection(conn);
        accessor.updateStoragePoolInfo(storagePoolName, dnIds, undeletableDnId);
    }

/**
 * Get locality of database with inherited from default
 */
    /**
     * Load all records in system-table to in-memory cache.
     */
    public synchronized void reloadStoragePoolInfoFromMetaDb() {
        try (Connection conn = MetaDbDataSource.getInstance().getConnection()) {
            StoragePoolInfoAccessor accessor = new StoragePoolInfoAccessor();
            accessor.setConnection(conn);
            Map<String, StoragePoolInfo> newCacheByName = new ConcurrentHashMap<>();
            Map<Long, StoragePoolInfo> newCache = new ConcurrentHashMap<>();
            Map<String, String> newStoragePoolMap = new ConcurrentHashMap<>();

            List<StoragePoolInfoRecord> records = accessor.getAllStoragePoolInfoRecord();
            Set<String> occupiedStorageIds = new HashSet<>();
            for (StoragePoolInfoRecord record : records) {
                StoragePoolInfo info = StoragePoolInfo.from(record);
                newCache.put(info.getId(), info);
                newCacheByName.put(info.getName(), info);
                Arrays.stream(info.getDnIds().split(",")).forEach(o -> newStoragePoolMap.put(o, info.getName()));
                occupiedStorageIds.addAll(Arrays.stream(info.getDnIds().split(",")).collect(Collectors.toList()));
                // setup system primary_zone
            }

            List<String> storageIds = new ArrayList<>();
            if (!newCacheByName.containsKey(DEFAULT_STORAGE_POOL_NAME)) {
                StorageInfoAccessor storageInfoAccessor = new StorageInfoAccessor();
                storageInfoAccessor.setConnection(conn);
                List<StorageInfoRecord> storageInfoRecords =
                    storageInfoAccessor.getStorageInfosByInstId(InstIdUtil.getInstId()).stream()
                        .filter(o -> o.instKind == StorageInfoRecord.INST_KIND_MASTER)
                        .filter(o -> !occupiedStorageIds.contains(o.storageInstId)).collect(Collectors.toList());
                storageIds =
                    storageInfoRecords.stream().map(o -> o.storageInstId).distinct().collect(Collectors.toList());
            } else {
                storageIds = newCacheByName.get(DEFAULT_STORAGE_POOL_NAME).getDnLists();
            }
            this.defaultStorageInstList = storageIds;
            this.storagePoolCache = newCache;
            this.storagePoolCacheByName = newCacheByName;
            this.storagePoolMap = newStoragePoolMap;

            logger.warn("reload storage pool cache from metadb: " + this.storagePoolCache.toString());
        } catch (SQLException e) {
            MetaDbLogUtil.META_DB_LOG.error(e);
            throw GeneralUtil.nestedException(e);
        }
    }

}
