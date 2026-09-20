package com.alibaba.polardbx.gms.topology;

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.metadb.table.LoadWeightAccessor;
import com.alibaba.polardbx.gms.metadb.table.LoadWeightRecord;
import com.alibaba.polardbx.gms.metadb.table.SubClusterAccessor;
import com.alibaba.polardbx.gms.metadb.table.SubClusterRecord;
import com.alibaba.polardbx.gms.util.MetaDbLogUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;

import java.sql.Connection;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ServerInstSubManager extends AbstractLifecycle {

    protected static ServerInstSubManager instance = new ServerInstSubManager();
    protected volatile Multimap<String, String> idToSubcluster = HashMultimap.create();
    private final ReadWriteLock idToSubclusterLock = new ReentrantReadWriteLock();
    protected volatile HashMap<String, String> idToLoadWeight = new HashMap<>();
    private final ReadWriteLock idToLoadWeightLock = new ReentrantReadWriteLock();

    public static ServerInstSubManager getInstance() {
        if (!instance.isInited()) {
            synchronized (instance) {
                if (!instance.isInited()) {
                    instance.init();
                }
            }
        }
        return instance;
    }
    // for test
    SubClusterAccessor newSubClusterAccessor() {
        return new SubClusterAccessor();
    }

    LoadWeightAccessor newLoadWeightAccessor() {
        return new LoadWeightAccessor();
    }


    @Override
    protected void doInit() {
        loadNodeSubCluster();
        loadNodeLoadWeight();
    }


    public void loadNodeSubCluster() {
        try (Connection connection = MetaDbUtil.getConnection()) {
            SubClusterAccessor accessor = newSubClusterAccessor();
            accessor.setConnection(connection);
            List<SubClusterRecord> subClusterRecords = accessor.query();

            idToSubclusterLock.writeLock().lock();
            try {
                idToSubcluster.clear();
                for (SubClusterRecord record : subClusterRecords) {
                    idToSubcluster.put(record.instId + record.node, record.subCluster);
                }
            } finally {
                idToSubclusterLock.writeLock().unlock();
            }
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
    }

    public void loadNodeLoadWeight() {
        try (Connection connection = MetaDbUtil.getConnection()) {
            LoadWeightAccessor loadWeightAccessor = newLoadWeightAccessor();
            loadWeightAccessor.setConnection(connection);
            List<LoadWeightRecord> loadWeightRecords = loadWeightAccessor.query();

            idToLoadWeightLock.writeLock().lock();
            try {
                idToLoadWeight.clear();
                for (LoadWeightRecord record : loadWeightRecords) {
                    idToLoadWeight.put(record.instId + record.node, record.loadWeight);
                }
            } finally {
                idToLoadWeightLock.writeLock().unlock();
            }
        } catch (Throwable ex) {
            MetaDbLogUtil.META_DB_LOG.error(ex);
            throw GeneralUtil.nestedException(ex);
        }
    }

    public Multimap<String, String> getIdToSubcluster() {
        idToSubclusterLock.readLock().lock();
        try {
            return ImmutableMultimap.copyOf(idToSubcluster);
        } finally {
            idToSubclusterLock.readLock().unlock();
        }
    }

    public HashMap<String, String> getIdToLoadWeight() {
        idToLoadWeightLock.readLock().lock();
        try {
            return new HashMap<>(idToLoadWeight);
        } finally {
            idToLoadWeightLock.readLock().unlock();
        }
    }
}
