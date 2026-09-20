package com.alibaba.polardbx.gms.metadb.external;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ExternalCatalogManager {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExternalCatalogManager.class);
    private static final ExternalCatalogManager INSTANCE = new ExternalCatalogManager();

    /**
     * Authoritative full set of catalogs, not an evicting cache: readers always see
     * either the previous or the fully-built snapshot, never a partial reload.
     */
    private volatile Map<String, ExternalCatalogInfo> catalogs = new ConcurrentHashMap<>();

    private ExternalCatalogManager() {
    }

    public static ExternalCatalogManager getInstance() {
        return INSTANCE;
    }

    public ExternalCatalogInfo get(String catalogName) {
        return catalogs.get(catalogName.toLowerCase());
    }

    public synchronized void register(ExternalCatalogInfo info) {
        catalogs.put(info.getName().toLowerCase(), info);
    }

    public synchronized void remove(String name) {
        catalogs.remove(name.toLowerCase());
    }

    public Collection<ExternalCatalogInfo> listAll() {
        return catalogs.values();
    }

    public boolean exists(String catalogName) {
        return catalogs.containsKey(catalogName.toLowerCase());
    }

    public boolean isEmpty() {
        return catalogs.isEmpty();
    }

    public synchronized void invalidateAll() {
        catalogs = new ConcurrentHashMap<>();
    }

    /**
     * Atomically replaces all cached entries with the given collection by swapping
     * the map reference. Package-private for testing.
     */
    synchronized void replaceWithEntries(Collection<ExternalCatalogInfo> newInfos) {
        Map<String, ExternalCatalogInfo> newCatalogs = new ConcurrentHashMap<>();
        for (ExternalCatalogInfo info : newInfos) {
            newCatalogs.put(info.getName().toLowerCase(), info);
        }
        catalogs = newCatalogs;
    }

    @SuppressWarnings("unchecked")
    public synchronized void loadFromGms() {
        try (Connection conn = MetaDbUtil.getConnection()) {
            ExternalCatalogInfoAccessor accessor = new ExternalCatalogInfoAccessor();
            accessor.setConnection(conn);
            List<ExternalCatalogInfoRecord> records = accessor.selectAll();
            List<ExternalCatalogInfo> infos = new ArrayList<>();
            for (ExternalCatalogInfoRecord record : records) {
                try {
                    infos.add(record.toInfo());
                } catch (Exception e) {
                    LOGGER.warn("Skipped external catalog '" + record.name
                        + "' during load: " + e.getMessage());
                }
            }
            replaceWithEntries(infos);
            LOGGER.info("Loaded " + infos.size() + " of " + records.size()
                + " external catalogs from GMS");
        } catch (Exception e) {
            LOGGER.warn("Failed to load external catalogs from GMS: " + e.getMessage());
        }
    }
}
