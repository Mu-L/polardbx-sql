package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.executor.columnar.ExternalColumnTableIdResolver;
import com.alibaba.polardbx.executor.cursor.ResultCursor;

import java.util.List;

/**
 * SyncAction that cleans up blob-related in-memory caches for a dropped table.
 * Broadcast to ALL CN nodes via SyncManagerHelper so that every CN releases stale
 * ExternalColumnTableIdResolver cache entries.
 */
public class CleanBlobCacheForTableSyncAction implements ISyncAction {

    private String schemaName;
    private String tableName;
    private List<Long> blobTableIds;

    public CleanBlobCacheForTableSyncAction() {
    }

    public CleanBlobCacheForTableSyncAction(String schemaName, String tableName, List<Long> blobTableIds) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.blobTableIds = blobTableIds;
    }

    @Override
    public ResultCursor sync() {
        if (tableName == null) {
            ExternalColumnTableIdResolver.getInstance().invalidateForSchema(schemaName);
        } else {
            ExternalColumnTableIdResolver.getInstance().invalidateForTable(schemaName, tableName);
        }

        return null;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<Long> getBlobTableIds() {
        return blobTableIds;
    }

    public void setBlobTableIds(List<Long> blobTableIds) {
        this.blobTableIds = blobTableIds;
    }
}
