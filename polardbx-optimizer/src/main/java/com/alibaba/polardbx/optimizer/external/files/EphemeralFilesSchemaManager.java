package com.alibaba.polardbx.optimizer.external.files;

import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.gms.metadb.external.ExternalCatalogConstants;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;

import java.util.Collection;
import java.util.EnumSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

public class EphemeralFilesSchemaManager implements SchemaManager {

    public static final String SCHEMA_NAME = ExternalCatalogConstants.EPHEMERAL_SCHEMA_NAME;

    private final Map<String, TableMeta> tables = new ConcurrentHashMap<>();
    private final AtomicInteger tableSeq = new AtomicInteger(0);

    /**
     * Generates a unique ephemeral table name within this request.
     * Uses an instance-level counter to guarantee uniqueness even when
     * multiple FILES() appear in a single query.
     */
    public String nextTableName() {
        return ExternalCatalogConstants.EPHEMERAL_TABLE_PREFIX + (tableSeq.getAndIncrement());
    }

    @Override
    public TableMeta getTable(String tableName) {
        TableMeta meta = tables.get(tableName.toLowerCase());
        if (meta == null) {
            throw new TableNotFoundException(ErrorCode.ERR_TABLE_NOT_EXIST, tableName);
        }
        return meta;
    }

    @Override
    public Collection<TableMeta> getAllTables() {
        return tables.values();
    }

    @Override
    public void putTable(String tableName, TableMeta tableMeta) {
        tables.put(tableName.toLowerCase(), tableMeta);
    }

    @Override
    public void reload(String tableName) {
    }

    @Override
    public void invalidate(String tableName) {
    }

    @Override
    public void invalidateAll() {
    }

    @Override
    public GsiMetaManager.GsiMetaBean getGsi(String primaryOrIndexTableName, EnumSet<IndexStatus> statusSet) {
        return GsiMetaManager.GsiMetaBean.empty();
    }

    @Override
    public String getSchemaName() {
        return SCHEMA_NAME;
    }

    @Override
    public void init() {
    }

    @Override
    public void destroy() {
    }

    @Override
    public boolean isInited() {
        return true;
    }
}
