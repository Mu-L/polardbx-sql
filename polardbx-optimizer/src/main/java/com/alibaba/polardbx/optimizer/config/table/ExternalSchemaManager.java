package com.alibaba.polardbx.optimizer.config.table;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.secret.SecretBundle;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.external.ExternalNameValidator;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.TableStatus;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager.GsiMetaBean;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorMetadata;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorRegistry;
import com.alibaba.polardbx.optimizer.external.connector.ConnectorTable;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import java.util.Collection;
import java.util.Collections;
import java.util.EnumSet;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class ExternalSchemaManager extends AbstractLifecycle implements SchemaManager {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExternalSchemaManager.class);

    private static final long CACHE_TTL_HOURS = 24;
    private static final int CACHE_MAX_SIZE = 10_000;
    private static final long NEGATIVE_CACHE_TTL_MINUTES = 1;
    private static final int NEGATIVE_CACHE_MAX_SIZE = 10_000;

    private final String catalogName;
    private final String dbName;
    private final String fullSchemaName;
    private final String connector;
    private final Map<String, String> properties;
    private final SecretBundle secret;
    private final String secretName;
    private final long secretVersion;

    /**
     * Shared metadata handle, populated only for a connector whose instances are
     * thread-safe. Stays null for a non-thread-safe one, where every lookup owns and
     * closes a private instance instead.
     */
    private volatile ConnectorMetadata cachedConnectorMetadata;

    private final Boolean threadSafe;

    private volatile boolean closed = false;

    /**
     * Second at which a lookup last started, and the only thing {@link #detachIfIdle} looks
     * at: no count of the lookups currently holding the shared handle, so a hung connector
     * call cannot pin that handle forever, at the price of failing a lookup that outlives
     * the idle TTL. Second granularity keeps {@link #getTable} down to one write per second.
     * <p>
     */
    private volatile long lastAccessSecond;

    private final ReadWriteLock lock = new ReentrantReadWriteLock();

    private final Cache<String, TableMeta> cache = Caffeine.newBuilder()
        .expireAfterWrite(CACHE_TTL_HOURS, TimeUnit.HOURS)
        .maximumSize(CACHE_MAX_SIZE)
        .build();

    private final Cache<String, Boolean> negativeCache = Caffeine.newBuilder()
        .expireAfterWrite(NEGATIVE_CACHE_TTL_MINUTES, TimeUnit.MINUTES)
        .maximumSize(NEGATIVE_CACHE_MAX_SIZE)
        .build();

    public ExternalSchemaManager(String catalogName, String dbName, String connector,
                                 Map<String, String> properties, SecretBundle secret,
                                 String secretName, long secretVersion) {
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.fullSchemaName = ExternalNameValidator.encodeSchemaName(catalogName, dbName);
        this.connector = connector;
        this.properties = properties;
        this.secret = secret;
        this.secretName = secretName;
        this.secretVersion = secretVersion;
        this.threadSafe = false;
        this.lastAccessSecond = currentSecond();
    }

    public ExternalSchemaManager(String catalogName, String dbName, String connector,
                                 Map<String, String> properties, SecretBundle secret,
                                 String secretName, long secretVersion, boolean threadSafe,
                                 ConnectorMetadata metadata) {
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.fullSchemaName = ExternalNameValidator.encodeSchemaName(catalogName, dbName);
        this.connector = connector;
        this.properties = properties;
        this.secret = secret;
        this.secretName = secretName;
        this.secretVersion = secretVersion;
        this.threadSafe = threadSafe;
        this.cachedConnectorMetadata = threadSafe ? metadata : null;
        this.lastAccessSecond = currentSecond();
    }

    /**
     * Monotonic, so an NTP adjustment cannot make a schema look idle or freshly used.
     */
    private static long currentSecond() {
        return TimeUnit.NANOSECONDS.toSeconds(System.nanoTime());
    }

    private boolean outdated(long idleTtlSeconds) {
        return currentSecond() - lastAccessSecond >= idleTtlSeconds;
    }

    private void touch() {
        long now = currentSecond();
        if (lastAccessSecond != now) {
            lastAccessSecond = now;
        }
    }

    private ConnectorMetadata createMetadata() {
        return ConnectorRegistry.getInstance().get(connector).createMetadata(properties, secret);
    }

    @Override
    public TableMeta getTable(String tableName) {
        touch();
        String key = tableName.toLowerCase();
        if (negativeCache.getIfPresent(key) != null) {
            throw new TableNotFoundException(ErrorCode.ERR_TABLE_NOT_EXIST, tableName);
        }
        TableMeta result = cache.get(key, k -> {
            Optional<ConnectorTable> ct;
            ConnectorMetadata metadata = threadSafe ? getCachedConnectorMetadata() : createMetadata();
            if (metadata == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE, " already closed");
            }
            try {
                ct = metadata.getTable(dbName.toLowerCase(), tableName.toLowerCase());
            } catch (TddlRuntimeException e) {
                if (e.getErrorCodeType() == ErrorCode.ERR_TABLE_NOT_EXIST) {
                    negativeCache.put(key, Boolean.TRUE);
                }
                throw e;
            } catch (Exception e) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE, e,
                    "Failed to get table '" + tableName + "' from external catalog '"
                        + catalogName + "': " + e.getMessage());
            } finally {
                if (!threadSafe) {
                    ConnectorMetadata.closeQuietly(metadata);
                }
            }
            if (!ct.isPresent()) {
                negativeCache.put(key, Boolean.TRUE);
                throw new TableNotFoundException(ErrorCode.ERR_TABLE_NOT_EXIST, tableName);
            }
            return toTableMeta(ct.get(), tableName);
        });
        return result;
    }

    private ConnectorMetadata getCachedConnectorMetadata() {
        touch();
        lock.readLock().lock();

        try {
            if (cachedConnectorMetadata == null) {
                synchronized (this) {
                    if (closed) {
                        throw new TddlRuntimeException(ErrorCode.ERR_EXTERNAL_TABLE, " already closed");
                    }
                    if (cachedConnectorMetadata == null) {
                        cachedConnectorMetadata = createMetadata();
                    }
                }
            }
        } finally {
            lock.readLock().unlock();
        }
        return cachedConnectorMetadata;
    }

    public int detachIfIdle(long idleTtlSeconds) {
        if (closed || !outdated(idleTtlSeconds)) {
            return 0;
        }
        ConnectorMetadata m;
        lock.writeLock().lock();
        try {
            if (!outdated(idleTtlSeconds)) {
                return 0;
            }
            m = cachedConnectorMetadata;
            cachedConnectorMetadata = null;
        } finally {
            lock.writeLock().unlock();
        }
        ConnectorMetadata.closeQuietly(m);
        cache.invalidateAll();
        negativeCache.invalidateAll();
        return 1;
    }

    private TableMeta toTableMeta(ConnectorTable ct, String tableName) {
        TableMeta tm = new TableMeta(fullSchemaName, tableName, ct.columns,
            null, Collections.emptyList(), false, TableStatus.PUBLIC, 0L, 0L);
        tm.setEngine(Engine.EXTERNAL);
        tm.setExternalCatalogName(catalogName);
        return tm;
    }

    @Override
    public Collection<TableMeta> getAllTables() {
        return cache.asMap().values();
    }

    @Override
    public void putTable(String tableName, TableMeta tableMeta) {
        String key = tableName.toLowerCase();
        cache.put(key, tableMeta);
        negativeCache.invalidate(key);
    }

    @Override
    public void reload(String tableName) {
        String key = tableName.toLowerCase();
        cache.invalidate(key);
        negativeCache.invalidate(key);
    }

    @Override
    public void invalidate(String tableName) {
        String key = tableName.toLowerCase();
        cache.invalidate(key);
        negativeCache.invalidate(key);
    }

    @Override
    public void invalidateAll() {
        cache.invalidateAll();
        negativeCache.invalidateAll();
    }

    @Override
    public GsiMetaBean getGsi(String primaryOrIndexTableName, EnumSet<IndexStatus> statusSet) {
        return GsiMetaBean.empty();
    }

    @Override
    public String getSchemaName() {
        return fullSchemaName;
    }

    @Override
    public void doInit() {
    }

    @Override
    public void doDestroy() {
        close();
    }

    /**
     * Closes the underlying ConnectorMetadata and invalidates the table cache.
     * Safe to call multiple times. Used by eviction paths that bypass lifecycle.
     */
    public void close() {
        ConnectorMetadata m;
        if (closed) {
            return;
        }
        synchronized (this) {
            if (closed) {
                return;
            }
            closed = true;
            m = cachedConnectorMetadata;
            cachedConnectorMetadata = null;
        }
        ConnectorMetadata.closeQuietly(m);
        cache.invalidateAll();
        negativeCache.invalidateAll();
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getDbName() {
        return dbName;
    }

    public String getSecretName() {
        return secretName;
    }

    public long getSecretVersion() {
        return secretVersion;
    }
}
