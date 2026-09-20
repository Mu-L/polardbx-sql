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

package com.alibaba.polardbx.common.oss.filesystem;

import com.alibaba.polardbx.cache.GeneralCache;
import com.alibaba.polardbx.cache.options.GetOptions;
import com.alibaba.polardbx.cache.statistics.CacheStatistics;
import com.alibaba.polardbx.common.oss.filesystem.cache.GeneralCacheConfig;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.aliyun.oss.OSS;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.util.SemaphoredDelegatingExecutor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.function.Supplier;

/**
 * Encapsulated singleton adapter for the GeneralCache system.
 * Replaces the old {@code OSSGeneralCacheProxy} with:
 * <ul>
 *   <li>Singleton lifecycle management (init/shutdown)</li>
 *   <li>Dynamic prefix resolution for cache file names</li>
 *   <li>DynamicConfig-based enable/disable switch integration</li>
 *   <li>Centralized cache routing logic (tryOpenCachedStream)</li>
 * </ul>
 * <p>
 * Does NOT hold any OSSFileSystem instance reference — only holds GeneralCache.
 * This decouples cache and filesystem lifecycles.
 */
public class OSSCacheAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(OSSCacheAdapter.class);

    private static volatile OSSCacheAdapter INSTANCE;

    /**
     * Global switch controlling whether the three cache-bypass guards
     * ({@link OSSFileSystem#open}, {@link OSSInputStream} constructor and
     * {@link OSSFileSystemStore#retrieve}) throw {@link IllegalStateException}
     * when a direct OSS read is detected while cache is enabled.
     * <p>
     * Default is {@code true} (strict mode, suitable for CN).
     * Columnar processes that reuse the CN common module should flip this to
     * {@code false} during startup via {@code setBypassDetectionEnabled(false)}
     * so that direct OSS reads are allowed silently (no exception, no log).
     */
    @Setter
    @Getter
    private static volatile boolean bypassDetectionEnabled = true;

    @Getter
    private final GeneralCache cache;
    @Getter
    private final GeneralCacheConfig config;

    /**
     * Supplier for the current columnar directory prefix.
     * Set during registerRemoteStorage(). Used by tryOpenCachedStream() to strip prefix from OSS keys.
     * Re-evaluated on every call to support dynamic columnarDirectory changes.
     */
    private volatile Supplier<String> columnarDirSupplier;

    /**
     * Reference to the registered PrefixRoutingRemoteStorageService for rate limit updates.
     */
    private volatile PrefixRoutingRemoteStorageService remoteStorageService;

    private OSSCacheAdapter(GeneralCache cache, GeneralCacheConfig config) {
        this.cache = cache;
        this.config = config;
    }

    // ===================== Lifecycle =====================

    /**
     * Initialize the singleton. Can only be called once (until shutdown).
     *
     * @param cache the GeneralCache instance (must not be null)
     * @param config the cache configuration (must not be null)
     * @throws IllegalStateException if already initialized
     */
    public static synchronized void init(GeneralCache cache, GeneralCacheConfig config) {
        if (INSTANCE != null) {
            throw new IllegalStateException("OSSCacheAdapter already initialized. Call shutdown() first.");
        }
        if (cache == null) {
            throw new IllegalArgumentException("GeneralCache must not be null");
        }
        if (config == null) {
            throw new IllegalArgumentException("GeneralCacheConfig must not be null");
        }
        INSTANCE = new OSSCacheAdapter(cache, config);
        LOG.info("OSSCacheAdapter initialized");
    }

    /**
     * Get the singleton instance, or null if not initialized.
     */
    public static OSSCacheAdapter getInstanceOrNull() {
        return INSTANCE;
    }

    /**
     * Shutdown the adapter. Cuts off external access, then the caller should
     * proceed to close GeneralCache/LocalBlockCache/RpcServer internally.
     */
    public static synchronized void shutdown() {
        INSTANCE = null;
        LOG.info("OSSCacheAdapter shut down");
    }

    // ===================== Routing =====================

    /**
     * Check if cache is available and enabled. Three conditions must all be true:
     * <ol>
     *   <li>INSTANCE != null (cache initialized) — caller checks via getInstanceOrNull()</li>
     *   <li>DynamicConfig switch is ON</li>
     *   <li>RemoteStorageService is registered (OSSFileSystem has completed initialization)</li>
     * </ol>
     */
    public boolean isEnabled() {
        return DynamicConfig.getInstance().isEnableOssGeneralCache()
            && cache.getRemoteStorageService() != null;
    }

    /**
     * Register the OSS client as a PrefixRoutingRemoteStorageService on the GeneralCache.
     * Idempotent: if already registered, this is a no-op.
     *
     * @param ossClient OSS client instance
     * @param bucket OSS bucket name
     * @param encAlg server-side encryption algorithm
     * @param columnarDirSupplier supplier for the current columnar directory prefix (re-evaluated dynamically)
     */
    public synchronized void registerRemoteStorage(
        OSS ossClient, String bucket, String encAlg,
        Supplier<String> columnarDirSupplier) {
        this.columnarDirSupplier = columnarDirSupplier;

        // Create pathMapper: pure file name -> full OSS key (dynamic prefix)
        PrefixRoutingRemoteStorageService rss = new PrefixRoutingRemoteStorageService(
            ossClient, bucket, encAlg, config.getOssRateLimit(),
            cacheFileName -> columnarDirSupplier.get() + "/" + cacheFileName);

        if (cache.getRemoteStorageService() != null) {
            LOG.info("Replacing existing RemoteStorageService with new OSSClient");
        }
        cache.setRemoteStorageService(rss);
        this.remoteStorageService = rss;
        LOG.info("PrefixRoutingRemoteStorageService registered with rateLimit=" + config.getOssRateLimit());

        // If a dynamic override was already set via SET GLOBAL before the service
        // was registered, apply it now so the newly created RateLimiter reflects it.
        long dynamicRateLimit = DynamicConfig.getInstance().getOssGeneralCacheRateLimit();
        if (dynamicRateLimit > 0 && dynamicRateLimit != config.getOssRateLimit()) {
            rss.updateRateLimit(dynamicRateLimit);
            LOG.info("Applied dynamic OSS rate limit override on registration: " + dynamicRateLimit);
        }
    }

    /**
     * Update the OSS read rate limit of the currently registered
     * {@link PrefixRoutingRemoteStorageService}. If no service is registered
     * yet, the call is a no-op; the value will be applied later in
     * {@link #registerRemoteStorage}.
     *
     * @param newRateLimit new rate limit in bytes/sec (must be positive)
     */
    public void updateRateLimit(long newRateLimit) {
        if (newRateLimit <= 0) {
            return;
        }
        PrefixRoutingRemoteStorageService rss = this.remoteStorageService;
        if (rss != null) {
            rss.updateRateLimit(newRateLimit);
            LOG.info("OSS rate limit dynamically updated to " + newRateLimit);
        }
    }

    /**
     * Try to open a cached stream for the given OSS key.
     * <p>
     * Strips the columnar directory prefix from the OSS key to get the pure cache file name.
     * Returns null if the key does not match any registered prefix (caller should handle this case).
     *
     * @param ossKey full OSS key (e.g. "instId/abc.orc")
     * @param fileSize file size in bytes
     * @param conf Hadoop configuration
     * @param pool thread pool for read-ahead
     * @param maxReadAhead max read-ahead part number
     * @param stats file system statistics
     * @return FSDataInputStream wrapping CachedInputStream, or null if key not in cache scope
     */
    public FSDataInputStream tryOpenCachedStream(
        String ossKey, long fileSize, Configuration conf,
        ExecutorService pool, int maxReadAhead, FileSystem.Statistics stats) {
        if (columnarDirSupplier == null) {
            return null;
        }
        String dir = columnarDirSupplier.get();
        if (dir != null && ossKey.startsWith(dir + "/")) {
            String cacheFileName = ossKey.substring(dir.length() + 1);
            return new FSDataInputStream(
                new CachedInputStream(conf,
                    new SemaphoredDelegatingExecutor(pool, maxReadAhead, true),
                    maxReadAhead, cache, 0, cacheFileName, fileSize,
                    GetOptions.DEFAULT, new CacheStatistics(), stats));
        }
        return null;
    }

    /**
     * Extract the per-statement override of {@link ConnectionProperties#ENABLE_OSS_GENERAL_CACHE}
     * from the ExecutionContext extraCmds map. Returns null when the HINT/session
     * is not set, meaning callers should fall back to {@link DynamicConfig#isEnableOssGeneralCache()}.
     *
     * @param extraCmds ExecutionContext.getExtraCmds(), may be null
     * @return Boolean override value, or null if absent
     */
    public static Boolean extractStatementOverride(Map<String, Object> extraCmds) {
        if (extraCmds == null) {
            return null;
        }
        Object v = extraCmds.get(ConnectionProperties.ENABLE_OSS_GENERAL_CACHE);
        if (v == null) {
            return null;
        }
        return Boolean.valueOf(String.valueOf(v));
    }
}
