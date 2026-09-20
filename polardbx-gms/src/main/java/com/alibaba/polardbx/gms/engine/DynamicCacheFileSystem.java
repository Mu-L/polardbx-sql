package com.alibaba.polardbx.gms.engine;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.oss.filesystem.FileSystemRateLimiter;
import com.alibaba.polardbx.common.oss.filesystem.OSSCacheAdapter;
import com.alibaba.polardbx.common.oss.filesystem.OSSFileSystem;
import com.alibaba.polardbx.common.oss.filesystem.RateLimitable;
import com.alibaba.polardbx.common.oss.filesystem.cache.CacheManager;
import com.alibaba.polardbx.common.oss.filesystem.cache.CachingFileSystem;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCacheManager;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCachingInputStream;
import com.alibaba.polardbx.common.properties.FileConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

/**
 * A proxy FileSystem that dynamically routes open() calls based on whether
 * GeneralCache is active at runtime, eliminating the startup-time decision
 * that previously locked in one caching strategy for the process lifetime.
 * <p>
 * When GeneralCache is enabled:
 * delegates to OSSFileSystem.open() which routes through OSSCacheAdapter.
 * <p>
 * When GeneralCache is disabled:
 * wraps the raw OSSInputStream with FileMergeCachingInputStream for local SSD cache.
 * <p>
 * CacheManager is lazily initialized on first use (DCL) to avoid overhead
 * when GeneralCache stays enabled the whole time.
 */
public class DynamicCacheFileSystem extends CachingFileSystem implements RateLimitable {

    private static final Logger LOGGER = LoggerFactory.getLogger(DynamicCacheFileSystem.class);

    private volatile CacheManager cacheManager;
    private final boolean cacheValidationEnabled;
    private final Object cacheManagerLock = new Object();

    public DynamicCacheFileSystem(OSSFileSystem ossFileSystem) {
        super(ossFileSystem, ossFileSystem.getUri());
        this.cacheValidationEnabled = FileConfig.getInstance().getCacheConfig().isValidationEnabled();
    }

    private boolean isGeneralCacheActive() {
        OSSCacheAdapter adapter = OSSCacheAdapter.getInstanceOrNull();
        return adapter != null && adapter.isEnabled();
    }

    /**
     * Lazy double-checked locking initialization of FileMergeCacheManager.
     * Only created when GeneralCache is off and a read request arrives.
     */
    private CacheManager getOrCreateCacheManager() throws IOException {
        CacheManager cm = cacheManager;
        if (cm == null) {
            synchronized (cacheManagerLock) {
                cm = cacheManager;
                if (cm == null) {
                    cm = FileMergeCacheManager.createMergeCacheManager(Engine.OSS);
                    cacheManager = cm;
                    LOGGER.info("Lazily initialized FileMergeCacheManager for DynamicCacheFileSystem");
                }
            }
        }
        return cm;
    }

    @Override
    public FSDataInputStream open(Path path, int bufferSize) throws IOException {
        return open(path, bufferSize, null);
    }

    /**
     * Variant that accepts a per-statement override of the GeneralCache switch.
     * When {@code cacheOverride} is non-null, it takes precedence over the
     * {@link #isGeneralCacheActive()} check (which is driven by DynamicConfig).
     * This is the single place that decides whether GeneralCache is active for
     * a read; the 2-arg overload and any future switches should delegate here.
     */
    public FSDataInputStream open(Path path, int bufferSize, Boolean cacheOverride) throws IOException {
        boolean active = (cacheOverride != null) ? cacheOverride : isGeneralCacheActive();
        OSSFileSystem oss = (OSSFileSystem) dataTier;
        if (active) {
            // GeneralCache handles caching transparently; forward override so that
            // OSSFileSystem.open() does not fall back to adapter.isEnabled() which
            // only reflects the DynamicConfig switch.
            return oss.open(path, bufferSize, Boolean.TRUE);
        }
        // Fallback: wrap with local SSD cache; force OSSFileSystem to return a raw
        // OSSInputStream (cacheOverride=false) instead of a CachedInputStream.
        CacheManager cm = getOrCreateCacheManager();
        return new FileMergeCachingInputStream(
            oss.open(path, bufferSize, Boolean.FALSE),
            cm, path, cm.getMaxCacheQuota(), cacheValidationEnabled);
    }

    /**
     * Optimized range-read for OSSFileSystem, mirrors
     * {@link com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCachingFileSystem#open(Path, long, long)}.
     */
    public FSDataInputStream open(Path path, long position, long length) throws IOException {
        return open(path, position, length, null);
    }

    /**
     * Variant of {@link #open(Path, long, long)} that accepts a per-statement
     * override of the GeneralCache switch.
     */
    public FSDataInputStream open(Path path, long position, long length, Boolean cacheOverride) throws IOException {
        boolean active = (cacheOverride != null) ? cacheOverride : isGeneralCacheActive();
        OSSFileSystem oss = (OSSFileSystem) dataTier;
        if (active) {
            FSDataInputStream in = oss.uncheckedOpen(path, position + length, Boolean.TRUE);
            in.seek(position);
            return in;
        }
        CacheManager cm = getOrCreateCacheManager();
        FSDataInputStream in = new FileMergeCachingInputStream(
            oss.uncheckedOpen(path, position + length, Boolean.FALSE),
            cm, path, cm.getMaxCacheQuota(), cacheValidationEnabled);
        in.seek(position);
        return in;
    }

    public CacheManager getCacheManager() {
        return cacheManager;
    }

    @Override
    public FileSystemRateLimiter getRateLimiter() {
        return ((RateLimitable) dataTier).getRateLimiter();
    }

    @Override
    public void close() throws IOException {
        CacheManager cm = cacheManager;
        if (cm != null) {
            try {
                cm.close();
            } catch (Throwable t) {
                LOGGER.warn("Failed to close CacheManager: " + t.getMessage());
            }
        }
        super.close();
    }
}
