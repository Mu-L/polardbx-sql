package com.alibaba.polardbx.common.orc;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.memory.SizeOf;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
import com.github.benmanes.caffeine.cache.RemovalCause;
import com.github.benmanes.caffeine.cache.RemovalListener;
import com.github.benmanes.caffeine.cache.Weigher;
import com.github.benmanes.caffeine.cache.stats.CacheStats;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.util.concurrent.MoreExecutors.directExecutor;

public class PreheatMetaManagerImpl implements PreheatMetaManager {
    private static final Logger OSS_LOGGER = LoggerFactory.getLogger("oss");
    public static final int CACHE_STATS_FIELD_COUNT = 13;
    private static final String PREHEATED_CACHE_NAME = "PREHEATED_CACHE";
    private long maxMemorySize = DynamicConfig.getInstance().getPreheatedCacheMaxMemorySize();

    private AtomicLong size;
    private AtomicLong quotaExceedCount;

    protected final Cache<String, PreheatFileMeta> cache = Caffeine.newBuilder()
        .maximumWeight(maxMemorySize)
        .executor(directExecutor())
        .weigher(new Weigher<String, PreheatFileMeta>() {
            @Override
            public int weigh(String s, PreheatFileMeta preheatFileMeta) {
                return (int) (estimateStringMemoryUsage(s) + preheatFileMeta.getMemorySize());
            }
        })
        .removalListener(new RemovalListener<String, PreheatFileMeta>() {
            @Override
            public void onRemoval(String s, PreheatFileMeta preheatFileMeta,
                                  RemovalCause removalCause) {
                size.getAndAdd(-(estimateStringMemoryUsage(s) + preheatFileMeta.getMemorySize()));
                quotaExceedCount.getAndIncrement();
            }
        })
        .recordStats()
        .build();

    private final Configuration configuration;

    public PreheatMetaManagerImpl() {
        this.configuration = new Configuration();
        this.size = new AtomicLong(0L);
        this.quotaExceedCount = new AtomicLong(0L);
    }

    @Override
    public void resizeMaximumMemorySize(long maxMemorySize) {
        this.maxMemorySize = maxMemorySize;
        cache.policy().eviction().ifPresent(eviction -> eviction.setMaximum(maxMemorySize));
    }

    @Override
    public PreheatFileMeta get(final Path path, FileSystem fileSystem) throws Throwable {
        String pathStr = path.toString();
        PreheatFileMeta preheat =
            cache.get(pathStr, any -> {
                try {
                    PreheatFileMeta preheatFileMeta = preheat(path, fileSystem, null);
                    size.getAndAdd(estimateStringMemoryUsage(pathStr) + preheatFileMeta.getMemorySize());
                    return preheatFileMeta;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        return preheat;
    }

    @Override
    public PreheatFileMeta get(final Path path, FileSystem fileSystem, Set<Integer> sortKeyColumns) throws Throwable {
        String pathStr = path.toString();
        PreheatFileMeta preheat =
            cache.get(pathStr, any -> {
                try {
                    PreheatFileMeta preheatFileMeta = preheat(path, fileSystem, sortKeyColumns);
                    size.getAndAdd(estimateStringMemoryUsage(pathStr) + preheatFileMeta.getMemorySize());
                    return preheatFileMeta;
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            });
        return preheat;
    }

    @Override
    public FileStatus getFileStatus(Path path) {
        String pathStr = path.toString();
        PreheatFileMeta preheat = cache.getIfPresent(pathStr);
        if (preheat != null) {
            return preheat.getFileStatus();
        }
        return null;
    }

    protected PreheatFileMeta preheat(Path filePath, FileSystem fileSystem, Set<Integer> sortKeyColumns) throws IOException {
        ORCMetaReader metaReader = null;
        try {
            metaReader = ORCMetaReader.create(configuration, fileSystem, sortKeyColumns);
            PreheatFileMeta preheatFileMeta = metaReader.preheat(filePath);

            return preheatFileMeta;
        } finally {
            metaReader.close();
        }
    }

    @Override
    public Object[] dumpTotalUsage() {
        final long memoryUsage = size.get();
        DecimalFormat decimalFormat = new DecimalFormat("0.000000");

        Object[] result = new Object[7];
        result[0] = "PREHEAT META".getBytes();
        result[1] = memoryUsage;
        result[2] = maxMemorySize;
        result[3] = decimalFormat.format(memoryUsage * 1.0d / maxMemorySize);

        result[4] = cache.estimatedSize();
        result[5] = -1;
        result[6] = -1;

        return result;
    }

    @Override
    public byte[][] getCacheStat() {
        CacheStats cacheStats = cache.stats();

        byte[][] results = new byte[CACHE_STATS_FIELD_COUNT][];
        int pos = 0;
        results[pos++] = PREHEATED_CACHE_NAME.getBytes();
        results[pos++] = String.valueOf(memorySize()).getBytes();
        results[pos++] = String.valueOf(entries()).getBytes();
        results[pos++] = String.valueOf(-1).getBytes();
        results[pos++] = String.valueOf(cacheStats.missCount()).getBytes();
        results[pos++] = String.valueOf(-1).getBytes();
        results[pos++] = String.valueOf(cacheStats.missCount()).getBytes();
        results[pos++] = String.valueOf(quotaExceedCount.get()).getBytes();
        results[pos++] = String.valueOf(-1).getBytes();
        results[pos++] = "IN MEMORY".getBytes();
        results[pos++] = String.valueOf(-1).getBytes();
        results[pos++] = String.valueOf(-1).getBytes();
        results[pos++] = new StringBuilder().append(maxMemorySize).append(" BYTES").toString().getBytes();
        return results;
    }

    private static final int STRING_INSTANCE_SIZE = ClassLayout.parseClass(String.class).instanceSize();

    private static long estimateStringMemoryUsage(String path) {
        if (path == null) {
            return 0L;
        }

        return STRING_INSTANCE_SIZE + VMSupport.align((int) SizeOf.sizeOfCharArray(path.length()));
    }

    @Override
    public long entries() {
        return cache.estimatedSize();
    }

    @Override
    public long memorySize() {
        return size.get();
    }

    @Override
    public long hitCount() {
        CacheStats cacheStats = cache.stats();
        return cacheStats.hitCount();
    }

    @Override
    public long missCount() {
        CacheStats cacheStats = cache.stats();
        return cacheStats.missCount();
    }

    @Override
    public long exceedCount() {
        return quotaExceedCount.get();
    }
}
