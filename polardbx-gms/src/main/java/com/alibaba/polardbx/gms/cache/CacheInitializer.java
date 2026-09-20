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

package com.alibaba.polardbx.gms.cache;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.cache.GeneralCache;
import com.alibaba.polardbx.cache.external.impl.DistributedRpcService;
import com.alibaba.polardbx.cache.external.impl.rpc.PeerManager;
import com.alibaba.polardbx.cache.external.impl.rpc.RpcServer;
import com.alibaba.polardbx.cache.external.impl.rpc.meta.PeerRole;
import com.alibaba.polardbx.cache.external.impl.rpc.net.NIOWorker;
import com.alibaba.polardbx.cache.local.LocalBlockCache;
import com.alibaba.polardbx.cache.local.LocalBlockCacheImpl;
import com.alibaba.polardbx.cache.local.LocalBlockCacheMemoryFootprint;
import com.alibaba.polardbx.cache.offheap.OffHeapArena;
import com.alibaba.polardbx.cache.offheap.bufferpool.ArenaClockBP;
import com.alibaba.polardbx.cache.statistics.CacheStatisticsCollector;
import com.alibaba.polardbx.cache.utils.ThreadPoolUtil;
import com.alibaba.polardbx.common.oss.filesystem.CachedInputStream;
import com.alibaba.polardbx.common.oss.filesystem.OSSCacheAdapter;
import com.alibaba.polardbx.common.oss.filesystem.cache.GeneralCacheConfig;
import com.alibaba.polardbx.common.properties.FileConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.cache.CacheUserAccessor;
import com.alibaba.polardbx.gms.metadb.cache.CacheUserRecord;
import com.alibaba.polardbx.gms.metadb.cache.GmsRpcMetaServiceFactory;
import com.alibaba.polardbx.gms.util.PasswdUtil;
import lombok.Getter;
import org.jetbrains.annotations.NotNull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.security.SecureRandom;
import java.sql.Connection;
import java.util.List;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.stream.Stream;

/**
 * CacheInitializer initializes the cache system including:
 * - RpcServer for remote cache access (skipped when cacheRpcPort == -1)
 * - GeneralCache with off-heap memory
 * - LocalCache for SSD-backed cache
 * - CachePeerRefreshTask for peer discovery and leader election (skipped when cacheRpcPort == -1)
 * - DistributedRpcService for client-side remote cache access
 * <p>
 * When cacheRpcPort == -1, operates in client-only mode:
 * no RPC server is started, no node registration occurs,
 * but the client (DistributedRpcService) works normally.
 */
public class CacheInitializer {
    private static final Logger LOGGER = LoggerFactory.getLogger(CacheInitializer.class);

    private static final long GB = 1024L * 1024L * 1024L;
    private static final long MB = 1024L * 1024L;
    private static final long MINIMUM_MEMORY = 16 * MB;
    private static final long RESERVED_DISK_SPACE = 2L * GB;
    // Fail-fast threshold for the disk budget passed to LocalBlockCacheImpl#estimateMemoryFootprint:
    // smaller values can produce zero-slot footprints and undefined LocalBlockCache behavior.
    private static final long MINIMUM_LOCAL_DISK_CACHE_SIZE = MINIMUM_MEMORY;

    private static volatile CacheInitializer instance = null;

    private final int cacheRpcPort;
    private final long cacheOffheapMemory;
    private final long dynamicArenaMemory;
    private final int cacheThreads;
    private final long cacheLocalDiskSize;
    private final long cacheLease;
    private final int queryThreads;
    private final String peerTag;
    private final GeneralCacheConfig cacheConfig;

    private volatile long effectiveLocalCacheSize;

    private OffHeapArena arena;
    private ArenaClockBP bp;
    private volatile ArenaClockBP.MemoryFootprint bpFootprint;
    private volatile LocalBlockCacheMemoryFootprint lcFootprint;
    @Getter
    private GeneralCache generalCache;
    private LocalBlockCache localCache;
    @Getter
    private RpcServer rpcServer;
    @Getter
    private CachePeerRefreshTask peerRefreshTask;
    @Getter
    private GmsRpcMetaServiceFactory factory;
    @Getter
    private PeerManager peerManager;
    @Getter
    private FileIdNameProvider fileIdNameProvider;

    // Client-only mode resources (used when cacheRpcPort == -1, i.e. no RpcServer)
    private ThreadPoolExecutor clientExecutor;
    private NIOWorker clientNioWorker;

    private CacheInitializer(GeneralCacheConfig cacheConfig) {
        this.cacheRpcPort = cacheConfig.getCacheRpcPort();
        this.cacheOffheapMemory = cacheConfig.getCacheOffheapMemory();
        this.dynamicArenaMemory = cacheConfig.getDynamicArenaMemory();
        this.cacheThreads = cacheConfig.getCacheThreads();
        this.cacheLocalDiskSize = cacheConfig.getCacheLocalDiskSize();
        this.cacheLease = cacheConfig.getCacheLease();
        this.queryThreads = cacheConfig.getQueryThreads();
        this.peerTag = cacheConfig.getPeerTag();
        this.cacheConfig = cacheConfig;
    }

    /**
     * Initialize the cache system with unified configuration.
     *
     * @param cacheConfig unified cache configuration from server.properties
     * @param collector cache statistics collector
     * @throws Exception if initialization fails
     */
    public static synchronized void initialize(GeneralCacheConfig cacheConfig,
                                               CacheStatisticsCollector collector)
        throws Exception {
        if (instance != null) {
            LOGGER.warn("CacheInitializer already initialized, skipping");
            return;
        }

        CacheInitializer initializer = new CacheInitializer(cacheConfig);
        initializer.factory = new GmsRpcMetaServiceFactory(collector);

        // Initialize myself PeerInfo in factory before init
        // Use port 0 for client-only mode since -1 is not a valid port for InetSocketAddress
        int myselfPort = cacheConfig.getCacheRpcPort() == -1 ? 0 : cacheConfig.getCacheRpcPort();
        initializer.factory.initMyself(myselfPort, cacheConfig.getPeerTag(),
            PeerRole.valueOf(cacheConfig.getPeerRole()));

        initializer.init();
        instance = initializer;

        LOGGER.info("CacheInitializer initialized successfully");
    }

    public static CacheInitializer getInstanceOrNull() {
        return instance;
    }

    public static CacheInitializer getInstance() {
        if (null == instance) {
            throw new IllegalStateException("CacheInitializer not initialized");
        }
        return instance;
    }

    private void init() throws Exception {
        boolean serverEnabled = cacheRpcPort > 0;

        // Step 0a: Ensure default admin user exists in cache_user table
        ensureDefaultAdminUser();

        // Step 0b: Check parameters
        if (dynamicArenaMemory < MINIMUM_MEMORY || cacheOffheapMemory < dynamicArenaMemory + MINIMUM_MEMORY
            || cacheThreads <= 0 || cacheLocalDiskSize <= 0 || cacheLease <= 0 || queryThreads <= 0) {
            throw new IllegalArgumentException("Invalid cache parameters: cacheRpcPort=" + cacheRpcPort +
                ", cacheOffheapMemory=" + cacheOffheapMemory + ", cacheThreads=" + cacheThreads +
                ", cacheLocalDiskSize=" + cacheLocalDiskSize + ", cacheLease=" + cacheLease +
                ", queryThreads=" + queryThreads);
        }
        if (!serverEnabled && cacheRpcPort != -1) {
            throw new IllegalArgumentException(
                "cacheRpcPort must be > 0 (server mode) or -1 (client-only mode), got: " + cacheRpcPort);
        }

        try {
            int executorThreads = Math.min(128, Runtime.getRuntime().availableProcessors() * 4);
            Executor connectionExecutor;
            NIOWorker nioWorker;

            if (serverEnabled) {
                // Step 1a: Initialize RpcServer (server mode)
                this.rpcServer = new RpcServer(executorThreads, cacheRpcPort, factory);
                connectionExecutor = rpcServer.getExecutor();
                nioWorker = rpcServer.getWorker();
                LOGGER.info(
                    "RpcServer initialized on port " + cacheRpcPort + " with " + executorThreads
                        + " executor threads");
            } else {
                // Step 1b: Client-only mode - create executor and NIOWorker independently
                this.clientExecutor = ThreadPoolUtil.fixedQueuedPool(executorThreads, "CACHE-Client-Worker");
                this.clientNioWorker = new NIOWorker(Runtime.getRuntime().availableProcessors());
                connectionExecutor = clientExecutor;
                nioWorker = clientNioWorker;
                LOGGER.info("Client-only mode: RpcServer skipped, created independent executor (" +
                    executorThreads + " threads) and NIOWorker");
            }

            // Step 2: Initialize GeneralCache with off-heap arena
            // Step 2.1: Compute BP off-heap budget (total minus dynamic-arena reservation)
            long bpBudget = cacheOffheapMemory - dynamicArenaMemory;

            // Step 2.2: Derive precise BP parameters via footprint API so that the
            // actual off-heap usage never exceeds the configured budget.
            this.bpFootprint = ArenaClockBP.computeMemoryFootprint(bpBudget);

            // bpFootprint.pageCount * BLOCK_SIZE <= bpBudget (truncated to page alignment)
            // arenaReserved = exact off-heap space the BP will pin
            long arenaReserved = (long) bpFootprint.pageCount * OffHeapArena.BLOCK_SIZE;
            // arenaTotal = BP exact reservation + dynamic-arena reservation
            long arenaTotal = arenaReserved + dynamicArenaMemory;

            LOGGER.info("[CacheInit] BP footprint: budget=" + (bpBudget / MB) + "MB, actual="
                + (arenaReserved / MB) + "MB, pages=" + bpFootprint.pageCount + ", onHeap="
                + (bpFootprint.onHeapBytes / MB) + "MB");

            // Step 2.3: Initialize arena and BP with the precise values
            this.arena = new OffHeapArena(arenaTotal, arenaReserved);
            this.bp = new ArenaClockBP(arena, -1); // all reserved memory goes to BP
            this.generalCache = GeneralCache.build(arena, bp, cacheThreads, cacheThreads, cacheThreads);
            LOGGER.info("GeneralCache initialized with " + (arenaTotal / (float) GB) + "GB arena memory ("
                + (arenaReserved / (float) GB) + "GB reserved for BP, "
                + (dynamicArenaMemory / (float) GB) + "GB for dynamic alloc)");

            // Step 3: Initialize LocalCache with spill to SSD
            // Step 3.1: Compute disk budget (existing logic: adjusted by free space)
            String cacheDir = createCacheDirectory();
            long diskBudget = adjustLocalCacheSize(cacheDir, cacheLocalDiskSize);

            // Step 3.2: Derive precise LocalCache parameters via footprint API so that the
            // actual disk usage never exceeds the configured budget.
            this.lcFootprint = LocalBlockCacheImpl.estimateMemoryFootprint(diskBudget);

            // lcFootprint.cacheSize <= diskBudget (footprint provides precise cache size)
            long actualCacheSize = lcFootprint.cacheSize;

            LOGGER.info("[CacheInit] LocalCache footprint: budget=" + (diskBudget / GB) + "GB, actual="
                + (actualCacheSize / GB) + "GB, slots=" + lcFootprint.slotCount + ", offHeap="
                + (lcFootprint.offHeapBytes / MB) + "MB, onHeap=" + (lcFootprint.onHeapBytes / MB) + "MB");

            // Step 3.3: Initialize LocalCache with the precise value
            this.effectiveLocalCacheSize = actualCacheSize;
            // Min 4 async writers, max 64 async writers
            int minAsyncWriters = Math.min(4, cacheThreads);
            int maxAsyncWriters = Math.min(64, cacheThreads * 2);
            this.localCache = LocalBlockCache.build(arena, cacheDir, actualCacheSize, minAsyncWriters, maxAsyncWriters);
            this.generalCache.setLocalBlockCache(localCache);
            LOGGER.info("LocalCache initialized at " + cacheDir + " with size " + (actualCacheSize / (float) GB) +
                "GB, async writers: " + minAsyncWriters + "-" + maxAsyncWriters);

            // Step 4: Set GeneralCache to factory
            factory.setGeneralCache(generalCache);
            LOGGER.info("GeneralCache set to GmsRpcMetaServiceFactory");

            // Step 5: Initialize RpcService connection pool (will be created in CacheRpcService)
            LOGGER.info("RpcService connection pool will be created on-demand");

            // Step 6: Initialize cache peer refresh task (server mode only)
            long refreshIntervalMs = cacheLease / 2;
            if (serverEnabled) {
                this.peerRefreshTask = new CachePeerRefreshTask(refreshIntervalMs, cacheLease, factory);
                this.peerRefreshTask.start();
                LOGGER.info("CachePeerRefreshTask started with lease " + cacheLease + "ms, refresh interval " +
                    refreshIntervalMs + "ms");
            } else {
                LOGGER.info("Client-only mode: CachePeerRefreshTask skipped (no node registration)");
            }

            // Step 7: Register DistributedRpcService to GeneralCache for remote cache access
            // In server mode, reuse PeerManager from RpcServer to avoid duplicate creation;
            // in client-only mode, create an independent PeerManager.
            if (serverEnabled) {
                peerManager = rpcServer.getPeerManager();
            } else {
                peerManager = new PeerManager(factory);
            }

            // Create DistributedRpcService with same refresh interval
            final DistributedRpcService rpcService =
                new DistributedRpcService(
                    factory,
                    connectionExecutor,
                    nioWorker,
                    peerManager,
                    refreshIntervalMs,
                    queryThreads
                );

            this.generalCache.setRpcService(rpcService);

            // register to OSSCacheAdapter (replaces old OSSGeneralCacheProxy.cache = generalCache)
            OSSCacheAdapter.init(generalCache, cacheConfig);
            LOGGER.info("OSSCacheAdapter initialized");
            LOGGER.info("DistributedRpcService registered to GeneralCache with " + cacheThreads +
                " cache threads, " + queryThreads + " query threads, refresh interval " + refreshIntervalMs + "ms");

            // Step 8: Initialize FileIdNameProvider
            this.fileIdNameProvider = new FileIdNameProvider();
            this.generalCache.setIdNameProvider(fileIdNameProvider);
            LOGGER.info(
                "FileIdNameProvider initialized with default cache configuration (4096 entries, 300s expiration)");

            // Step 9: Emit comprehensive initialization summary for ops visibility
            LOGGER.info("[CacheInit] === Initialization Summary ==="
                + "\n  BP: " + (bpFootprint.offHeapBytes / MB) + "MB off-heap (" + bpFootprint.pageCount
                + " pages), " + (bpFootprint.onHeapBytes / MB) + "MB on-heap"
                + "\n  LocalCache: " + (actualCacheSize / GB) + "GB disk (" + lcFootprint.slotCount
                + " slots), " + (lcFootprint.offHeapBytes / MB) + "MB off-heap, "
                + (lcFootprint.onHeapBytes / MB) + "MB on-heap"
                + "\n  DynamicArena: " + (dynamicArenaMemory / MB) + "MB"
                + "\n  Total off-heap: "
                + ((bpFootprint.offHeapBytes + lcFootprint.offHeapBytes + dynamicArenaMemory) / MB)
                + "MB (budget: " + (cacheOffheapMemory / MB) + "MB)");
        } catch (Exception e) {
            // Cleanup partially initialized resources on failure
            LOGGER.error("Failed to initialize cache system, cleaning up resources", e);
            cleanupResources();
            throw e;
        }
    }

    /**
     * Ensure a default admin user exists in the cache_user table.
     * If the table is empty, generate one admin user with a random password (encrypted by PasswdUtil).
     * This is done transactionally to avoid race conditions between multiple CN nodes.
     */
    private void ensureDefaultAdminUser() {
        try (Connection connection = MetaDbDataSource.getInstance().getConnection()) {
            connection.setAutoCommit(false);

            CacheUserAccessor userAccessor = new CacheUserAccessor();
            userAccessor.setConnection(connection);

            List<CacheUserRecord> users = userAccessor.getAllUsers();
            if (users.isEmpty()) {
                // Generate random username and password
                String userName = "cache_admin_" + generateRandomHex(8);
                String rawPassword = generateRandomPassword(24);
                String encryptedPassword = PasswdUtil.encrypt(rawPassword);

                // Insert admin user with full privileges (read=1, write=1, admin=1)
                boolean inserted = userAccessor.insertUser(userName, encryptedPassword, 1, 1, 1);
                if (inserted) {
                    LOGGER.info("Default cache admin user created: " + userName);
                } else {
                    // Duplicate key — another CN node already created the user concurrently
                    LOGGER.info("Default cache admin user already exists (concurrent initialization)");
                }
            } else {
                LOGGER.info("Cache user table is not empty, skipping default admin user creation");
            }

            connection.commit();
        } catch (Exception e) {
            LOGGER.error("Failed to ensure default admin user", e);
            throw new RuntimeException("Failed to ensure default admin user", e);
        }
    }

    private static String generateRandomPassword(int length) {
        final String chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789";
        return getRandomString(length, chars);
    }

    @NotNull
    private static String getRandomString(int length, String chars) {
        final SecureRandom random = new SecureRandom();
        final StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++) {
            sb.append(chars.charAt(random.nextInt(chars.length())));
        }
        return sb.toString();
    }

    private static String generateRandomHex(int length) {
        final String hexChars = "0123456789abcdef";
        return getRandomString(length, hexChars);
    }

    /**
     * Create cache directory.
     * If spillRootPath is configured in GeneralCacheConfig, use it;
     * otherwise fall back to CN default (FileConfig.getInstance().getRootPath()).
     */
    private String createCacheDirectory() throws IOException {
        Path spillRootPath;
        String configuredPath = cacheConfig.getSpillRootPath();
        if (configuredPath != null && !configuredPath.isEmpty()) {
            spillRootPath = Paths.get(configuredPath);
            LOGGER.info("Using externally configured spillRootPath: " + spillRootPath);
        } else {
            // CN default: use FileConfig spill directory
            spillRootPath = FileConfig.getInstance().getRootPath();
            LOGGER.info("Using FileConfig default spillRootPath: " + spillRootPath);
        }

        // Create cache subdirectory under spill root
        File cacheDir = new File(spillRootPath.toFile(), peerTag + "_general_cache");
        if (!cacheDir.exists()) {
            if (!cacheDir.mkdirs()) {
                throw new IOException("Failed to create cache directory: " + cacheDir.getAbsolutePath());
            }
        }
        return cacheDir.getAbsolutePath();
    }

    private long adjustLocalCacheSize(String cacheDir, long configuredSize) {
        File dir = new File(cacheDir);
        long existingSize = dirSize(dir);
        long freeSpace = dir.getUsableSpace();
        long maxCacheSize = existingSize + freeSpace - RESERVED_DISK_SPACE;

        if (maxCacheSize < MINIMUM_LOCAL_DISK_CACHE_SIZE) {
            LOGGER.warn("Disk space critically low: existingCacheSize=" + (existingSize / (float) GB) +
                "GB, freeSpace=" + (freeSpace / (float) GB) +
                "GB, cannot reserve " + (RESERVED_DISK_SPACE / (float) GB) +
                "GB while leaving at least " + (MINIMUM_LOCAL_DISK_CACHE_SIZE / (float) MB) + "MB for cache");
            throw new IllegalStateException(
                "Insufficient disk space for local cache. Free space: " + freeSpace +
                    " bytes, existing cache: " + existingSize + " bytes, required reserve: " + RESERVED_DISK_SPACE
                    + " bytes, minimum cache size: " + MINIMUM_LOCAL_DISK_CACHE_SIZE + " bytes");
        }

        if (configuredSize <= maxCacheSize) {
            LOGGER.info("Disk space sufficient for configured cacheLocalDiskSize: " + (configuredSize / (float) GB) +
                "GB (existingCache=" + (existingSize / (float) GB) +
                "GB, freeSpace=" + (freeSpace / (float) GB) + "GB)");
            return configuredSize;
        }

        LOGGER.warn("Disk space insufficient for configured cacheLocalDiskSize " + (configuredSize / (float) GB) +
            "GB. Adjusting to " + (maxCacheSize / (float) GB) +
            "GB (existingCache=" + (existingSize / (float) GB) +
            "GB, freeSpace=" + (freeSpace / (float) GB) +
            "GB, reserved=" + (RESERVED_DISK_SPACE / (float) GB) + "GB)");
        return maxCacheSize;
    }

    private static long dirSize(File dir) {
        Path root = dir.toPath();
        if (!Files.isDirectory(root)) {
            return 0L;
        }
        try (Stream<Path> stream = Files.walk(root)) {
            return stream.mapToLong(p -> {
                try {
                    BasicFileAttributes attrs = Files.readAttributes(p, BasicFileAttributes.class);
                    return attrs.isRegularFile() ? attrs.size() : 0L;
                } catch (IOException e) {
                    return 0L;
                }
            }).sum();
        } catch (IOException e) {
            LOGGER.warn("Failed to compute dirSize for " + dir, e);
            return 0L;
        }
    }

    /**
     * Cleanup partially initialized resources (used during init failure)
     */
    private void cleanupResources() {
        // Clean up in reverse order of initialization
        try {
            if (peerRefreshTask != null) {
                try {
                    peerRefreshTask.stop();
                } catch (Exception e) {
                    LOGGER.error("Error stopping peerRefreshTask during cleanup", e);
                }
            }

            // Only close peerManager if we created it independently (client-only mode).
            // In server mode, PeerManager is owned by RpcServer and will be closed via rpcServer.shutdown().
            if (peerManager != null && rpcServer == null) {
                try {
                    peerManager.close();
                } catch (Exception e) {
                    LOGGER.error("Error closing peerManager during cleanup", e);
                }
            }

            // Shutdown RpcServer (server mode only)
            if (rpcServer != null) {
                try {
                    rpcServer.shutdown();
                } catch (Exception e) {
                    LOGGER.error("Error shutting down rpcServer during cleanup", e);
                }
            }

            if (localCache != null) {
                try {
                    localCache.close();
                } catch (Exception e) {
                    LOGGER.error("Error closing localCache during cleanup", e);
                }
            }

            if (generalCache != null) {
                try {
                    generalCache.close();
                } catch (Exception e) {
                    LOGGER.error("Error closing generalCache during cleanup", e);
                }
            }

            // Close BP before arena (BP holds references to arena pages)
            if (bp != null) {
                try {
                    bp.close();
                } catch (Exception e) {
                    LOGGER.error("Error closing bp during cleanup", e);
                }
            }

            if (arena != null) {
                try {
                    arena.close();
                } catch (Exception e) {
                    LOGGER.error("Error closing arena during cleanup", e);
                }
            }

            // Cleanup client-only mode resources
            if (clientExecutor != null) {
                try {
                    clientExecutor.shutdownNow();
                } catch (Exception e) {
                    LOGGER.error("Error shutting down clientExecutor during cleanup", e);
                }
            }

            if (fileIdNameProvider != null) {
                try {
                    fileIdNameProvider.invalidateAll();
                } catch (Exception e) {
                    LOGGER.error("Error clearing fileIdNameProvider cache during cleanup", e);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Error during cleanup", e);
        }
    }

    /**
     * Collect cache status as a JSON string for reporting in MetaDB.
     * Includes arena memory, buffer pool, local cache, and RPC statistics.
     */
    public String collectStatusJson() {
        try {
            JSONObject root = new JSONObject(true);

            // Arena memory info
            JSONObject arenaJson = new JSONObject(true);
            arenaJson.put("totalMemory", cacheOffheapMemory);
            arenaJson.put("dynamicArenaMemory", dynamicArenaMemory);
            arenaJson.put("bpReserveMemory", cacheOffheapMemory - dynamicArenaMemory);
            arenaJson.put("blockSize", OffHeapArena.BLOCK_SIZE);
            if (arena != null) {
                arenaJson.put("reservedBlockCount", arena.getReservedBlockCount());
                arenaJson.put("restReservedBlockCount", arena.getRestReservedBlockCount());
            }
            root.put("arena", arenaJson);

            // Buffer pool info
            JSONObject bpJson = new JSONObject(true);
            if (bp != null) {
                bpJson.put("loadedCount", bp.getLoadedCount());
            }
            if (generalCache != null) {
                // Pages currently held by BPRefer
                bpJson.put("referredCount", generalCache.bufferPoolReferredCount());
            }
            root.put("bp", bpJson);

            // Local cache info
            JSONObject localJson = new JSONObject(true);
            localJson.put("configuredSize", cacheLocalDiskSize);
            localJson.put("effectiveSize", effectiveLocalCacheSize);
            localJson.put("cachedSize", localCache.cachedSize());
            localJson.put("cacheUsage", localCache.cacheUsage());
            localJson.put("cacheUtilization", localCache.cacheUtilization());
            if (generalCache != null) {
                // Slots currently held by active references
                localJson.put("referredSlotCount", generalCache.localReferredSlotCount());
            }
            root.put("localCache", localJson);

            // RPC statistics from CacheStatisticsCollector
            CacheStatisticsCollector collector = factory != null ? factory.getCacheStatisticsCollector() : null;
            if (collector != null) {
                root.put("rpc", collectStatisticsJson(collector));
            }

            // CN-side cache statistics from CachedInputStream
            CacheStatisticsCollector cnCollector = CachedInputStream.getCollector();
            if (cnCollector != null) {
                root.put("cnCache", collectStatisticsJson(cnCollector));
            }

            // Config info
            JSONObject configJson = new JSONObject(true);
            configJson.put("cacheRpcPort", cacheRpcPort);
            configJson.put("cacheThreads", cacheThreads);
            configJson.put("queryThreads", queryThreads);
            configJson.put("peerTag", peerTag);
            root.put("config", configJson);

            return root.toJSONString();
        } catch (Exception e) {
            LOGGER.warn("Failed to collect cache status JSON", e);
            return null;
        }
    }

    /**
     * Collect statistics from a CacheStatisticsCollector into a JSONObject.
     * Maps all @Getter AtomicLong counters: buffer pool, read ops, write ops, bytes, and timing nanos.
     */
    private JSONObject collectStatisticsJson(CacheStatisticsCollector c) {
        JSONObject json = new JSONObject(true);
        // Buffer pool hit/miss
        json.put("bufferPoolHit", c.getBufferPoolHit().get());
        json.put("bufferPoolMiss", c.getBufferPoolMiss().get());
        // Read invocation counters
        json.put("batchedRead", c.getBatchedRead().get());
        json.put("localBatchRead", c.getLocalBatchRead().get());
        json.put("localPageRead", c.getLocalPageRead().get());
        json.put("localBadCrc", c.getLocalBadCrc().get());
        json.put("rpcBatchRead", c.getRpcBatchRead().get());
        json.put("rpcPageRead", c.getRpcPageRead().get());
        json.put("remoteBatchRead", c.getRemoteBatchRead().get());
        json.put("remotePageRead", c.getRemotePageRead().get());
        json.put("remotePatchRead", c.getRemotePatchRead().get());
        // Write and eviction counters
        json.put("localWrite", c.getLocalWrite().get());
        json.put("remoteWrite", c.getRemoteWrite().get());
        json.put("remoteFlushSkipped", c.getRemoteFlushSkipped().get());
        json.put("remoteFlushGrouped", c.getRemoteFlushGrouped().get());
        json.put("localEvict", c.getLocalEvict().get());
        // Page push counters
        json.put("pushCallCount", c.getPushCallCount().get());
        json.put("pushAttemptPeers", c.getPushAttemptPeers().get());
        json.put("pushSuccessPeers", c.getPushSuccessPeers().get());
        json.put("pushReceived", c.getPushReceived().get());
        json.put("pushBytes", c.getPushBytes().get());
        json.put("pushReceivedBytes", c.getPushReceivedBytes().get());
        json.put("pushNanos", c.getPushNanos().get());
        // Data volume counters (bytes)
        json.put("batchedReadBytes", c.getBatchedReadBytes().get());
        json.put("localReadDataBytes", c.getLocalReadDataBytes().get());
        json.put("rpcReadBytes", c.getRpcReadBytes().get());
        json.put("remoteReadBytes", c.getRemoteReadBytes().get());
        json.put("localWriteMetaBytes", c.getLocalWriteMetaBytes().get());
        json.put("localWriteDataBytes", c.getLocalWriteDataBytes().get());
        json.put("remoteWriteBytes", c.getRemoteWriteBytes().get());
        // Time counters (nanoseconds)
        json.put("getFileIdNanos", c.getGetFileIdNanos().get());
        json.put("localOpenDataNanos", c.getLocalOpenDataNanos().get());
        json.put("localAllocateMemoryNanos", c.getLocalAllocateMemoryNanos().get());
        json.put("localReadDataNanos", c.getLocalReadDataNanos().get());
        json.put("localCrcNanos", c.getLocalCrcNanos().get());
        json.put("localOpenMetaNanos", c.getLocalOpenMetaNanos().get());
        json.put("localWriteMetaNanos", c.getLocalWriteMetaNanos().get());
        json.put("localWaitContextNanos", c.getLocalWaitContextNanos().get());
        json.put("localWaitSlotNanos", c.getLocalWaitSlotNanos().get());
        json.put("localLookupNanos", c.getLocalLookupNanos().get());
        json.put("localEvictNanos", c.getLocalEvictNanos().get());
        json.put("localWriteDataNanos", c.getLocalWriteDataNanos().get());
        json.put("localPutNanos", c.getLocalPutNanos().get());
        json.put("localVerifyNanos", c.getLocalVerifyNanos().get());
        json.put("localWaitFileNanos", c.getLocalWaitFileNanos().get());
        json.put("localScheduleAsyncWriteNanos", c.getLocalScheduleAsyncWriteNanos().get());
        json.put("localWriteCleanupNanos", c.getLocalWriteCleanupNanos().get());
        json.put("localUpdateMetaNanos", c.getLocalUpdateMetaNanos().get());
        json.put("localLoadNanos", c.getLocalLoadNanos().get());
        json.put("rpcNanos", c.getRpcNanos().get());
        json.put("remoteReadNanos", c.getRemoteReadNanos().get());
        json.put("remoteWriteNanos", c.getRemoteWriteNanos().get());
        json.put("notifyNanos", c.getNotifyNanos().get());
        return json;
    }

    /**
     * Shutdown the cache system
     */
    public static synchronized void shutdown() {
        if (instance == null) {
            return;
        }

        try {
            // Step 0: Cut off external access first
            OSSCacheAdapter.shutdown();
            LOGGER.info("OSSCacheAdapter shut down");

            // Stop in reverse order: stop tasks first, then close resources

            // Step 1: Stop peer refresh task (stops using resources)
            if (instance.peerRefreshTask != null) {
                instance.peerRefreshTask.stop();
                LOGGER.info("CachePeerRefreshTask stopped");
            }

            // Step 2: Close PeerManager (only in client-only mode).
            // In server mode, PeerManager is owned by RpcServer and will be closed via rpcServer.shutdown().
            if (instance.peerManager != null && instance.rpcServer == null) {
                try {
                    instance.peerManager.close();
                    LOGGER.info("PeerManager closed");
                } catch (Exception e) {
                    LOGGER.error("Error closing PeerManager", e);
                }
            }

            // Step 3: Shutdown RpcServer (server mode only)
            if (instance.rpcServer != null) {
                try {
                    instance.rpcServer.shutdown();
                    LOGGER.info("RpcServer shut down");
                } catch (Exception e) {
                    LOGGER.error("Error shutting down RpcServer", e);
                }
            }

            // Step 4: Close GeneralCache (will close executors)
            if (instance.generalCache != null) {
                instance.generalCache.close();
                LOGGER.info("GeneralCache closed");
            }

            // Step 5: Close LocalCache
            if (instance.localCache != null) {
                instance.localCache.close();
                LOGGER.info("LocalCache closed");
            }

            // Step 6: Close BP (before arena, as BP uses arena memory)
            if (instance.bp != null) {
                instance.bp.close();
                LOGGER.info("ArenaClockBP closed");
            }

            // Step 7: Close arena
            if (instance.arena != null) {
                instance.arena.close();
                LOGGER.info("OffHeapArena closed");
            }

            // Step 8: Cleanup client-only mode resources
            if (instance.clientExecutor != null) {
                instance.clientExecutor.shutdownNow();
                LOGGER.info("Client executor shut down");
            }

            // Step 9: Clear FileIdNameProvider cache
            if (instance.fileIdNameProvider != null) {
                instance.fileIdNameProvider.invalidateAll();
                LOGGER.info("FileIdNameProvider cache cleared");
            }

            instance = null;
            LOGGER.info("CacheInitializer shutdown completed");
        } catch (Exception e) {
            LOGGER.error("Error during CacheInitializer shutdown", e);
        }
    }
}
