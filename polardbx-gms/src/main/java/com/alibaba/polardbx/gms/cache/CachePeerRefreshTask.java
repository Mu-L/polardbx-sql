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

import com.alibaba.polardbx.cache.external.impl.rpc.meta.PeerInfo;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.cache.CachePeerAccessor;
import com.alibaba.polardbx.gms.metadb.cache.GmsRpcMetaServiceFactory;

import java.sql.Connection;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * CachePeerRefreshTask periodically refreshes cache peer information in MetaDB and performs leader election.
 * <p>
 * The task:
 * 1. Refreshes its own peer info (lease, host, nodeHash)
 * 2. Attempts leader election to ensure exactly one leader in the cluster
 * 3. Runs at half of the lease time interval (e.g., lease=10s, interval=5s)
 */
public class CachePeerRefreshTask {
    private static final Logger LOGGER = LoggerFactory.getLogger(CachePeerRefreshTask.class);

    // Database operation timeout to avoid hanging on slow/unresponsive MetaDB
    private static final int DB_TIMEOUT_SECONDS = 3;

    private final long refreshIntervalMs;
    private final long leaseMs;

    // factory reference to get the latest PeerInfo dynamically (peerName/host/nodeHash/role may all change at runtime)
    private final GmsRpcMetaServiceFactory factory;

    private final ScheduledExecutorService scheduler;
    private final AtomicBoolean running = new AtomicBoolean(false);

    // Cache whether this node is the leader (use AtomicBoolean for thread safety)
    private final AtomicBoolean isLeader = new AtomicBoolean(false);

    /**
     * Create a CachePeerRefreshTask
     *
     * @param refreshIntervalMs Refresh interval in milliseconds (should be lease/2)
     * @param leaseMs Lease duration in milliseconds
     * @param factory GmsRpcMetaServiceFactory to get myself PeerInfo
     */
    public CachePeerRefreshTask(long refreshIntervalMs, long leaseMs, GmsRpcMetaServiceFactory factory) {
        this.refreshIntervalMs = refreshIntervalMs;
        this.leaseMs = leaseMs;
        this.factory = factory;

        // Create scheduler with daemon thread
        this.scheduler = Executors.newScheduledThreadPool(1, new ThreadFactory() {
            private final AtomicInteger threadNumber = new AtomicInteger(1);

            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r, "CachePeerRefresh-" + threadNumber.getAndIncrement());
                thread.setDaemon(true);
                return thread;
            }
        });

        final PeerInfo myself = factory.getMyself();
        LOGGER.info("CachePeerRefreshTask created: peerName=" + myself.name
            + ", host=" + myself.address.toString().substring(1)
            + ", nodeHash=" + myself.nodeHash + ", role=" + myself.role.name()
            + ", refreshInterval=" + refreshIntervalMs + "ms, lease=" + leaseMs + "ms");
    }

    /**
     * Start the periodic refresh task
     */
    public void start() {
        if (running.compareAndSet(false, true)) {
            // Schedule at fixed rate: first execution after initial delay, then every refreshIntervalMs
            scheduler.scheduleAtFixedRate(
                this::refreshAndElect,
                0, // start immediately
                refreshIntervalMs,
                TimeUnit.MILLISECONDS
            );
            LOGGER.info("CachePeerRefreshTask started");
        } else {
            LOGGER.warn("CachePeerRefreshTask already started");
        }
    }

    /**
     * Stop the periodic refresh task
     */
    public void stop() {
        if (running.compareAndSet(true, false)) {
            scheduler.shutdown();
            try {
                if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                    scheduler.shutdownNow();
                }
                LOGGER.info("CachePeerRefreshTask stopped");
            } catch (InterruptedException e) {
                scheduler.shutdownNow();
                Thread.currentThread().interrupt();
                LOGGER.warn("CachePeerRefreshTask stop interrupted", e);
            }
        }
    }

    /**
     * Refresh peer info in MetaDB and attempt leader election.
     * <p>
     * The workflow is split into three short, independent transactions to minimize
     * lock hold time and eliminate cross-phase lock waits that previously caused
     * InnoDB deadlocks (ER_LOCK_DEADLOCK) when multiple peers ran concurrently:
     * <ol>
     *   <li>Refresh self (critical, retry once on deadlock)</li>
     *   <li>Cleanup expired peers (best-effort, ignore deadlock)</li>
     *   <li>Leader election (retry once on deadlock)</li>
     * </ol>
     */
    private void refreshAndElect() {
        // Fetch latest PeerInfo (all fields may change at runtime)
        final PeerInfo myself = factory.getMyself();
        final String currentPeerName = myself.name;
        final String currentHost = myself.address.toString().substring(1); // Remove leading '/'
        final long currentNodeHash = myself.nodeHash;
        final String currentRole = myself.role.name();
        final long nowUTC = System.currentTimeMillis();

        // Collect cache status JSON from CacheInitializer (non-critical)
        String statusJson = null;
        try {
            statusJson = CacheInitializer.getInstance().collectStatusJson();
        } catch (Exception e) {
            LOGGER.debug("Failed to collect cache status JSON for peer refresh", e);
        }
        final String finalStatusJson = statusJson;

        // Phase 1: Refresh self (critical) -- retry once on deadlock
        boolean refreshOk = runInTxn("refresh self", true, accessor ->
            accessor.refresh(currentPeerName, currentHost, currentNodeHash, currentRole,
                nowUTC, leaseMs, finalStatusJson)
        );
        if (!refreshOk) {
            // Self refresh failed; skip remaining phases, next cycle will retry
            return;
        }

        // Phase 2: Cleanup expired peers (best-effort, ignore deadlock)
        runInTxn("cleanup expired peers", false, accessor -> accessor.cleanup(nowUTC));

        // Phase 3: Leader election -- retry once on deadlock
        final boolean[] becameLeaderHolder = new boolean[] {false};
        boolean electOk = runInTxn("elect leader", true, accessor ->
            becameLeaderHolder[0] = accessor.elect(currentPeerName, nowUTC, leaseMs)
        );
        if (!electOk) {
            // Election failed this round; keep previous leader state, next cycle will retry
            return;
        }

        // Update cached leader status atomically and log transitions
        boolean becameLeader = becameLeaderHolder[0];
        boolean wasLeader = this.isLeader.getAndSet(becameLeader);
        if (becameLeader && !wasLeader) {
            LOGGER.info("Node " + currentPeerName + " became the leader");
        } else if (!becameLeader && wasLeader) {
            LOGGER.info("Node " + currentPeerName + " lost leadership");
        }
        LOGGER.debug("Node " + currentPeerName + " refresh completed. Leader: " + becameLeader);
    }

    /**
     * Run a short MetaDB transaction against {@link CachePeerAccessor}.
     * <p>
     * On deadlock (MySQL ER_LOCK_DEADLOCK), if {@code retryOnDeadlock} is true the
     * operation is retried once after a tiny back-off; otherwise the deadlock is
     * swallowed as a best-effort failure. Deadlocks are logged at WARN level since
     * the scheduler will naturally recover on the next cycle.
     *
     * @return true if the transaction committed successfully, false otherwise
     */
    private boolean runInTxn(String phase, boolean retryOnDeadlock, AccessorAction action) {
        final int maxAttempts = retryOnDeadlock ? 2 : 1;
        Throwable lastDeadlock = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            try (final Connection connection = MetaDbDataSource.getInstance().getConnection()) {
                connection.setNetworkTimeout(Executors.newSingleThreadExecutor(), DB_TIMEOUT_SECONDS * 1000);
                connection.setAutoCommit(false);

                CachePeerAccessor accessor = new CachePeerAccessor();
                accessor.setConnection(connection);

                action.run(accessor);
                connection.commit();
                return true;
            } catch (Exception e) {
                if (isDeadlockException(e)) {
                    lastDeadlock = e;
                    if (attempt < maxAttempts) {
                        LOGGER.warn("Deadlock during '" + phase + "' (attempt " + attempt
                            + "/" + maxAttempts + "), will retry");
                        sleepQuietly(50L * attempt);
                        continue;
                    }
                    // Retry exhausted or retry disabled: log as WARN, next cycle will retry
                    LOGGER.warn("Deadlock during '" + phase + "' in CachePeerRefreshTask, "
                        + "will retry on next refresh cycle", e);
                    return false;
                }
                LOGGER.error("Error during '" + phase + "' in CachePeerRefreshTask", e);
                return false;
            }
        }
        if (lastDeadlock != null) {
            LOGGER.warn("Deadlock during '" + phase + "' after " + maxAttempts + " attempts",
                lastDeadlock);
        }
        return false;
    }

    private static void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Detects MySQL deadlock exceptions (ER_LOCK_DEADLOCK, SQLState 40001)
     * by scanning the exception chain for the keyword 'deadlock'.
     */
    private static boolean isDeadlockException(Throwable e) {
        while (e != null) {
            String msg = e.getMessage();
            if (msg != null && msg.toLowerCase().contains("deadlock")) {
                return true;
            }
            e = e.getCause();
        }
        return false;
    }

    @FunctionalInterface
    private interface AccessorAction {
        void run(CachePeerAccessor accessor) throws Exception;
    }

    /**
     * Check if this node is currently the leader.
     * This is a cached value updated during each refresh cycle.
     */
    public boolean isLeader() {
        return isLeader.get();
    }
}
