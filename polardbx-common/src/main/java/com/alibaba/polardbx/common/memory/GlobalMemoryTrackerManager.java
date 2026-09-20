package com.alibaba.polardbx.common.memory;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.DecimalRoundMod;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import it.unimi.dsi.fastutil.objects.ObjectHeapPriorityQueue;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

public class GlobalMemoryTrackerManager extends AbstractLifecycle implements MemoryTrackerManager {
    // Use AtomicReference to hold current active state
    private final AtomicReference<MemoryManagerState> currentState;

    // Keep historical states for cleanup
    private final ConcurrentHashMap<Long, MemoryManagerState> historicalStates;

    // Version generator
    private final AtomicLong versionGenerator;

    private final long initialMemoryQuota;

    GlobalMemoryTrackerManager() {
        double queryMemoryQuotaRatio = DynamicConfig.getInstance().getTotalQueryMemoryQuotaRatio();
        initialMemoryQuota = (long) (Runtime.getRuntime().maxMemory() * queryMemoryQuotaRatio);

        versionGenerator = new AtomicLong(0);
        currentState = new AtomicReference<>(
            new MemoryManagerState(versionGenerator.incrementAndGet(), initialMemoryQuota)
        );
        historicalStates = new ConcurrentHashMap<>();

        init();
    }

    @Override
    protected void doInit() {
        // State is already initialized in constructor
    }

    @Override
    protected void doDestroy() {
        // Clear current state
        MemoryManagerState state = currentState.get();
        if (state != null) {
            state.markImmutable();
            state.getOperatorMemoryTrackers().clear();
            state.getDriverMemoryTrackers().clear();
            state.getPipelineMemoryTrackers().clear();
            state.getQueryMemoryTrackers().clear();
            state.getQueryMemoryOwnerIdMap().clear();
        }

        // Clear historical states
        historicalStates.clear();
    }

    /**
     * Resize memory quota with version-based state management.
     * Similar to LSM's memtable -> immutable memtable pattern.
     *
     * @param newQueryMemoryQuota new memory quota
     */
    public void resize(long newQueryMemoryQuota) {
        // 1. Create new state with new quota
        long newVersion = versionGenerator.incrementAndGet();
        MemoryManagerState newState = new MemoryManagerState(newVersion, newQueryMemoryQuota);

        // 2. Atomically replace current state
        MemoryManagerState oldState = currentState.getAndSet(newState);

        // 3. Mark old state as immutable (no new trackers allowed)
        oldState.markImmutable();

        // 4. Save old state for historical reference
        historicalStates.put(oldState.getVersion(), oldState);

        LOG.info("Memory manager resized: version " + oldState.getVersion() + " -> " + newVersion +
            ", quota " + oldState.getTotalQueryMemoryQuota() + " -> " + newQueryMemoryQuota +
            ", old state has " + oldState.getTotalTrackerCount() + " trackers");

        // 5. Schedule async cleanup of old state
        scheduleOldStateCleanup(oldState);
    }

    /**
     * Schedule async cleanup of old state.
     * Wait for all trackers in old state to be released naturally.
     */
    private void scheduleOldStateCleanup(MemoryManagerState oldState) {
        Thread cleanupThread = new Thread(() -> {
            try {
                long startTime = System.currentTimeMillis();
                long maxWaitTime = 300000; // 5 minutes max wait

                while (!oldState.isEmpty()) {
                    Thread.sleep(1000); // Check every second

                    long elapsed = System.currentTimeMillis() - startTime;
                    if (elapsed > maxWaitTime) {
                        LOG.warn("Old state cleanup timeout after " + elapsed + "ms, version=" +
                            oldState.getVersion() + ", remaining trackers=" + oldState.getTotalTrackerCount());
                        break;
                    }

                    if (elapsed % 10000 == 0) { // Log every 10 seconds
                        LOG.info("Waiting for old state cleanup: version=" + oldState.getVersion() +
                            ", remaining trackers=" + oldState.getTotalTrackerCount() + ", elapsed=" + elapsed + "ms");
                    }
                }

                // Remove old state from historical states
                historicalStates.remove(oldState.getVersion());

                LOG.info("Old memory manager state cleaned up: version=" + oldState.getVersion() +
                    ", took " + (System.currentTimeMillis() - startTime) + "ms");

            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                LOG.warn("Old state cleanup interrupted for version " + oldState.getVersion(), e);
            }
        }, "MemoryStateCleanup-v" + oldState.getVersion());

        cleanupThread.setDaemon(true);
        cleanupThread.start();
    }

    public QueryMemoryOwnerId createQueryMemoryOwnerId(String queryId) {
        MemoryManagerState state = currentState.get();

        // Reject creation if current state is immutable (resize in progress)
        if (state.isImmutable()) {
            LOG.error(
                "Cannot create new tracker: memory manager is being resized (version " + state.getVersion() + ")");
        }

        QueryMemoryOwnerId queryMemoryOwnerId =
            state.getQueryMemoryOwnerIdMap().computeIfAbsent(queryId, id -> new QueryMemoryOwnerId(id));
        createQueryMemoryTracker(queryMemoryOwnerId, state);
        return queryMemoryOwnerId;
    }

    public long getMaximumQueryMemoryAllocated(String queryId) {
        // Search in current state first
        MemoryManagerState state = currentState.get();
        QueryMemoryOwnerId queryMemoryOwnerId = state.getQueryMemoryOwnerIdMap().get(queryId);
        if (queryMemoryOwnerId != null) {
            QueryMemoryTracker queryMemoryTracker = state.getQueryMemoryTrackers().get(queryMemoryOwnerId);
            if (queryMemoryTracker != null) {
                return queryMemoryTracker.getMaxMemoryUsage();
            }
        }

        // Search in historical states
        for (MemoryManagerState historicalState : historicalStates.values()) {
            queryMemoryOwnerId = historicalState.getQueryMemoryOwnerIdMap().get(queryId);
            if (queryMemoryOwnerId != null) {
                QueryMemoryTracker queryMemoryTracker =
                    historicalState.getQueryMemoryTrackers().get(queryMemoryOwnerId);
                if (queryMemoryTracker != null) {
                    return queryMemoryTracker.getMaxMemoryUsage();
                }
            }
        }

        return 0;
    }

    /**
     * Allocate memory from a specific state.
     * This allows old trackers to continue using old state's quota.
     */
    protected void allocateQueryMemory(long memoryUsage, MemoryManagerState state) {
        state.allocateMemory(memoryUsage);
    }

    /**
     * Release memory back to a specific state.
     */
    protected void releaseQueryMemoryQuota(long memoryUsage, MemoryManagerState state) {
        state.releaseMemory(memoryUsage);
    }

    /**
     * Legacy method for backward compatibility - uses current state.
     */
    protected void allocateQueryMemory(long memoryUsage) {
        allocateQueryMemory(memoryUsage, currentState.get());
    }

    /**
     * Legacy method for backward compatibility - uses current state.
     */
    protected void releaseQueryMemoryQuota(long memoryUsage) {
        releaseQueryMemoryQuota(memoryUsage, currentState.get());
    }

    @Override
    public OperatorMemoryTracker getOperatorMemoryTracker(OperatorMemoryOwnerId operatorMemoryOwnerId) {
        // Find which state this tracker belongs to
        MemoryManagerState state = findStateForTracker(operatorMemoryOwnerId);

        if (state == null) {
            // Not found in any state, need to determine which state to use for creation
            // First, check if parent tracker exists and use its state
            DriverMemoryOwnerId driverMemoryOwnerId = operatorMemoryOwnerId.getParentId();
            state = findStateForTracker(driverMemoryOwnerId);

            // If parent also doesn't exist, use current state to create
            if (state == null) {
                state = currentState.get();
                if (state.isImmutable()) {
                    LOG.error("Cannot create tracker on immutable state");
                }
            }
        }

        OperatorMemoryTracker operatorMemoryTracker = state.getOperatorMemoryTrackers().get(operatorMemoryOwnerId);
        if (operatorMemoryTracker != null) {
            return operatorMemoryTracker;
        }

        // Create new operatorMemoryTracker
        // Ensure parent tracker is in the same state
        DriverMemoryOwnerId driverMemoryOwnerId = operatorMemoryOwnerId.getParentId();
        DriverMemoryTracker driverMemoryTracker = state.getDriverMemoryTrackers().get(driverMemoryOwnerId);

        if (driverMemoryTracker == null) {
            // Parent doesn't exist in this state, need to create it first
            driverMemoryTracker = getDriverMemoryTracker(driverMemoryOwnerId);

            // Verify parent is in the same state to prevent cross-state reference
            MemoryManagerState parentState = findStateForTracker(driverMemoryOwnerId);
            if (parentState != state) {
                LOG.error("Parent tracker is not in the same state. Expected state version: " + state.getVersion() +
                    ", but parent is in state version: " + (parentState != null ? parentState.getVersion() : "null") +
                    ". Using parent tracker from different state.");
            }
        }

        final DriverMemoryTracker finalDriverMemoryTracker = driverMemoryTracker;
        return state.getOperatorMemoryTrackers().computeIfAbsent(operatorMemoryOwnerId,
            operatorId -> finalDriverMemoryTracker.getOperatorMemoryTracker(operatorId));
    }

    @Override
    public DriverMemoryTracker getDriverMemoryTracker(DriverMemoryOwnerId driverMemoryOwnerId) {
        // Find which state this tracker belongs to
        MemoryManagerState state = findStateForTracker(driverMemoryOwnerId);

        if (state == null) {
            // Not found in any state, need to determine which state to use for creation
            // First, check if parent tracker exists and use its state
            PipelineMemoryOwnerId pipelineMemoryOwnerId = driverMemoryOwnerId.getParentId();
            state = findStateForTracker(pipelineMemoryOwnerId);

            // If parent also doesn't exist, use current state to create
            if (state == null) {
                state = currentState.get();
                if (state.isImmutable()) {
                    LOG.error("Cannot create tracker on immutable state");
                }
            }
        }

        DriverMemoryTracker driverMemoryTracker = state.getDriverMemoryTrackers().get(driverMemoryOwnerId);
        if (driverMemoryTracker != null) {
            return driverMemoryTracker;
        }

        // Create new DriverMemoryTracker
        // Ensure parent tracker is in the same state
        PipelineMemoryOwnerId pipelineMemoryOwnerId = driverMemoryOwnerId.getParentId();
        PipelineMemoryTracker pipelineMemoryTracker = state.getPipelineMemoryTrackers().get(pipelineMemoryOwnerId);

        if (pipelineMemoryTracker == null) {
            // Parent doesn't exist in this state, need to create it first
            pipelineMemoryTracker = getPipelineMemoryTracker(pipelineMemoryOwnerId);

            // Verify parent is in the same state to prevent cross-state reference
            MemoryManagerState parentState = findStateForTracker(pipelineMemoryOwnerId);
            if (parentState != state) {
                LOG.error("Parent tracker is not in the same state. Expected state version: " + state.getVersion() +
                    ", but parent is in state version: " + (parentState != null ? parentState.getVersion() : "null") +
                    ". Using parent tracker from different state.");
            }
        }

        final PipelineMemoryTracker finalPipelineMemoryTracker = pipelineMemoryTracker;
        return state.getDriverMemoryTrackers().computeIfAbsent(driverMemoryOwnerId,
            driverId -> finalPipelineMemoryTracker.getDriverMemoryTracker(driverId)
        );
    }

    @Override
    public PipelineMemoryTracker getPipelineMemoryTracker(PipelineMemoryOwnerId pipelineMemoryOwnerId) {
        // Find which state this tracker belongs to
        MemoryManagerState state = findStateForTracker(pipelineMemoryOwnerId);

        if (state == null) {
            // Not found in any state, need to determine which state to use for creation
            // First, check if parent tracker exists and use its state
            QueryMemoryOwnerId queryMemoryOwnerId = pipelineMemoryOwnerId.getParentId();
            QueryMemoryTracker queryMemoryTracker = getQueryMemoryTracker(queryMemoryOwnerId);

            if (queryMemoryTracker == null) {
                QueryMemTrackerRemovedException e =
                    new QueryMemTrackerRemovedException(Objects.toString(queryMemoryOwnerId));
                LOG.warn("Query memory tracker has been removed", e);
                throw e;
            }

            // Use the state where parent query tracker exists
            state = findStateForTracker(queryMemoryOwnerId);

            // If parent state is immutable, cannot create new child trackers
            if (state != null && state.isImmutable()) {
                LOG.error("Cannot create tracker on immutable state (version " + state.getVersion() + ")");
            }

            // If parent also doesn't exist in any state (shouldn't happen), use current state
            if (state == null) {
                state = currentState.get();
                if (state.isImmutable()) {
                    LOG.error("Cannot create tracker on immutable state");
                }
            }
        }

        PipelineMemoryTracker pipelineMemoryTracker = state.getPipelineMemoryTrackers().get(pipelineMemoryOwnerId);
        if (pipelineMemoryTracker != null) {
            return pipelineMemoryTracker;
        }

        // Create new PipelineMemoryTracker
        // Ensure parent tracker is in the same state
        QueryMemoryOwnerId queryMemoryOwnerId = pipelineMemoryOwnerId.getParentId();
        QueryMemoryTracker queryMemoryTracker = state.getQueryMemoryTrackers().get(queryMemoryOwnerId);

        if (queryMemoryTracker == null) {
            // Parent doesn't exist in this state, this is an error
            // because query tracker should always be created before pipeline tracker
            QueryMemTrackerRemovedException e =
                new QueryMemTrackerRemovedException(Objects.toString(queryMemoryOwnerId));
            LOG.warn("Query memory tracker not found in expected state", e);
            throw e;
        }

        return state.getPipelineMemoryTrackers().computeIfAbsent(pipelineMemoryOwnerId,
            pipelineId -> queryMemoryTracker.getPipelineMemoryTracker(pipelineId));
    }

    @Override
    public QueryMemoryTracker createQueryMemoryTracker(QueryMemoryOwnerId queryMemoryOwnerId) {
        return createQueryMemoryTracker(queryMemoryOwnerId, currentState.get());
    }

    private QueryMemoryTracker createQueryMemoryTracker(QueryMemoryOwnerId queryMemoryOwnerId,
                                                        MemoryManagerState state) {
        return state.getQueryMemoryTrackers().computeIfAbsent(queryMemoryOwnerId,
            id -> new QueryMemoryTracker(id, state)
        );
    }

    private QueryMemoryTracker getQueryMemoryTracker(QueryMemoryOwnerId queryMemoryOwnerId) {
        // Search in current state first
        MemoryManagerState state = currentState.get();
        QueryMemoryTracker tracker = state.getQueryMemoryTrackers().get(queryMemoryOwnerId);
        if (tracker != null) {
            return tracker;
        }

        // Search in historical states
        for (MemoryManagerState historicalState : historicalStates.values()) {
            tracker = historicalState.getQueryMemoryTrackers().get(queryMemoryOwnerId);
            if (tracker != null) {
                return tracker;
            }
        }

        return null;
    }

    /**
     * Find which state a tracker belongs to.
     */
    private MemoryManagerState findStateForTracker(MemoryOwnerId ownerId) {
        // Check current state first
        MemoryManagerState state = currentState.get();
        if (state.containsTracker(ownerId)) {
            return state;
        }

        // Check historical states
        for (MemoryManagerState historicalState : historicalStates.values()) {
            if (historicalState.containsTracker(ownerId)) {
                return historicalState;
            }
        }

        return null;
    }

    @Override
    public void removeQueryMemoryTracker(QueryMemoryOwnerId queryMemoryOwnerId) {
        QueryMemoryTracker queryMemoryTracker = getQueryMemoryTracker(queryMemoryOwnerId);
        if (queryMemoryTracker == null) {
            return;
        }

        // will release pipeline -> driver -> operator memory in cascade.
        queryMemoryTracker.releaseAll();

        removeQueryMemoryOwnerId(queryMemoryOwnerId);
    }

    @Override
    public void releaseStageMemory(String queryId, int stageId, boolean failed) {
        // Search in all states
        MemoryManagerState state = currentState.get();
        QueryMemoryOwnerId queryMemoryOwnerId = state.getQueryMemoryOwnerIdMap().get(queryId);

        if (queryMemoryOwnerId == null) {
            // Try historical states
            for (MemoryManagerState historicalState : historicalStates.values()) {
                queryMemoryOwnerId = historicalState.getQueryMemoryOwnerIdMap().get(queryId);
                if (queryMemoryOwnerId != null) {
                    state = historicalState;
                    break;
                }
            }
        }

        if (queryMemoryOwnerId == null) {
            return;
        }

        QueryMemoryTracker queryMemoryTracker = state.getQueryMemoryTrackers().get(queryMemoryOwnerId);
        if (queryMemoryTracker == null) {
            return;
        }

        queryMemoryTracker.releaseStage(stageId, failed);
    }

    @Override
    public void releaseQueryMemory(String queryId) {
        // Search in all states
        MemoryManagerState state = currentState.get();
        QueryMemoryOwnerId queryMemoryOwnerId = state.getQueryMemoryOwnerIdMap().get(queryId);

        if (queryMemoryOwnerId == null) {
            // Try historical states
            for (MemoryManagerState historicalState : historicalStates.values()) {
                queryMemoryOwnerId = historicalState.getQueryMemoryOwnerIdMap().get(queryId);
                if (queryMemoryOwnerId != null) {
                    state = historicalState;
                    break;
                }
            }
        }

        if (queryMemoryOwnerId == null) {
            return;
        }

        QueryMemoryTracker queryMemoryTracker = state.getQueryMemoryTrackers().get(queryMemoryOwnerId);
        if (queryMemoryTracker == null) {
            return;
        }
        queryMemoryTracker.releaseAll();
    }

    // used by logic in tracker
    protected void removeQueryMemoryOwnerId(QueryMemoryOwnerId queryMemoryOwnerId) {
        // Remove from the state it belongs to
        MemoryManagerState state = findStateForTracker(queryMemoryOwnerId);
        if (state != null) {
            state.getQueryMemoryTrackers().remove(queryMemoryOwnerId);
            if (queryMemoryOwnerId != null && queryMemoryOwnerId.getQueryId() != null) {
                state.getQueryMemoryOwnerIdMap().remove(queryMemoryOwnerId.getQueryId());
            }
        }
    }

    // used by logic in tracker
    protected void remove(PipelineMemoryOwnerId pipelineMemoryOwnerId) {
        MemoryManagerState state = findStateForTracker(pipelineMemoryOwnerId);
        if (state != null) {
            state.getPipelineMemoryTrackers().remove(pipelineMemoryOwnerId);
        }
    }

    // used by logic in tracker
    protected void remove(DriverMemoryOwnerId driverMemoryOwnerId) {
        MemoryManagerState state = findStateForTracker(driverMemoryOwnerId);
        if (state != null) {
            state.getDriverMemoryTrackers().remove(driverMemoryOwnerId);
        }
    }

    // used by logic in tracker
    protected void remove(OperatorMemoryOwnerId operatorMemoryOwnerId) {
        MemoryManagerState state = findStateForTracker(operatorMemoryOwnerId);
        if (state != null) {
            state.getOperatorMemoryTrackers().remove(operatorMemoryOwnerId);
        }
    }

    /**
     * Remove entire pipeline subtree including all drivers and operators.
     * This is used by QueryMemoryTracker to avoid deep recursive releaseAll() calls.
     * <p>
     * Note: ConcurrentHashMap.values() returns a thread-safe collection view that reflects
     * the current state of the map. Iterating over this view is safe even if the map is
     * modified concurrently, as it provides a weakly consistent iterator.
     * <p>
     * NOTE: Don't call this method if any lock held.
     */
    protected void removePipelineSubtree(PipelineMemoryOwnerId pipelineMemoryOwnerId) {
        MemoryManagerState state = findStateForTracker(pipelineMemoryOwnerId);
        if (state == null) {
            return;
        }

        // Remove pipeline tracker
        state.getPipelineMemoryTrackers().remove(pipelineMemoryOwnerId);

        // Remove all driver trackers under this pipeline
        // ConcurrentHashMap.values() provides a thread-safe view, no need for ArrayList snapshot
        ConcurrentHashMap<Long, DriverMemoryOwnerId> driverOwnerIdMap =
            pipelineMemoryOwnerId.getDriverMemoryOwnerIdMap();
        for (DriverMemoryOwnerId driverMemoryOwnerId : driverOwnerIdMap.values()) {
            // Remove all operator trackers under this driver
            // ConcurrentHashMap.values() provides a thread-safe view, no need for ArrayList snapshot
            ConcurrentHashMap<Long, OperatorMemoryOwnerId> operatorOwnerIdMap =
                driverMemoryOwnerId.getOperatorMemoryOwnerIdMap();
            for (OperatorMemoryOwnerId operatorMemoryOwnerId : operatorOwnerIdMap.values()) {
                state.getOperatorMemoryTrackers().remove(operatorMemoryOwnerId);
            }

            // Remove driver tracker
            state.getDriverMemoryTrackers().remove(driverMemoryOwnerId);
        }
    }

    public String dump() {
        DecimalFormat decimalFormat = new DecimalFormat("0.00");
        StringBuilder builder = new StringBuilder();

        MemoryManagerState state = currentState.get();
        ConcurrentHashMap<String, QueryMemoryOwnerId> queryMemoryOwnerIdMap = state.getQueryMemoryOwnerIdMap();
        ConcurrentHashMap<QueryMemoryOwnerId, QueryMemoryTracker> queryMemoryTrackers = state.getQueryMemoryTrackers();
        ConcurrentHashMap<PipelineMemoryOwnerId, PipelineMemoryTracker> pipelineMemoryTrackers =
            state.getPipelineMemoryTrackers();
        ConcurrentHashMap<DriverMemoryOwnerId, DriverMemoryTracker> driverMemoryTrackers =
            state.getDriverMemoryTrackers();
        ConcurrentHashMap<OperatorMemoryOwnerId, OperatorMemoryTracker> operatorMemoryTrackers =
            state.getOperatorMemoryTrackers();

        for (Map.Entry<String, QueryMemoryOwnerId> entry : queryMemoryOwnerIdMap.entrySet()) {

            // for each query.
            QueryMemoryOwnerId queryMemoryOwnerId = entry.getValue();
            QueryMemoryTracker queryMemoryTracker = queryMemoryTrackers.get(queryMemoryOwnerId);
            if (queryMemoryTracker == null) {
                continue;
            }

            builder.append(
                "query: " + entry.getKey() + "@" + System.identityHashCode(queryMemoryTracker)
                    + ", allocated = " + decimalFormat.format(queryMemoryTracker.getMemoryUsage() / 1024.0 / 1024)
                    + " MB, preallocate = " + decimalFormat.format(
                    queryMemoryTracker.getMemoryFreeSize() / 1024.0 / 1024)
                    + " MB, total = " + decimalFormat.format(
                    queryMemoryTracker.getMemoryAllocatedSize() / 1024.0 / 1024)
                    + " MB\n");

            for (Map.Entry<Long, PipelineMemoryOwnerId> pipelineEntry : entry.getValue().getPipelineMemoryOwnerIdMap()
                .entrySet()) {

                // for each stage-pipeline.
                builder.append("  ");
                Long stagePipelineId = pipelineEntry.getKey();
                PipelineMemoryOwnerId pipelineMemoryOwnerId = pipelineEntry.getValue();
                PipelineMemoryTracker pipelineMemoryTracker = pipelineMemoryTrackers.get(pipelineMemoryOwnerId);
                if (pipelineMemoryTracker == null) {
                    continue;
                }

                builder.append("pipeline: " + PipelineMemoryOwnerId.getStageId(stagePipelineId) + "-"
                    + PipelineMemoryOwnerId.getPipelineId(stagePipelineId)
                    + "@" + System.identityHashCode(pipelineMemoryTracker)
                    + ", allocated = " + decimalFormat.format(pipelineMemoryTracker.getMemoryUsage() / 1024.0 / 1024)
                    + " MB, preallocate = " + decimalFormat.format(
                    pipelineMemoryTracker.getMemoryFreeSize() / 1024.0 / 1024)
                    + " MB, total = " + decimalFormat.format(
                    pipelineMemoryTracker.getMemoryAllocatedSize() / 1024.0 / 1024)
                    + " MB\n");

                for (Map.Entry<Long, DriverMemoryOwnerId> driverEntry : pipelineEntry.getValue()
                    .getDriverMemoryOwnerIdMap().entrySet()) {

                    // for each driver.
                    builder.append("  ");
                    builder.append("  ");
                    DriverMemoryOwnerId driverMemoryOwnerId = driverEntry.getValue();
                    DriverMemoryTracker driverMemoryTracker = driverMemoryTrackers.get(driverMemoryOwnerId);
                    if (driverMemoryTracker == null) {
                        continue;
                    }

                    builder.append(
                        "driver: " + driverEntry.getKey() + "@" + System.identityHashCode(driverMemoryTracker)
                            + ", allocated = " + decimalFormat.format(
                            driverMemoryTracker.getMemoryUsage() / 1024.0 / 1024)
                            + " MB, preallocate = " + decimalFormat.format(
                            driverMemoryTracker.getMemoryFreeSize() / 1024.0 / 1024)
                            + " MB, total = " + decimalFormat.format(
                            driverMemoryTracker.getMemoryAllocatedSize() / 1024.0 / 1024)
                            + " MB\n");

                    for (Map.Entry<Long, OperatorMemoryOwnerId> operatorEntry : driverEntry.getValue()
                        .getOperatorMemoryOwnerIdMap().entrySet()) {

                        // for each operator.
                        builder.append("  ");
                        builder.append("  ");
                        builder.append("  ");
                        OperatorMemoryOwnerId operatorMemoryOwnerId = operatorEntry.getValue();
                        OperatorMemoryTracker operatorMemoryTracker = operatorMemoryTrackers.get(operatorMemoryOwnerId);
                        if (operatorMemoryTracker == null) {
                            continue;
                        }

                        builder.append(
                            "operator: " + operatorEntry.getKey() + "@" + System.identityHashCode(operatorMemoryTracker)
                                + " , allocated = " + decimalFormat.format(
                                operatorMemoryTracker.getMemoryUsage() / 1024.0 / 1024)
                                + " MB, preallocate = " + decimalFormat.format(
                                operatorMemoryTracker.getMemoryFreeSize() / 1024.0 / 1024)
                                + " MB, total = " + decimalFormat.format(
                                operatorMemoryTracker.getMemoryAllocatedSize() / 1024.0 / 1024)
                                + " MB\n");
                    }

                }
            }

        }
        return builder.toString();
    }

    // query-id, query-alloc, query-total,
    // pipeline-id, pipe-alloc, pipe-total,
    // driver-id, driver-alloc, driver-total
    // operator-id, operator-alloc, operator-total
    public List<Object[]> dumpTableResult() {
        List<Object[]> list = new ArrayList<>();

        MemoryManagerState state = currentState.get();
        ConcurrentHashMap<String, QueryMemoryOwnerId> queryMemoryOwnerIdMap = state.getQueryMemoryOwnerIdMap();
        ConcurrentHashMap<QueryMemoryOwnerId, QueryMemoryTracker> queryMemoryTrackers = state.getQueryMemoryTrackers();
        ConcurrentHashMap<PipelineMemoryOwnerId, PipelineMemoryTracker> pipelineMemoryTrackers =
            state.getPipelineMemoryTrackers();
        ConcurrentHashMap<DriverMemoryOwnerId, DriverMemoryTracker> driverMemoryTrackers =
            state.getDriverMemoryTrackers();
        ConcurrentHashMap<OperatorMemoryOwnerId, OperatorMemoryTracker> operatorMemoryTrackers =
            state.getOperatorMemoryTrackers();

        for (Map.Entry<String, QueryMemoryOwnerId> queryEntry : queryMemoryOwnerIdMap.entrySet()) {

            // for each query.
            QueryMemoryOwnerId queryMemoryOwnerId = queryEntry.getValue();
            QueryMemoryTracker queryMemoryTracker = queryMemoryTrackers.get(queryMemoryOwnerId);
            if (queryMemoryTracker == null) {
                continue;
            }

            for (Map.Entry<Long, PipelineMemoryOwnerId> pipelineEntry : queryEntry.getValue()
                .getPipelineMemoryOwnerIdMap()
                .entrySet()) {

                // for each stage-pipeline.
                Long stagePipelineId = pipelineEntry.getKey();
                PipelineMemoryOwnerId pipelineMemoryOwnerId = pipelineEntry.getValue();
                PipelineMemoryTracker pipelineMemoryTracker = pipelineMemoryTrackers.get(pipelineMemoryOwnerId);
                if (pipelineMemoryTracker == null) {
                    continue;
                }

                String pipelineName = PipelineMemoryOwnerId.getStageId(stagePipelineId) + "-"
                    + PipelineMemoryOwnerId.getPipelineId(stagePipelineId);

                for (Map.Entry<Long, DriverMemoryOwnerId> driverEntry : pipelineEntry.getValue()
                    .getDriverMemoryOwnerIdMap().entrySet()) {

                    // for each driver.
                    DriverMemoryOwnerId driverMemoryOwnerId = driverEntry.getValue();
                    DriverMemoryTracker driverMemoryTracker = driverMemoryTrackers.get(driverMemoryOwnerId);
                    if (driverMemoryTracker == null) {
                        continue;
                    }

                    for (Map.Entry<Long, OperatorMemoryOwnerId> operatorEntry : driverEntry.getValue()
                        .getOperatorMemoryOwnerIdMap().entrySet()) {

                        // for each operator.
                        OperatorMemoryOwnerId operatorMemoryOwnerId = operatorEntry.getValue();
                        OperatorMemoryTracker operatorMemoryTracker = operatorMemoryTrackers.get(operatorMemoryOwnerId);
                        if (operatorMemoryTracker == null) {
                            continue;
                        }

                        Object[] array = new Object[13];

                        // query-id, query-total, query-alloc
                        array[0] = queryEntry.getKey();
                        array[1] = toMegabytes(queryMemoryTracker.getMemoryAllocatedSize());
                        array[2] = toMegabytes(queryMemoryTracker.getMemoryUsage());

                        // pipeline-id, pipe-total, pipe-alloc
                        array[3] = pipelineName;
                        array[4] = toMegabytes(pipelineMemoryTracker.getMemoryAllocatedSize());
                        array[5] = toMegabytes(pipelineMemoryTracker.getMemoryUsage());

                        // driver-id, driver-total, driver-alloc
                        array[6] = String.valueOf(driverEntry.getKey());
                        array[7] = toMegabytes(driverMemoryTracker.getMemoryAllocatedSize());
                        array[8] = toMegabytes(driverMemoryTracker.getMemoryUsage());

                        // operator-id, operator-total, operator-alloc
                        array[9] = String.valueOf(operatorEntry.getKey());
                        array[10] = toMegabytes(operatorMemoryTracker.getMemoryAllocatedSize());
                        array[11] = toMegabytes(operatorMemoryTracker.getMemoryUsage());
                        array[12] = operatorMemoryOwnerId.getOperatorName();

                        list.add(array);
                    }

                }
            }

        }
        return list;
    }

    // query-id, query-alloc, query-total,
    // pipeline-id, pipe-alloc, pipe-total,
    // driver-id, driver-alloc, driver-total
    // operator-id, operator-alloc, operator-total
    public List<Object[]> dumpTableResult(int topN) {
        if (topN <= 0) {
            return new ArrayList<>();
        }

        // Use min heap to maintain topN largest pipelineMemoryTracker.getMemoryAllocatedSize()
        ObjectHeapPriorityQueue<Object[]> minHeap = new ObjectHeapPriorityQueue<>(
            (Object[] a, Object[] b) -> Long.compare((Long) a[4], (Long) b[4])  // Compare pipeline allocated size
        );

        MemoryManagerState state = currentState.get();
        ConcurrentHashMap<String, QueryMemoryOwnerId> queryMemoryOwnerIdMap = state.getQueryMemoryOwnerIdMap();
        ConcurrentHashMap<QueryMemoryOwnerId, QueryMemoryTracker> queryMemoryTrackers = state.getQueryMemoryTrackers();
        ConcurrentHashMap<PipelineMemoryOwnerId, PipelineMemoryTracker> pipelineMemoryTrackers =
            state.getPipelineMemoryTrackers();
        ConcurrentHashMap<DriverMemoryOwnerId, DriverMemoryTracker> driverMemoryTrackers =
            state.getDriverMemoryTrackers();
        ConcurrentHashMap<OperatorMemoryOwnerId, OperatorMemoryTracker> operatorMemoryTrackers =
            state.getOperatorMemoryTrackers();

        for (Map.Entry<String, QueryMemoryOwnerId> queryEntry : queryMemoryOwnerIdMap.entrySet()) {

            // for each query.
            QueryMemoryOwnerId queryMemoryOwnerId = queryEntry.getValue();
            QueryMemoryTracker queryMemoryTracker = queryMemoryTrackers.get(queryMemoryOwnerId);
            if (queryMemoryTracker == null) {
                continue;
            }

            for (Map.Entry<Long, PipelineMemoryOwnerId> pipelineEntry : queryEntry.getValue()
                .getPipelineMemoryOwnerIdMap()
                .entrySet()) {

                // for each stage-pipeline.
                Long stagePipelineId = pipelineEntry.getKey();
                PipelineMemoryOwnerId pipelineMemoryOwnerId = pipelineEntry.getValue();
                PipelineMemoryTracker pipelineMemoryTracker = pipelineMemoryTrackers.get(pipelineMemoryOwnerId);
                if (pipelineMemoryTracker == null) {
                    continue;
                }

                String pipelineName = PipelineMemoryOwnerId.getStageId(stagePipelineId) + "-"
                    + PipelineMemoryOwnerId.getPipelineId(stagePipelineId);

                for (Map.Entry<Long, DriverMemoryOwnerId> driverEntry : pipelineEntry.getValue()
                    .getDriverMemoryOwnerIdMap().entrySet()) {

                    // for each driver.
                    DriverMemoryOwnerId driverMemoryOwnerId = driverEntry.getValue();
                    DriverMemoryTracker driverMemoryTracker = driverMemoryTrackers.get(driverMemoryOwnerId);
                    if (driverMemoryTracker == null) {
                        continue;
                    }

                    for (Map.Entry<Long, OperatorMemoryOwnerId> operatorEntry : driverEntry.getValue()
                        .getOperatorMemoryOwnerIdMap().entrySet()) {

                        // for each operator.
                        OperatorMemoryOwnerId operatorMemoryOwnerId = operatorEntry.getValue();
                        OperatorMemoryTracker operatorMemoryTracker = operatorMemoryTrackers.get(operatorMemoryOwnerId);
                        if (operatorMemoryTracker == null) {
                            continue;
                        }

                        Object[] array = new Object[13];

                        // query-id, query-total, query-alloc
                        array[0] = queryEntry.getKey();
                        array[1] = toMegabytes(queryMemoryTracker.getMemoryAllocatedSize());
                        array[2] = toMegabytes(queryMemoryTracker.getMemoryUsage());

                        // pipeline-id, pipe-total, pipe-alloc
                        array[3] = pipelineName;
                        array[4] = pipelineMemoryTracker.getMemoryAllocatedSize(); // Keep original value for comparison
                        array[5] = toMegabytes(pipelineMemoryTracker.getMemoryUsage());

                        // driver-id, driver-total, driver-alloc
                        array[6] = String.valueOf(driverEntry.getKey());
                        array[7] = toMegabytes(driverMemoryTracker.getMemoryAllocatedSize());
                        array[8] = toMegabytes(driverMemoryTracker.getMemoryUsage());

                        // operator-id, operator-total, operator-alloc
                        array[9] = String.valueOf(operatorEntry.getKey());
                        array[10] = toMegabytes(operatorMemoryTracker.getMemoryAllocatedSize());
                        array[11] = toMegabytes(operatorMemoryTracker.getMemoryUsage());
                        array[12] = operatorMemoryOwnerId.getOperatorName();

                        // Use min heap to maintain topN
                        if (minHeap.size() < topN) {
                            minHeap.enqueue(array);
                        } else if ((Long) array[4] > (Long) minHeap.first()[4]) {
                            minHeap.dequeue(); // Remove smallest value from heap top
                            minHeap.enqueue(array); // Add new larger value
                        }
                    }

                }
            }

        }

        // Convert heap results to list and convert pipeline allocated size to MB
        List<Object[]> result = new ArrayList<>();
        while (minHeap.size() > 0) {
            Object[] array = minHeap.dequeue();
            array[4] = toMegabytes((Long) array[4]); // Convert to MB
            result.add(0, array); // Insert at beginning to maintain descending order
        }

        return result;
    }

    public List<Object[]> dumpQueryLevelTableResult() {
        List<Object[]> list = new ArrayList<>();

        MemoryManagerState state = currentState.get();
        ConcurrentHashMap<String, QueryMemoryOwnerId> queryMemoryOwnerIdMap = state.getQueryMemoryOwnerIdMap();
        ConcurrentHashMap<QueryMemoryOwnerId, QueryMemoryTracker> queryMemoryTrackers = state.getQueryMemoryTrackers();

        for (Map.Entry<String, QueryMemoryOwnerId> queryEntry : queryMemoryOwnerIdMap.entrySet()) {

            // for each query.
            QueryMemoryOwnerId queryMemoryOwnerId = queryEntry.getValue();
            QueryMemoryTracker queryMemoryTracker = queryMemoryTrackers.get(queryMemoryOwnerId);
            if (queryMemoryTracker == null) {
                continue;
            }

            Object[] array = new Object[5];
            array[0] = queryEntry.getKey();
            array[1] = toMegabytes(queryMemoryTracker.getMemoryAllocatedSize());
            array[2] = toMegabytes(queryMemoryTracker.getMemoryUsage());
            array[3] = toMegabytes(queryMemoryTracker.getMaxMemoryUsage());
            array[4] = null;
            list.add(array);
        }
        return list;
    }

    private static final Decimal MEGA_BYTES = Decimal.fromLong(1024 * 1024);

    private static Decimal toMegabytes(long bytes) {
        Decimal result = Decimal.fromLong(bytes).divide(MEGA_BYTES);
        DecimalStructure structure = result.getDecimalStructure();
        FastDecimalUtils.round(structure, structure, 2, DecimalRoundMod.HALF_UP);
        return new Decimal(structure);
    }

    public Object[] dumpTotalUsage() {
        DecimalFormat decimalFormat = new DecimalFormat("0.000000");

        MemoryManagerState state = currentState.get();
        long totalQuota = state.getTotalQueryMemoryQuota();
        long currentQuota = state.getAvailableQuota().get();

        Object[] result = new Object[7];
        result[0] = "EXECUTOR".getBytes();
        result[1] = totalQuota - currentQuota;
        result[2] = totalQuota;
        result[3] = decimalFormat.format((totalQuota - currentQuota) * 1.0d / totalQuota);

        result[4] = state.getQueryMemoryOwnerIdMap().size();
        result[5] = -1;
        result[6] = -1;

        return result;
    }

    /**
     * Executor memory metrics snapshot for thread-safe batch retrieval
     */
    public static class ExecutorMemoryMetrics {
        // Basic metrics
        private final long executorMemoryUsedSize;
        private final int executorMemoryUsedCount;
        private final double executorMemoryQuotaUsageRatio;
        private final long executorMemoryAvailableQuota;

        // Query-level metrics (aggregated from single iteration)
        private final long maxQueryMemoryPeak;
        private final long avgQueryMemoryUsage;
        private final long totalAllocatedMemory;
        private final long totalFreeMemory;

        // Execution hierarchy metrics
        private final int activePipelineCount;
        private final int activeDriverCount;
        private final int activeOperatorCount;

        public ExecutorMemoryMetrics(
            long executorMemoryUsedSize,
            int executorMemoryUsedCount,
            double executorMemoryQuotaUsageRatio,
            long executorMemoryAvailableQuota,
            long maxQueryMemoryPeak,
            long avgQueryMemoryUsage,
            long totalAllocatedMemory,
            long totalFreeMemory,
            int activePipelineCount,
            int activeDriverCount,
            int activeOperatorCount) {

            this.executorMemoryUsedSize = executorMemoryUsedSize;
            this.executorMemoryUsedCount = executorMemoryUsedCount;
            this.executorMemoryQuotaUsageRatio = executorMemoryQuotaUsageRatio;
            this.executorMemoryAvailableQuota = executorMemoryAvailableQuota;
            this.maxQueryMemoryPeak = maxQueryMemoryPeak;
            this.avgQueryMemoryUsage = avgQueryMemoryUsage;
            this.totalAllocatedMemory = totalAllocatedMemory;
            this.totalFreeMemory = totalFreeMemory;
            this.activePipelineCount = activePipelineCount;
            this.activeDriverCount = activeDriverCount;
            this.activeOperatorCount = activeOperatorCount;
        }

        public long getExecutorMemoryUsedSize() {
            return executorMemoryUsedSize;
        }

        public int getExecutorMemoryUsedCount() {
            return executorMemoryUsedCount;
        }

        public double getExecutorMemoryQuotaUsageRatio() {
            return executorMemoryQuotaUsageRatio;
        }

        public long getExecutorMemoryAvailableQuota() {
            return executorMemoryAvailableQuota;
        }

        public long getMaxQueryMemoryPeak() {
            return maxQueryMemoryPeak;
        }

        public long getAvgQueryMemoryUsage() {
            return avgQueryMemoryUsage;
        }

        public long getTotalAllocatedMemory() {
            return totalAllocatedMemory;
        }

        public long getTotalFreeMemory() {
            return totalFreeMemory;
        }

        public int getActivePipelineCount() {
            return activePipelineCount;
        }

        public int getActiveDriverCount() {
            return activeDriverCount;
        }

        public int getActiveOperatorCount() {
            return activeOperatorCount;
        }
    }

    /**
     * Get all executor memory metrics in a single snapshot
     * This method performs ONE iteration over queryMemoryTrackers to collect all metrics,
     * ensuring thread-safety and performance optimization
     *
     * @return ExecutorMemoryMetrics snapshot containing all metrics
     */
    public ExecutorMemoryMetrics getExecutorMemoryMetricsSnapshot() {
        MemoryManagerState state = currentState.get();
        long totalQuota = state.getTotalQueryMemoryQuota();
        long availableQuota = state.getAvailableQuota().get();

        // Capture quota state first (atomic operations)
        long executorMemoryUsedSize = totalQuota - availableQuota;
        long executorMemoryAvailableQuota = availableQuota;
        double executorMemoryQuotaUsageRatio = executorMemoryUsedSize * 100.0 / totalQuota;

        // Capture hierarchy counts (atomic size operations)
        int activePipelineCount = state.getPipelineMemoryTrackers().size();
        int activeDriverCount = state.getDriverMemoryTrackers().size();
        int activeOperatorCount = state.getOperatorMemoryTrackers().size();

        // Single iteration to collect all query-level metrics
        long maxQueryMemoryPeak = 0;
        long totalMemoryUsage = 0;
        long totalAllocatedMemory = 0;
        long totalFreeMemory = 0;
        int queryCount = 0;

        // Create a snapshot of trackers to avoid concurrent modification issues
        // Note: values() returns a view, we iterate it once
        for (QueryMemoryTracker tracker : state.getQueryMemoryTrackers().values()) {
            if (tracker == null) {
                continue;
            }

            try {
                // Collect all metrics in one pass
                long maxMemoryUsage = tracker.getMaxMemoryUsage();
                long memoryUsage = tracker.getMemoryUsage();
                long allocatedSize = tracker.getMemoryAllocatedSize();
                long freeSize = tracker.getMemoryFreeSize();

                maxQueryMemoryPeak = Math.max(maxQueryMemoryPeak, maxMemoryUsage);
                totalMemoryUsage += memoryUsage;
                totalAllocatedMemory += allocatedSize;
                totalFreeMemory += freeSize;
                queryCount++;
            } catch (Exception e) {
                // Skip this tracker if any error occurs during metric collection
                continue;
            }
        }

        // Calculate average (avoid division by zero)
        long avgQueryMemoryUsage = queryCount > 0 ? totalMemoryUsage / queryCount : 0;

        return new ExecutorMemoryMetrics(
            executorMemoryUsedSize,
            queryCount,
            executorMemoryQuotaUsageRatio,
            executorMemoryAvailableQuota,
            maxQueryMemoryPeak,
            avgQueryMemoryUsage,
            totalAllocatedMemory,
            totalFreeMemory,
            activePipelineCount,
            activeDriverCount,
            activeOperatorCount
        );
    }

    /**
     * Get executor runtime memory usage in bytes
     *
     * @return memory used by executor runtime
     */
    public long getExecutorMemoryUsedSize() {
        MemoryManagerState state = currentState.get();
        long availableQuota = state.getAvailableQuota().get();
        return state.getTotalQueryMemoryQuota() - availableQuota;
    }

    /**
     * Get executor runtime query count
     *
     * @return number of active queries
     */
    public int getExecutorMemoryUsedCount() {
        return currentState.get().getQueryMemoryOwnerIdMap().size();
    }
}