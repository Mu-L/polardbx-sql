package com.alibaba.polardbx.common.memory;

import com.alibaba.polardbx.common.utils.logger.Logger;
import org.roaringbitmap.RoaringBitmap;

public interface MemoryTrackerManager {
    Logger LOG = com.alibaba.polardbx.common.utils.logger.LoggerFactory.getLogger(MemoryTrackerManager.class);

    GlobalMemoryTrackerManager INSTANCE = new GlobalMemoryTrackerManager();

    static GlobalMemoryTrackerManager getGlobalMemoryTrackerManager() {
        return INSTANCE;
    }

    ThreadLocal<OperatorMemoryOwnerId> CURRENT_STACK_MEMORY_OWNER = new ThreadLocal<>();
    ThreadLocal<RoaringBitmap> CURRENT_STACK_ROARING_BITMAP = new ThreadLocal<>();

    static void setCurrentMemoryOwner(OperatorMemoryOwnerId operatorMemoryOwnerId) {
        CURRENT_STACK_MEMORY_OWNER.set(operatorMemoryOwnerId);
    }

    static OperatorMemoryOwnerId getCurrentMemoryOwner() {
        return CURRENT_STACK_MEMORY_OWNER.get();
    }

    static void removeCurrentMemoryOwner() {
        CURRENT_STACK_MEMORY_OWNER.remove();
    }

    static void setCurrentRoaringBitmap(RoaringBitmap roaringBitmap) {
        CURRENT_STACK_ROARING_BITMAP.set(roaringBitmap);
    }

    static RoaringBitmap getCurrentRoaringBitmap() {
        return CURRENT_STACK_ROARING_BITMAP.get();
    }

    static void removeCurrentRoaringBitmap() {
        CURRENT_STACK_ROARING_BITMAP.remove();
    }

    OperatorMemoryTracker getOperatorMemoryTracker(OperatorMemoryOwnerId operatorMemoryOwnerId);

    QueryMemoryTracker createQueryMemoryTracker(QueryMemoryOwnerId queryMemoryOwnerId);

    DriverMemoryTracker getDriverMemoryTracker(DriverMemoryOwnerId driverMemoryOwnerId);

    PipelineMemoryTracker getPipelineMemoryTracker(PipelineMemoryOwnerId pipelineMemoryOwnerId);

    void removeQueryMemoryTracker(QueryMemoryOwnerId queryMemoryOwnerId);

    void releaseStageMemory(String queryId, int stageId, boolean queryFailed);

    void releaseQueryMemory(String queryId);

    static long getMaximumQueryMemoryUsage(String queryId) {
        return getGlobalMemoryTrackerManager().getMaximumQueryMemoryAllocated(queryId);
    }

    static long memoryWatermark(OperatorMemoryOwnerId operatorMemoryOwnerId) {
        if (operatorMemoryOwnerId == null) {
            // only queries in MPP mode would have a memory-owner id.
            return 0L;
        }
        try {
            return getGlobalMemoryTrackerManager()
                .getOperatorMemoryTracker(operatorMemoryOwnerId)
                .getMemoryUsage();
        } catch (QueryMemTrackerRemovedException e) {
            // ignore here, because error log has been printed once exception created 
            return 0L;
        } catch (MemoryTrackerOutOfMemoryException outOfMemoryException) {
            dumpMemoryInfoOnError();
            throw outOfMemoryException;
        } catch (Throwable t) {
            throw t;
        }
    }

    static long memoryQuota(OperatorMemoryOwnerId operatorMemoryOwnerId) {
        if (operatorMemoryOwnerId == null) {
            // only queries in MPP mode would have a memory-owner id.
            return 0L;
        }
        try {
            return getGlobalMemoryTrackerManager()
                .getOperatorMemoryTracker(operatorMemoryOwnerId)
                .getMemoryFreeSize();
        } catch (QueryMemTrackerRemovedException e) {
            // ignore here, because error log has been printed once exception created 
            return 0L;
        } catch (MemoryTrackerOutOfMemoryException outOfMemoryException) {
            dumpMemoryInfoOnError();
            throw outOfMemoryException;
        } catch (Throwable t) {
            throw t;
        }
    }

    static void adjustMemoryUsage(OperatorMemoryOwnerId operatorMemoryOwnerId) {
        if (operatorMemoryOwnerId == null) {
            // only queries in MPP mode would have a memory-owner id.
            return;
        }
        try {
            getGlobalMemoryTrackerManager()
                .getOperatorMemoryTracker(operatorMemoryOwnerId)
                .adjustMemoryUsage();
        } catch (QueryMemTrackerRemovedException e) {
            // ignore here, because error log has been printed once exception created 
        } catch (MemoryTrackerOutOfMemoryException outOfMemoryException) {
            dumpMemoryInfoOnError();
            throw outOfMemoryException;
        } catch (Throwable t) {
            throw t;
        }
    }

    static void tryAllocate(OperatorMemoryOwnerId operatorMemoryOwnerId, long allocateSize) {
        if (operatorMemoryOwnerId == null) {
            // only queries in MPP mode would have a memory-owner id.
            return;
        }
        try {
            getGlobalMemoryTrackerManager()
                .getOperatorMemoryTracker(operatorMemoryOwnerId)
                .tryAllocateMemory(allocateSize);
        } catch (QueryMemTrackerRemovedException e) {
            // ignore here, because error log has been printed once exception created 
        } catch (MemoryTrackerOutOfMemoryException outOfMemoryException) {
            dumpMemoryInfoOnError();
            throw outOfMemoryException;
        } catch (Throwable t) {
            throw t;
        }
    }

    static void tryReverseReference(OperatorMemoryOwnerId operatorMemoryOwnerId, long reverseSize) {
        if (operatorMemoryOwnerId == null) {
            // only queries in MPP mode would have a memory-owner id.
            return;
        }
        try {
            getGlobalMemoryTrackerManager()
                .getOperatorMemoryTracker(operatorMemoryOwnerId)
                .tryReverseReference(reverseSize);
        } catch (QueryMemTrackerRemovedException e) {
            // ignore here, because error log has been printed once exception created 
        } catch (MemoryTrackerOutOfMemoryException outOfMemoryException) {
            dumpMemoryInfoOnError();
            throw outOfMemoryException;
        } catch (Throwable t) {
            throw t;
        }
    }

    static void releaseReference(OperatorMemoryOwnerId operatorMemoryOwnerId, long reverseSize) {
        if (operatorMemoryOwnerId == null) {
            // only queries in MPP mode would have a memory-owner id.
            return;
        }
        try {
            getGlobalMemoryTrackerManager()
                .getOperatorMemoryTracker(operatorMemoryOwnerId)
                .releaseReference(reverseSize);
        } catch (QueryMemTrackerRemovedException e) {
            // ignore here, because error log has been printed once exception created 
        } catch (MemoryTrackerOutOfMemoryException outOfMemoryException) {
            dumpMemoryInfoOnError();
            throw outOfMemoryException;
        } catch (Throwable t) {
            throw t;
        }
    }

    static void releaseAll(OperatorMemoryOwnerId operatorMemoryOwnerId) {
        if (operatorMemoryOwnerId == null) {
            // only queries in MPP mode would have a memory-owner id.
            return;
        }
        try {
            getGlobalMemoryTrackerManager()
                .getOperatorMemoryTracker(operatorMemoryOwnerId)
                .releaseAll();
        } catch (QueryMemTrackerRemovedException e) {
            // ignore here, because error log has been printed once exception created 
        } catch (MemoryTrackerOutOfMemoryException outOfMemoryException) {
            dumpMemoryInfoOnError();
            throw outOfMemoryException;
        } catch (Throwable t) {
            throw t;
        }
    }

    /**
     * Dump memory information when error occurs.
     * This method logs detailed memory usage information from GlobalMemoryTrackerManager
     * including both table-level and query-level memory statistics.
     */
    static void dumpMemoryInfoOnError() {
        try {
            GlobalMemoryTrackerManager globalManager = getGlobalMemoryTrackerManager();
            StringBuilder dumpInfo = new StringBuilder();

            // Build query-level memory information
            dumpInfo.append("=== Query Level Memory Dump ===\n");
            java.util.List<Object[]> queryLevelResults = globalManager.dumpQueryLevelTableResult();
            if (queryLevelResults.isEmpty()) {
                dumpInfo.append("No active queries found in memory tracker\n");
            } else {
                dumpInfo.append("Query-Level Memory Usage (QueryId, TotalMB, UsedMB, MaxUsedMB):\n");
                for (Object[] row : queryLevelResults) {
                    dumpInfo.append("  Query: ").append(row[0])
                        .append(", Total: ").append(row[1]).append(" MB")
                        .append(", Used: ").append(row[2]).append(" MB")
                        .append(", Max: ").append(row[3]).append(" MB\n");
                }
            }

            // Build detailed table-level memory information
            dumpInfo.append("=== Detailed Memory Dump (Top-5 pipeline) ===\n");
            java.util.List<Object[]> tableResults = globalManager.dumpTableResult(5);
            if (tableResults.isEmpty()) {
                dumpInfo.append("No detailed memory information available\n");
            } else {
                dumpInfo.append("Detailed Memory Usage (QueryId, Pipeline, Driver, Operator):\n");
                for (Object[] row : tableResults) {
                    dumpInfo.append("  Query: ").append(row[0])
                        .append(", Pipeline: ").append(row[3])
                        .append(" (Total: ").append(row[4]).append(" MB, Used: ").append(row[5]).append(" MB)")
                        .append(", Driver: ").append(row[6])
                        .append(" (Total: ").append(row[7]).append(" MB, Used: ").append(row[8]).append(" MB)")
                        .append(", Operator: ").append(row[9]).append(" ").append(row[12])
                        .append(" (Total: ").append(row[10]).append(" MB, Used: ").append(row[11]).append(" MB)\n");
                }
            }

            // Build total memory usage
            Object[] totalUsage = globalManager.dumpTotalUsage();
            dumpInfo.append("=== Total Memory Usage ===\n");
            dumpInfo.append("Total Memory - Used: ").append(totalUsage[1]).append(" bytes")
                .append(", Quota: ").append(totalUsage[2]).append(" bytes")
                .append(", Usage Ratio: ").append(totalUsage[3])
                .append(", Active Queries: ").append(totalUsage[4]);

            // Output all dump information in one LOG.error call
            LOG.error("Memory dump information:\n" + dumpInfo.toString());

        } catch (Exception e) {
            LOG.error("Failed to dump memory information on error", e);
        }
    }
}