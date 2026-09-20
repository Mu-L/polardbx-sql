package com.alibaba.polardbx.executor.scheduler.executor;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.executor.scheduler.ScheduledJobsManager;
import com.alibaba.polardbx.gms.metadb.cache.CacheFileMappingAccessor;
import com.alibaba.polardbx.gms.metadb.cache.CachePeerAccessor;
import com.alibaba.polardbx.gms.metadb.cache.CachePeerRecord;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.scheduler.ExecutableScheduledJob;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.FAILED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.QUEUED;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.RUNNING;
import static com.alibaba.polardbx.common.scheduler.FiredScheduledJobState.SUCCESS;
import static com.alibaba.polardbx.gms.module.LogLevel.CRITICAL;
import static com.alibaba.polardbx.gms.module.LogLevel.WARNING;
import static com.alibaba.polardbx.gms.module.LogPattern.STATE_CHANGE_FAIL;
import static com.alibaba.polardbx.gms.module.LogPattern.UNEXPECTED;
import static com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType.CLEAN_CACHE_FILE_MAPPING;
import static com.alibaba.polardbx.gms.topology.SystemDbHelper.DEFAULT_DB_NAME;

/**
 * Maintenance-window scheduled job that scans cache_file_mapping table
 * and removes orphan records whose file_name no longer exists in reference tables.
 * <p>
 * Two scan passes:
 * <ol>
 *   <li>LEFT JOIN files table for .orc/.csv/.del/.sst suffixes</li>
 *   <li>LEFT JOIN columnar_appended_files table for .PkIdx.log suffix</li>
 * </ol>
 * Uses cursor-based pagination with LIMIT to efficiently find orphans in batches.
 */
public class CleanCacheFileMappingScheduledJob extends SchedulerExecutor {

    private static final Logger logger = LoggerFactory.getLogger(CleanCacheFileMappingScheduledJob.class);

    private static final long ONE_DAY_MS = 24L * 60 * 60 * 1000;
    private static final AtomicLong LAST_CACHE_STATUS_LOG_TIME = new AtomicLong(0);

    /**
     * Suffixes checked against the files table.
     */
    private static final String[] FILES_SUFFIXES = {".orc", ".csv", ".del", ".sst"};

    /**
     * Suffixes checked against the columnar_appended_files table.
     */
    private static final String[] COLUMNAR_APPENDED_SUFFIXES = {".PkIdx.log"};

    private final ExecutableScheduledJob executableScheduledJob;

    public CleanCacheFileMappingScheduledJob(ExecutableScheduledJob executableScheduledJob) {
        this.executableScheduledJob = executableScheduledJob;
    }

    @Override
    public boolean execute() {
        long scheduleId = executableScheduledJob.getScheduleId();
        long fireTime = executableScheduledJob.getFireTime();
        long startTime = ZonedDateTime.now().toEpochSecond();

        try {
            // CAS: QUEUED -> RUNNING
            boolean casSuccess =
                ScheduledJobsManager.casStateWithStartTime(scheduleId, fireTime, QUEUED, RUNNING, startTime);
            if (!casSuccess) {
                ModuleLogInfo.getInstance()
                    .logRecord(
                        Module.SCHEDULE_JOB,
                        STATE_CHANGE_FAIL,
                        new String[] {CLEAN_CACHE_FILE_MAPPING + "," + fireTime, QUEUED.name(), RUNNING.name()},
                        WARNING);
                return false;
            }

            final Map savedMdcContext = MDC.getCopyOfContextMap();
            String remark;
            try {
                MDC.put(MDC.MDC_KEY_APP, DEFAULT_DB_NAME);
                remark = doCleanup();
            } finally {
                MDC.setContextMap(savedMdcContext);
            }

            // CAS: RUNNING -> SUCCESS
            long finishTime = System.currentTimeMillis() / 1000;
            return ScheduledJobsManager
                .casStateWithFinishTime(scheduleId, fireTime, RUNNING, SUCCESS, finishTime, remark);
        } catch (Throwable t) {
            logger.error("CleanCacheFileMappingScheduledJob error: " + t.getMessage());
            ModuleLogInfo.getInstance()
                .logRecord(
                    Module.SCHEDULE_JOB,
                    UNEXPECTED,
                    new String[] {
                        CLEAN_CACHE_FILE_MAPPING + "," + fireTime,
                        t.getMessage()
                    },
                    CRITICAL,
                    t
                );
            String remark = "clean cache_file_mapping error: " + t.getMessage();
            ScheduledJobsManager.updateState(scheduleId, fireTime, FAILED, remark, t.getMessage());
            return false;
        }
    }

    /**
     * Perform the actual cleanup:
     * 1. Scan .orc/.csv/.del/.sst against files table
     * 2. Scan .PkIdx.log against columnar_appended_files table
     *
     * @return remark string with cleanup statistics
     */
    private String doCleanup() {
        if (!inMaintenanceWindow()) {
            return "SKIP: not in maintenance window";
        }

        // Log cache cluster status once per day before purge (leader only)
        logCacheStatusIfNeeded();

        int batchSize = DynamicConfig.getInstance().getCacheFileMappingCleanBatchSize();
        long sleepMs = DynamicConfig.getInstance().getCacheFileMappingCleanSleepMs();

        try (Connection connection = MetaDbUtil.getConnection()) {
            CacheFileMappingAccessor accessor = new CacheFileMappingAccessor();
            accessor.setConnection(connection);

            // Pass 1: .orc/.csv/.del/.sst vs files table
            long[] stats1 = scanAndClean(accessor, FILES_SUFFIXES, "files",
                accessor::scanWithFileCheck, batchSize, sleepMs);

            // Pass 2: .PkIdx.log vs columnar_appended_files table
            long[] stats2 = scanAndClean(accessor, COLUMNAR_APPENDED_SUFFIXES, "columnar_appended_files",
                accessor::scanWithColumnarAppendedFileCheck, batchSize, sleepMs);

            long totalScanned = stats1[0] + stats2[0];
            long totalOrphans = stats1[1] + stats2[1];
            long totalDeleted = stats1[2] + stats2[2];
            long totalBatches = stats1[3] + stats2[3];

            String remark = "scanned " + totalScanned + " records in " + totalBatches
                + " batches, found " + totalOrphans + " orphans, deleted " + totalDeleted;
            logger.info("CleanCacheFileMapping: finished. " + remark);
            return remark;
        } catch (Exception e) {
            throw new RuntimeException("CleanCacheFileMapping failed", e);
        }
    }

    /**
     * Log cache cluster status once per day.
     * Already guaranteed to run on CN leader only (ScheduledJobsManager checks hasLeadership).
     * Reads status_json from all active peers in cache_peer table and logs one event per peer,
     * since each peer's status_json is large enough to warrant a separate log entry.
     */
    private void logCacheStatusIfNeeded() {
        long now = System.currentTimeMillis();
        long lastLog = LAST_CACHE_STATUS_LOG_TIME.get();
        if (now - lastLog < ONE_DAY_MS) {
            return;
        }

        // CAS to prevent concurrent execution
        if (!LAST_CACHE_STATUS_LOG_TIME.compareAndSet(lastLog, now)) {
            return;
        }

        try (Connection connection = MetaDbUtil.getConnection()) {
            CachePeerAccessor accessor = new CachePeerAccessor();
            accessor.setConnection(connection);

            List<CachePeerRecord> activePeers = accessor.getActivePeers(now);
            if (activePeers.isEmpty()) {
                // Nothing to log this cycle (e.g. transient MetaDB hiccup); release the daily window
                // so the next scheduled run retries instead of skipping a full day.
                LAST_CACHE_STATUS_LOG_TIME.compareAndSet(now, lastLog);
                return;
            }

            for (CachePeerRecord peer : activePeers) {
                EventLogger.log(EventType.CACHE_STATUS, buildPeerStatusJson(peer));
            }
            logger.info("Cache cluster status logged: " + activePeers.size() + " active peers");
        } catch (Exception e) {
            logger.warn("Failed to log cache cluster status", e);
            // Reset so we retry next cycle; CAS to avoid clobbering a concurrent successful write.
            LAST_CACHE_STATUS_LOG_TIME.compareAndSet(now, lastLog);
        }
    }

    /**
     * Serialize a single peer's status as a JSON object so the consumer can parse it
     * without worrying about delimiter collisions inside status_json.
     */
    private static String buildPeerStatusJson(CachePeerRecord peer) {
        JSONObject obj = new JSONObject(true);
        obj.put("peerName", peer.peerName);
        obj.put("host", peer.host);
        obj.put("role", peer.role);
        obj.put("leader", peer.leader);
        Object statusValue = null;
        if (peer.statusJson != null && !peer.statusJson.isEmpty()) {
            try {
                statusValue = JSONObject.parse(peer.statusJson);
            } catch (Exception parseEx) {
                // Fall back to the raw string if status_json is malformed
                statusValue = peer.statusJson;
            }
        }
        obj.put("status", statusValue != null ? statusValue : new JSONObject());
        return obj.toJSONString();
    }

    /**
     * Generic scan-and-clean loop for a given set of suffixes against a reference table.
     *
     * @param accessor the accessor instance with active connection
     * @param suffixes file suffixes to filter
     * @param refName reference table name for logging
     * @param scanFn scan function: (cursor, limit, suffixes) -> batch results
     * @param batchSize records per batch
     * @param sleepMs sleep between batches
     * @return [totalScanned, totalOrphans, totalDeleted, batchCount]
     */
    private long[] scanAndClean(CacheFileMappingAccessor accessor, String[] suffixes, String refName,
                                ScanFunction scanFn, int batchSize, long sleepMs) {
        long totalScanned = 0;
        long totalOrphans = 0;
        long totalDeleted = 0;
        long batchCount = 0;

        logger.info("CleanCacheFileMapping: start scanning vs " + refName
            + ", batchSize=" + batchSize
            + ", sleepMs=" + sleepMs
            + ", suffixes=" + String.join(",", suffixes));

        long cursor = 0;

        while (true) {
            if (!inMaintenanceWindow()) {
                logger.info("CleanCacheFileMapping: exiting maintenance window, refTable=" + refName
                    + ", cursor=" + cursor);
                break;
            }

            batchCount++;
            List<Long[]> batch = scanFn.scan(cursor, batchSize, suffixes);
            if (batch.isEmpty()) {
                break;
            }

            totalScanned += batch.size();

            // Collect orphan ids (ref id is null means not found in reference table)
            List<Long> orphanIds = new ArrayList<>();
            for (Long[] row : batch) {
                if (row[1] == null) {
                    orphanIds.add(row[0]);
                }
            }

            if (!orphanIds.isEmpty()) {
                totalOrphans += orphanIds.size();
                int deleted = accessor.deleteByIds(orphanIds);
                totalDeleted += deleted;
            }

            // Advance cursor to last id in this batch
            cursor = batch.get(batch.size() - 1)[0];

            // If batch is not full, we've reached the end
            if (batch.size() < batchSize) {
                break;
            }

            // Sleep between batches to reduce MetaDB load
            if (sleepMs > 0) {
                try {
                    Thread.sleep(sleepMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    logger.info("CleanCacheFileMapping: interrupted, refTable=" + refName
                        + ", cursor=" + cursor);
                    break;
                }
            }
        }

        logger.info("CleanCacheFileMapping: pass [" + refName + "] done, scanned="
            + totalScanned + ", orphans=" + totalOrphans + ", deleted=" + totalDeleted);
        return new long[] {totalScanned, totalOrphans, totalDeleted, batchCount};
    }

    @FunctionalInterface
    private interface ScanFunction {
        List<Long[]> scan(long cursor, int limit, String[] suffixes);
    }
}
