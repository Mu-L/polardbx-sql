package com.alibaba.polardbx.executor.ddl.job.task.columnar;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.columnar.StagingTableManager;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.sync.ExtStagingEvictCacheSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.metadb.table.ExtStagingMetaAccessor;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.config.schema.DefaultDbSchema;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.util.List;

/**
 * Drain tail — block until every staging seq on the given DNs is gone, then
 * sweep up any stragglers whose meta rows have already been deleted.
 *
 * <p>Loop: poll {@code ext_staging_meta} for {@code dn_id IN (?)}
 * AND {@code status IN ('CREATING','ACTIVE','SEALED','FLUSHING','FAILED')}; finish when count == 0.
 *
 * <p>Auto-cleanup: after meta rows are zero, scan each DN's
 * {@code __polarx_ext_staging} for residual physical tables and DROP only the
 * ones whose meta row is also gone (double check via MetaDB). Tables whose
 * meta rows still exist are left alone — they may belong to another CN's
 * just-allocated active seq.
 */
@TaskName(name = "ExtStagingDrainWaitTask")
@Getter
@Setter
public class ExtStagingDrainWaitTask extends BaseDdlTask {

    private static final Logger LOG = LoggerFactory.getLogger("EXT_COLUMN");

    private List<String> dnIds;

    @JSONCreator
    public ExtStagingDrainWaitTask(String schema, List<String> dnIds) {
        super(schema);
        this.dnIds = dnIds;
        setExceptionAction(DdlExceptionAction.TRY_RECOVERY_THEN_PAUSE);
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        if (dnIds == null || dnIds.isEmpty()) {
            return;
        }
        executeDrainWait();
    }

    /**
     * Core drain-wait logic: poll until all seqs are gone, sweep residuals, evict cache.
     * Public entry for diagnostic/testing procedures (e.g. CALL polardbx.ext_staging_drain_simulate).
     */
    public void executeDrainWait() {
        if (dnIds == null || dnIds.isEmpty()) {
            return;
        }
        String dnList = StringUtils.join(dnIds, ",");
        long timeoutMs = DynamicConfig.getInstance().getExtStagingDrainWaitTimeoutMs();
        long pollMs = Math.max(1000L, DynamicConfig.getInstance().getExtStagingDrainWaitPollIntervalMs());
        long startTime = System.currentTimeMillis();
        long deadline = startTime + timeoutMs;

        LOG.warn("[EXT_STAGING_DRAIN_WAIT] begin: dns=" + dnList
            + ", timeoutMs=" + timeoutMs + ", pollMs=" + pollMs);
        EventLogger.log(EventType.EXT_COL_CREATE,
            "[EXT_STAGING_DRAIN_WAIT] begin: dns=" + dnList);

        try {
            // Phase 1: wait for meta rows to drain.
            int pollCount = 0;
            while (true) {
                long alive = countAlive();
                pollCount++;
                if (alive == 0L) {
                    LOG.warn("[EXT_STAGING_DRAIN_WAIT] Phase1 done: countAlive=0 after "
                        + pollCount + " polls, elapsed=" + (System.currentTimeMillis() - startTime) + "ms");
                    break;
                }
                if (System.currentTimeMillis() > deadline) {
                    throw GeneralUtil.nestedException(String.format(
                        "ext-staging drain wait timeout: dns=%s, alive=%d, timeoutMs=%d",
                        dnList, alive, timeoutMs));
                }
                LOG.warn("[EXT_STAGING_DRAIN_WAIT] polling: dns=" + dnList
                    + ", alive=" + alive + ", poll#" + pollCount + " (retry in " + pollMs + "ms)");
                Thread.sleep(pollMs);
            }

            // Phase 2: physical sweep — drop residuals whose meta is gone.
            LOG.warn("[EXT_STAGING_DRAIN_WAIT] Phase2 sweep: dns=" + dnList);
            for (String dnId : dnIds) {
                sweepResidualTables(dnId);
            }

            // Phase 3: broadcast cache eviction to all CNs — clears stale
            // seqDnCache entries and refreshes watermark so reads fall through to OSS.
            LOG.warn("[EXT_STAGING_DRAIN_WAIT] Phase3 cache eviction broadcast: dns=" + dnList);
            SyncManagerHelper.syncThrowExceptions(
                new ExtStagingEvictCacheSyncAction(dnIds), DefaultDbSchema.NAME, SyncScope.ALL);

            long totalElapsed = System.currentTimeMillis() - startTime;
            LOG.warn("[EXT_STAGING_DRAIN_WAIT] done: dns=" + dnList
                + ", totalElapsed=" + totalElapsed + "ms, polls=" + pollCount);
            EventLogger.log(EventType.EXT_COL_CREATE,
                "[EXT_STAGING_DRAIN_WAIT] done: dns=" + dnList + ", elapsed=" + totalElapsed + "ms");

            // Post-execution sleep: all phases done, sleep to allow pause for idempotency testing
            long sleepMs = DynamicConfig.getInstance().getExtStagingDrainWaitSleepMs();
            if (sleepMs > 0) {
                LOG.warn("[EXT_STAGING_DRAIN_WAIT] post-execution sleep " + sleepMs + "ms (idempotency test)");
                Thread.sleep(sleepMs);
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw GeneralUtil.nestedException("ext-staging drain wait interrupted", e);
        } catch (Exception e) {
            LOG.error("[EXT_STAGING_DRAIN_WAIT] FAILED: dns=" + dnList, e);
            throw GeneralUtil.nestedException(e);
        }
    }

    private long countAlive() {
        try (Connection conn = MetaDbUtil.getConnection()) {
            ExtStagingMetaAccessor accessor = new ExtStagingMetaAccessor();
            accessor.setConnection(conn);
            return accessor.countAliveOnDns(dnIds);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Drop residual {@code polarx_ext_staging_*} tables on the given DN whose
     * meta rows have been deleted. Delegates to the shared utility in
     * {@link StagingTableManager#dropOrphanTablesOnDn}.
     */
    private void sweepResidualTables(String dnId) {
        // excludeSeqId=0: in drain context, the active seq has already rotated off
        // the draining DN, so no exclusion needed.
        StagingTableManager.dropOrphanTablesOnDnOrThrow(dnId, 0);
    }

    @Override
    public String getDescription() {
        return "ext-staging drain wait: dns=" + StringUtils.join(dnIds, ",");
    }
}
