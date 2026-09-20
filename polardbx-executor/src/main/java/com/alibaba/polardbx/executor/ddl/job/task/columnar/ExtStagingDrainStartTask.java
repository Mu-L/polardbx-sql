package com.alibaba.polardbx.executor.ddl.job.task.columnar;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.sync.ExtStagingExcludeDnSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.config.schema.DefaultDbSchema;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;

import java.util.List;

/**
 * Drain head — broadcast to every CN that the given DNs are being drained.
 * After this task completes, no CN will allocate new staging seqs on those DNs,
 * and any CN whose active seq is currently on a draining DN will rotate off.
 *
 * @see ExtStagingExcludeDnSyncAction
 */
@TaskName(name = "ExtStagingDrainStartTask")
@Getter
@Setter
public class ExtStagingDrainStartTask extends BaseDdlTask {

    private static final Logger LOG = LoggerFactory.getLogger("EXT_COLUMN");

    private List<String> dnIds;

    @JSONCreator
    public ExtStagingDrainStartTask(String schema, List<String> dnIds) {
        super(schema);
        this.dnIds = dnIds;
        setExceptionAction(DdlExceptionAction.TRY_RECOVERY_THEN_PAUSE);
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        if (dnIds == null || dnIds.isEmpty()) {
            return;
        }
        executeDrainStart();
    }

    /**
     * Core drain-start logic: broadcast exclude-DN to all CNs.
     * Public entry for diagnostic/testing procedures (e.g. CALL polardbx.ext_staging_drain_simulate).
     */
    public void executeDrainStart() {
        if (dnIds == null || dnIds.isEmpty()) {
            return;
        }
        try {
            String dnList = StringUtils.join(dnIds, ",");
            LOG.warn("[EXT_STAGING_DRAIN_START] begin: dns=" + dnList);
            EventLogger.log(EventType.EXT_COL_CREATE,
                "[EXT_STAGING_DRAIN_START] broadcasting exclude-DN to all CNs: dns=" + dnList);

            SyncManagerHelper.syncThrowExceptions(
                new ExtStagingExcludeDnSyncAction(dnIds), DefaultDbSchema.NAME, SyncScope.ALL);

            LOG.warn("[EXT_STAGING_DRAIN_START] done: dns=" + dnList);
            EventLogger.log(EventType.EXT_COL_CREATE,
                "[EXT_STAGING_DRAIN_START] broadcast complete: dns=" + dnList);

            // Post-execution sleep: task logic done, sleep to allow pause for idempotency testing
            long sleepMs = DynamicConfig.getInstance().getExtStagingDrainStartSleepMs();
            if (sleepMs > 0) {
                LOG.warn("[EXT_STAGING_DRAIN_START] post-execution sleep " + sleepMs + "ms (idempotency test)");
                Thread.sleep(sleepMs);
            }
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
            throw GeneralUtil.nestedException("ext-staging drain start interrupted", ie);
        } catch (Exception e) {
            LOG.error("Failed to " + this.getDescription(), e);
            throw GeneralUtil.nestedException(
                String.format("Failed to mark staging-draining for DNs(%s)", StringUtils.join(dnIds, ",")), e);
        }
    }

    @Override
    public String getDescription() {
        return "ext-staging drain start: dns=" + StringUtils.join(dnIds, ",");
    }
}
