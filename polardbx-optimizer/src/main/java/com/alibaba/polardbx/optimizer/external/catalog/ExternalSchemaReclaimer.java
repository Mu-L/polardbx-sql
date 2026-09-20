package com.alibaba.polardbx.optimizer.external.catalog;

import com.alibaba.polardbx.common.model.lifecycle.AbstractLifecycle;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ExternalSchemaManager;

import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Periodically releases the connector metadata and cached table metas of external
 * catalog schemas nobody queries any more.
 * <p>
 * Started once during cluster init, next to the rest of the external catalog subsystem.
 * The sweep walks whatever external schemas are in memory at the time, so an instance
 * that never touches an external catalog just finds nothing to do.
 */
public class ExternalSchemaReclaimer extends AbstractLifecycle {

    private static final Logger LOGGER = LoggerFactory.getLogger(ExternalSchemaReclaimer.class);

    private static final long SWEEP_PERIOD_SECONDS = 60;

    private static final ExternalSchemaReclaimer INSTANCE = new ExternalSchemaReclaimer();

    private ScheduledThreadPoolExecutor sweeper;

    public static ExternalSchemaReclaimer getInstance() {
        return INSTANCE;
    }

    @Override
    protected void doInit() {
        sweeper = ExecutorUtil.createScheduler(1,
            new NamedThreadFactory("External-Schema-Reclaim-Thread", true),
            new ThreadPoolExecutor.DiscardPolicy());
        sweeper.scheduleWithFixedDelay(this::sweepOnce,
            SWEEP_PERIOD_SECONDS, SWEEP_PERIOD_SECONDS, TimeUnit.SECONDS);
        LOGGER.info("External schema reclaimer started, sweeping every "
            + SWEEP_PERIOD_SECONDS + "s");
    }

    /**
     * One sweep. The TTL is re-read here rather than captured at start-up so a
     * {@code set global} takes effect on the next tick, zero included as a kill switch.
     * <p>
     * Nothing may escape: an uncaught throwable would silently cancel the schedule and
     * leave the leak unattended for the rest of the process lifetime.
     */
    void sweepOnce() {
        try {
            int idleTtlMinutes = InstConfUtil.getInt(
                ConnectionParams.EXTERNAL_CATALOG_METADATA_IDLE_TTL_MINUTES);
            if (idleTtlMinutes <= 0) {
                return;
            }
            int reclaimed = reclaimIdle(TimeUnit.MINUTES.toSeconds(idleTtlMinutes));
            if (reclaimed > 0) {
                LOGGER.info("Released connector metadata of " + reclaimed
                    + " idle external schema(s)");
            }
        } catch (Throwable t) {
            LOGGER.warn("External schema idle reclaim sweep failed", t);
        }
    }

    int reclaimIdle(long idleTtlSeconds) {
        int reclaimed = 0;
        for (ExternalSchemaManager esm : OptimizerContext.getExternalSchemaManagers()) {
            try {
                reclaimed += esm.detachIfIdle(idleTtlSeconds);
            } catch (RuntimeException | Error t) {
                // One misbehaving connector must not stop the rest of the sweep.
                LOGGER.warn("Failed to reclaim idle external schema '"
                    + esm.getSchemaName() + "'", t);
            }
        }
        return reclaimed;
    }

}
