package com.alibaba.polardbx.executor.ddl.job.task.mce;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.executor.backfill.Throttle;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.util.concurrent.RateLimiter;

/**
 * Row-rate governor shared by all physical-table pipelines of one MCE backfill task, mirroring
 * the Extractor/OMC backfill pattern: a shared RateLimiter caps the SELECT admission rate, the
 * adaptive {@link Throttle} retargets it from per-batch feedback, and the instance-level
 * GENERAL_DYNAMIC_SPEED_LIMITATION lowers the ceiling at runtime. A non-positive
 * MCE_BACKFILL_SPEED_LIMITATION disables throttling entirely.
 */
final class MceBackfillThrottle {

    private final RateLimiter rateLimiter;
    private final Throttle throttle;

    MceBackfillThrottle(ExecutionContext ec, String schemaName, long batchRows, Long backfillId) {
        long speedLimit = ec.getParamManager().getLong(ConnectionParams.MCE_BACKFILL_SPEED_LIMITATION);
        long speedMin = ec.getParamManager().getLong(ConnectionParams.MCE_BACKFILL_SPEED_MIN);
        if (speedLimit <= 0) {
            this.rateLimiter = null;
            this.throttle = null;
            return;
        }
        this.rateLimiter = RateLimiter.create(speedLimit);
        this.throttle = new Throttle(speedMin, speedLimit, schemaName, batchRows);
        this.throttle.setBackFillId(backfillId);
    }

    /**
     * Blocks until the requested batch may start; also applies the dynamic global ceiling like
     * Extractor does before each batch SELECT.
     */
    void acquire(int batchRows) {
        if (rateLimiter == null) {
            return;
        }
        rateLimiter.acquire(batchRows);
        long dynamicRate = DynamicConfig.getInstance().getGeneralDynamicSpeedLimitation();
        if (dynamicRate > 0) {
            throttle.resetMaxRate(dynamicRate);
        }
    }

    /**
     * Reports one finished batch and lets the adaptive throttle retarget the shared limiter.
     */
    void feedback(long startMillis, long rows) {
        if (throttle == null) {
            return;
        }
        throttle.feedback(new Throttle.FeedbackStats(System.currentTimeMillis() - startMillis, startMillis, rows));
        rateLimiter.setRate(throttle.getNewRate());
    }

    /**
     * Stops the adaptive throttle cycle thread. Must be called when the owning task finishes.
     */
    void stop() {
        if (throttle != null) {
            throttle.stop();
        }
    }
}
