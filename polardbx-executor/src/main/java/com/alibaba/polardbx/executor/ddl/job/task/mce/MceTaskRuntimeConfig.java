package com.alibaba.polardbx.executor.ddl.job.task.mce;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.MceDynamicConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;

/**
 * Resolves task-local MCE settings against later instance-level changes.
 *
 * <p>CMD_EXTRA/session values remain the start values. Each field switches to the global value
 * only after that field's version changes after this task captured its baseline.</p>
 */
final class MceTaskRuntimeConfig {

    private final Logger logger;
    private final String taskLabel;
    private final MceDynamicConfig.Snapshot baseline;

    private final int baseBackfillBatchRows;
    private final int baseBackfillUpdateBatchRows;
    private final long baseBackfillBatchBytes;
    private final int baseBackfillParallelism;
    private final long baseMaxInflightBytes;
    private final int baseInternalizeBatchRows;
    private final long baseInternalizeBatchBytes;
    private final int baseCheckerBatchRows;
    private final int baseCheckerParallelism;
    private final int basePhysicalDdlParallelism;

    private volatile ExternalizeSettings lastExternalizeSettings;
    private volatile InternalizeSettings lastInternalizeSettings;
    private volatile CheckerSettings lastCheckerSettings;
    private volatile PhysicalDdlSettings lastPhysicalDdlSettings;

    MceTaskRuntimeConfig(ExecutionContext ec, Logger logger, long jobId, long taskId, String taskName) {
        this.logger = logger;
        this.taskLabel = taskName + " job=" + jobId + " task=" + taskId;
        this.baseline = MceDynamicConfig.getInstance().snapshot();
        this.baseBackfillBatchRows = ec.getParamManager().getInt(ConnectionParams.MCE_BACKFILL_BATCH_ROWS);
        this.baseBackfillUpdateBatchRows =
            ec.getParamManager().getInt(ConnectionParams.MCE_BACKFILL_UPDATE_BATCH_ROWS);
        this.baseBackfillBatchBytes = ec.getParamManager().getLong(ConnectionParams.MCE_BACKFILL_BATCH_BYTES);
        this.baseBackfillParallelism = ec.getParamManager().getInt(ConnectionParams.MCE_BACKFILL_PARALLELISM);
        this.baseMaxInflightBytes =
            ec.getParamManager().getLong(ConnectionParams.MCE_BACKFILL_MAX_INFLIGHT_BYTES);
        this.baseInternalizeBatchRows =
            ec.getParamManager().getInt(ConnectionParams.MCE_INTERNALIZE_BACKFILL_BATCH_ROWS);
        this.baseInternalizeBatchBytes =
            ec.getParamManager().getLong(ConnectionParams.MCE_INTERNALIZE_BACKFILL_BATCH_BYTES);
        this.baseCheckerBatchRows = ec.getParamManager().getInt(ConnectionParams.MCE_CHECKER_BATCH_ROWS);
        this.baseCheckerParallelism = ec.getParamManager().getInt(ConnectionParams.MCE_CHECKER_PARALLELISM);
        this.basePhysicalDdlParallelism =
            ec.getParamManager().getInt(ConnectionParams.MCE_PHYSICAL_DDL_PARALLELISM);

        this.lastExternalizeSettings = computeExternalizeSettings(baseline);
        this.lastInternalizeSettings = computeInternalizeSettings(baseline);
        this.lastCheckerSettings = computeCheckerSettings(baseline);
        this.lastPhysicalDdlSettings = computePhysicalDdlSettings(baseline);
    }

    ExternalizeSettings externalizeSettings() {
        ExternalizeSettings current = computeExternalizeSettings(MceDynamicConfig.getInstance().snapshot());
        ExternalizeSettings previous = lastExternalizeSettings;
        if (!current.equals(previous)) {
            synchronized (this) {
                previous = lastExternalizeSettings;
                if (!current.equals(previous)) {
                    lastExternalizeSettings = current;
                    logChange(previous, current);
                }
            }
        }
        return current;
    }

    InternalizeSettings internalizeSettings() {
        InternalizeSettings current = computeInternalizeSettings(MceDynamicConfig.getInstance().snapshot());
        InternalizeSettings previous = lastInternalizeSettings;
        if (!current.equals(previous)) {
            synchronized (this) {
                previous = lastInternalizeSettings;
                if (!current.equals(previous)) {
                    lastInternalizeSettings = current;
                    logChange(previous, current);
                }
            }
        }
        return current;
    }

    CheckerSettings checkerSettings() {
        CheckerSettings current = computeCheckerSettings(MceDynamicConfig.getInstance().snapshot());
        CheckerSettings previous = lastCheckerSettings;
        if (!current.equals(previous)) {
            synchronized (this) {
                previous = lastCheckerSettings;
                if (!current.equals(previous)) {
                    lastCheckerSettings = current;
                    logChange(previous, current);
                }
            }
        }
        return current;
    }

    PhysicalDdlSettings physicalDdlSettings() {
        PhysicalDdlSettings current = computePhysicalDdlSettings(MceDynamicConfig.getInstance().snapshot());
        PhysicalDdlSettings previous = lastPhysicalDdlSettings;
        if (!current.equals(previous)) {
            synchronized (this) {
                previous = lastPhysicalDdlSettings;
                if (!current.equals(previous)) {
                    lastPhysicalDdlSettings = current;
                    logChange(previous, current);
                }
            }
        }
        return current;
    }

    private ExternalizeSettings computeExternalizeSettings(MceDynamicConfig.Snapshot current) {
        return new ExternalizeSettings(
            effective(baseBackfillBatchRows, baseline.getBackfillBatchRowsRevision(),
                current.getBackfillBatchRowsRevision(), current.getBackfillBatchRows()),
            effective(baseBackfillUpdateBatchRows, baseline.getBackfillUpdateBatchRowsRevision(),
                current.getBackfillUpdateBatchRowsRevision(), current.getBackfillUpdateBatchRows()),
            effective(baseBackfillBatchBytes, baseline.getBackfillBatchBytesRevision(),
                current.getBackfillBatchBytesRevision(), current.getBackfillBatchBytes()),
            effective(baseBackfillParallelism, baseline.getBackfillParallelismRevision(),
                current.getBackfillParallelismRevision(), current.getBackfillParallelism()),
            effective(baseMaxInflightBytes, baseline.getMaxInflightBytesRevision(),
                current.getMaxInflightBytesRevision(), current.getMaxInflightBytes()));
    }

    private InternalizeSettings computeInternalizeSettings(MceDynamicConfig.Snapshot current) {
        return new InternalizeSettings(
            effective(baseInternalizeBatchRows, baseline.getInternalizeBatchRowsRevision(),
                current.getInternalizeBatchRowsRevision(), current.getInternalizeBatchRows()),
            effective(baseInternalizeBatchBytes, baseline.getInternalizeBatchBytesRevision(),
                current.getInternalizeBatchBytesRevision(), current.getInternalizeBatchBytes()),
            effective(baseBackfillParallelism, baseline.getBackfillParallelismRevision(),
                current.getBackfillParallelismRevision(), current.getBackfillParallelism()));
    }

    private CheckerSettings computeCheckerSettings(MceDynamicConfig.Snapshot current) {
        return new CheckerSettings(
            effective(baseCheckerBatchRows, baseline.getCheckerBatchRowsRevision(),
                current.getCheckerBatchRowsRevision(), current.getCheckerBatchRows()),
            effective(baseCheckerParallelism, baseline.getCheckerParallelismRevision(),
                current.getCheckerParallelismRevision(), current.getCheckerParallelism()));
    }

    private PhysicalDdlSettings computePhysicalDdlSettings(MceDynamicConfig.Snapshot current) {
        return new PhysicalDdlSettings(
            effective(basePhysicalDdlParallelism, baseline.getPhysicalDdlParallelismRevision(),
                current.getPhysicalDdlParallelismRevision(), current.getPhysicalDdlParallelism()));
    }

    private static int effective(int baseValue, long baselineRevision, long currentRevision, int currentValue) {
        return currentRevision == baselineRevision ? baseValue : currentValue;
    }

    private static long effective(long baseValue, long baselineRevision, long currentRevision, long currentValue) {
        return currentRevision == baselineRevision ? baseValue : currentValue;
    }

    private void logChange(Object previous, Object current) {
        logger.info(String.format("[MCE] runtime config changed: %s, old={%s}, new={%s}",
            taskLabel, previous, current));
    }

    static final class ExternalizeSettings {
        final int batchRows;
        final int updateBatchRows;
        final long batchBytes;
        final int parallelism;
        final long maxInflightBytes;

        private ExternalizeSettings(int batchRows, int updateBatchRows, long batchBytes,
                                    int parallelism, long maxInflightBytes) {
            this.batchRows = batchRows;
            this.updateBatchRows = updateBatchRows;
            this.batchBytes = batchBytes;
            this.parallelism = parallelism;
            this.maxInflightBytes = maxInflightBytes;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof ExternalizeSettings)) {
                return false;
            }
            ExternalizeSettings that = (ExternalizeSettings) other;
            return batchRows == that.batchRows && updateBatchRows == that.updateBatchRows
                && batchBytes == that.batchBytes && parallelism == that.parallelism
                && maxInflightBytes == that.maxInflightBytes;
        }

        @Override
        public int hashCode() {
            int result = batchRows;
            result = 31 * result + updateBatchRows;
            result = 31 * result + Long.hashCode(batchBytes);
            result = 31 * result + parallelism;
            result = 31 * result + Long.hashCode(maxInflightBytes);
            return result;
        }

        @Override
        public String toString() {
            return "batchRows=" + batchRows + ", updateBatchRows=" + updateBatchRows
                + ", batchBytes=" + batchBytes + ", parallelism=" + parallelism
                + ", maxInflightBytes=" + maxInflightBytes;
        }
    }

    static final class InternalizeSettings {
        final int batchRows;
        final long batchBytes;
        final int parallelism;

        private InternalizeSettings(int batchRows, long batchBytes, int parallelism) {
            this.batchRows = batchRows;
            this.batchBytes = batchBytes;
            this.parallelism = parallelism;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof InternalizeSettings)) {
                return false;
            }
            InternalizeSettings that = (InternalizeSettings) other;
            return batchRows == that.batchRows && batchBytes == that.batchBytes
                && parallelism == that.parallelism;
        }

        @Override
        public int hashCode() {
            int result = batchRows;
            result = 31 * result + Long.hashCode(batchBytes);
            result = 31 * result + parallelism;
            return result;
        }

        @Override
        public String toString() {
            return "batchRows=" + batchRows + ", batchBytes=" + batchBytes + ", parallelism=" + parallelism;
        }
    }

    static final class CheckerSettings {
        final int batchRows;
        final int parallelism;

        private CheckerSettings(int batchRows, int parallelism) {
            this.batchRows = batchRows;
            this.parallelism = parallelism;
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof CheckerSettings)) {
                return false;
            }
            CheckerSettings that = (CheckerSettings) other;
            return batchRows == that.batchRows && parallelism == that.parallelism;
        }

        @Override
        public int hashCode() {
            return 31 * batchRows + parallelism;
        }

        @Override
        public String toString() {
            return "batchRows=" + batchRows + ", parallelism=" + parallelism;
        }
    }

    static final class PhysicalDdlSettings {
        final int parallelism;

        private PhysicalDdlSettings(int parallelism) {
            this.parallelism = parallelism;
        }

        @Override
        public boolean equals(Object other) {
            return other instanceof PhysicalDdlSettings
                && parallelism == ((PhysicalDdlSettings) other).parallelism;
        }

        @Override
        public int hashCode() {
            return parallelism;
        }

        @Override
        public String toString() {
            return "physicalDdlParallelism=" + parallelism;
        }
    }
}
