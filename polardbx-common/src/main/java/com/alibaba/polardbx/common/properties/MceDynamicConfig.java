package com.alibaba.polardbx.common.properties;

import java.util.Locale;

/**
 * Versioned instance-level MCE settings loaded through the normal inst_config listener.
 *
 * <p>A task captures one baseline snapshot when it starts. A field keeps the task-local
 * (including CMD_EXTRA) value until that field's revision changes; a later SET GLOBAL then
 * overrides the running task at its next safe batch boundary.</p>
 */
public final class MceDynamicConfig {

    private static final MceDynamicConfig INSTANCE = new MceDynamicConfig();

    private volatile Snapshot snapshot = Snapshot.defaults();
    private long nextRevision;

    private MceDynamicConfig() {
    }

    public static MceDynamicConfig getInstance() {
        return INSTANCE;
    }

    public Snapshot snapshot() {
        return snapshot;
    }

    public static boolean isSupportedKey(String key) {
        if (key == null) {
            return false;
        }
        switch (key.toUpperCase(Locale.ROOT)) {
        case ConnectionProperties.MCE_BACKFILL_BATCH_ROWS:
        case ConnectionProperties.MCE_BACKFILL_UPDATE_BATCH_ROWS:
        case ConnectionProperties.MCE_CHECKER_BATCH_ROWS:
        case ConnectionProperties.MCE_CHECKER_PARALLELISM:
        case ConnectionProperties.MCE_BACKFILL_BATCH_BYTES:
        case ConnectionProperties.MCE_INTERNALIZE_BACKFILL_BATCH_ROWS:
        case ConnectionProperties.MCE_INTERNALIZE_BACKFILL_BATCH_BYTES:
        case ConnectionProperties.MCE_PHYSICAL_DDL_PARALLELISM:
        case ConnectionProperties.MCE_BACKFILL_PARALLELISM:
        case ConnectionProperties.MCE_BACKFILL_MAX_INFLIGHT_BYTES:
            return true;
        default:
            return false;
        }
    }

    public static void validateValue(String key, String value) {
        ConfigParam param = ConnectionParams.SUPPORTED_PARAMS.get(key.toUpperCase(Locale.ROOT));
        if (param == null || !isSupportedKey(key)) {
            throw new IllegalArgumentException("unsupported dynamic MCE parameter: " + key);
        }
        param.validateValue(value);
    }

    public synchronized void loadValue(String key, String value) {
        validateValue(key, value);
        String normalized = key.toUpperCase(Locale.ROOT);
        Snapshot current = snapshot;
        switch (normalized) {
        case ConnectionProperties.MCE_BACKFILL_BATCH_ROWS:
            snapshot = current.withBackfillBatchRows(Integer.parseInt(value), nextRevision(current,
                current.backfillBatchRows != Integer.parseInt(value)));
            break;
        case ConnectionProperties.MCE_BACKFILL_UPDATE_BATCH_ROWS:
            snapshot = current.withBackfillUpdateBatchRows(Integer.parseInt(value), nextRevision(current,
                current.backfillUpdateBatchRows != Integer.parseInt(value)));
            break;
        case ConnectionProperties.MCE_CHECKER_BATCH_ROWS:
            snapshot = current.withCheckerBatchRows(Integer.parseInt(value), nextRevision(current,
                current.checkerBatchRows != Integer.parseInt(value)));
            break;
        case ConnectionProperties.MCE_CHECKER_PARALLELISM:
            snapshot = current.withCheckerParallelism(Integer.parseInt(value), nextRevision(current,
                current.checkerParallelism != Integer.parseInt(value)));
            break;
        case ConnectionProperties.MCE_BACKFILL_BATCH_BYTES:
            snapshot = current.withBackfillBatchBytes(Long.parseLong(value), nextRevision(current,
                current.backfillBatchBytes != Long.parseLong(value)));
            break;
        case ConnectionProperties.MCE_INTERNALIZE_BACKFILL_BATCH_ROWS:
            snapshot = current.withInternalizeBatchRows(Integer.parseInt(value), nextRevision(current,
                current.internalizeBatchRows != Integer.parseInt(value)));
            break;
        case ConnectionProperties.MCE_INTERNALIZE_BACKFILL_BATCH_BYTES:
            snapshot = current.withInternalizeBatchBytes(Long.parseLong(value), nextRevision(current,
                current.internalizeBatchBytes != Long.parseLong(value)));
            break;
        case ConnectionProperties.MCE_PHYSICAL_DDL_PARALLELISM:
            snapshot = current.withPhysicalDdlParallelism(Integer.parseInt(value), nextRevision(current,
                current.physicalDdlParallelism != Integer.parseInt(value)));
            break;
        case ConnectionProperties.MCE_BACKFILL_PARALLELISM:
            snapshot = current.withBackfillParallelism(Integer.parseInt(value), nextRevision(current,
                current.backfillParallelism != Integer.parseInt(value)));
            break;
        case ConnectionProperties.MCE_BACKFILL_MAX_INFLIGHT_BYTES:
            snapshot = current.withMaxInflightBytes(Long.parseLong(value), nextRevision(current,
                current.maxInflightBytes != Long.parseLong(value)));
            break;
        default:
            throw new IllegalArgumentException("unsupported dynamic MCE parameter: " + key);
        }
    }

    private long nextRevision(Snapshot current, boolean changed) {
        return changed ? ++nextRevision : current.revision;
    }

    public static final class Snapshot {
        private final long revision;
        private final int backfillBatchRows;
        private final long backfillBatchRowsRevision;
        private final int backfillUpdateBatchRows;
        private final long backfillUpdateBatchRowsRevision;
        private final int checkerBatchRows;
        private final long checkerBatchRowsRevision;
        private final int checkerParallelism;
        private final long checkerParallelismRevision;
        private final long backfillBatchBytes;
        private final long backfillBatchBytesRevision;
        private final int internalizeBatchRows;
        private final long internalizeBatchRowsRevision;
        private final long internalizeBatchBytes;
        private final long internalizeBatchBytesRevision;
        private final int physicalDdlParallelism;
        private final long physicalDdlParallelismRevision;
        private final int backfillParallelism;
        private final long backfillParallelismRevision;
        private final long maxInflightBytes;
        private final long maxInflightBytesRevision;

        private Snapshot(long revision,
                         int backfillBatchRows, long backfillBatchRowsRevision,
                         int backfillUpdateBatchRows, long backfillUpdateBatchRowsRevision,
                         int checkerBatchRows, long checkerBatchRowsRevision,
                         int checkerParallelism, long checkerParallelismRevision,
                         long backfillBatchBytes, long backfillBatchBytesRevision,
                         int internalizeBatchRows, long internalizeBatchRowsRevision,
                         long internalizeBatchBytes, long internalizeBatchBytesRevision,
                         int physicalDdlParallelism, long physicalDdlParallelismRevision,
                         int backfillParallelism, long backfillParallelismRevision,
                         long maxInflightBytes, long maxInflightBytesRevision) {
            this.revision = revision;
            this.backfillBatchRows = backfillBatchRows;
            this.backfillBatchRowsRevision = backfillBatchRowsRevision;
            this.backfillUpdateBatchRows = backfillUpdateBatchRows;
            this.backfillUpdateBatchRowsRevision = backfillUpdateBatchRowsRevision;
            this.checkerBatchRows = checkerBatchRows;
            this.checkerBatchRowsRevision = checkerBatchRowsRevision;
            this.checkerParallelism = checkerParallelism;
            this.checkerParallelismRevision = checkerParallelismRevision;
            this.backfillBatchBytes = backfillBatchBytes;
            this.backfillBatchBytesRevision = backfillBatchBytesRevision;
            this.internalizeBatchRows = internalizeBatchRows;
            this.internalizeBatchRowsRevision = internalizeBatchRowsRevision;
            this.internalizeBatchBytes = internalizeBatchBytes;
            this.internalizeBatchBytesRevision = internalizeBatchBytesRevision;
            this.physicalDdlParallelism = physicalDdlParallelism;
            this.physicalDdlParallelismRevision = physicalDdlParallelismRevision;
            this.backfillParallelism = backfillParallelism;
            this.backfillParallelismRevision = backfillParallelismRevision;
            this.maxInflightBytes = maxInflightBytes;
            this.maxInflightBytesRevision = maxInflightBytesRevision;
        }

        private static Snapshot defaults() {
            return new Snapshot(0,
                Integer.parseInt(ConnectionParams.MCE_BACKFILL_BATCH_ROWS.getDefault()), 0,
                Integer.parseInt(ConnectionParams.MCE_BACKFILL_UPDATE_BATCH_ROWS.getDefault()), 0,
                Integer.parseInt(ConnectionParams.MCE_CHECKER_BATCH_ROWS.getDefault()), 0,
                Integer.parseInt(ConnectionParams.MCE_CHECKER_PARALLELISM.getDefault()), 0,
                Long.parseLong(ConnectionParams.MCE_BACKFILL_BATCH_BYTES.getDefault()), 0,
                Integer.parseInt(ConnectionParams.MCE_INTERNALIZE_BACKFILL_BATCH_ROWS.getDefault()), 0,
                Long.parseLong(ConnectionParams.MCE_INTERNALIZE_BACKFILL_BATCH_BYTES.getDefault()), 0,
                Integer.parseInt(ConnectionParams.MCE_PHYSICAL_DDL_PARALLELISM.getDefault()), 0,
                Integer.parseInt(ConnectionParams.MCE_BACKFILL_PARALLELISM.getDefault()), 0,
                Long.parseLong(ConnectionParams.MCE_BACKFILL_MAX_INFLIGHT_BYTES.getDefault()), 0);
        }

        private Snapshot withBackfillBatchRows(int value, long newRevision) {
            if (value == backfillBatchRows) {
                return this;
            }
            return copy(newRevision, value, newRevision, backfillUpdateBatchRows, backfillUpdateBatchRowsRevision,
                checkerBatchRows, checkerBatchRowsRevision, checkerParallelism, checkerParallelismRevision,
                backfillBatchBytes, backfillBatchBytesRevision, internalizeBatchRows, internalizeBatchRowsRevision,
                internalizeBatchBytes, internalizeBatchBytesRevision, physicalDdlParallelism,
                physicalDdlParallelismRevision, backfillParallelism, backfillParallelismRevision,
                maxInflightBytes, maxInflightBytesRevision);
        }

        private Snapshot withBackfillUpdateBatchRows(int value, long newRevision) {
            if (value == backfillUpdateBatchRows) {
                return this;
            }
            return copy(newRevision, backfillBatchRows, backfillBatchRowsRevision, value, newRevision,
                checkerBatchRows, checkerBatchRowsRevision, checkerParallelism, checkerParallelismRevision,
                backfillBatchBytes, backfillBatchBytesRevision, internalizeBatchRows, internalizeBatchRowsRevision,
                internalizeBatchBytes, internalizeBatchBytesRevision, physicalDdlParallelism,
                physicalDdlParallelismRevision, backfillParallelism, backfillParallelismRevision,
                maxInflightBytes, maxInflightBytesRevision);
        }

        private Snapshot withCheckerBatchRows(int value, long newRevision) {
            if (value == checkerBatchRows) {
                return this;
            }
            return copy(newRevision, backfillBatchRows, backfillBatchRowsRevision, backfillUpdateBatchRows,
                backfillUpdateBatchRowsRevision, value, newRevision, checkerParallelism, checkerParallelismRevision,
                backfillBatchBytes, backfillBatchBytesRevision, internalizeBatchRows, internalizeBatchRowsRevision,
                internalizeBatchBytes, internalizeBatchBytesRevision, physicalDdlParallelism,
                physicalDdlParallelismRevision, backfillParallelism, backfillParallelismRevision,
                maxInflightBytes, maxInflightBytesRevision);
        }

        private Snapshot withCheckerParallelism(int value, long newRevision) {
            if (value == checkerParallelism) {
                return this;
            }
            return copy(newRevision, backfillBatchRows, backfillBatchRowsRevision, backfillUpdateBatchRows,
                backfillUpdateBatchRowsRevision, checkerBatchRows, checkerBatchRowsRevision, value, newRevision,
                backfillBatchBytes, backfillBatchBytesRevision, internalizeBatchRows, internalizeBatchRowsRevision,
                internalizeBatchBytes, internalizeBatchBytesRevision, physicalDdlParallelism,
                physicalDdlParallelismRevision, backfillParallelism, backfillParallelismRevision,
                maxInflightBytes, maxInflightBytesRevision);
        }

        private Snapshot withBackfillBatchBytes(long value, long newRevision) {
            if (value == backfillBatchBytes) {
                return this;
            }
            return copy(newRevision, backfillBatchRows, backfillBatchRowsRevision, backfillUpdateBatchRows,
                backfillUpdateBatchRowsRevision, checkerBatchRows, checkerBatchRowsRevision, checkerParallelism,
                checkerParallelismRevision, value, newRevision, internalizeBatchRows, internalizeBatchRowsRevision,
                internalizeBatchBytes, internalizeBatchBytesRevision, physicalDdlParallelism,
                physicalDdlParallelismRevision, backfillParallelism, backfillParallelismRevision,
                maxInflightBytes, maxInflightBytesRevision);
        }

        private Snapshot withInternalizeBatchRows(int value, long newRevision) {
            if (value == internalizeBatchRows) {
                return this;
            }
            return copy(newRevision, backfillBatchRows, backfillBatchRowsRevision, backfillUpdateBatchRows,
                backfillUpdateBatchRowsRevision, checkerBatchRows, checkerBatchRowsRevision, checkerParallelism,
                checkerParallelismRevision, backfillBatchBytes, backfillBatchBytesRevision, value, newRevision,
                internalizeBatchBytes, internalizeBatchBytesRevision, physicalDdlParallelism,
                physicalDdlParallelismRevision, backfillParallelism, backfillParallelismRevision,
                maxInflightBytes, maxInflightBytesRevision);
        }

        private Snapshot withInternalizeBatchBytes(long value, long newRevision) {
            if (value == internalizeBatchBytes) {
                return this;
            }
            return copy(newRevision, backfillBatchRows, backfillBatchRowsRevision, backfillUpdateBatchRows,
                backfillUpdateBatchRowsRevision, checkerBatchRows, checkerBatchRowsRevision, checkerParallelism,
                checkerParallelismRevision, backfillBatchBytes, backfillBatchBytesRevision, internalizeBatchRows,
                internalizeBatchRowsRevision, value, newRevision, physicalDdlParallelism,
                physicalDdlParallelismRevision, backfillParallelism, backfillParallelismRevision,
                maxInflightBytes, maxInflightBytesRevision);
        }

        private Snapshot withPhysicalDdlParallelism(int value, long newRevision) {
            if (value == physicalDdlParallelism) {
                return this;
            }
            return copy(newRevision, backfillBatchRows, backfillBatchRowsRevision, backfillUpdateBatchRows,
                backfillUpdateBatchRowsRevision, checkerBatchRows, checkerBatchRowsRevision, checkerParallelism,
                checkerParallelismRevision, backfillBatchBytes, backfillBatchBytesRevision, internalizeBatchRows,
                internalizeBatchRowsRevision, internalizeBatchBytes, internalizeBatchBytesRevision, value,
                newRevision, backfillParallelism, backfillParallelismRevision, maxInflightBytes,
                maxInflightBytesRevision);
        }

        private Snapshot withBackfillParallelism(int value, long newRevision) {
            if (value == backfillParallelism) {
                return this;
            }
            return copy(newRevision, backfillBatchRows, backfillBatchRowsRevision, backfillUpdateBatchRows,
                backfillUpdateBatchRowsRevision, checkerBatchRows, checkerBatchRowsRevision, checkerParallelism,
                checkerParallelismRevision, backfillBatchBytes, backfillBatchBytesRevision, internalizeBatchRows,
                internalizeBatchRowsRevision, internalizeBatchBytes, internalizeBatchBytesRevision,
                physicalDdlParallelism, physicalDdlParallelismRevision, value, newRevision,
                maxInflightBytes, maxInflightBytesRevision);
        }

        private Snapshot withMaxInflightBytes(long value, long newRevision) {
            if (value == maxInflightBytes) {
                return this;
            }
            return copy(newRevision, backfillBatchRows, backfillBatchRowsRevision, backfillUpdateBatchRows,
                backfillUpdateBatchRowsRevision, checkerBatchRows, checkerBatchRowsRevision, checkerParallelism,
                checkerParallelismRevision, backfillBatchBytes, backfillBatchBytesRevision, internalizeBatchRows,
                internalizeBatchRowsRevision, internalizeBatchBytes, internalizeBatchBytesRevision,
                physicalDdlParallelism, physicalDdlParallelismRevision, backfillParallelism,
                backfillParallelismRevision, value, newRevision);
        }

        private Snapshot copy(long newRevision,
                              int newBackfillBatchRows, long newBackfillBatchRowsRevision,
                              int newBackfillUpdateBatchRows, long newBackfillUpdateBatchRowsRevision,
                              int newCheckerBatchRows, long newCheckerBatchRowsRevision,
                              int newCheckerParallelism, long newCheckerParallelismRevision,
                              long newBackfillBatchBytes, long newBackfillBatchBytesRevision,
                              int newInternalizeBatchRows, long newInternalizeBatchRowsRevision,
                              long newInternalizeBatchBytes, long newInternalizeBatchBytesRevision,
                              int newPhysicalDdlParallelism, long newPhysicalDdlParallelismRevision,
                              int newBackfillParallelism, long newBackfillParallelismRevision,
                              long newMaxInflightBytes, long newMaxInflightBytesRevision) {
            return new Snapshot(newRevision,
                newBackfillBatchRows, newBackfillBatchRowsRevision,
                newBackfillUpdateBatchRows, newBackfillUpdateBatchRowsRevision,
                newCheckerBatchRows, newCheckerBatchRowsRevision,
                newCheckerParallelism, newCheckerParallelismRevision,
                newBackfillBatchBytes, newBackfillBatchBytesRevision,
                newInternalizeBatchRows, newInternalizeBatchRowsRevision,
                newInternalizeBatchBytes, newInternalizeBatchBytesRevision,
                newPhysicalDdlParallelism, newPhysicalDdlParallelismRevision,
                newBackfillParallelism, newBackfillParallelismRevision,
                newMaxInflightBytes, newMaxInflightBytesRevision);
        }

        public long getRevision() {
            return revision;
        }

        public int getBackfillBatchRows() {
            return backfillBatchRows;
        }

        public long getBackfillBatchRowsRevision() {
            return backfillBatchRowsRevision;
        }

        public int getBackfillUpdateBatchRows() {
            return backfillUpdateBatchRows;
        }

        public long getBackfillUpdateBatchRowsRevision() {
            return backfillUpdateBatchRowsRevision;
        }

        public int getCheckerBatchRows() {
            return checkerBatchRows;
        }

        public long getCheckerBatchRowsRevision() {
            return checkerBatchRowsRevision;
        }

        public int getCheckerParallelism() {
            return checkerParallelism;
        }

        public long getCheckerParallelismRevision() {
            return checkerParallelismRevision;
        }

        public long getBackfillBatchBytes() {
            return backfillBatchBytes;
        }

        public long getBackfillBatchBytesRevision() {
            return backfillBatchBytesRevision;
        }

        public int getInternalizeBatchRows() {
            return internalizeBatchRows;
        }

        public long getInternalizeBatchRowsRevision() {
            return internalizeBatchRowsRevision;
        }

        public long getInternalizeBatchBytes() {
            return internalizeBatchBytes;
        }

        public long getInternalizeBatchBytesRevision() {
            return internalizeBatchBytesRevision;
        }

        public int getPhysicalDdlParallelism() {
            return physicalDdlParallelism;
        }

        public long getPhysicalDdlParallelismRevision() {
            return physicalDdlParallelismRevision;
        }

        public int getBackfillParallelism() {
            return backfillParallelism;
        }

        public long getBackfillParallelismRevision() {
            return backfillParallelismRevision;
        }

        public long getMaxInflightBytes() {
            return maxInflightBytes;
        }

        public long getMaxInflightBytesRevision() {
            return maxInflightBytesRevision;
        }
    }
}
