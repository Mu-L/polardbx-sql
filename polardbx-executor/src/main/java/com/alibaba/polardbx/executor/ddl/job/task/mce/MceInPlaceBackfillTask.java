package com.alibaba.polardbx.executor.ddl.job.task.mce;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.oss.blob.BlobPageFormat;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.columnar.BlobWriter;
import com.alibaba.polardbx.executor.columnar.ExternalColumnTableIdResolver;
import com.alibaba.polardbx.executor.columnar.ExternalizedColumnDmlHelper;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineStats;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.omc.DirectXaTransactionExecutor;
import com.alibaba.polardbx.executor.ddl.omc.OmcStorageInfo;
import com.alibaba.polardbx.executor.ddl.omc.OmcUtils;
import com.alibaba.polardbx.gms.metadb.misc.MceColumnStateRecord;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.rpc.pool.XConnection;
import lombok.Getter;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;

/**
 * MCE Task: in-place backfill of the addr column on the source table.
 * <p>
 * Physical partitions run as bounded backfill pipelines through the shared backfill thread pool.
 * All pipelines share a raw-byte admission window from the streaming physical read until the
 * corresponding Page becomes remote durable. Each physical partition loops:
 * <ol>
 *   <li>SELECT the ordered effective-PK tuple and content from rows inside the typed
 *       DNF keyset range whose content_addr_ is still ''.</li>
 *   <li>Upload content directly to remote storage (never staging), using the Page builder and
 *       uploader's global memory admission; obtain BlobRef hex per row.</li>
 *   <li>Split the durable Page's rows into bounded parameterized searched-CASE UPDATEs on one
 *       physical connection. Every UPDATE commits independently as a one-phase, CDC-ignored XA
 *       branch and is guarded by content_addr_ = ''.</li>
 *   <li>Advance the typed tuple checkpoint to the last returned key; repeat until empty.</li>
 * </ol>
 * <p>
 * The addr column has three states: {@code ''} = not yet migrated (the ADD COLUMN
 * default), {@code NULL} = migrated and content was NULL, and a BlobRef hex =
 * migrated with content externalized to OSS. The {@code WHERE content_addr_ = ''}
 * clause acts as a CAS guard: any row a concurrent DML has already dual-written
 * (moving content_addr_ off the '' sentinel to a BlobRef or NULL) will not be
 * overwritten by this stale backfill value.
 * <p>
 * Checkpoints are persisted per physical partition in {@code mce_column_state}. The first run
 * snapshots the actual greatest effective-PK tuple; every successful Page window stores the full
 * typed tuple, so PAUSE/CONTINUE resumes from the last completed keyset range instead of
 * scanning from the beginning.
 * Physical rows are consumed through X Protocol stream mode. Page aggregation may span multiple
 * row-limit SELECT windows; the in-memory scan cursor advances independently while only a remotely
 * durable Page whose CAS micro-batches completed advances the persisted checkpoint.
 */
@Getter
@TaskName(name = "MceInPlaceBackfillTask")
public class MceInPlaceBackfillTask extends BaseDdlTask {

    /**
     * MCE state-transition timeline goes to tddl.log (SLS-collected) instead of the
     * ddl-engine log, so migration troubleshooting survives log collection.
     */
    private static final Logger MCE_LOGGER = LoggerFactory.getLogger("EXT_COLUMN");

    private final String tableName;
    private final String columnName;
    private final String addrColumnName;

    public MceInPlaceBackfillTask(String schemaName, String tableName,
                                  String columnName, String addrColumnName) {
        super(schemaName);
        onExceptionTryRecoveryThenPause();
        this.tableName = tableName;
        this.columnName = columnName;
        this.addrColumnName = addrColumnName;
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        injectFailPointBeforeBackfill(executionContext);
        configureBackfillFailureFailPoints(executionContext);
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);

        // Resolve the effective primary key (including the auto-partition implicit key).
        IndexMeta primaryIndex =
            McePrimaryKeyValidator.requirePrimaryKey(tableMeta, schemaName, tableName);
        long tableId = ExternalColumnTableIdResolver.getInstance().resolve(schemaName, tableName, columnName);
        List<String> pkColumnNames = primaryIndex.getKeyColumns().stream()
            .map(ColumnMeta::getName)
            .collect(Collectors.toList());
        List<String> autoUpdateColumnNames = tableMeta.getAutoUpdateColumns().stream()
            .map(ColumnMeta::getName)
            .collect(Collectors.toList());

        List<McePhysicalTableResolver.PhysicalTableTarget> physicalPartitions =
            McePhysicalTableResolver.resolve(schemaName, tableName, tableMeta, executionContext);
        MCE_LOGGER.info(String.format("[MCE] backfill start: schema=%s, table=%s, column=%s→%s, pk=%s, partitions=%d",
            schemaName, tableName, columnName, addrColumnName, pkColumnNames, physicalPartitions.size()));
        MCE_LOGGER.info(String.format("[MCE] MceInPlaceBackfillTask START table=%s.%s columnState=%s partitions=%s",
            schemaName, tableName, tableMeta.getColumnMceState(columnName), physicalPartitions.size()));

        MceTaskRuntimeConfig runtimeConfig = new MceTaskRuntimeConfig(
            executionContext, MCE_LOGGER, jobId(), taskId(), "externalize-backfill");
        MceTaskRuntimeConfig.ExternalizeSettings initialSettings = runtimeConfig.externalizeSettings();
        int batchRows = initialSettings.batchRows;
        int updateBatchRows = Math.min(batchRows, MceCompositeKeySql.limitCasBatchRows(
            initialSettings.updateBatchRows, pkColumnNames.size()));
        long batchBytes = initialSettings.batchBytes;
        int parallelism = Math.min(initialSettings.parallelism, physicalPartitions.size());
        long maxInflightBytes = initialSettings.maxInflightBytes;
        MCE_LOGGER.info(String.format("[MCE] backfill limits: batchRows=%d, updateBatchRows=%d, batchBytes=%d, "
                + "parallelism=%d, maxInflightRawBytes=%d",
            batchRows, updateBatchRows, batchBytes, parallelism, maxInflightBytes));

        InflightRawBytesLimiter rawBytesLimiter =
            new InflightRawBytesLimiter(() -> runtimeConfig.externalizeSettings().maxInflightBytes);
        MceBackfillThrottle backfillThrottle =
            new MceBackfillThrottle(executionContext, schemaName, batchRows, taskId());
        long totalProcessed;
        try {
            totalProcessed = backfillPartitions(
                physicalPartitions, pkColumnNames, autoUpdateColumnNames, tableId,
                runtimeConfig, rawBytesLimiter, backfillThrottle, executionContext);
        } finally {
            backfillThrottle.stop();
        }
        clearBackfillFailureFailPoints();

        MCE_LOGGER.info(String.format("[MCE] backfill done: schema=%s, table=%s, totalRowsProcessed=%d",
            schemaName, tableName, totalProcessed));
        MCE_LOGGER.info(String.format("[MCE] MceInPlaceBackfillTask DONE table=%s.%s totalRows=%s",
            schemaName, tableName, totalProcessed));
    }

    private static void injectFailPointBeforeBackfill(ExecutionContext executionContext) {
        MceTaskFailPoint.pauseWhileEnabled(MceTaskFailPoint.FP_MCE_BEFORE_BACKFILL, executionContext);
    }

    private void configureBackfillFailureFailPoints(ExecutionContext executionContext) {
        if (MceTaskFailPoint.isEnabled(MceTaskFailPoint.FP_MCE_FAIL_AFTER_REMOTE_UPLOAD, executionContext)
            || MceTaskFailPoint.isEnabled(
            MceTaskFailPoint.FP_MCE_FAIL_AFTER_BACKFILL_UPDATE_BATCH, executionContext)) {
            setExceptionAction(DdlExceptionAction.PAUSE);
        }
    }

    private void injectFailPointAfterRemoteUpload(ExecutionContext executionContext) {
        MceTaskFailPoint.failOnce(MceTaskFailPoint.FP_MCE_FAIL_AFTER_REMOTE_UPLOAD,
            jobId(), taskId(), executionContext);
    }

    private void injectFailPointAfterBackfillUpdateBatch(ExecutionContext executionContext) {
        MceTaskFailPoint.failOnce(MceTaskFailPoint.FP_MCE_FAIL_AFTER_BACKFILL_UPDATE_BATCH,
            jobId(), taskId(), executionContext);
    }

    private void clearBackfillFailureFailPoints() {
        MceTaskFailPoint.clearFailOnce(
            MceTaskFailPoint.FP_MCE_FAIL_AFTER_REMOTE_UPLOAD, jobId(), taskId());
        MceTaskFailPoint.clearFailOnce(
            MceTaskFailPoint.FP_MCE_FAIL_AFTER_BACKFILL_UPDATE_BATCH, jobId(), taskId());
    }

    private static void injectFailPointAfterBackfillBatch(ExecutionContext executionContext) {
        MceTaskFailPoint.pauseWhileEnabled(MceTaskFailPoint.FP_MCE_AFTER_BACKFILL_BATCH, executionContext);
    }

    private long backfillPartitions(
        List<McePhysicalTableResolver.PhysicalTableTarget> physicalPartitions,
        List<String> pkColumnNames, List<String> autoUpdateColumnNames, long tableId,
        MceTaskRuntimeConfig runtimeConfig, InflightRawBytesLimiter rawBytesLimiter,
        MceBackfillThrottle backfillThrottle, ExecutionContext ec) {
        AtomicBoolean interrupted = new AtomicBoolean(false);
        McePartitionWorkerCoordinator<McePhysicalTableResolver.PhysicalTableTarget> coordinator =
            new McePartitionWorkerCoordinator<>(
                physicalPartitions,
                () -> runtimeConfig.externalizeSettings().parallelism,
                (target, shouldYield) -> backfillOnePartition(
                    target, pkColumnNames, autoUpdateColumnNames, tableId, runtimeConfig,
                    rawBytesLimiter, backfillThrottle, interrupted, ec, shouldYield),
                () -> checkInterrupted(ec, interrupted),
                interrupted,
                MCE_LOGGER,
                "externalize-backfill job=" + jobId() + " task=" + taskId(),
                "[MCE] concurrent backfill failed: ");
        return coordinator.execute();
    }

    private McePartitionWorkerCoordinator.PartitionResult backfillOnePartition(
        McePhysicalTableResolver.PhysicalTableTarget target,
        List<String> pkColumnNames, List<String> autoUpdateColumnNames,
        long tableId, MceTaskRuntimeConfig runtimeConfig,
        InflightRawBytesLimiter rawBytesLimiter,
        MceBackfillThrottle backfillThrottle,
        AtomicBoolean interrupted, ExecutionContext ec, BooleanSupplier shouldYield) {
        String phyTable = target.physicalTable;
        String partitionName = target.partitionName;
        String physicalDb = target.physicalDb;
        String groupName = target.groupName;
        OmcStorageInfo storageInfo = target.storageInfo;

        MceColumnStateRecord checkpoint = MceCheckpointHelper.loadOrCreate(jobId(), taskId(), schemaName, tableName,
            columnName, addrColumnName, physicalDb, phyTable, partitionName,
            MceColumnStateRecord.STATE_DUAL_WRITE, "task=backfill");
        if (checkpoint.getStatus() == MceColumnStateRecord.STATUS_SUCCESS) {
            MCE_LOGGER.info(String.format("[MCE] backfill partition already done: phyDb=%s, phyTable=%s",
                physicalDb, phyTable));
            return McePartitionWorkerCoordinator.PartitionResult.done(checkpoint.getProcessedRows());
        }

        MCE_LOGGER.info(String.format("[MCE] backfill partition begin: phyDb=%s, phyTable=%s",
            physicalDb, phyTable));
        List<ParameterContext> stopPk;
        if (isEmpty(checkpoint.getMaxPk())) {
            stopPk = queryStopPk(storageInfo, physicalDb, phyTable, pkColumnNames, ec);
            if (stopPk == null) {
                MceCheckpointHelper.markSuccess(checkpoint);
                MCE_LOGGER.info(String.format(
                    "[MCE] backfill partition skip (no un-migrated rows): phyDb=%s, phyTable=%s",
                    physicalDb, phyTable));
                return McePartitionWorkerCoordinator.PartitionResult.done(0);
            }
            String encodedStopPk = MceCheckpointCodec.encode(stopPk);
            MceCheckpointHelper.initMaxPk(
                checkpoint, encodedStopPk, MceCheckpointCodec.TYPE, "task=backfill");
            MCE_LOGGER.info(String.format("[MCE] backfill partition snapshot: phyDb=%s, phyTable=%s, stopPk=%s",
                physicalDb, phyTable, encodedStopPk));
        } else {
            stopPk = MceCheckpointCodec.decode(
                checkpoint.getMaxPk(), pkColumnNames.size(), checkpoint.getPkType());
            MCE_LOGGER.info(
                String.format("[MCE] backfill partition resume: phyDb=%s, phyTable=%s, lastPk=%s, stopPk=%s",
                    physicalDb, phyTable, checkpoint.getLastPk(), checkpoint.getMaxPk()));
        }

        List<ParameterContext> lastPk = isEmpty(checkpoint.getLastPk()) ? null
            : MceCheckpointCodec.decode(checkpoint.getLastPk(), pkColumnNames.size(), checkpoint.getPkType());
        long total = checkpoint.getProcessedRows();
        List<ParameterContext> scanPk = lastPk;
        StreamingBackfillWindow window = new StreamingBackfillWindow(
            storageInfo, groupName, physicalDb, phyTable, pkColumnNames, autoUpdateColumnNames, tableId,
            runtimeConfig, rawBytesLimiter, checkpoint, total, 0, lastPk, interrupted, ec);
        try {
            while (true) {
                checkInterrupted(ec, interrupted);
                MceTaskRuntimeConfig.ExternalizeSettings settings = runtimeConfig.externalizeSettings();
                int batchRows = settings.batchRows;
                // Admission-throttle each streaming SELECT. The scan cursor may move ahead of the
                // durable checkpoint while an underfilled Page continues into the next window.
                backfillThrottle.acquire(batchRows);
                final long batchStartMillis = System.currentTimeMillis();
                MceCompositeKeySql.PreparedSql select = buildSelectSql(
                    physicalDb, phyTable, pkColumnNames, scanPk, stopPk, batchRows);
                window.beginSelectWindow();
                OmcUtils.queryWithNewConnStreaming(
                    ec, storageInfo, select.getSql(), select.getParams(), window::accept);
                int scannedRows = window.finishSelectWindow();
                if (scannedRows == 0) {
                    window.finishPartition();
                    break;
                }
                scanPk = window.getLastScannedPk();
                backfillThrottle.feedback(batchStartMillis, scannedRows);
                if (shouldYield.getAsBoolean()) {
                    window.finishForWorkerYield();
                    return McePartitionWorkerCoordinator.PartitionResult.yielded(window.getTotal());
                }
            }
            total = window.getTotal();
        } finally {
            window.close();
        }

        MceCheckpointHelper.markSuccess(checkpoint);
        MCE_LOGGER.info(String.format("[MCE] backfill partition done: phyDb=%s, phyTable=%s, rowsProcessed=%d",
            physicalDb, phyTable, total));
        return McePartitionWorkerCoordinator.PartitionResult.done(total);
    }

    private final class StreamingBackfillWindow implements AutoCloseable {
        private final OmcStorageInfo storageInfo;
        private final String groupName;
        private final String physicalDb;
        private final String physicalTable;
        private final List<String> pkColumnNames;
        private final List<String> autoUpdateColumnNames;
        private final long tableId;
        private final MceTaskRuntimeConfig runtimeConfig;
        private final InflightRawBytesLimiter rawBytesLimiter;
        private final MceColumnStateRecord checkpoint;
        private final AtomicBoolean interrupted;
        private final ExecutionContext executionContext;
        private PendingBatch pendingBatch = new PendingBatch();
        private long total;
        private int round;
        private List<ParameterContext> lastPk;
        private List<ParameterContext> lastScannedPk;
        private int scannedRowsInWindow;

        private StreamingBackfillWindow(OmcStorageInfo storageInfo,
                                        String groupName,
                                        String physicalDb,
                                        String physicalTable,
                                        List<String> pkColumnNames,
                                        List<String> autoUpdateColumnNames,
                                        long tableId,
                                        MceTaskRuntimeConfig runtimeConfig,
                                        InflightRawBytesLimiter rawBytesLimiter,
                                        MceColumnStateRecord checkpoint,
                                        long total,
                                        int round,
                                        List<ParameterContext> lastPk,
                                        AtomicBoolean interrupted,
                                        ExecutionContext executionContext) {
            this.storageInfo = storageInfo;
            this.groupName = groupName;
            this.physicalDb = physicalDb;
            this.physicalTable = physicalTable;
            this.pkColumnNames = pkColumnNames;
            this.autoUpdateColumnNames = autoUpdateColumnNames;
            this.tableId = tableId;
            this.runtimeConfig = runtimeConfig;
            this.rawBytesLimiter = rawBytesLimiter;
            this.checkpoint = checkpoint;
            this.total = total;
            this.round = round;
            this.lastPk = lastPk;
            this.interrupted = interrupted;
            this.executionContext = executionContext;
        }

        private void accept(Map<Integer, ParameterContext> row) {
            checkInterrupted(executionContext, interrupted);
            List<ParameterContext> pkTuple = MceCompositeKeySql.extractTuple(
                row, 1, pkColumnNames.size(), "MCE streaming backfill row");
            lastScannedPk = pkTuple;
            scannedRowsInWindow++;
            ParameterContext contentParam = row.get(pkColumnNames.size() + 1);
            boolean nullContent = ExternalizedColumnDmlHelper.isNullParam(contentParam);
            byte[] raw = nullContent ? null : ExternalizedColumnDmlHelper.extractBlobBytes(contentParam);

            if (raw != null && pendingBatch.hasUploadItems()
                && (pendingBatch.getUploadItemCount() >= BlobPageFormat.MAX_ENTRIES
                || pendingBatch.getRawBytes() >= pendingBatch.getTargetRawBytes()
                || raw.length > pendingBatch.getTargetRawBytes() - pendingBatch.getRawBytes())) {
                flushPendingBatch();
            }

            if (raw != null && !pendingBatch.hasReservation()) {
                long pageTargetBytes = currentPageTargetBytes();
                long reservation = Math.max(pageTargetBytes, raw.length);
                rawBytesLimiter.acquire(reservation, () -> checkInterrupted(executionContext, interrupted));
                pendingBatch.reserve(reservation, pageTargetBytes);
            }
            pendingBatch.add(pkTuple, raw, tableId);

            if (pendingBatch.getUploadItemCount() >= BlobPageFormat.MAX_ENTRIES
                || pendingBatch.hasUploadItems()
                && pendingBatch.getRawBytes() >= pendingBatch.getTargetRawBytes()
                || !pendingBatch.hasUploadItems() && pendingBatch.getRowCount() >= currentUpdateBatchRows()
                // Bound PK/addr metadata for very sparse nullable columns. The normal non-NULL path
                // seals only on Page entry/raw limits and therefore forms full Pages across windows.
                || pendingBatch.getRowCount() >= 2 * BlobPageFormat.MAX_ENTRIES) {
                flushPendingBatch();
            }
        }

        private void beginSelectWindow() {
            scannedRowsInWindow = 0;
        }

        private int finishSelectWindow() {
            return scannedRowsInWindow;
        }

        private void finishPartition() {
            flushPendingBatch();
        }

        private void finishForWorkerYield() {
            flushPendingBatch();
        }

        private long currentPageTargetBytes() {
            MceTaskRuntimeConfig.ExternalizeSettings settings = runtimeConfig.externalizeSettings();
            return Math.min(
                Math.min(settings.batchBytes, BlobPageFormat.DEFAULT_TARGET_PAGE_RAW_BYTES),
                rawBytesLimiter.getMaxBytes());
        }

        private int currentUpdateBatchRows() {
            MceTaskRuntimeConfig.ExternalizeSettings settings = runtimeConfig.externalizeSettings();
            return Math.min(settings.batchRows,
                MceCompositeKeySql.limitCasBatchRows(settings.updateBatchRows, pkColumnNames.size()));
        }

        private void flushPendingBatch() {
            if (pendingBatch.isEmpty()) {
                return;
            }
            PendingBatch current = pendingBatch;
            pendingBatch = new PendingBatch();
            try {
                processBatch(current);
            } finally {
                current.releaseRawBytes(rawBytesLimiter);
            }
        }

        private void processBatch(PendingBatch batch) {
            round++;
            long rawBytes = batch.getRawBytes();
            List<BlobWriter.WriteResult> results = new ArrayList<>();
            if (!batch.uploadItems.isEmpty()) {
                results = BlobWriter.writeRemoteOnlyBatch(
                    batch.uploadItems, () -> checkInterrupted(executionContext, interrupted));
                try {
                    validateBlobBatchResults(batch.uploadItems, results, physicalDb, physicalTable);
                    for (int i = 0; i < results.size(); i++) {
                        batch.addrValues.set(
                            batch.uploadTargetIndexes.get(i), results.get(i).getBlobRefHex());
                    }
                    batch.releaseRawBytes(rawBytesLimiter);
                    injectFailPointAfterRemoteUpload(executionContext);
                } catch (RuntimeException e) {
                    BlobWriter.retainRemoteOnlyBatchForPurge(results, "pre_cas_validation_failed", e);
                    throw e;
                }
            } else {
                batch.releaseRawBytes(rawBytesLimiter);
            }

            try {
                checkInterrupted(executionContext, interrupted);
            } catch (RuntimeException e) {
                BlobWriter.retainRemoteOnlyBatchForPurge(results, "interrupted_before_cas", e);
                throw e;
            }

            int affected;
            int updateBatchRows = currentUpdateBatchRows();
            try {
                affected = executeIgnoreBinlogXaCasUpdates(batch, updateBatchRows);
                if (affected == 0) {
                    BlobWriter.retainRemoteOnlyBatchForPurge(results, "cas_not_published", null);
                } else if (affected < batch.pkTuples.size()) {
                    MCE_LOGGER.warn(String.format("[MCE] partial CAS retained Page with unreferenced slots: "
                            + "phyDb=%s, phyTable=%s, selected=%d, affected=%d, objects=%d",
                        physicalDb, physicalTable, batch.pkTuples.size(), affected, results.size()));
                }
                total += affected;
                executionContext.getStats().backfillRows.addAndGet(affected);
                DdlEngineStats.METRIC_BACKFILL_ROWS_FINISHED.update(affected);
            } catch (RuntimeException | Error e) {
                BlobWriter.retainRemoteOnlyBatchForPurge(
                    results, "cas_update_failed_after_possible_partial_publish", e);
                throw e;
            }

            lastPk = batch.pkTuples.get(batch.pkTuples.size() - 1);
            MceCheckpointHelper.saveProgress(checkpoint, MceCheckpointCodec.encode(lastPk), total);
            injectFailPointAfterBackfillBatch(executionContext);
            checkInterrupted(executionContext, interrupted);

            MCE_LOGGER.info(String.format("[MCE] backfill round=%d, phyDb=%s, phyTable=%s, "
                    + "batch=%d, updateBatch=%d, rawBytes=%d, affected=%d",
                round, physicalDb, physicalTable, batch.pkTuples.size(), updateBatchRows, rawBytes, affected));
        }

        private int executeIgnoreBinlogXaCasUpdates(PendingBatch batch, int updateBatchRows) {
            try (Connection connection = OmcUtils.getPhysicalConnection(storageInfo)) {
                connection.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                connection.setAutoCommit(true);
                if (connection.isWrapperFor(XConnection.class)) {
                    OmcUtils.setException(connection);
                    OmcUtils.setEncoding(connection, executionContext.getEncoding());
                    OmcUtils.setServerVariables(connection, executionContext.getServerVariables());
                    OmcUtils.setTraceId(connection, executionContext.getTraceId());
                }

                int totalAffected = 0;
                for (int from = 0; from < batch.pkTuples.size(); from += updateBatchRows) {
                    checkInterrupted(executionContext, interrupted);
                    int to = Math.min(batch.pkTuples.size(), from + updateBatchRows);
                    List<MceCompositeKeySql.CasRow> casRows = new ArrayList<>(to - from);
                    for (int i = from; i < to; i++) {
                        casRows.add(new MceCompositeKeySql.CasRow(
                            batch.pkTuples.get(i), batch.addrValues.get(i)));
                    }
                    MceCompositeKeySql.PreparedSql update = MceCompositeKeySql.buildCasUpdate(
                        physicalDb, physicalTable, pkColumnNames, addrColumnName, autoUpdateColumnNames, casRows);
                    int affected = DirectXaTransactionExecutor.executeIgnoreBinlogOnePhase(
                        executionContext, connection, groupName, update.getSql(), update.getParams(),
                        xaAffected -> {
                            if (xaAffected < 0 || xaAffected > casRows.size()) {
                                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                                    "[MCE] unexpected CAS affected rows: affected=" + xaAffected
                                        + ", selected=" + casRows.size());
                            }
                        });
                    totalAffected += affected;
                    injectFailPointAfterBackfillUpdateBatch(executionContext);
                }
                return totalAffected;
            } catch (SQLException e) {
                throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN, e,
                    "[MCE] failed to execute ignore-binlog XA CAS update: " + e.getMessage());
            }
        }

        private long getTotal() {
            return total;
        }

        private List<ParameterContext> getLastScannedPk() {
            return lastScannedPk;
        }

        @Override
        public void close() {
            pendingBatch.releaseRawBytes(rawBytesLimiter);
        }
    }

    private static final class PendingBatch {
        private final List<List<ParameterContext>> pkTuples = new ArrayList<>();
        private final List<String> addrValues = new ArrayList<>();
        private final List<BlobWriter.BlobItem> uploadItems = new ArrayList<>();
        private final List<Integer> uploadTargetIndexes = new ArrayList<>();
        private long rawBytes;
        private long reservedBytes;
        private long targetRawBytes;

        private void add(List<ParameterContext> pkTuple, byte[] raw, long tableId) {
            pkTuples.add(pkTuple);
            addrValues.add(null);
            if (raw != null) {
                rawBytes += raw.length;
                uploadItems.add(new BlobWriter.BlobItem(tableId, raw, false));
                uploadTargetIndexes.add(addrValues.size() - 1);
            }
        }

        private boolean isEmpty() {
            return pkTuples.isEmpty();
        }

        private int getRowCount() {
            return pkTuples.size();
        }

        private boolean hasUploadItems() {
            return !uploadItems.isEmpty();
        }

        private boolean hasReservation() {
            return reservedBytes > 0;
        }

        private void reserve(long bytes, long targetBytes) {
            if (bytes <= 0 || targetBytes <= 0 || reservedBytes != 0) {
                throw new IllegalStateException("invalid MCE Page reservation: current=" + reservedBytes
                    + ", requested=" + bytes + ", target=" + targetBytes);
            }
            reservedBytes = bytes;
            targetRawBytes = targetBytes;
        }

        private int getUploadItemCount() {
            return uploadItems.size();
        }

        private long getRawBytes() {
            return rawBytes;
        }

        private long getTargetRawBytes() {
            return targetRawBytes;
        }

        private void releaseRawBytes(InflightRawBytesLimiter limiter) {
            long releasedBytes = reservedBytes;
            rawBytes = 0;
            reservedBytes = 0;
            targetRawBytes = 0;
            uploadItems.clear();
            if (releasedBytes > 0) {
                limiter.release(releasedBytes);
            }
        }
    }

    private static final class InflightRawBytesLimiter {
        private static final long CANCELLATION_POLL_MS = 1_000L;

        private final LongSupplier maxBytesSupplier;
        private long admittedBytes;

        private InflightRawBytesLimiter(LongSupplier maxBytesSupplier) {
            this.maxBytesSupplier = maxBytesSupplier;
        }

        private long getMaxBytes() {
            return Math.max(1L, maxBytesSupplier.getAsLong());
        }

        private void acquire(long bytes, Runnable cancellationCheck) {
            if (bytes <= 0) {
                return;
            }
            synchronized (this) {
                while (admittedBytes > 0 && bytes > getMaxBytes() - admittedBytes) {
                    cancellationCheck.run();
                    try {
                        wait(CANCELLATION_POLL_MS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                            "MCE raw-byte admission interrupted");
                    }
                }
                cancellationCheck.run();
                admittedBytes += bytes;
            }
        }

        private synchronized void release(long bytes) {
            admittedBytes = Math.max(0L, admittedBytes - bytes);
            notifyAll();
        }
    }

    private static void validateBlobBatchResults(
        List<BlobWriter.BlobItem> items,
        List<BlobWriter.WriteResult> results,
        String physicalDb,
        String physicalTable) {
        int actual = results == null ? -1 : results.size();
        if (actual != items.size()) {
            throw invalidBlobBatchResult(physicalDb, physicalTable,
                "expected " + items.size() + " results but received "
                    + (results == null ? "null" : actual));
        }
        for (int i = 0; i < results.size(); i++) {
            BlobWriter.WriteResult result = results.get(i);
            if (result == null) {
                throw invalidBlobBatchResult(physicalDb, physicalTable, "result " + i + " is null");
            }
            if (result.getBlobRefHex() == null || result.getBlobRefHex().isEmpty()) {
                throw invalidBlobBatchResult(physicalDb, physicalTable, "result " + i + " has no BlobRef");
            }
            if (result.getFuture() == null) {
                throw invalidBlobBatchResult(physicalDb, physicalTable, "result " + i + " has no upload future");
            }
            if (result.getTableId() != items.get(i).tableId) {
                throw invalidBlobBatchResult(physicalDb, physicalTable,
                    "result " + i + " tableId " + result.getTableId()
                        + " does not match input tableId " + items.get(i).tableId);
            }
        }
    }

    private static TddlRuntimeException invalidBlobBatchResult(
        String physicalDb, String physicalTable, String detail) {
        return new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
            "[MCE] invalid blob batch result for " + physicalDb + "." + physicalTable + ": " + detail);
    }

    // ==================== SQL Builders ====================

    private MceCompositeKeySql.PreparedSql buildSelectSql(
        String physicalDb, String phyTable, List<String> pkColumnNames,
        List<ParameterContext> lastPk, List<ParameterContext> stopPk, int batchRows) {
        MceCompositeKeySql.PreparedPredicate range =
            MceCompositeKeySql.buildRangePredicate(pkColumnNames, lastPk, stopPk);
        String sql = "SELECT " + MceCompositeKeySql.quotedColumns(pkColumnNames)
            + ", " + MceCompositeKeySql.quote(columnName)
            + " FROM " + MceCompositeKeySql.qualifiedTable(physicalDb, phyTable)
            + " WHERE " + MceCompositeKeySql.quote(addrColumnName) + " = ''"
            + " AND " + range.getSql()
            + " ORDER BY " + MceCompositeKeySql.orderBy(pkColumnNames, false)
            + " LIMIT " + batchRows;
        return new MceCompositeKeySql.PreparedSql(sql, range.getParams());
    }

    /**
     * Snapshot the actual greatest effective-primary-key tuple among rows still carrying the
     * "not migrated" sentinel. Independent MAX expressions are not a valid composite stop key.
     */
    private List<ParameterContext> queryStopPk(OmcStorageInfo storageInfo, String physicalDb, String phyTable,
                                               List<String> pkColumnNames, ExecutionContext ec) {
        String sql = "SELECT " + MceCompositeKeySql.quotedColumns(pkColumnNames)
            + " FROM " + MceCompositeKeySql.qualifiedTable(physicalDb, phyTable)
            + " WHERE " + MceCompositeKeySql.quote(addrColumnName) + " = ''"
            + " ORDER BY " + MceCompositeKeySql.orderBy(pkColumnNames, true)
            + " LIMIT 1";
        List<Map<Integer, ParameterContext>> rows = OmcUtils.queryWithNewConn(ec, storageInfo, sql);
        if (rows.isEmpty()) {
            return null;
        }
        return MceCompositeKeySql.extractTuple(rows.get(0), 1, pkColumnNames.size(), "MCE stop-key row");
    }

    private static boolean isEmpty(String value) {
        return value == null || value.isEmpty();
    }

    private long jobId() {
        return getJobId() == null ? 0L : getJobId();
    }

    private long taskId() {
        return getTaskId() == null ? 0L : getTaskId();
    }

    private void checkInterrupted(ExecutionContext ec, AtomicBoolean interrupted) {
        if (interrupted.get() || CrossEngineValidator.isJobInterrupted(ec)
            || Thread.currentThread().isInterrupted()) {
            long jobId = ec.getDdlJobId();
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "The job '" + jobId + "' has been paused or cancelled");
        }
    }

    @Override
    protected String remark() {
        return String.format("|MCE in-place backfill, table=%s, column=%s→%s",
            tableName, columnName, addrColumnName);
    }
}
