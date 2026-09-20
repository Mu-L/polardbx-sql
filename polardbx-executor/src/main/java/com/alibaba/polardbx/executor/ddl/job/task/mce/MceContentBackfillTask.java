package com.alibaba.polardbx.executor.ddl.job.task.mce;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineStats;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.ddl.omc.OmcStorageInfo;
import com.alibaba.polardbx.executor.ddl.omc.OmcUtils;
import com.alibaba.polardbx.executor.function.calc.FetchBlob;
import com.alibaba.polardbx.gms.metadb.misc.MceColumnStateRecord;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

/**
 * Internalize Task: in-place backfill of the plaintext content column from the externalized
 * BlobRefs — the reverse of {@link MceInPlaceBackfillTask}.
 * <p>
 * Runs while the column is in the reverse READ_ADDR phase (dual-write active, reads on addr).
 * There is no explicit sentinel: {@code content IS NULL AND addr IS NOT NULL} exactly identifies
 * the historical rows to restore — dual-write keeps both sides consistent for new writes, and rows
 * whose addr is NULL already carry the correct terminal value (NULL content).
 * <p>
 * Per physical partition:
 * <ol>
 *   <li>SELECT the ordered effective-PK tuple and addr hex of un-restored rows inside the typed
 *       DNF keyset range (only the 66-char addr travels here, never the payload).</li>
 *   <li>Fetch each payload through the FULL high-watermark read path (staging → cache/staging
 *       race → OSS): values written just before the reverse job started may still
 *       sit in unflushed staging, so a remote-only shortcut would be incorrect.</li>
 *   <li>Execute a parameterized searched-CASE UPDATE guarded per row by
 *       {@code content IS NULL AND addr <=> <captured>}: a concurrent user write flips either side
 *       (dual-write), so the stale backfill value can never overwrite it. A lost CAS only wastes
 *       one fetch — there is no orphan side effect in this direction.</li>
 *   <li>Advance the typed tuple checkpoint ({@code extra=task=internalize-backfill}).</li>
 * </ol>
 * A non-NULL addr whose payload cannot be found is a referenced-object loss and fails the DDL
 * closed; backfill never writes NULL for it.
 * <p>
 * Reverse-specific limits: batches are cut by the decoded V2 {@code rawSize} sum against
 * {@code MCE_INTERNALIZE_BACKFILL_BATCH_BYTES} before any payload is fetched, bounding CN heap per batch.
 * Restored plaintext is written with {@code setBytes}, byte-identical to the uploaded value; the
 * MD5 checker before read cutover independently verifies every row.
 */
@Getter
@TaskName(name = "MceContentBackfillTask")
public class MceContentBackfillTask extends BaseDdlTask {

    /**
     * MCE state-transition timeline goes to tddl.log (SLS-collected) instead of the
     * ddl-engine log, so migration troubleshooting survives log collection.
     */
    private static final Logger MCE_LOGGER = LoggerFactory.getLogger("EXT_COLUMN");

    private static final int MAX_PREPARED_STATEMENT_PARAMETERS = 65535;

    private final String tableName;
    private final String columnName;
    private final String addrColumnName;

    public MceContentBackfillTask(String schemaName, String tableName,
                                  String columnName, String addrColumnName) {
        super(schemaName);
        onExceptionTryRecoveryThenPause();
        this.tableName = tableName;
        this.columnName = columnName;
        this.addrColumnName = addrColumnName;
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        injectFailPointBeforeInternalizeBackfill(executionContext);
        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);

        IndexMeta primaryIndex =
            McePrimaryKeyValidator.requirePrimaryKey(tableMeta, schemaName, tableName);
        List<String> pkColumnNames = primaryIndex.getKeyColumns().stream()
            .map(ColumnMeta::getName)
            .collect(Collectors.toList());
        List<String> autoUpdateColumnNames = tableMeta.getAutoUpdateColumns().stream()
            .map(ColumnMeta::getName)
            .collect(Collectors.toList());

        List<McePhysicalTableResolver.PhysicalTableTarget> physicalPartitions =
            McePhysicalTableResolver.resolve(schemaName, tableName, tableMeta, executionContext);
        MCE_LOGGER.info(String.format(
            "[MCE] internalize backfill start: schema=%s, table=%s, column=%s←%s, pk=%s, partitions=%d",
            schemaName, tableName, columnName, addrColumnName, pkColumnNames, physicalPartitions.size()));
        MCE_LOGGER.info(String.format("[MCE] MceContentBackfillTask START table=%s.%s columnState=%s partitions=%s",
            schemaName, tableName, tableMeta.getColumnMceState(columnName), physicalPartitions.size()));

        MceTaskRuntimeConfig runtimeConfig = new MceTaskRuntimeConfig(
            executionContext, MCE_LOGGER, jobId(), taskId(), "internalize-backfill");
        MceTaskRuntimeConfig.InternalizeSettings initialSettings = runtimeConfig.internalizeSettings();
        int batchRows = limitBatchRows(initialSettings.batchRows, pkColumnNames.size());
        long batchBytes = initialSettings.batchBytes;
        int parallelism = Math.min(initialSettings.parallelism, physicalPartitions.size());
        MCE_LOGGER.info(String.format("[MCE] internalize backfill limits: batchRows=%d, batchBytes=%d, parallelism=%d",
            batchRows, batchBytes, parallelism));

        MceBackfillThrottle backfillThrottle =
            new MceBackfillThrottle(executionContext, schemaName, batchRows, taskId());
        long totalProcessed;
        try {
            totalProcessed = backfillPartitions(
                physicalPartitions, pkColumnNames, autoUpdateColumnNames, runtimeConfig,
                backfillThrottle, executionContext);
        } finally {
            backfillThrottle.stop();
        }

        MCE_LOGGER.info(String.format("[MCE] internalize backfill done: schema=%s, table=%s, totalRowsProcessed=%d",
            schemaName, tableName, totalProcessed));
        MCE_LOGGER.info(String.format("[MCE] MceContentBackfillTask DONE table=%s.%s totalRows=%s",
            schemaName, tableName, totalProcessed));
    }

    private static void injectFailPointBeforeInternalizeBackfill(ExecutionContext executionContext) {
        MceTaskFailPoint.pauseWhileEnabled(MceTaskFailPoint.FP_MCE_INTERNALIZE_BEFORE_BACKFILL, executionContext);
    }

    private static void injectFailPointAfterInternalizeBackfillBatch(ExecutionContext executionContext) {
        MceTaskFailPoint.pauseWhileEnabled(MceTaskFailPoint.FP_MCE_INTERNALIZE_AFTER_BACKFILL_BATCH,
            executionContext);
    }

    private long backfillPartitions(
        List<McePhysicalTableResolver.PhysicalTableTarget> physicalPartitions,
        List<String> pkColumnNames, List<String> autoUpdateColumnNames,
        MceTaskRuntimeConfig runtimeConfig, MceBackfillThrottle backfillThrottle, ExecutionContext ec) {
        AtomicBoolean interrupted = new AtomicBoolean(false);
        McePartitionWorkerCoordinator<McePhysicalTableResolver.PhysicalTableTarget> coordinator =
            new McePartitionWorkerCoordinator<>(
                physicalPartitions,
                () -> runtimeConfig.internalizeSettings().parallelism,
                (target, shouldYield) -> backfillOnePartition(
                    target, pkColumnNames, autoUpdateColumnNames, runtimeConfig, backfillThrottle,
                    interrupted, ec, shouldYield),
                () -> checkInterrupted(ec, interrupted),
                interrupted,
                MCE_LOGGER,
                "internalize-backfill job=" + jobId() + " task=" + taskId(),
                "[MCE] concurrent internalize backfill failed: ");
        return coordinator.execute();
    }

    private McePartitionWorkerCoordinator.PartitionResult backfillOnePartition(
        McePhysicalTableResolver.PhysicalTableTarget target,
        List<String> pkColumnNames, List<String> autoUpdateColumnNames,
        MceTaskRuntimeConfig runtimeConfig, MceBackfillThrottle backfillThrottle,
        AtomicBoolean interrupted, ExecutionContext ec, BooleanSupplier shouldYield) {
        String phyTable = target.physicalTable;
        String partitionName = target.partitionName;
        String physicalDb = target.physicalDb;
        OmcStorageInfo storageInfo = target.storageInfo;

        MceColumnStateRecord checkpoint = MceCheckpointHelper.loadOrCreate(jobId(), taskId(), schemaName, tableName,
            columnName, addrColumnName, physicalDb, phyTable, partitionName,
            MceColumnStateRecord.STATE_READ_ADDR, "task=internalize-backfill");
        if (checkpoint.getStatus() == MceColumnStateRecord.STATUS_SUCCESS) {
            MCE_LOGGER.info(String.format("[MCE] internalize backfill partition already done: phyDb=%s, phyTable=%s",
                physicalDb, phyTable));
            return McePartitionWorkerCoordinator.PartitionResult.done(checkpoint.getProcessedRows());
        }

        List<ParameterContext> stopPk;
        if (isEmpty(checkpoint.getMaxPk())) {
            stopPk = queryStopPk(storageInfo, physicalDb, phyTable, pkColumnNames, ec);
            if (stopPk == null) {
                MceCheckpointHelper.markSuccess(checkpoint);
                MCE_LOGGER.info(String.format(
                    "[MCE] internalize backfill partition skip (no un-restored rows): phyDb=%s, phyTable=%s",
                    physicalDb, phyTable));
                return McePartitionWorkerCoordinator.PartitionResult.done(0);
            }
            String encodedStopPk = MceCheckpointCodec.encode(stopPk);
            MceCheckpointHelper.initMaxPk(
                checkpoint, encodedStopPk, MceCheckpointCodec.TYPE, "task=internalize-backfill");
        } else {
            stopPk = MceCheckpointCodec.decode(
                checkpoint.getMaxPk(), pkColumnNames.size(), checkpoint.getPkType());
            MCE_LOGGER.info(String.format(
                "[MCE] internalize backfill partition resume: phyDb=%s, phyTable=%s, lastPk=%s, stopPk=%s",
                physicalDb, phyTable, checkpoint.getLastPk(), checkpoint.getMaxPk()));
        }

        List<ParameterContext> lastPk = isEmpty(checkpoint.getLastPk()) ? null
            : MceCheckpointCodec.decode(checkpoint.getLastPk(), pkColumnNames.size(), checkpoint.getPkType());
        long total = checkpoint.getProcessedRows();
        int round = 0;
        while (true) {
            checkInterrupted(ec, interrupted);
            int batchRows = limitBatchRows(
                runtimeConfig.internalizeSettings().batchRows, pkColumnNames.size());
            // Admission-throttle each batch SELECT like Extractor: shared across all partition
            // pipelines of this task, retargeted by adaptive feedback after the batch completes.
            backfillThrottle.acquire(batchRows);
            final long batchStartMillis = System.currentTimeMillis();
            MceCompositeKeySql.PreparedSql select = buildSelectSql(
                physicalDb, phyTable, pkColumnNames, lastPk, stopPk, batchRows);
            List<Map<Integer, ParameterContext>> rows = OmcUtils.queryWithNewConn(
                ec, storageInfo, select.getSql(), select.getParams(), false, null);
            if (rows.isEmpty()) {
                break;
            }

            int rowIndex = 0;
            while (rowIndex < rows.size()) {
                checkInterrupted(ec, interrupted);
                long batchBytes = runtimeConfig.internalizeSettings().batchBytes;
                // Cut a byte-bounded sub-batch by the decoded rawSize BEFORE fetching any payload.
                List<List<ParameterContext>> pkTuples = new ArrayList<>();
                List<String> addrHexes = new ArrayList<>();
                long plannedBytes = 0;
                while (rowIndex < rows.size()) {
                    Map<Integer, ParameterContext> row = rows.get(rowIndex);
                    List<ParameterContext> pkTuple = MceCompositeKeySql.extractTuple(
                        row, 1, pkColumnNames.size(), "MCE internalize backfill row");
                    String addrHex = extractAddrHex(row, pkColumnNames.size() + 1, physicalDb, phyTable);
                    long rawSize = decodeRawSize(addrHex, physicalDb, phyTable);
                    if (!pkTuples.isEmpty() && plannedBytes + rawSize > batchBytes) {
                        break;
                    }
                    pkTuples.add(pkTuple);
                    addrHexes.add(addrHex);
                    plannedBytes += rawSize;
                    rowIndex++;
                    if (plannedBytes >= batchBytes) {
                        break;
                    }
                }

                round++;
                total += restoreBatch(storageInfo, physicalDb, phyTable, pkColumnNames, autoUpdateColumnNames,
                    pkTuples, addrHexes, round, interrupted, ec);
                lastPk = pkTuples.get(pkTuples.size() - 1);
                MceCheckpointHelper.saveProgress(checkpoint, MceCheckpointCodec.encode(lastPk), total);
                injectFailPointAfterInternalizeBackfillBatch(ec);
                if (shouldYield.getAsBoolean()) {
                    backfillThrottle.feedback(batchStartMillis, rowIndex);
                    return McePartitionWorkerCoordinator.PartitionResult.yielded(total);
                }
            }
            backfillThrottle.feedback(batchStartMillis, rows.size());
        }

        MceCheckpointHelper.markSuccess(checkpoint);
        MCE_LOGGER.info(String.format("[MCE] internalize backfill partition done: phyDb=%s, phyTable=%s, rows=%d",
            physicalDb, phyTable, total));
        return McePartitionWorkerCoordinator.PartitionResult.done(total);
    }

    /**
     * Fetch every payload through the full read path, then CAS the plaintext back in one searched
     * CASE UPDATE. Rows whose CAS is lost to a concurrent dual-write are simply skipped.
     */
    private long restoreBatch(OmcStorageInfo storageInfo, String physicalDb, String phyTable,
                              List<String> pkColumnNames, List<String> autoUpdateColumnNames,
                              List<List<ParameterContext>> pkTuples, List<String> addrHexes, int round,
                              AtomicBoolean interrupted, ExecutionContext ec) {
        FetchBlob fetchBlob = new FetchBlob();
        List<byte[]> payloads = new ArrayList<>(addrHexes.size());
        for (String addrHex : addrHexes) {
            checkInterrupted(ec, interrupted);
            Object data = fetchBlob.compute(
                new Object[] {addrHex, schemaName, tableName, columnName, "BLOB"}, ec);
            if (data == null) {
                // A committed non-NULL addr without a resolvable payload is a lost referenced
                // object. Never coerce it to NULL plaintext.
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, String.format(
                    "[MCE] internalize backfill cannot resolve payload for addr=%s on %s.%s",
                    addrHex, physicalDb, phyTable));
            }
            payloads.add((byte[]) data);
        }

        MceCompositeKeySql.PreparedSql update = buildContentCasUpdate(
            physicalDb, phyTable, pkColumnNames, autoUpdateColumnNames, pkTuples, addrHexes, payloads);
        int affected = OmcUtils.executeWithNewConn(ec, storageInfo, update.getSql(), update.getParams());
        if (affected < 0 || affected > pkTuples.size()) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "[MCE] unexpected internalize CAS affected rows: affected=" + affected
                    + ", selected=" + pkTuples.size());
        }
        if (affected < pkTuples.size()) {
            MCE_LOGGER.info(String.format("[MCE] internalize CAS lost %d rows to concurrent dual-writes: "
                    + "phyDb=%s, phyTable=%s, selected=%d, affected=%d",
                pkTuples.size() - affected, physicalDb, phyTable, pkTuples.size(), affected));
        }
        ec.getStats().backfillRows.addAndGet(affected);
        DdlEngineStats.METRIC_BACKFILL_ROWS_FINISHED.update(affected);
        checkInterrupted(ec, interrupted);

        MCE_LOGGER.info(String.format("[MCE] internalize backfill round=%d, phyDb=%s, phyTable=%s, "
                + "batch=%d, affected=%d",
            round, physicalDb, phyTable, pkTuples.size(), affected));
        return affected;
    }

    // ==================== SQL Builders ====================

    private MceCompositeKeySql.PreparedSql buildSelectSql(
        String physicalDb, String phyTable, List<String> pkColumnNames,
        List<ParameterContext> lastPk, List<ParameterContext> stopPk, int batchRows) {
        MceCompositeKeySql.PreparedPredicate range =
            MceCompositeKeySql.buildRangePredicate(pkColumnNames, lastPk, stopPk);
        String sql = "SELECT " + MceCompositeKeySql.quotedColumns(pkColumnNames)
            + ", " + MceCompositeKeySql.quote(addrColumnName)
            + " FROM " + MceCompositeKeySql.qualifiedTable(physicalDb, phyTable)
            + " WHERE " + unrestoredPredicate()
            + " AND " + range.getSql()
            + " ORDER BY " + MceCompositeKeySql.orderBy(pkColumnNames, false)
            + " LIMIT " + batchRows;
        return new MceCompositeKeySql.PreparedSql(sql, range.getParams());
    }

    private List<ParameterContext> queryStopPk(OmcStorageInfo storageInfo, String physicalDb, String phyTable,
                                               List<String> pkColumnNames, ExecutionContext ec) {
        String sql = "SELECT " + MceCompositeKeySql.quotedColumns(pkColumnNames)
            + " FROM " + MceCompositeKeySql.qualifiedTable(physicalDb, phyTable)
            + " WHERE " + unrestoredPredicate()
            + " ORDER BY " + MceCompositeKeySql.orderBy(pkColumnNames, true)
            + " LIMIT 1";
        List<Map<Integer, ParameterContext>> rows = OmcUtils.queryWithNewConn(ec, storageInfo, sql);
        if (rows.isEmpty()) {
            return null;
        }
        return MceCompositeKeySql.extractTuple(rows.get(0), 1, pkColumnNames.size(), "MCE internalize stop-key row");
    }

    private String unrestoredPredicate() {
        // The '' guard is defensive: a terminal externalized column has no '' sentinel left, but a
        // sentinel row must never be treated as a restorable BlobRef.
        return MceCompositeKeySql.quote(columnName) + " IS NULL"
            + " AND " + MceCompositeKeySql.quote(addrColumnName) + " IS NOT NULL"
            + " AND " + MceCompositeKeySql.quote(addrColumnName) + " <> ''";
    }

    /**
     * {@code UPDATE t SET content = CASE WHEN (pk<=>? AND addr<=>?) THEN ? ... ELSE content END,
     * auto_update_col = auto_update_col WHERE content IS NULL AND ((pk<=>? AND addr<=>?) OR ...)}
     * — per-row CAS on both the NULL content and the exact captured addr. Auto-update columns are
     * explicitly self-assigned so this internal maintenance write preserves their current values.
     */
    private MceCompositeKeySql.PreparedSql buildContentCasUpdate(
        String physicalDb, String phyTable, List<String> pkColumnNames,
        List<String> autoUpdateColumnNames, List<List<ParameterContext>> pkTuples,
        List<String> addrHexes, List<byte[]> payloads) {
        StringBuilder sql = new StringBuilder();
        Map<Integer, ParameterContext> params = new LinkedHashMap<>();
        int nextIndex = 1;
        sql.append("UPDATE ").append(MceCompositeKeySql.qualifiedTable(physicalDb, phyTable))
            .append(" SET ").append(MceCompositeKeySql.quote(columnName)).append(" = CASE");
        for (int i = 0; i < pkTuples.size(); i++) {
            sql.append(" WHEN ").append(exactKeyAndAddrPredicate(pkColumnNames)).append(" THEN ?");
            nextIndex = addTupleParams(params, nextIndex, pkTuples.get(i));
            params.put(nextIndex, new ParameterContext(ParameterMethod.setString,
                new Object[] {nextIndex, addrHexes.get(i)}));
            nextIndex++;
            params.put(nextIndex, new ParameterContext(ParameterMethod.setBytes,
                new Object[] {nextIndex, payloads.get(i)}));
            nextIndex++;
        }
        sql.append(" ELSE ").append(MceCompositeKeySql.quote(columnName)).append(" END");
        MceCompositeKeySql.appendSelfAssignments(sql, autoUpdateColumnNames);
        sql.append(" WHERE ").append(MceCompositeKeySql.quote(columnName)).append(" IS NULL AND (");
        for (int i = 0; i < pkTuples.size(); i++) {
            if (i > 0) {
                sql.append(" OR ");
            }
            sql.append(exactKeyAndAddrPredicate(pkColumnNames));
            nextIndex = addTupleParams(params, nextIndex, pkTuples.get(i));
            params.put(nextIndex, new ParameterContext(ParameterMethod.setString,
                new Object[] {nextIndex, addrHexes.get(i)}));
            nextIndex++;
        }
        sql.append(")");
        return new MceCompositeKeySql.PreparedSql(sql.toString(), params);
    }

    private String exactKeyAndAddrPredicate(List<String> pkColumnNames) {
        List<String> predicates = new ArrayList<>(pkColumnNames.size() + 1);
        for (String primaryKey : pkColumnNames) {
            predicates.add(MceCompositeKeySql.quote(primaryKey) + " <=> ?");
        }
        predicates.add(MceCompositeKeySql.quote(addrColumnName) + " <=> ?");
        return "(" + String.join(" AND ", predicates) + ")";
    }

    private static int addTupleParams(Map<Integer, ParameterContext> target, int nextIndex,
                                      List<ParameterContext> tuple) {
        for (ParameterContext parameter : tuple) {
            Object[] args = parameter.getArgs().clone();
            args[0] = nextIndex;
            target.put(nextIndex, new ParameterContext(parameter.getParameterMethod(), args));
            nextIndex++;
        }
        return nextIndex;
    }

    /**
     * Per CAS row: (pkArity + addr + payload) in the CASE arm plus (pkArity + addr) in the WHERE.
     */
    private static int limitBatchRows(int requestedRows, int primaryKeyArity) {
        if (requestedRows <= 0) {
            throw new IllegalArgumentException("MCE batch row count must be positive");
        }
        long paramsPerRow = 2L * (primaryKeyArity + 1L) + 1L;
        int maxRows = (int) (MAX_PREPARED_STATEMENT_PARAMETERS / paramsPerRow);
        if (maxRows <= 0) {
            throw new IllegalArgumentException("MCE primary-key arity " + primaryKeyArity
                + " exceeds prepared-statement parameter limit");
        }
        return Math.min(requestedRows, maxRows);
    }

    private String extractAddrHex(Map<Integer, ParameterContext> row, int columnIndex,
                                  String physicalDb, String phyTable) {
        Object value = MceCompositeKeySql.value(row.get(columnIndex));
        String hex = value instanceof byte[]
            ? new String((byte[]) value, java.nio.charset.StandardCharsets.US_ASCII)
            : value == null ? null : value.toString();
        if (hex == null || hex.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, String.format(
                "[MCE] internalize backfill selected a row without addr on %s.%s", physicalDb, phyTable));
        }
        return hex;
    }

    private long decodeRawSize(String addrHex, String physicalDb, String phyTable) {
        long[] decoded = FetchBlob.decodeBlobRef(addrHex);
        if (decoded == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR, String.format(
                "[MCE] internalize backfill found an invalid BlobRef '%s' on %s.%s",
                addrHex, physicalDb, phyTable));
        }
        // decoded[3] is the uncompressed/raw size across BlobRef versions.
        return Math.max(0L, decoded[3]);
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
        return String.format("|MCE internalize content backfill, table=%s, column=%s←%s",
            tableName, columnName, addrColumnName);
    }
}
