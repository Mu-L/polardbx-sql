package com.alibaba.polardbx.executor.ddl.job.task.mce;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.oss.blob.BlobObjectId;
import com.alibaba.polardbx.common.oss.blob.BlobPageFormat;
import com.alibaba.polardbx.common.oss.blob.BlobRef;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlExceptionAction;
import com.alibaba.polardbx.executor.ddl.omc.OmcStorageInfo;
import com.alibaba.polardbx.executor.ddl.omc.OmcUtils;
import com.alibaba.polardbx.gms.metadb.misc.MceColumnStateRecord;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.stream.Collectors;

@Getter
@TaskName(name = "MceBlobRefMd5CheckTask")
public class MceBlobRefMd5CheckTask extends BaseDdlTask {

    /**
     * MCE state-transition timeline goes to tddl.log (SLS-collected) instead of the
     * ddl-engine log, so migration troubleshooting survives log collection.
     */
    private static final Logger MCE_LOGGER = LoggerFactory.getLogger("EXT_COLUMN");

    private static final int ERROR_LIMIT = 20;
    private static final int HEX_CHARS_PER_BYTE = 2;
    private static final int SQL_SUBSTR_INDEX_BASE = 1;
    private static final int V2_VERSION_SQL_OFFSET = sqlOffset(BlobRef.VERSION_OFFSET_V2);
    private static final int V2_VERSION_HEX_LENGTH = hexLength(BlobRef.VERSION_LENGTH_V2);
    private static final int V2_SEQ_ID_SQL_OFFSET = sqlOffset(BlobRef.SEQ_ID_OFFSET_V2);
    private static final int V2_SEQ_ID_HEX_LENGTH = hexLength(BlobRef.SEQ_ID_LENGTH_V2);
    private static final int V2_SLOT_ADDR_SQL_OFFSET = sqlOffset(BlobRef.SLOT_ADDR_OFFSET_V2);
    private static final int V2_SLOT_ADDR_HEX_LENGTH = hexLength(BlobRef.SLOT_ADDR_LENGTH_V2);
    private static final int V2_SLOT_MARKER_HEX_LENGTH = Byte.BYTES * HEX_CHARS_PER_BYTE;
    private static final int V2_SLOT_LOW_BITS_SQL_OFFSET =
        V2_SLOT_ADDR_SQL_OFFSET + V2_SLOT_MARKER_HEX_LENGTH;
    private static final int V2_SLOT_LOW_BITS_HEX_LENGTH =
        (Long.BYTES - Byte.BYTES) * HEX_CHARS_PER_BYTE;
    private static final int V2_RAW_SIZE_SQL_OFFSET = sqlOffset(BlobRef.RAW_SIZE_OFFSET_V2);
    private static final int V2_RAW_SIZE_HEX_LENGTH = hexLength(BlobRef.RAW_SIZE_LENGTH_V2);
    private static final int V2_RAW_MD5_SQL_OFFSET = sqlOffset(BlobRef.MD5_OFFSET_V2);
    private static final int V2_RAW_MD5_HEX_LENGTH = hexLength(BlobRef.MD5_LENGTH);
    private static final String V2_VERSION_HEX = oneByteHex(BlobRef.VERSION_2);
    private static final String V2_SLOT_MARKER_HEX = oneByteHex(
        BlobObjectId.MARKER >>> (Long.SIZE - Byte.SIZE));

    private static int sqlOffset(int byteOffset) {
        return byteOffset * HEX_CHARS_PER_BYTE + SQL_SUBSTR_INDEX_BASE;
    }

    private static int hexLength(int bytes) {
        return bytes * HEX_CHARS_PER_BYTE;
    }

    private final String tableName;
    private final String columnName;
    private final String addrColumnName;

    public MceBlobRefMd5CheckTask(String schemaName, String tableName, String columnName, String addrColumnName) {
        super(schemaName);
        onExceptionTryRecoveryThenPause();
        this.tableName = tableName;
        this.columnName = columnName;
        this.addrColumnName = addrColumnName;
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        injectFailPointBeforeBlobRefMd5Check(executionContext);
        int extColumnVersion = DynamicConfig.getInstance().getExtColumnVersion();
        boolean checkBlobRefMd5 = extColumnVersion == BlobRef.VERSION_2;
        if (!checkBlobRefMd5) {
            MCE_LOGGER.info(String.format("[MCE] skip BlobRef V2 MD5 check: schema=%s, table=%s, column=%s, version=%d",
                schemaName, tableName, columnName, extColumnVersion));
        }

        TableMeta tableMeta = executionContext.getSchemaManager(schemaName).getTable(tableName);
        IndexMeta primaryIndex = McePrimaryKeyValidator.requirePrimaryKey(tableMeta, schemaName, tableName);
        List<String> primaryKeys = primaryIndex.getKeyColumns().stream()
            .map(column -> column.getName())
            .collect(Collectors.toList());

        List<McePhysicalTableResolver.PhysicalTableTarget> physicalPartitions =
            McePhysicalTableResolver.resolve(schemaName, tableName, tableMeta, executionContext);
        MceTaskRuntimeConfig runtimeConfig = new MceTaskRuntimeConfig(
            executionContext, MCE_LOGGER, jobId(), taskId(), "blob-md5-check");
        MceTaskRuntimeConfig.CheckerSettings initialSettings = runtimeConfig.checkerSettings();
        int batchRows = initialSettings.batchRows;
        int parallelism = Math.min(initialSettings.parallelism, physicalPartitions.size());
        MCE_LOGGER.info(String.format(checkBlobRefMd5
                ?
                "[MCE] BlobRef V2 MD5 check start: schema=%s, table=%s, column=%s→%s, pk=%s, partitions=%d, batchRows=%d, parallelism=%d"
                :
                "[MCE] unmigrated sentinel check start: schema=%s, table=%s, column=%s→%s, pk=%s, partitions=%d, batchRows=%d, parallelism=%d",
            schemaName, tableName, columnName, addrColumnName, primaryKeys, physicalPartitions.size(), batchRows,
            parallelism));
        long checkedRows = checkPartitions(
            physicalPartitions, primaryKeys, executionContext, checkBlobRefMd5, runtimeConfig);
        clearFailPointBeforeBlobRefMd5Check();
        MCE_LOGGER.info(String.format(checkBlobRefMd5
                ? "[MCE] BlobRef V2 MD5 check done: schema=%s, table=%s, column=%s, checkedRows=%d"
                : "[MCE] unmigrated sentinel check done: schema=%s, table=%s, column=%s, checkedRows=%d",
            schemaName, tableName, columnName, checkedRows));
    }

    private void injectFailPointBeforeBlobRefMd5Check(ExecutionContext executionContext) {
        if (MceTaskFailPoint.isEnabled(MceTaskFailPoint.FP_MCE_FAIL_BEFORE_MD5_CHECK, executionContext)) {
            setExceptionAction(DdlExceptionAction.PAUSE);
        }
        MceTaskFailPoint.failOnce(MceTaskFailPoint.FP_MCE_FAIL_BEFORE_MD5_CHECK,
            jobId(), taskId(), executionContext);
    }

    private void clearFailPointBeforeBlobRefMd5Check() {
        MceTaskFailPoint.clearFailOnce(
            MceTaskFailPoint.FP_MCE_FAIL_BEFORE_MD5_CHECK, jobId(), taskId());
    }

    private static void injectFailPointAfterMd5CheckBatch(ExecutionContext executionContext) {
        MceTaskFailPoint.pauseWhileEnabled(MceTaskFailPoint.FP_MCE_AFTER_MD5_CHECK_BATCH, executionContext);
    }

    private long checkPartitions(List<McePhysicalTableResolver.PhysicalTableTarget> physicalPartitions,
                                 List<String> primaryKeys, ExecutionContext ec, boolean checkBlobRefMd5,
                                 MceTaskRuntimeConfig runtimeConfig) {
        AtomicBoolean interrupted = new AtomicBoolean(false);
        McePartitionWorkerCoordinator<McePhysicalTableResolver.PhysicalTableTarget> coordinator =
            new McePartitionWorkerCoordinator<>(
                physicalPartitions,
                () -> runtimeConfig.checkerSettings().parallelism,
                (target, shouldYield) -> checkOnePartition(
                    target, primaryKeys, ec, checkBlobRefMd5, runtimeConfig, interrupted, shouldYield),
                () -> checkInterrupted(ec, interrupted),
                interrupted,
                MCE_LOGGER,
                "blob-md5-check job=" + jobId() + " task=" + taskId(),
                "[MCE] concurrent BlobRef V2 MD5 check failed: ");
        return coordinator.execute();
    }

    private McePartitionWorkerCoordinator.PartitionResult checkOnePartition(
        McePhysicalTableResolver.PhysicalTableTarget target,
        List<String> primaryKeys, ExecutionContext ec,
        boolean checkBlobRefMd5, MceTaskRuntimeConfig runtimeConfig,
        AtomicBoolean interrupted, BooleanSupplier shouldYield) {
        String phyTable = target.physicalTable;
        String partitionName = target.partitionName;
        String physicalDb = target.physicalDb;
        OmcStorageInfo storageInfo = target.storageInfo;

        failIfUnmigratedOrInvalidNullState(storageInfo, physicalDb, phyTable, primaryKeys, ec, "start");
        if (!checkBlobRefMd5) {
            MCE_LOGGER.info(String.format("[MCE] unmigrated sentinel check done: phyDb=%s, phyTable=%s",
                physicalDb, phyTable));
            return McePartitionWorkerCoordinator.PartitionResult.done(0L);
        }

        MceColumnStateRecord checkpoint = MceCheckpointHelper.loadOrCreate(jobId(), taskId(), schemaName, tableName,
            columnName, addrColumnName, physicalDb, phyTable, partitionName,
            MceColumnStateRecord.STATE_DUAL_WRITE, "task=blob-md5-check");
        if (checkpoint.getStatus() == MceColumnStateRecord.STATUS_SUCCESS) {
            MCE_LOGGER.info(String.format("[MCE] BlobRef V2 MD5 check partition already done: phyDb=%s, phyTable=%s",
                physicalDb, phyTable));
            return McePartitionWorkerCoordinator.PartitionResult.done(checkpoint.getProcessedRows());
        }

        List<ParameterContext> stopPk;
        if (isEmpty(checkpoint.getMaxPk())) {
            stopPk = queryStopPk(storageInfo, physicalDb, phyTable, primaryKeys, ec);
            if (stopPk == null) {
                failIfUnmigratedOrInvalidNullState(
                    storageInfo, physicalDb, phyTable, primaryKeys, ec, "end");
                MceCheckpointHelper.markSuccess(checkpoint);
                MCE_LOGGER.info(String.format("[MCE] BlobRef V2 MD5 check skip empty partition: phyDb=%s, phyTable=%s",
                    physicalDb, phyTable));
                return McePartitionWorkerCoordinator.PartitionResult.done(0L);
            }
            MceCheckpointHelper.initMaxPk(checkpoint, MceCheckpointCodec.encode(stopPk), MceCheckpointCodec.TYPE,
                "task=blob-md5-check");
        } else {
            stopPk = MceCheckpointCodec.decode(checkpoint.getMaxPk(), primaryKeys.size(), checkpoint.getPkType());
        }

        List<ParameterContext> lastPk = isEmpty(checkpoint.getLastPk()) ? null
            : MceCheckpointCodec.decode(checkpoint.getLastPk(), primaryKeys.size(), checkpoint.getPkType());
        long checkedRows = checkpoint.getProcessedRows();
        while (true) {
            checkInterrupted(ec, interrupted);
            int batchRows = runtimeConfig.checkerSettings().batchRows;
            List<List<ParameterContext>> batchPks =
                queryNextBatchPks(storageInfo, physicalDb, phyTable, primaryKeys, lastPk, stopPk, batchRows, ec);
            if (batchPks.isEmpty()) {
                break;
            }
            List<ParameterContext> batchStopPk = batchPks.get(batchPks.size() - 1);
            List<MismatchRow> mismatches = queryMismatchRows(storageInfo, physicalDb, phyTable, primaryKeys,
                lastPk, batchStopPk, ec);
            if (!mismatches.isEmpty()) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    String.format("[MCE] BlobRef V2 MD5 check failed: schema=%s, table=%s, column=%s, "
                            + "phyDb=%s, phyTable=%s, firstErrors=%s",
                        schemaName, tableName, columnName, physicalDb, phyTable,
                        mismatches.stream().map(MismatchRow::toString).collect(Collectors.joining("; "))));
            }
            checkedRows += batchPks.size();
            lastPk = batchStopPk;
            MceCheckpointHelper.saveProgress(checkpoint, MceCheckpointCodec.encode(lastPk), checkedRows);
            injectFailPointAfterMd5CheckBatch(ec);
            checkInterrupted(ec, interrupted);
            if (shouldYield.getAsBoolean()) {
                return McePartitionWorkerCoordinator.PartitionResult.yielded(checkedRows);
            }
        }

        failIfUnmigratedOrInvalidNullState(storageInfo, physicalDb, phyTable, primaryKeys, ec, "end");
        MceCheckpointHelper.markSuccess(checkpoint);
        MCE_LOGGER.info(
            String.format("[MCE] BlobRef V2 MD5 check partition done: phyDb=%s, phyTable=%s, checkedRows=%d",
                physicalDb, phyTable, checkedRows));
        return McePartitionWorkerCoordinator.PartitionResult.done(checkedRows);
    }

    private List<ParameterContext> queryStopPk(OmcStorageInfo storageInfo, String physicalDb, String phyTable,
                                               List<String> primaryKeys, ExecutionContext ec) {
        String sql = "SELECT " + MceCompositeKeySql.quotedColumns(primaryKeys)
            + " FROM " + MceCompositeKeySql.qualifiedTable(physicalDb, phyTable)
            + migratedBlobPredicate()
            + " ORDER BY " + MceCompositeKeySql.orderBy(primaryKeys, true) + " LIMIT 1";
        List<Map<Integer, ParameterContext>> rows = OmcUtils.queryWithNewConn(ec, storageInfo, sql);
        return rows.isEmpty() ? null
            : MceCompositeKeySql.extractTuple(rows.get(0), 1, primaryKeys.size(), "MCE checker stop key");
    }

    private List<List<ParameterContext>> queryNextBatchPks(OmcStorageInfo storageInfo, String physicalDb,
                                                           String phyTable, List<String> primaryKeys,
                                                           List<ParameterContext> lastPk,
                                                           List<ParameterContext> stopPk, int batchRows,
                                                           ExecutionContext ec) {
        MceCompositeKeySql.PreparedPredicate range =
            MceCompositeKeySql.buildRangePredicate(primaryKeys, lastPk, stopPk);
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ").append(MceCompositeKeySql.quotedColumns(primaryKeys))
            .append(" FROM ").append(MceCompositeKeySql.qualifiedTable(physicalDb, phyTable))
            .append(migratedBlobPredicate())
            .append(" AND ").append(range.getSql())
            .append(" ORDER BY ").append(MceCompositeKeySql.orderBy(primaryKeys, false))
            .append(" LIMIT ").append(batchRows);

        List<Map<Integer, ParameterContext>> rows =
            OmcUtils.queryWithNewConn(ec, storageInfo, sql.toString(), range.getParams(), false, null);
        List<List<ParameterContext>> result = new ArrayList<>(rows.size());
        for (Map<Integer, ParameterContext> row : rows) {
            result.add(MceCompositeKeySql.extractTuple(row, 1, primaryKeys.size(), "MCE checker batch key"));
        }
        return result;
    }

    private List<MismatchRow> queryMismatchRows(OmcStorageInfo storageInfo, String physicalDb, String phyTable,
                                                List<String> primaryKeys, List<ParameterContext> lastPk,
                                                List<ParameterContext> batchStopPk, ExecutionContext ec) {
        MceCompositeKeySql.PreparedPredicate range =
            MceCompositeKeySql.buildRangePredicate(primaryKeys, lastPk, batchStopPk);
        String quotedAddrColumn = MceCompositeKeySql.quote(addrColumnName);
        String quotedContentColumn = MceCompositeKeySql.quote(columnName);
        String addrLenExpr = "LENGTH(" + quotedAddrColumn + ")";
        String hexCharsExpr = "(" + quotedAddrColumn
            + " REGEXP '^[0-9A-Fa-f]{" + BlobRef.HEX_LENGTH_V2 + "}$')";
        String lowercaseExpr = "(BINARY " + quotedAddrColumn
            + " = BINARY LOWER(" + quotedAddrColumn + "))";
        String canonicalExpr = "(" + hexCharsExpr + " AND " + lowercaseExpr + ")";
        String versionExpr = "SUBSTR(" + quotedAddrColumn + ", " + V2_VERSION_SQL_OFFSET + ", "
            + V2_VERSION_HEX_LENGTH + ")";
        String seqIdHexExpr = "SUBSTR(" + quotedAddrColumn + ", " + V2_SEQ_ID_SQL_OFFSET + ", "
            + V2_SEQ_ID_HEX_LENGTH + ")";
        String seqIdValueExpr = "CAST(CONV(" + seqIdHexExpr + ", 16, 10) AS UNSIGNED)";
        String slotAddrExpr = "SUBSTR(" + quotedAddrColumn + ", " + V2_SLOT_ADDR_SQL_OFFSET + ", "
            + V2_SLOT_ADDR_HEX_LENGTH + ")";
        String slotMarkerExpr = "SUBSTR(" + slotAddrExpr + ", 1, " + V2_SLOT_MARKER_HEX_LENGTH + ")";
        String slotLowBitsExpr = "SUBSTR(" + quotedAddrColumn + ", " + V2_SLOT_LOW_BITS_SQL_OFFSET + ", "
            + V2_SLOT_LOW_BITS_HEX_LENGTH + ")";
        String pageIdValueExpr = "((CAST(CONV(" + slotLowBitsExpr + ", 16, 10) AS UNSIGNED) >> "
            + BlobObjectId.SLOT_BITS + ") & " + BlobObjectId.BLOB_PAGE_ID_MASK + ")";
        String addrRawSizeExpr = "SUBSTR(" + quotedAddrColumn + ", " + V2_RAW_SIZE_SQL_OFFSET + ", "
            + V2_RAW_SIZE_HEX_LENGTH + ")";
        String addrRawSizeValueExpr = "CAST(CONV(" + addrRawSizeExpr + ", 16, 10) AS UNSIGNED)";
        String contentRawSizeExpr = "LOWER(LPAD(HEX(OCTET_LENGTH(" + quotedContentColumn + ")), "
            + V2_RAW_SIZE_HEX_LENGTH + ", '0'))";
        String addrMd5Expr = "SUBSTR(" + quotedAddrColumn + ", " + V2_RAW_MD5_SQL_OFFSET + ", "
            + V2_RAW_MD5_HEX_LENGTH + ")";
        String contentMd5Expr = "LOWER(MD5(" + quotedContentColumn + "))";

        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ").append(MceCompositeKeySql.quotedColumns(primaryKeys)).append(", ")
            .append(addrLenExpr).append(", ")
            .append(canonicalExpr).append(", ")
            .append(versionExpr).append(", ")
            .append(seqIdValueExpr).append(", ")
            .append(slotMarkerExpr).append(", ")
            .append(pageIdValueExpr).append(", ")
            .append(addrRawSizeExpr).append(", ")
            .append(contentRawSizeExpr).append(", ")
            .append(addrMd5Expr).append(", ")
            .append(contentMd5Expr)
            .append(" FROM ").append(MceCompositeKeySql.qualifiedTable(physicalDb, phyTable))
            .append(migratedBlobPredicate())
            .append(" AND ").append(range.getSql())
            .append(" AND (").append(quotedContentColumn).append(" IS NULL")
            .append(" OR ").append(addrLenExpr).append(" <> ").append(BlobRef.HEX_LENGTH_V2)
            .append(" OR NOT ").append(canonicalExpr)
            .append(" OR BINARY ").append(versionExpr).append(" <> '").append(V2_VERSION_HEX).append("'")
            .append(" OR ").append(seqIdValueExpr).append(" > ").append(Integer.MAX_VALUE)
            .append(" OR BINARY ").append(slotMarkerExpr).append(" <> '")
            .append(V2_SLOT_MARKER_HEX).append("'")
            .append(" OR ").append(pageIdValueExpr).append(" = 0")
            .append(" OR ").append(addrRawSizeValueExpr).append(" > ")
            .append(BlobPageFormat.MAX_LOGICAL_VALUE_BYTES)
            .append(" OR BINARY ").append(addrRawSizeExpr).append(" <> ").append(contentRawSizeExpr)
            .append(" OR BINARY ").append(addrMd5Expr).append(" <> ").append(contentMd5Expr)
            .append(") ORDER BY ").append(MceCompositeKeySql.orderBy(primaryKeys, false))
            .append(" LIMIT ").append(ERROR_LIMIT);

        List<Map<Integer, ParameterContext>> rows =
            OmcUtils.queryWithNewConn(ec, storageInfo, sql.toString(), range.getParams(), false, null);
        List<MismatchRow> result = new ArrayList<>(rows.size());
        for (Map<Integer, ParameterContext> row : rows) {
            List<ParameterContext> primaryKey =
                MceCompositeKeySql.extractTuple(row, 1, primaryKeys.size(), "MCE checker mismatch key");
            int valueOffset = primaryKeys.size();
            result.add(new MismatchRow(primaryKey,
                valueAt(row, valueOffset + 1), valueAt(row, valueOffset + 2),
                valueAt(row, valueOffset + 3), valueAt(row, valueOffset + 4),
                valueAt(row, valueOffset + 5), valueAt(row, valueOffset + 6),
                valueAt(row, valueOffset + 7), valueAt(row, valueOffset + 8),
                valueAt(row, valueOffset + 9), valueAt(row, valueOffset + 10)));
        }
        return result;
    }

    private String migratedBlobPredicate() {
        String quotedAddrColumn = MceCompositeKeySql.quote(addrColumnName);
        return " WHERE " + quotedAddrColumn + " IS NOT NULL AND " + quotedAddrColumn + " <> ''";
    }

    private void failIfUnmigratedOrInvalidNullState(OmcStorageInfo storageInfo, String physicalDb, String phyTable,
                                                    List<String> primaryKeys, ExecutionContext ec, String phase) {
        String quotedAddrColumn = MceCompositeKeySql.quote(addrColumnName);
        String quotedContentColumn = MceCompositeKeySql.quote(columnName);
        String sql = "SELECT " + MceCompositeKeySql.quotedColumns(primaryKeys)
            + " FROM " + MceCompositeKeySql.qualifiedTable(physicalDb, phyTable)
            + " WHERE " + quotedAddrColumn + " = ''"
            + " OR (" + quotedContentColumn + " IS NOT NULL AND " + quotedAddrColumn + " IS NULL)"
            + " ORDER BY " + MceCompositeKeySql.orderBy(primaryKeys, false) + " LIMIT 1";
        List<Map<Integer, ParameterContext>> rows = OmcUtils.queryWithNewConn(ec, storageInfo, sql);
        if (!rows.isEmpty()) {
            List<ParameterContext> primaryKey =
                MceCompositeKeySql.extractTuple(rows.get(0), 1, primaryKeys.size(), "MCE checker sentinel key");
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                String.format("[MCE] BlobRef V2 MD5 check found unmigrated or inconsistent content/addr state: "
                        + "schema=%s, table=%s, "
                        + "column=%s, phyDb=%s, phyTable=%s, phase=%s, pk=%s",
                    schemaName, tableName, columnName, physicalDb, phyTable, phase,
                    printableTuple(primaryKey)));
        }
    }

    private static Object valueAt(Map<Integer, ParameterContext> row, int columnIndex) {
        ParameterContext pc = row.get(columnIndex);
        return pc == null || pc.getArgs() == null || pc.getArgs().length < 2 ? null : pc.getArgs()[1];
    }

    private static String bytesToHex(byte[] bytes) {
        char[] chars = new char[bytes.length * 2];
        char[] hex = "0123456789abcdef".toCharArray();
        for (int i = 0; i < bytes.length; i++) {
            int v = bytes[i] & 0xFF;
            chars[i * 2] = hex[v >>> 4];
            chars[i * 2 + 1] = hex[v & 0x0F];
        }
        return new String(chars);
    }

    private static String oneByteHex(long value) {
        return bytesToHex(new byte[] {(byte) value});
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
        if (interrupted.get() || CrossEngineValidator.isJobInterrupted(ec) || Thread.currentThread().isInterrupted()) {
            long jobId = ec.getDdlJobId();
            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                "The job '" + jobId + "' has been cancelled");
        }
    }

    @Override
    protected String remark() {
        return String.format("|MCE BlobRef V2 MD5 check, table=%s, column=%s→%s",
            tableName, columnName, addrColumnName);
    }

    private static final class MismatchRow {
        private final List<ParameterContext> primaryKey;
        private final Object addrLength;
        private final Object canonicalLowerHex;
        private final Object versionHex;
        private final Object seqId;
        private final Object slotMarker;
        private final Object pageId;
        private final Object addrRawSize;
        private final Object contentRawSize;
        private final Object addrMd5;
        private final Object contentMd5;

        private MismatchRow(List<ParameterContext> primaryKey, Object addrLength, Object canonicalLowerHex,
                            Object versionHex, Object seqId, Object slotMarker, Object pageId,
                            Object addrRawSize, Object contentRawSize,
                            Object addrMd5, Object contentMd5) {
            this.primaryKey = primaryKey;
            this.addrLength = addrLength;
            this.canonicalLowerHex = canonicalLowerHex;
            this.versionHex = versionHex;
            this.seqId = seqId;
            this.slotMarker = slotMarker;
            this.pageId = pageId;
            this.addrRawSize = addrRawSize;
            this.contentRawSize = contentRawSize;
            this.addrMd5 = addrMd5;
            this.contentMd5 = contentMd5;
        }

        @Override
        public String toString() {
            return "{pk=" + printableTuple(primaryKey)
                + ", addrLen=" + printable(addrLength)
                + ", canonicalLowerHex=" + printable(canonicalLowerHex)
                + ", version=" + printable(versionHex)
                + ", seqId=" + printable(seqId)
                + ", slotMarker=" + printable(slotMarker)
                + ", pageId=" + printable(pageId)
                + ", addrRawSize=" + printable(addrRawSize)
                + ", contentRawSize=" + printable(contentRawSize)
                + ", addrMd5=" + printable(addrMd5)
                + ", contentMd5=" + printable(contentMd5) + "}";
        }

        private static String printable(Object value) {
            if (value == null) {
                return "NULL";
            }
            if (value instanceof byte[]) {
                return "X'" + bytesToHex((byte[]) value) + "'";
            }
            return value.toString();
        }
    }

    private static String printableTuple(List<ParameterContext> tuple) {
        return tuple.stream()
            .map(MceCompositeKeySql::value)
            .map(MismatchRow::printable)
            .collect(Collectors.joining(", ", "[", "]"));
    }
}
