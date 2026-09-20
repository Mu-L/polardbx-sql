package com.alibaba.polardbx.server.handler.pl.inner;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.SpillableArrayResultCursor;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.operator.spill.SpillerFactory;
import com.alibaba.polardbx.gms.metadb.table.ColumnarAppendedFilesAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarCheckpointsRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPurgeHistoryAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarPurgeHistoryRecord;
import com.alibaba.polardbx.gms.metadb.table.FilesAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.ServerConnection;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

/**
 * @author lijiu
 */
public class ColumnarSnapshotFilesProcedure extends BaseInnerProcedure {

    private static final Logger LOGGER = LoggerFactory.getLogger(ColumnarSnapshotFilesProcedure.class);

    /**
     * 固定长度文件
     */
    private static final int FILE_TYPE_FIXED = 0;
    /**
     * 追加写文件
     */
    private static final int FILE_TYPE_APPEND = 1;

    @Override
    public void execute(ServerConnection c, SQLCallStatement statement, ArrayResultCursor cursor) {
        //参数解析
        long tso = checkParameters(statement.getParameters(), statement);

        //返回结果
        cursor.addColumn("file_name", DataTypes.StringType);
        cursor.addColumn("file_length", DataTypes.LongType);
        cursor.addColumn("file_type", DataTypes.IntegerType);
        cursor.initMeta();
        fetchSnapshotFiles(tso, cursor::addRow);
    }

    @Override
    public ResultCursor getResultCursor(ServerConnection c, SQLCallStatement statement, String procedureName) {
        //参数解析
        long tso = checkParameters(statement.getParameters(), statement);

        //文件可能很多，使用支持spill到本地文件的cursor，超过内存阈值后写入本地临时文件
        SpillerFactory spillerFactory = null;
        if (ServiceProvider.getInstance().getServer() != null) {
            spillerFactory = ServiceProvider.getInstance().getServer().getSpillerFactory();
        }
        SpillableArrayResultCursor cursor = new SpillableArrayResultCursor(procedureName, spillerFactory,
            DynamicConfig.getInstance().getColumnarSnapshotSpillMemoryLimit());
        cursor.addColumn("file_name", DataTypes.StringType);
        cursor.addColumn("file_length", DataTypes.LongType);
        cursor.addColumn("file_type", DataTypes.IntegerType);
        cursor.initMeta();
        try {
            //流式获取文件，边读边写入cursor
            fetchSnapshotFiles(tso, cursor::addRow);
            cursor.completeWrite();
        } catch (Throwable t) {
            //失败时释放cursor持有的spill临时文件
            cursor.close(new ArrayList<>());
            throw t;
        }
        return cursor;
    }

    private long checkParameters(List<SQLExpr> params, SQLCallStatement statement) {
        if (params.size() != 1) {
            throw new IllegalArgumentException(statement.toString() + " parameters is not match 1 parameters");
        }
        if (!(params.get(0) instanceof SQLIntegerExpr)) {
            throw new IllegalArgumentException(statement.toString() + " parameters need Long number");
        }
        SQLIntegerExpr sqlIntegerExpr = (SQLIntegerExpr) params.get(0);
        long tso = sqlIntegerExpr.getNumber().longValue();
        if (tso < 0) {
            throw new IllegalArgumentException(statement.toString() + " parameters is invalid Long number, need >= 0");
        }
        return tso;
    }

    /**
     * 流式获取快照文件列表，逐行回调rowConsumer，行格式为{file_name, file_length, file_type}。
     * 输出不保证顺序，仅保证固定长度文件（orc、sst）先于追加写文件输出。
     * 是否包含主键索引相关文件（sst、pk_idx_log_meta、pk_idx_log）由动态参数
     * COLUMNAR_SNAPSHOT_INCLUDE_PK_INDEX_FILES控制，默认包含。
     */
    private void fetchSnapshotFiles(long binlogTso, Consumer<Object[]> rowConsumer) {
        boolean includePkIndexFiles = DynamicConfig.getInstance().isColumnarSnapshotIncludePkIndexFiles();
        long totalStartMillis = System.currentTimeMillis();
        //累计输出行数与文件总长度，供各阶段日志统计
        long[] totalRows = new long[1];
        long[] totalBytes = new long[1];
        Consumer<Object[]> countingConsumer = row -> {
            totalRows[0]++;
            totalBytes[0] += ((Number) row[1]).longValue();
            rowConsumer.accept(row);
        };
        //各阶段耗时与行数统计，结束时合并成一条日志输出
        StringBuilder phaseStats = new StringBuilder();
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            //1,根据行存的tso获取列存的tso
            long phaseStartMillis = System.currentTimeMillis();
            ColumnarCheckpointsAccessor checkpointsAccessor = new ColumnarCheckpointsAccessor();
            checkpointsAccessor.setConnection(metaDbConn);
            ColumnarCheckpointsRecord columnarCheckPoint;
            long waitTimeLimit = DynamicConfig.getInstance().getWaitForColumnarCommitMS();
            long waitTime = 0;
            while (true) {
                //循环等待1s查询，直到列存同步到该位点，或60s超时
                List<ColumnarCheckpointsRecord> checkpointsRecords =
                    checkpointsAccessor.queryColumnarTsoByBinlogTso(binlogTso);
                if (!checkpointsRecords.isEmpty()) {
                    columnarCheckPoint = checkpointsRecords.get(0);
                    break;
                }
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                waitTime += 1000;
                if (waitTime > waitTimeLimit) {
                    throw new RuntimeException(
                        "Wait for Columnar commit to " + binlogTso + ", wait " + waitTimeLimit + " ms timeout.");
                }
            }
            phaseStats.append("wait_checkpoint(checkpointTso=").append(columnarCheckPoint.checkpointTso)
                .append(")=").append(System.currentTimeMillis() - phaseStartMillis).append("ms, ");

            //2、根据列存版本tso获取所有有效文件，长度，类型
            //a、获取purge的水位线，当作下水位限
            phaseStartMillis = System.currentTimeMillis();
            ColumnarPurgeHistoryAccessor purgeHistoryAccessor = new ColumnarPurgeHistoryAccessor();
            purgeHistoryAccessor.setConnection(metaDbConn);
            List<ColumnarPurgeHistoryRecord> purgeRecords = purgeHistoryAccessor.queryLastPurgeTso();
            long purgeTso = 0;
            if (!purgeRecords.isEmpty()) {
                purgeTso = purgeRecords.get(0).tso;
            }
            phaseStats.append("query_purge_tso(purgeTso=").append(purgeTso).append(")=")
                .append(System.currentTimeMillis() - phaseStartMillis).append("ms, ");
            //b、直接files系统表扫描，COLUMNAR_TABLE_MAPPING_TABLE表直接关联，以防冷数据归档文件，这样获取的文件可能会多一些，精细化的话要列存版本tso需要排除ddl操作的表，在恢复时处理更好
            //先流式获取固定长度文件，orc文件
            phaseStartMillis = System.currentTimeMillis();
            long phaseStartRows = totalRows[0];
            long phaseStartBytes = totalBytes[0];
            FilesAccessor filesAccessor = new FilesAccessor();
            filesAccessor.setConnection(metaDbConn);
            filesAccessor.streamOrcFileInfoByTso(purgeTso, columnarCheckPoint.checkpointTso,
                record -> countingConsumer.accept(new Object[] {record.fileName, record.extentSize, FILE_TYPE_FIXED}));
            appendPhaseStats(phaseStats, "orc_files", phaseStartMillis, totalRows[0] - phaseStartRows,
                totalBytes[0] - phaseStartBytes);
            //主键索引sst文件，属于主键索引类别文件
            if (includePkIndexFiles) {
                phaseStartMillis = System.currentTimeMillis();
                phaseStartRows = totalRows[0];
                phaseStartBytes = totalBytes[0];
                filesAccessor.streamSstFileInfoByTso(purgeTso, columnarCheckPoint.checkpointTso,
                    record -> countingConsumer.accept(
                        new Object[] {record.fileName, record.extentSize, FILE_TYPE_FIXED}));
                appendPhaseStats(phaseStats, "sst_files", phaseStartMillis, totalRows[0] - phaseStartRows,
                    totalBytes[0] - phaseStartBytes);
            }
            // 快照表csv,del文件获取,append记录清理了的文件
            phaseStartMillis = System.currentTimeMillis();
            phaseStartRows = totalRows[0];
            phaseStartBytes = totalBytes[0];
            filesAccessor.streamSnapshotCsvDelFileInfoByTso(purgeTso, columnarCheckPoint.checkpointTso,
                record -> countingConsumer.accept(new Object[] {record.fileName, record.extentSize, FILE_TYPE_APPEND}));
            appendPhaseStats(phaseStats, "snapshot_csv_del_files", phaseStartMillis, totalRows[0] - phaseStartRows,
                totalBytes[0] - phaseStartBytes);

            //c、再获取追加写文件，csv文件、del文件、set文件，也需获取长度
            phaseStartMillis = System.currentTimeMillis();
            phaseStartRows = totalRows[0];
            phaseStartBytes = totalBytes[0];
            ColumnarAppendedFilesAccessor columnarAppendedFilesAccessor = new ColumnarAppendedFilesAccessor();
            columnarAppendedFilesAccessor.setConnection(metaDbConn);
            columnarAppendedFilesAccessor.streamLastValidAppendByStartTsoAndEndTso(purgeTso,
                columnarCheckPoint.checkpointTso,
                record -> {
                    long fileLength = record.getAppendOffset() + record.getAppendLength();
                    if (fileLength == 0) {
                        //过滤长度为0的文件
                        return;
                    }
                    countingConsumer.accept(new Object[] {record.fileName, fileLength, FILE_TYPE_APPEND});
                });
            appendPhaseStats(phaseStats, "last_valid_append_files", phaseStartMillis, totalRows[0] - phaseStartRows,
                totalBytes[0] - phaseStartBytes);

            if (includePkIndexFiles) {
                //d、获取主键索引的log meta文件、log文件和长度
                for (String fileType : new String[] {"pk_idx_log_meta", "pk_idx_log"}) {
                    phaseStartMillis = System.currentTimeMillis();
                    phaseStartRows = totalRows[0];
                    phaseStartBytes = totalBytes[0];
                    columnarAppendedFilesAccessor.streamFilesByFileType(fileType,
                        record -> {
                            long fileLength = record.getAppendOffset() + record.getAppendLength();
                            if (fileLength == 0) {
                                //过滤长度为0的文件
                                return;
                            }
                            countingConsumer.accept(new Object[] {record.fileName, fileLength, FILE_TYPE_APPEND});
                        });
                    appendPhaseStats(phaseStats, fileType + "_files", phaseStartMillis,
                        totalRows[0] - phaseStartRows, totalBytes[0] - phaseStartBytes);
                }
            }
            LOGGER.warn("columnar_snapshot_files: fetch snapshot files done, binlogTso=" + binlogTso
                + ", includePkIndexFiles=" + includePkIndexFiles + ", " + phaseStats + "total " + totalRows[0]
                + " rows, total size " + formatSize(totalBytes[0]) + ", total cost "
                + (System.currentTimeMillis() - totalStartMillis) + " ms");
        } catch (Exception e) {
            //失败时输出已完成阶段的统计，便于定位失败阶段
            LOGGER.warn("columnar_snapshot_files: fetch snapshot files failed, binlogTso=" + binlogTso
                + ", finished phases: " + phaseStats + "total " + totalRows[0] + " rows, total size "
                + formatSize(totalBytes[0]) + ", total cost "
                + (System.currentTimeMillis() - totalStartMillis) + " ms", e);
            throw new RuntimeException(e);
        }
    }

    /**
     * 追加单个阶段的SQL耗时、新增行数与新增文件长度统计，格式：phase=耗时ms/行数rows/长度(自动单位)
     */
    private static void appendPhaseStats(StringBuilder phaseStats, String phase, long phaseStartMillis,
                                         long phaseRows, long phaseBytes) {
        phaseStats.append(phase).append("=").append(System.currentTimeMillis() - phaseStartMillis).append("ms/")
            .append(phaseRows).append("rows/").append(formatSize(phaseBytes)).append(", ");
    }

    /**
     * 字节数自动单位化，B/KB/MB/GB/TB，保留两位小数
     */
    private static String formatSize(long bytes) {
        if (bytes < 1024) {
            return bytes + "B";
        }
        String[] units = {"KB", "MB", "GB", "TB"};
        double size = bytes;
        int unitIndex = -1;
        while (size >= 1024 && unitIndex < units.length - 1) {
            size /= 1024;
            unitIndex++;
        }
        return String.format("%.2f%s", size, units[unitIndex]);
    }
}
