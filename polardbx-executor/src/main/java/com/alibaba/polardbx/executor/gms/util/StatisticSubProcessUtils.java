package com.alibaba.polardbx.executor.gms.util;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.LoggerUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.thread.ExecutorUtil;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.statistic.ndv.NDVShardSketch;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.UpdateStatisticSyncAction;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.module.LogLevel;
import com.alibaba.polardbx.gms.module.Module;
import com.alibaba.polardbx.gms.module.ModuleLogInfo;
import com.alibaba.polardbx.gms.module.StatisticModuleLogUtil;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticResultSource;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableColumnStatistic;
import com.alibaba.polardbx.optimizer.config.table.statistic.inf.SystemTableTableStatistic;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertType;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertUtil;
import com.google.common.collect.Maps;
import org.apache.commons.lang.StringUtils;
import org.glassfish.jersey.internal.guava.Sets;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.Set;
import java.util.concurrent.ThreadPoolExecutor;

import static com.alibaba.polardbx.common.utils.GeneralUtil.unixTimeStamp;
import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.buildCardinalityAndNullCount;
import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.buildSkew;
import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.buildTopnAndHistogram;
import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.checkFailPoint;
import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.collectRowCountAll;
import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.getBoolFromEcIfNotNull;
import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.getFileStoreStatistic;
import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.getHistogramBucketSize;
import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.getIndexInfoFromGsi;
import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.getIndexInfoFromLocalIndex;
import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.getMasterStorageAllDnIds;
import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.getSampleRate;
import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.getTopology;
import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.isFileStore;
import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.scanAnalyze;
import static com.alibaba.polardbx.executor.gms.util.StatisticUtils.sumRowCount;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESS_END;
import static com.alibaba.polardbx.gms.module.LogPattern.PROCESS_START;
import static com.alibaba.polardbx.gms.module.LogPattern.UNEXPECTED;
import static com.alibaba.polardbx.gms.scheduler.ScheduledJobExecutorType.STATISTIC_ROWCOUNT_COLLECTION;
import static com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.getColumnMetas;

/**
 * 统计信息子任务：（1）collect row count （2）sample （3）hll （4）persist （5） sync
 * <p>
 * 1. 此类中的process如果执行失败或者因为异常情况提前返回，必须进行statistic alert
 * 2. 此类中的process中调用的函数必须执行成功或者报出异常。例如某个函数返回false，表示执行失败，那么process需要进行判断处理。不允许调用的某个函数内吞掉异常。
 * 3. 此类中的process都是针对单个表的统计信息收集过程，且收集过程中对异常是尽可能抛出的。
 * 所以上层统计模块对多表进行统计信息收集时，需要catch异常，避免单个表收集失败影响整个任务。因为process中已经进行了statistic alert，所以上层原则无需再进行statistic alert。
 *
 * @author pangzhaoxing
 */
public class StatisticSubProcessUtils {

    // Statistics logger
    public final static Logger logger = LoggerUtil.statisticsLogger;

    public static void collectCardinalityFromDn(String schemaName, String logicalTableName, ExecutionContext ec) {
        try {
            checkFailPoint(FailPointKey.FP_INJECT_IGNORE_STATISTIC_COLLECT_FROM_DN_EXCEPTION);

            //获取主表的统计信息
            StatisticManager.CacheLine cacheLine =
                StatisticManager.getInstance().getCacheLine(schemaName, logicalTableName, false);
            if (cacheLine == null) {
                return;
            }
            OptimizerContext op = OptimizerContext.getContext(schemaName);
            if (op == null) {
                return;
            }
            TableMeta tableMeta = op.getLatestSchemaManager().getTable(logicalTableName);
            if (tableMeta == null) {
                return;
            }
            //只支持单列一级分区
            List<List<String>> allLevelTableActualPartCols = tableMeta.getPartitionInfo().getAllLevelActualPartCols();
            String shardColumnName = null;
            if (allLevelTableActualPartCols != null && allLevelTableActualPartCols.size() >= 1
                && allLevelTableActualPartCols.get(0).size() == 1) {
                for (int i = 1; i < allLevelTableActualPartCols.size(); i++) {
                    if (!allLevelTableActualPartCols.get(i).isEmpty()) {
                        return;
                    }
                }
                shardColumnName = allLevelTableActualPartCols.get(0).get(0);
            }
            if (shardColumnName == null) {
                return;
            }

            if (isFileStore(schemaName, logicalTableName)) {
                return;
            } else {
                Map<String, Set<String>> topologyMap = NDVShardSketch.getTopology(schemaName, logicalTableName, op);
                if (topologyMap == null) {
                    String errMsg = schemaName + "," + logicalTableName + " topology is null";
                    OptimizerAlertUtil.statisticsAlert(schemaName, logicalTableName,
                        OptimizerAlertType.STATISTIC_COLLECT_CARDINALITY_FROM_DN_FAIL, ec, errMsg);
                    return;
                }
                StatisticModuleLogUtil.logNormal(PROCESS_START,
                    new String[] {"collect cardinality from dn", schemaName + "," + logicalTableName});

                Map<String, Long> columnNdv = new HashMap<>();
                for (Map.Entry<String, Set<String>> topologyEntry : topologyMap.entrySet()) {
                    String groupName = topologyEntry.getKey();
                    Set<String> physicalTableNames = topologyEntry.getValue();
                    IDataSource ds =
                        ExecutorContext.getContext(schemaName).getTopologyHandler().get(groupName).getDataSource();
                    try (IConnection connection = ds.getConnection()) {
                        for (String physicalTableName : physicalTableNames) {
                            Map<String, Long> columnNdvInPhyTb = new HashMap<>();
                            String showIndexStatSql = String.format("show index from %s", physicalTableName);
                            ResultSet rs = connection.createStatement().executeQuery(showIndexStatSql);
                            while (rs.next()) {
                                if (rs.getInt("Seq_in_index") != 1) {
                                    continue;
                                }
                                String columnName = rs.getString("Column_name");
                                if (columnName == null) {
                                    continue;
                                }
                                columnName = columnName.toLowerCase().trim();
                                long ndv = rs.getLong("Cardinality");
                                if (rs.wasNull()) {
                                    continue;
                                }
                                columnNdvInPhyTb.putIfAbsent(columnName, ndv);
                                columnNdvInPhyTb.put(columnName, Math.max(columnNdvInPhyTb.get(columnName), ndv));
                            }
                            for (Map.Entry<String, Long> entry : columnNdvInPhyTb.entrySet()) {
                                String columnName = entry.getKey();
                                long ndv = entry.getValue();
                                if (StringUtils.equalsIgnoreCase(entry.getKey(), shardColumnName)) {
                                    columnNdv.put(columnName, ndv + columnNdv.getOrDefault(columnName, 0L));
                                } else {
                                    columnNdv.putIfAbsent(columnName, ndv);
                                    columnNdv.put(columnName, Math.max(columnNdv.get(columnName), ndv));
                                }
                            }
                        }
                    } catch (SQLException e) {
                        throw new TddlNestableRuntimeException(e);
                    }
                }
                Map<String, Long> oldCardinalityMap = cacheLine.getCardinalityMap();
                for (Map.Entry<String, Long> entry : columnNdv.entrySet()) {
                    String columnName = entry.getKey();
                    long ndv = entry.getValue();
                    Long oldNdv = oldCardinalityMap != null ? oldCardinalityMap.get(columnName) : null;
                    if (oldNdv == null || oldNdv < ndv) {
                        cacheLine.setCardinality(columnName, ndv);
                        cacheLine.setCardinalitySource(columnName, StatisticResultSource.DN_STAT.name());
                    }
                }
                StatisticModuleLogUtil.logNormal(PROCESS_END,
                    new String[] {
                        "collect cardinality from dn",
                        schemaName + "," + logicalTableName + "," + JSON.toJSONString(columnNdv)});
            }
        } catch (Throwable e) {
            OptimizerAlertUtil.statisticsAlert(schemaName, logicalTableName,
                OptimizerAlertType.STATISTIC_COLLECT_CARDINALITY_FROM_DN_FAIL, ec, e);
            throw e;
        }

    }

    /**
     * sample table process
     * build statistic(Cardinality, NullCount, SkewCol, Topn, Histogram) setting to cache line
     */
    public static void sampleTableDdl(String schemaName, String logicalTableName, ExecutionContext ec) {
        try {
            StatisticModuleLogUtil.logNormal(PROCESS_START,
                new String[] {"statistic sample", schemaName + "," + logicalTableName});

            checkFailPoint(FailPointKey.FP_INJECT_IGNORE_SAMPLE_TASK_EXCEPTION);

            List<ColumnMeta> analyzeColumnList = getColumnMetas(false, schemaName, logicalTableName);
            if (analyzeColumnList == null || analyzeColumnList.isEmpty()) {
                String errMsg = "column meta is empty :" + schemaName + "," + logicalTableName;
                StatisticModuleLogUtil.logNormal(UNEXPECTED, new String[] {"statistic sample", errMsg});
                OptimizerAlertUtil.statisticsAlert(schemaName, logicalTableName,
                    OptimizerAlertType.STATISTIC_SAMPLE_FAIL, ec, errMsg);
                return;
            }

            StatisticManager.CacheLine cacheLine =
                StatisticManager.getInstance().getCacheLine(schemaName, logicalTableName);
            // delete cols statistic that not exists
            cacheLine.remainColumns(analyzeColumnList);

            /**
             * prepare
             */
            long rowCount = cacheLine.getRowCount();
            float sampleRate = getSampleRate(rowCount);

            if (sampleRate * rowCount >= Integer.MAX_VALUE) {
                String errMsg =
                    "Size of sampling is too large :" + schemaName + "," + logicalTableName + "," + rowCount;
                StatisticModuleLogUtil.logNormal(UNEXPECTED, new String[] {"statistic sample", errMsg});
                OptimizerAlertUtil.statisticsAlert(schemaName, logicalTableName,
                    OptimizerAlertType.STATISTIC_SAMPLE_FAIL, ec, errMsg);
                return;
            }

            long sampleSize = (int) (sampleRate * rowCount);
            int histogramBucketSize = getHistogramBucketSize(sampleSize);
            int maxSampleSize = InstConfUtil.getInt(ConnectionParams.HISTOGRAM_MAX_SAMPLE_SIZE);
            boolean collectCharHistogram = InstConfUtil.getBool(ConnectionParams.STATISTICS_COLLECT_HISTOGRAM_STRING);
            List<Row> rows = new ArrayList<>();

            /**
             * sample process
             */
            double sampleRateUp =
                scanAnalyze(schemaName, logicalTableName, analyzeColumnList, sampleRate, maxSampleSize, rows, true);

            // build statistic from sample rows, and set to cache line
            if (!FailPoint.isKeyEnable(FailPointKey.FP_INJECT_IGNORE_STATISTIC_SAMPLE_SKETCH)) {
                buildCardinalityAndNullCount(schemaName, logicalTableName, analyzeColumnList, rows, ec, rowCount);
                buildSkew(schemaName, logicalTableName, analyzeColumnList, rows, sampleRate);
                buildTopnAndHistogram(schemaName, logicalTableName, analyzeColumnList, rows, ec, sampleRate,
                    sampleRateUp,
                    histogramBucketSize, collectCharHistogram);
            }
            cacheLine.setSampleRate(sampleRate);

            if (getBoolFromEcIfNotNull(ec, ConnectionParams.ENABLE_COLLECT_CARDINALITY_FROM_DN)) {
                //利用dn的统计信息进行优化
                try {
                    collectCardinalityFromDn(schemaName, logicalTableName, ec);
                    TableMeta tableMeta =
                        OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName);
                    if (getBoolFromEcIfNotNull(ec, ConnectionParams.ENABLE_COLLECT_CARDINALITY_FROM_DN_FOR_GSI)
                        && tableMeta.getGsiPublished() != null) {
                        for (Map.Entry<String, GsiMetaManager.GsiIndexMetaBean> entry : tableMeta.getGsiPublished()
                            .entrySet()) {
                            collectCardinalityFromDn(schemaName, entry.getKey(), ec);
                        }
                    }

                } catch (Throwable e) {
                    logger.error(e);
                    if (FailPoint.isKeyEnable(FailPointKey.FP_INJECT_IGNORE_STATISTIC_COLLECT_FROM_DN_EXCEPTION)) {
                        throw e;
                    }
                }

            }

            cacheLine.setLastModifyTime(unixTimeStamp());

            StatisticModuleLogUtil.logNormal(PROCESS_END,
                new String[] {"statistic sample", schemaName + "," + logicalTableName});

            OptimizerAlertUtil.checkStatisticsMiss(schemaName, logicalTableName,
                StatisticManager.getInstance().getCacheLine(schemaName, logicalTableName), rows.size());

        } catch (Throwable e) {
            OptimizerAlertUtil.statisticsAlert(schemaName, logicalTableName, OptimizerAlertType.STATISTIC_SAMPLE_FAIL,
                ec, e);
            throw e;
        }
    }

    /**
     * collect table rowcount
     * build statistic(rowcount) setting to cache line
     */
    public static void collectRowCount(String schema, String logicalTableName, ExecutionContext ec)
        throws SQLException {
        try {
            checkFailPoint(FailPointKey.FP_INJECT_IGNORE_ROWCOUNT_TASK_EXCEPTION);

            long sum = 0;
            if (isFileStore(schema, logicalTableName)) {
                try {
                    Map<String, Long> fileStorageStatisticMap = getFileStoreStatistic(schema, logicalTableName);
                    sum = fileStorageStatisticMap.get("TABLE_ROWS");
                } catch (Throwable e) {
                    String remark = "file storage statistic collection rowcount error: " + e.getMessage();
                    StatisticModuleLogUtil.logWarning(UNEXPECTED,
                        new String[] {
                            STATISTIC_ROWCOUNT_COLLECTION + " FROM ANALYZE",
                            schema + "," + logicalTableName + ":" + remark}, e);
                    throw e;
                }
            } else {
                Map<String, Set<String>> topologyMap = getTopology(schema, logicalTableName);
                if (topologyMap == null) {
                    String errMsg = schema + "," + logicalTableName + " topology is null";
                    StatisticModuleLogUtil.logWarning(UNEXPECTED,
                        new String[] {STATISTIC_ROWCOUNT_COLLECTION + " FROM ANALYZE", errMsg});
                    OptimizerAlertUtil.statisticsAlert(schema, logicalTableName,
                        OptimizerAlertType.STATISTIC_COLLECT_ROWCOUNT_FAIL, ec, errMsg);
                    return;
                }

                Collection<String> tbls = Sets.newHashSet();
                topologyMap.values().stream().forEach(names -> tbls.addAll(names));

                Set<String> dnIds = getMasterStorageAllDnIds();
                Map<String, Map<String, Long>> rowCountMap = Maps.newHashMap();
                try {
                    for (String dnId : dnIds) {
                        Map<String, Map<String, Long>> rowRs = collectRowCountAll(dnId, tbls);
                        if (rowRs != null) {
                            rowCountMap.putAll(rowRs);
                        }
                    }
                } catch (Throwable e) {
                    String remark = "statistic collection rowcount error: " + e.getMessage();
                    StatisticModuleLogUtil.logWarning(UNEXPECTED,
                        new String[] {
                            STATISTIC_ROWCOUNT_COLLECTION + " FROM ANALYZE",
                            schema + "," + logicalTableName + ":" + remark}, e);
                    throw e;
                }

                sum = sumRowCount(topologyMap, rowCountMap);
            }

            StatisticManager.CacheLine cacheLine =
                StatisticManager.getInstance().getCacheLine(schema, logicalTableName);
            cacheLine.setRowCount(sum);

            StatisticModuleLogUtil.logNormal(PROCESS_END, new String[] {
                STATISTIC_ROWCOUNT_COLLECTION + " FROM ANALYZE", schema + "," + logicalTableName + ":" + sum});
        } catch (Throwable e) {
            OptimizerAlertUtil.statisticsAlert(schema, logicalTableName,
                OptimizerAlertType.STATISTIC_COLLECT_ROWCOUNT_FAIL, ec, e);
            throw e;
        }
    }

    /**
     * sketch table process , including hll, persist and sync
     * <p>
     * Executes the sketch creation or update process for the specified table.
     *
     * @param schema Database name
     * @param logicalTableName Logical table name
     * @param needRebuild Whether to force a rebuild of the sketches
     * @param ec Execution context
     * @throws TddlRuntimeException When the DDL job is interrupted
     */
    public static void sketchTableDdl(String schema, String logicalTableName, boolean needRebuild, ExecutionContext ec)
        throws Exception {
        try {
            checkFailPoint(FailPointKey.FP_INJECT_IGNORE_HLL_TASK_EXCEPTION);

            // don't sketch archive table
            if (isFileStore(schema, logicalTableName)) {
                return;
            }

            int hllParallelism = 1;
            ThreadPoolExecutor sketchHllExecutor = null;
            if (ec != null) {
                hllParallelism = ec.getParamManager().getInt(ConnectionParams.HLL_PARALLELISM);
                if (hllParallelism > 1) {
                    sketchHllExecutor = ExecutorUtil.createExecutor("SketchHllExecutor", hllParallelism);
                }
            }
            logger.warn(
                String.format("Sketch table %s.%s with parallelism: %d", schema, logicalTableName, hllParallelism));

            /**
             * handle columns inside index
             */
            Set<String> colDoneSet = Sets.newHashSet();
            TableMeta tableMeta =
                OptimizerContext.getContext(schema).getLatestSchemaManager().getTable(logicalTableName);

            Map<String, Set<String>> indexColsMap = new HashMap<>();
            getIndexInfoFromLocalIndex(tableMeta, colDoneSet, indexColsMap);
            getIndexInfoFromGsi(tableMeta, colDoneSet, indexColsMap);
            getIndexInfoFromColumnarIndex(schema, logicalTableName, ec, tableMeta, colDoneSet, indexColsMap);

            try {
                for (Set<String> cols : indexColsMap.values()) {
                    for (String colsName : cols) {
                        if (ec != null && CrossEngineValidator.isJobInterrupted(ec)) {
                            Long jobId = ec.getDdlJobId();
                            throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                                "The job '" + jobId + "' has been cancelled");
                        }
                        try {
                            if (needRebuild) {
                                // analyze table would rebuild full ndv sketch info
                                StatisticManager.getInstance()
                                    .rebuildShardParts(schema, logicalTableName, colsName, ec, sketchHllExecutor);
                            } else {
                                // schedule job only update ndv sketch info
                                StatisticManager.getInstance()
                                    .updateAllShardParts(schema, logicalTableName, colsName, ec, sketchHllExecutor);
                            }
                        } catch (Exception e) {
                            // 这里没有直接抛出，是因为当前列执行失败，但是其他列还有可能执行成功
                            OptimizerAlertUtil.statisticsAlert(schema, logicalTableName,
                                OptimizerAlertType.STATISTIC_HLL_FAIL, ec, e);
                            if (FailPoint.isKeyEnable(FailPointKey.FP_INJECT_IGNORE_STATISTIC_QUICK_FAIL)) {
                                throw e;
                            }
                        }
                    }
                }
            } finally {
                if (sketchHllExecutor != null) {
                    sketchHllExecutor.shutdown();
                }
            }

            StatisticModuleLogUtil.logNormal(PROCESS_END,
                new String[] {
                    "statistic sketch table ",
                    schema + "," + logicalTableName + ",is force:" + needRebuild + ",cols:" + indexColsMap});

        } catch (Throwable e) {
            OptimizerAlertUtil.statisticsAlert(schema, logicalTableName, OptimizerAlertType.STATISTIC_HLL_FAIL, ec, e);
            throw e;
        }
    }

    public static void getIndexInfoFromColumnarIndex(String schema,
                                                     String table,
                                                     ExecutionContext ec,
                                                     TableMeta tableMeta,
                                                     Set<String> colDoneSet,
                                                     Map<String, Set<String>> indexColsMap) {
        // handle all columns for columnar index
        List<String> columnarIndexNames = CBOUtil.getColumnarIndexNamesWithoutArchive(table, schema, ec);
        if (columnarIndexNames == null || columnarIndexNames.isEmpty()) {
            return;
        }

        // Get all column information from table meta with null check
        List<ColumnMeta> allColumns = tableMeta.getAllColumns();
        if (allColumns == null || allColumns.isEmpty()) {
            return;
        }

        // Use stream processing to optimize column filtering logic for better performance
        // Skip externalized columns — their physical storage is blob_addr (BIGINT),
        // collecting HLL on addresses yields meaningless NDV ≈ row_count
        Set<String> availableColumns = allColumns.stream()
            .filter(col -> !col.isExternalizedColumn())
            .map(ColumnMeta::getName)
            .filter(columnName -> !colDoneSet.contains(columnName))
            .collect(Collectors.toSet());

        // Return early if no available columns
        if (availableColumns.isEmpty()) {
            return;
        }

        // Use only the first columnar index for configuration
        String indexName = columnarIndexNames.iterator().next();
        indexColsMap.put(indexName, availableColumns);
    }

    /**
     * persist table and column statistic and update metadb.tables
     */
    public static void persistStatistic(String schema, String logicalTableName, boolean withColumnStatistic,
                                        ExecutionContext ec) throws SQLException {
        try {
            checkFailPoint(FailPointKey.FP_INJECT_IGNORE_PERSIST_TASK_EXCEPTION);

            long start = System.currentTimeMillis();
            ArrayList<SystemTableTableStatistic.Row> rowList = new ArrayList<>();
            StatisticManager.CacheLine cacheLine =
                StatisticManager.getInstance().getCacheLine(schema, logicalTableName);
            rowList.add(
                new SystemTableTableStatistic.Row(schema, logicalTableName.toLowerCase(), cacheLine.getRowCount(),
                    cacheLine.getLastModifyTime()));
            StatisticManager.getInstance().getSds().batchReplace(rowList);

            ArrayList<SystemTableColumnStatistic.Row> columnRowList = new ArrayList<>();
            if (withColumnStatistic && cacheLine.getCardinalityMap() != null
                && cacheLine.getHistogramMap() != null && cacheLine.getNullCountMap() != null) {
                for (String columnName : cacheLine.getCardinalityMap().keySet()) {
                    columnRowList.add(new SystemTableColumnStatistic.Row(schema,
                        logicalTableName.toLowerCase(),
                        columnName,
                        cacheLine.getCardinalityMap().get(columnName),
                        cacheLine.getHistogramMap().get(columnName),
                        cacheLine.getTopN(columnName),
                        cacheLine.getNullCountMap().get(columnName),
                        cacheLine.getSampleRate(),
                        cacheLine.getLastModifyTime(),
                        cacheLine.getExtend()));
                }
            }
            StatisticManager.getInstance().getSds().batchReplace(columnRowList);

            StatisticUtils.updateMetaDbInformationSchemaTables(schema, logicalTableName);

            long end = System.currentTimeMillis();
            ModuleLogInfo.getInstance().logRecord(Module.STATISTICS, PROCESS_END,
                new String[] {
                    "persist tables statistic:" + schema + "," + logicalTableName + "," + withColumnStatistic,
                    "consuming " + (end - start) / 1000.0 + " seconds"
                }, LogLevel.NORMAL
            );
        } catch (Throwable e) {
            OptimizerAlertUtil.statisticsAlert(schema, logicalTableName, OptimizerAlertType.STATISTIC_PERSIST_FAIL, ec,
                e);
        }
    }

    /**
     * sync statistic process
     */
    public static void syncUpdateStatistic(String schema, String logicalTableName, StatisticManager.CacheLine cacheLine,
                                           ExecutionContext ec) {
        try {
            checkFailPoint(FailPointKey.FP_INJECT_IGNORE_SYNC_TASK_EXCEPTION);

            /** sync other nodes */
            SyncManagerHelper.sync(
                new UpdateStatisticSyncAction(
                    schema,
                    logicalTableName,
                    cacheLine),
                schema, SyncScope.ALL, true);
        } catch (Throwable e) {
            OptimizerAlertUtil.statisticsAlert(schema, logicalTableName, OptimizerAlertType.STATISTIC_SYNC_FAIL, ec, e);
        }
    }

}
