package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.polardbx.common.ColumnarOptions;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ColumnarConfig;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.time.core.OriginalTimestamp;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.common.utils.timezone.TimeZoneUtils;
import com.alibaba.polardbx.druid.util.StringUtils;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.exception.TtlJobRuntimeException;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.log.TtlLoggerUtil;
import com.alibaba.polardbx.executor.ddl.job.task.ttl.scheduler.TtlScheduledJobStatManager;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineDagExecutor;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineDagExecutorMap;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.alibaba.polardbx.executor.ddl.newengine.serializable.SerializableClassMapper;
import com.alibaba.polardbx.executor.ddl.newengine.utils.TaskHelper;
import com.alibaba.polardbx.executor.scheduler.executor.TtlArchivedDataScheduledJob;
import com.alibaba.polardbx.executor.utils.PartitionMetaUtil;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineTaskRecord;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.server.DefaultServerConfigManager;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartKeyLevel;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionField;
import com.alibaba.polardbx.optimizer.partition.datatype.PartitionFieldBuilder;
import com.alibaba.polardbx.optimizer.partition.pruning.PartPrunedResult;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStep;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruneStepBuilder;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPruner;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.ttl.RefreshArcTblViewContext;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import com.alibaba.polardbx.optimizer.ttl.TtlTimeUnit;
import com.alibaba.polardbx.optimizer.ttl.TtlUtil;
import com.alibaba.polardbx.optimizer.utils.OptimizerHelper;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.jetbrains.annotations.NotNull;

import java.io.UnsupportedEncodingException;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author chenghui.lch
 */
public class TtlJobUtil {

    /**
     * The formatter of ISO_LOCAL_DATE_TIME: yyyy-MM-dd HH:mm:ss
     */
    public static final DateTimeFormatter ISO_DATETIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    public static List<TtlIntraTaskRunner> zigzagSortTasks(List<TtlIntraTaskRunner> taskRunners) {

        Map<String, List<TtlIntraTaskRunner>> dn2RunnersMapping = groupTasksByDnId(taskRunners);

        Map<String, Iterator<TtlIntraTaskRunner>> oldDnGrpItorMapping =
            new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        for (Map.Entry<String, List<TtlIntraTaskRunner>> taskItem : dn2RunnersMapping.entrySet()) {
            String dnId = taskItem.getKey();
            List<TtlIntraTaskRunner> queue = taskItem.getValue();
            oldDnGrpItorMapping.put(dnId, queue.iterator());
        }

        List<TtlIntraTaskRunner> zigzagTaskList = new ArrayList<>();
        while (true) {
            if (oldDnGrpItorMapping.isEmpty()) {
                break;
            }
            List<String> dnIdSetWithoutNextRunner = new ArrayList<>();
            for (Map.Entry<String, Iterator<TtlIntraTaskRunner>> taskItem : oldDnGrpItorMapping.entrySet()) {
                String dnId = taskItem.getKey();
                Iterator<TtlIntraTaskRunner> queueItor = taskItem.getValue();
                if (queueItor.hasNext()) {
                    zigzagTaskList.add(queueItor.next());
                } else {
                    dnIdSetWithoutNextRunner.add(dnId);
                }
            }
            for (int i = 0; i < dnIdSetWithoutNextRunner.size(); i++) {
                oldDnGrpItorMapping.remove(dnIdSetWithoutNextRunner.get(i));
            }
        }
        return zigzagTaskList;
    }

    /**
     * Key: dnId
     * Val: phyPart runners of same dnId
     */
    protected static Map<String, List<TtlIntraTaskRunner>> groupTasksByDnId(List<TtlIntraTaskRunner> taskRunners) {

        Map<String, List<TtlIntraTaskRunner>> dn2RunnersMapping = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        for (int i = 0; i < taskRunners.size(); i++) {

            TtlIntraTaskRunner runner = taskRunners.get(i);
            String dnId = runner.getDnId();

            List<TtlIntraTaskRunner> runners = dn2RunnersMapping.get(dnId);
            if (runners == null) {
                runners = new LinkedList<>();
                dn2RunnersMapping.put(dnId, runners);
            }
            runners.add(runner);
        }

        return dn2RunnersMapping;
    }

    public static <R> R wrapWithDistributedTrx(IServerConfigManager serverMgr,
                                               String schemaName,
                                               Map<String, Object> sessionVariables,
                                               Function<Object, R> caller) {

        R result = null;
        Object transConn = null;
        try {
            transConn = serverMgr.getTransConnection(schemaName, sessionVariables);
            serverMgr.transConnectionBegin(transConn);
            result = caller.apply(transConn);
            serverMgr.transConnectionCommit(transConn);
        } catch (SQLException ex) {
            if (transConn != null) {
                try {
                    serverMgr.transConnectionRollback(transConn);
                } catch (Throwable err) {
                    // ignore
                }
            }
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, ex);
        } finally {
            if (null != transConn) {
                try {
                    serverMgr.closeTransConnection(transConn);
                } catch (Throwable ex) {
                    // ignore
                }
            }
        }
        return result;
    }

    public static IServerConfigManager getServerConfigManager() {
        IServerConfigManager serverConfigManager = OptimizerHelper.getServerConfigManager();
        if (serverConfigManager == null) {
            serverConfigManager = new DefaultServerConfigManager(null);
        }
        return serverConfigManager;
    }

    public static int execLogicalDmlOnInnerConnection(IServerConfigManager serverMgr,
                                                      String schemaName,
                                                      Object transConn,
                                                      ExecutionContext ec,
                                                      String sql) {
        /**
         * Set timezone & sql_mode
         */
//            serverMgr.executeBackgroundDmlByTransConnection(sql, schemaName, null,  transConn);

        /**
         * Exec dml
         */
        int rows = serverMgr.executeBackgroundDmlByTransConnection(sql, schemaName, null, transConn);
        return rows;
    }

    public static List<Map<String, Object>> execLogicalQueryOnInnerConnection(IServerConfigManager serverMgr,
                                                                              String schemaName,
                                                                              Object transConn,
                                                                              ExecutionContext ec,
                                                                              String sql) {

        /**
         * Exec query
         //         */
        List<Map<String, Object>> results =
            serverMgr.executeBackgroundQueryByTransConnection(sql, schemaName, null, transConn);
        return results;
    }

    public static TtlJobContext fetchTtlJobContextFromPreviousTaskByTaskName(Long jobId,
                                                                             Class previousTaskClass,
                                                                             String schemaName,
                                                                             String tableName) {
        DdlJobManager jobManager = new DdlJobManager();
        String previousTaskName = SerializableClassMapper.getNameByTaskClass(previousTaskClass);
        List<DdlTask> prevTasks = jobManager.getTasksFromMetaDB(jobId, previousTaskName);
        if (prevTasks.isEmpty()) {
            throw new TtlJobRuntimeException(
                String.format("No found previous task for %s.%s, task name is %s", schemaName, tableName,
                    previousTaskName));
        }
        assert prevTasks.size() == 1;
        AbstractTtlJobTask previousTask = (AbstractTtlJobTask) prevTasks.get(0);
        TtlJobContext jobContext = previousTask.getJobContext();
        return jobContext;
    }

    public static long updateSubJobTaskIdAndStmtByJobIdAndOldStmt(Long jobId,
                                                                  String schemaName,
                                                                  String tableName,
                                                                  String oldDdlStmt,
                                                                  Long newSubId,
                                                                  String newDdlStmt,
                                                                  String newDdlStmtForRollback,
                                                                  Connection metaDbConn
    ) {
        DdlJobManager jobManager = new DdlJobManager();
        String subJobTaskName = SubJobTask.getTaskName();
        List<DdlTask> allTasks = jobManager.getTasksFromMetaDB(jobId, subJobTaskName);

        if (allTasks.isEmpty()) {
            throw new TtlJobRuntimeException(
                String.format("No found any posterior task [oldStmt:%s] for job[%s] of [%s.%s]", oldDdlStmt, jobId,
                    schemaName, tableName));
        }

        List<DdlTask> targetSubJobTasks = new ArrayList<>();
        for (int i = 0; i < allTasks.size(); i++) {
            DdlTask task = allTasks.get(i);
            String taskName = task.getName();
            if (!taskName.equalsIgnoreCase(SubJobTask.getTaskName())) {
                continue;
            }
            SubJobTask subJobTask = (SubJobTask) task;
            String ddlStmt = subJobTask.getDdlStmt();
            if (ddlStmt.equalsIgnoreCase(oldDdlStmt)) {
                targetSubJobTasks.add(task);
            }
        }

        if (targetSubJobTasks.size() != 1) {
            throw new TtlJobRuntimeException(
                String.format("Invalid to find more than one posterior task [oldStmt:%s] for job[%s] of [%s.%s]",
                    oldDdlStmt, jobId,
                    schemaName, tableName));
        }
        SubJobTask subJobTask = (SubJobTask) targetSubJobTasks.get(0);
        Long targetSubJobTaskId = subJobTask.getTaskId();

        /**
         * Update the mem obj of subjob task of current job
         */
        DdlEngineDagExecutor ddlEngineDagExecutorOfCurrJob = DdlEngineDagExecutorMap.get(schemaName, jobId);
        DdlJob ddlJob = ddlEngineDagExecutorOfCurrJob.getDdlJob();
        DdlTask targetTask = ddlJob.getTaskById(targetSubJobTaskId);
        SubJobTask targetSubJobTask = null;
        if (targetTask == null) {
            throw new TtlJobRuntimeException(
                String.format("No found any posterior subtask [oldStmt:%s] for current jobObj[%s] of [%s.%s]",
                    oldDdlStmt, jobId,
                    schemaName, tableName));
        }
        if (targetTask instanceof SubJobTask) {
            targetSubJobTask = (SubJobTask) targetTask;
            if (newSubId != null) {
                targetSubJobTask.setSubJobId(newSubId);
            }

            if (!StringUtils.isEmpty(newDdlStmt)) {
                targetSubJobTask.setDdlStmt(newDdlStmt);
            }
            if (!StringUtils.isEmpty(newDdlStmtForRollback)) {
                targetSubJobTask.setRollbackDdlStmt(newDdlStmtForRollback);
            }
        }

        /**
         * Convert newTask to taskRecord to store into metadb
         */
        DdlEngineTaskRecord targetSubJobTaskRecord = TaskHelper.toDdlEngineTaskRecord(targetSubJobTask);

        /**
         * Update ddl_engine_task of Metadb by MetaDbConn
         */
        DdlEngineTaskAccessor taskAccessor = new DdlEngineTaskAccessor();
        taskAccessor.setConnection(metaDbConn);
        taskAccessor.updateTask(targetSubJobTaskRecord);

        return targetSubJobTaskId;
    }

    public static List<PhysicalPartitionInfo> routeTargetPartsOfTtlTmpTblByTtlColVal(ExecutionContext ec,
                                                                                     TableMeta arcTmpTableMeta,
                                                                                     String ttlTimezone,
                                                                                     String ttlColValStr) {
        PartitionInfo arcTmpTblPartInfo = arcTmpTableMeta.getPartitionInfo();
        InternalTimeZone internalTimeZone = TimeZoneUtils.convertFromMySqlTZ(ttlTimezone);
        ec.setTimeZone(internalTimeZone);
        List<Object> pointValue = new ArrayList<>();
        pointValue.add(ttlColValStr);
        List<DataType> pointValueOpTypes = new ArrayList<>();
        pointValueOpTypes.add(DataTypes.StringType);
        ExecutionContext[] newEcOutput = new ExecutionContext[1];
        RelDataTypeFactory type = PartitionPrunerUtils.getTypeFactory();
        RelDataType tbRelRowType = arcTmpTableMeta.getPhysicalRowType(type);
        PartitionPruneStep pruneStep = PartitionPruneStepBuilder.genPointSelectPruneStepInfoForTtlRouting(pointValue,
            pointValueOpTypes, ec, newEcOutput, arcTmpTblPartInfo, PartKeyLevel.PARTITION_KEY, tbRelRowType);

        PartPrunedResult prunedResult = PartitionPruner.doPruningByStepInfo(pruneStep, newEcOutput[0]);
        List<PhysicalPartitionInfo> phyPartList = prunedResult.getPrunedPartitions();

        return phyPartList;
    }

    public static LocalDateTime plusDeltaIntervals(LocalDateTime normalizedDatetime,
                                                   TtlTimeUnit intervalUnit, int intervalCount) {
        LocalDateTime result = null;
        switch (intervalUnit) {
        case YEAR:
            result = normalizedDatetime.plusYears(intervalCount);
            break;
        case MONTH:
            result = normalizedDatetime.plusMonths(intervalCount);
            break;
        case WEEK:
            result = normalizedDatetime.plusWeeks(intervalCount);
            break;
        case DAY:
            result = normalizedDatetime.plusDays(intervalCount);
            break;
        case HOUR:
            result = normalizedDatetime.plusHours(intervalCount);
            break;
        case MINUTE:
            result = normalizedDatetime.plusMinutes(intervalCount);
            break;
        case SECOND:
            result = normalizedDatetime.plusSeconds(intervalCount);
            break;
        }
        return result;
    }

    /**
     * Find the actual table name from arc cci of tableMeta of ttlTable
     */
    public static String getActualTableNameForArcCci(TableMeta ttlTblMeta,
                                                     ExecutionContext ec) {

        TtlDefinitionInfo ttlInfo = ttlTblMeta.getTtlDefinitionInfo();
        String arcTmpTblSchema = ttlInfo.getTmpTableSchema();
        String arcTmpTblName = ttlInfo.getTmpTableName();
        String arcTmpTblNameLowercase = "";
        if (!StringUtils.isEmpty(arcTmpTblName)) {
            arcTmpTblNameLowercase = arcTmpTblName.toLowerCase();
        }
        String actualTblNameOfCci = null;
//        if (StringUtils.isEmpty(arcTmpTblName)) {
//            return actualTblNameOfCci;
//        }
        Map<String, GsiMetaManager.GsiIndexMetaBean> allArcCciPublished = ttlTblMeta.getArchiveColumnarIndexPublished();

        /**
         * A switch used by testcases only for using gsi index instead of cci index, default is false
         */
        boolean useGsiInsteadOfCci = TtlConfigUtil.isUseGsiInsteadOfCciForCreateColumnarArcTbl(ec);
        if (useGsiInsteadOfCci) {
            allArcCciPublished = ttlTblMeta.getGsiPublished();
        }

        if (allArcCciPublished != null) {
            /**
             * For each cci, just find the arc cci which is started with arcTmpTblName
             */
            for (String cciTblName : allArcCciPublished.keySet()) {
                String cciTblNameLowerCase = cciTblName.toLowerCase();
                if (!cciTblNameLowerCase.startsWith(arcTmpTblNameLowercase)) {
                    continue;
                }
                actualTblNameOfCci = cciTblNameLowerCase;
                break;
            }
        }
        return actualTblNameOfCci;
    }

    public static String getActualTableNameAndCheckCciTypeForArcCci(TableMeta ttlTblMeta,
                                                                    ExecutionContext ec) {
        String tblNameOfCci = getActualTableNameForArcCci(ttlTblMeta, ec);
        if (StringUtils.isEmpty(tblNameOfCci)) {
            return tblNameOfCci;
        }

        String finalTblNameOfCci = tblNameOfCci;
        Map<String, GsiMetaManager.GsiIndexMetaBean> allArcCciPublished = ttlTblMeta.getArchiveColumnarIndexPublished();
        GsiMetaManager.GsiIndexMetaBean cciMeta = allArcCciPublished.get(tblNameOfCci);
        String columnarType = cciMeta.columnarOptions.get().get(ColumnarOptions.TYPE);
        if (columnarType == null || !columnarType.equalsIgnoreCase(ColumnarConfig.ARCHIVE)) {
            finalTblNameOfCci = null;
        }
        return finalTblNameOfCci;
    }

    public static TableMeta getLatestTableMetaBySchemaNameAndTableName(String schemaName, String tableName) {
        return OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(tableName);
    }

    public static boolean checkIfArcTmlTableDataLengthExceedLimit(long arcTmpTblDataLength) {
        if (arcTmpTblDataLength > TtlConfigUtil.maxTtlTmpTableDataLength) {
            return true;
        }
        return false;
    }

    public static long fetchArcCciTableDataLength(ExecutionContext ec,
                                                  TtlJobContext jobContext,
                                                  List<String> targetPartNames) {
        String arcTmpTblSchema = jobContext.getTtlInfo().getTtlInfoRecord().getArcTmpTblSchema();
        String arcTmpTblName = jobContext.getTtlInfo().getTtlInfoRecord().getArcTmpTblName();
        TopologyHandler topologyHandler = ExecutorContext.getContext(arcTmpTblSchema).getTopologyHandler();
        TableMeta ttlTmpTblTableMeta =
            TtlJobUtil.getLatestTableMetaBySchemaNameAndTableName(arcTmpTblSchema, arcTmpTblName);
        PartitionInfo ttlTmpTblPartInfo = ttlTmpTblTableMeta.getPartitionInfo();
        Long dataLength =
            TtlJobUtil.fetchArcTmpTblPartDataLength(ec, topologyHandler, ttlTmpTblPartInfo, targetPartNames);
        return dataLength;
    }

    protected static Long fetchArcTmpTblPartDataLength(
        ExecutionContext ec,
        TopologyHandler topology,
        PartitionInfo ttlTmpTblPartInfo,
        List<String> targetFirstLevelPartNames
    ) {

        Long dataLength = 0L;
        Map<String, List<String>> phyGrpToPhyTbListMapping = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        Map<String, String> phyGrpToPhyDbMapping = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        final AtomicLong dataLenOfTtlTmpTbl = new AtomicLong(0);
        final AtomicLong tableRowsOfTtlTmpTbl = new AtomicLong(0);
        List<PartitionSpec> partSpecList = ttlTmpTblPartInfo.getPartitionBy().getPartitions();
        final String ttlTmpTblSchema = ttlTmpTblPartInfo.getTableSchema();
        final String tblTmpTblName = ttlTmpTblPartInfo.getTableName();
        final Set<String> targetFirstLevelPartNameSet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        if (targetFirstLevelPartNames != null) {
            for (int i = 0; i < targetFirstLevelPartNames.size(); i++) {
                String part = targetFirstLevelPartNames.get(i);
                if (!targetFirstLevelPartNameSet.contains(part)) {
                    targetFirstLevelPartNameSet.add(part);
                }
            }
        }

        for (int k = 0; k < partSpecList.size(); k++) {
            PartitionSpec partSpec = partSpecList.get(k);
            String partName = partSpec.getName();
            if (targetFirstLevelPartNames != null && !targetFirstLevelPartNameSet.isEmpty()) {
                if (!targetFirstLevelPartNameSet.contains(partName)) {
                    continue;
                }
            }
            List<PartitionSpec> subPartSpecs = partSpec.getSubPartitions();
            for (int i = 0; i < subPartSpecs.size(); i++) {
                PartitionSpec subPartSpec = subPartSpecs.get(i);
                String grpKey = subPartSpec.getLocation().getGroupKey();
                String phyTb = subPartSpec.getLocation().getPhyTableName();
                String phyDb = phyGrpToPhyDbMapping.get(grpKey);
                if (phyDb == null) {
                    phyDb = PartitionMetaUtil.getPhyDbByGroupName(topology, grpKey);
                    phyGrpToPhyDbMapping.put(grpKey, phyDb);
                }
                List<String> phyTbList = phyGrpToPhyTbListMapping.get(grpKey);
                if (phyTbList == null) {
                    phyTbList = new ArrayList<>();
                    phyGrpToPhyTbListMapping.put(grpKey, phyTbList);
                }
                phyTbList.add(phyTb);
            }
        }

        for (Map.Entry<String, List<String>> phyGrpToPhyTbListItem : phyGrpToPhyTbListMapping.entrySet()) {
            String grpKey = phyGrpToPhyTbListItem.getKey();
            List<String> phyTbList = phyGrpToPhyTbListItem.getValue();
            String phyDb = phyGrpToPhyDbMapping.get(grpKey);
            String querySql = TtlTaskSqlBuilder.buildQueryPhyInfoSchemaTablesByGroup(grpKey, phyDb, phyTbList);

            IServerConfigManager serverConfigManager = TtlJobUtil.getServerConfigManager();
            Map<String, Object> sessionVariables = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            TtlJobUtil.wrapWithDistributedTrx(
                serverConfigManager,
                ttlTmpTblSchema,
                sessionVariables,
                (transConn) -> {

                    List<Map<String, Object>> queryRs = TtlJobUtil.execLogicalQueryOnInnerConnection(
                        serverConfigManager,
                        ttlTmpTblSchema,
                        transConn,
                        ec,
                        querySql);

                    if (queryRs.isEmpty()) {
                        throw new TtlJobRuntimeException(
                            String.format("No found any stats of [%s.%s] from information_schema", ttlTmpTblSchema,
                                tblTmpTblName));
                    }

                    BigDecimal dataLen =
                        (BigDecimal) queryRs.get(0).get(TtlTaskSqlBuilder.COL_NAME_FOR_SELECT_PARTITION_DATA_LENGTH);
                    dataLenOfTtlTmpTbl.addAndGet(dataLen.longValue());

                    BigDecimal tableRows =
                        (BigDecimal) queryRs.get(0).get(TtlTaskSqlBuilder.COL_NAME_FOR_SELECT_PARTITION_TABLE_ROWS);
                    tableRowsOfTtlTmpTbl.addAndGet(tableRows.longValue());

                    return 0;
                }
            );
        }
        dataLength = dataLenOfTtlTmpTbl.get();
        return dataLength;

    }

    /**
     * Fetch the stat info of data free for the ttl table and its gsi tables
     */
    public static boolean fetchTtlTableDataFreeStat(ExecutionContext ec,
                                                    TtlJobContext jobContext,
                                                    TtlTblFullDataFreeInfo[] fullDataFreeInfoOutput) {
        String ttlTblSchema = jobContext.getTtlInfo().getTtlInfoRecord().getTableSchema();
        String ttlTblName = jobContext.getTtlInfo().getTtlInfoRecord().getTableName();

        TableMeta ttlTblMeta = ec.getSchemaManager(ttlTblSchema).getTable(ttlTblName);
        boolean isTtlOnAutoDb = DbInfoManager.getInstance().isNewPartitionDb(ttlTblSchema);
        TopologyHandler topology = ExecutorContext.getContext(ttlTblSchema).getTopologyHandler();

        if (fullDataFreeInfoOutput == null) {
            return true;
        }

        TtlTblFullDataFreeInfo fullDataFreeInfo = new TtlTblFullDataFreeInfo();
        fullDataFreeInfoOutput[0] = fullDataFreeInfo;

        Set<String> gsiNameSet = new TreeSet<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        if (ttlTblMeta.withGsi()) {
            gsiNameSet = ttlTblMeta.getGsiPublished().keySet();
        }
        TtlTblFullDataFreeInfo.OneTblDataFreeInfo primDfInfo = fullDataFreeInfo.getPrimDataFreeInfo();
        Map<String, TtlTblFullDataFreeInfo.OneTblDataFreeInfo> gsiDfInfoMap = fullDataFreeInfo.getGsiDataFreeInfoMap();

        /**
         * Fetch dataFreeInfo for primary table
         */
        if (isTtlOnAutoDb) {
            primDfInfo.setTblMeta(ttlTblMeta);
            fullDataFreeInfo.setPrimDataFreeInfo(primDfInfo);
            PartitionInfo ttlPartInfo = ttlTblMeta.getPartitionInfo();
            fetchDataLengthAndDataFreeForAutoTtlTbl(ec, topology, ttlPartInfo, primDfInfo);
        } else {
            // Impl for drds ttl-tbl
            throw new NotSupportException();
        }

        if (gsiNameSet.isEmpty()) {
            return true;
        }

        /**
         * Fetch dataFreeInfo for gsi tables
         */
        if (isTtlOnAutoDb) {
            for (String gsiTblName : gsiNameSet) {
                String gsiSchemaName = ttlTblSchema;
                TtlTblFullDataFreeInfo.OneTblDataFreeInfo gsiTblDfInfo =
                    new TtlTblFullDataFreeInfo.OneTblDataFreeInfo();
                TableMeta gsiMeta = ec.getSchemaManager(gsiSchemaName).getTable(gsiTblName);
                PartitionInfo gsiPartInfo = gsiMeta.getPartitionInfo();
                gsiTblDfInfo.setTblMeta(gsiMeta);
                fetchDataLengthAndDataFreeForAutoTtlTbl(ec, topology, gsiPartInfo, gsiTblDfInfo);
                gsiDfInfoMap.put(gsiTblName, gsiTblDfInfo);
            }
        } else {
            // Impl for drds ttl-tbl
            throw new NotSupportException();
        }
        return true;
    }

//    protected static Long fetchDataLengthAndDataFreeForDrdsTtlTbl(
//        ExecutionContext ec,
//        TopologyHandler topology,
//        TableRule tableRule,
//        TtlTblFullDataFreeInfo.OneTblDataFreeInfo oneTblDataFreeInfoOutput
//    ) {
//        /**
//         * To impl
//         */
//        return null;
//    }

    protected static void fetchDataLengthAndDataFreeForAutoTtlTbl(
        ExecutionContext ec,
        TopologyHandler topology,
        PartitionInfo ttlTblPartInfo,
        TtlTblFullDataFreeInfo.OneTblDataFreeInfo oneTblDataFreeInfoOutput
    ) {
        Map<String, List<String>> phyGrpToPhyTbListMapping = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        Map<String, String> phyGrpToPhyDbMapping = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        List<PartitionSpec> phyPartSpecList = ttlTblPartInfo.getPartitionBy().getPhysicalPartitions();
        final String ttlTblSchema = ttlTblPartInfo.getTableSchema();
        final String tblTblName = ttlTblPartInfo.getTableName();
        for (int k = 0; k < phyPartSpecList.size(); k++) {
            PartitionSpec phyPartSpec = phyPartSpecList.get(k);
            String grpKey = phyPartSpec.getLocation().getGroupKey();
            String phyTb = phyPartSpec.getLocation().getPhyTableName();
            String phyDb = phyGrpToPhyDbMapping.get(grpKey);
            if (phyDb == null) {
                phyDb = PartitionMetaUtil.getPhyDbByGroupName(topology, grpKey);
                phyGrpToPhyDbMapping.put(grpKey, phyDb);
            }
            List<String> phyTbList = phyGrpToPhyTbListMapping.get(grpKey);
            if (phyTbList == null) {
                phyTbList = new ArrayList<>();
                phyGrpToPhyTbListMapping.put(grpKey, phyTbList);
            }
            phyTbList.add(phyTb);
        }

        TtlJobUtil.queryAndStateDataLengthAndDataFreeByPhyDbsAndPhyTbs(
            ec,
            phyGrpToPhyTbListMapping,
            phyGrpToPhyDbMapping,
            ttlTblSchema,
            tblTblName,
            oneTblDataFreeInfoOutput);

        return;

    }

    /**
     * fetch sum(data_length + index_length）and sum(data_free) from information_schema.tables
     * for each phy table
     * notice:
     * <pre>
     *     here does not use logical information_schema.table_detail to fetch these stat, just for:
     *     (1) to support both autodb ttl-table and drdsdb ttl-table later;
     *     (2) More efficient
     * </pre>
     */
    @NotNull
    private static void queryAndStateDataLengthAndDataFreeByPhyDbsAndPhyTbs(
        ExecutionContext ec,
        Map<String, List<String>> phyGrpToPhyTbListMapping,
        Map<String, String> phyGrpToPhyDbMapping,
        String ttlTblSchema,
        String tblTblName,
        TtlTblFullDataFreeInfo.OneTblDataFreeInfo oneTblDataFreeInfoOutput) {

        for (Map.Entry<String, List<String>> phyGrpToPhyTbListItem : phyGrpToPhyTbListMapping.entrySet()) {
            String grpKey = phyGrpToPhyTbListItem.getKey();
            List<String> phyTbList = phyGrpToPhyTbListItem.getValue();
            String phyDb = phyGrpToPhyDbMapping.get(grpKey);
            String querySql = TtlTaskSqlBuilder.buildQueryPhyDataFreeInfoSchemaTablesByGroup(grpKey, phyDb, phyTbList);

            IServerConfigManager serverConfigManager = TtlJobUtil.getServerConfigManager();
            Map<String, Object> sessionVariables = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
            TtlJobUtil.wrapWithDistributedTrx(
                serverConfigManager,
                ttlTblSchema,
                sessionVariables,
                (transConn) -> {

                    List<Map<String, Object>> queryRs = TtlJobUtil.execLogicalQueryOnInnerConnection(
                        serverConfigManager,
                        ttlTblSchema,
                        transConn,
                        ec,
                        querySql);

                    if (queryRs.isEmpty()) {
                        throw new TtlJobRuntimeException(
                            String.format("No found any stats of [%s.%s] from information_schema", ttlTblSchema,
                                tblTblName));
                    }

                    if (oneTblDataFreeInfoOutput != null) {
                        BigDecimal dataLen =
                            (BigDecimal) queryRs.get(0)
                                .get(TtlTaskSqlBuilder.COL_NAME_FOR_SELECT_PARTITION_DATA_LENGTH);
                        oneTblDataFreeInfoOutput.getDataLength().addAndGet(dataLen.longValue());

                        BigDecimal dataFree =
                            (BigDecimal) queryRs.get(0).get(TtlTaskSqlBuilder.COL_NAME_FOR_SELECT_PARTITION_DATA_FREE);
                        oneTblDataFreeInfoOutput.getDataFree().addAndGet(dataFree.longValue());

                        BigDecimal tableRows =
                            (BigDecimal) queryRs.get(0).get(TtlTaskSqlBuilder.COL_NAME_FOR_SELECT_PARTITION_TABLE_ROWS);
                        oneTblDataFreeInfoOutput.getTableRows().addAndGet(tableRows.longValue());

                    }

                    return 0L;
                }
            );
        }
    }

    /**
     * <pre>
     *
     * Generate the bound list as the following, using the interval of deltaIntervalCount and unit of intervalUnit:
     * 1. generate the bounds list which  is less than lowerBound( x < lowerBound ), the count is postPartCntForPast;
     * 2. generate the bounds list which is between lowerBound and upperBound ( lowerBound <= x < upperBound );
     * 3. generate the bounds list which more than upperBound ( x >= upperBound ); the count is prePartCntForPast;
     *
     * </pre>
     * <p>
     * General the new range bound list
     */
    public static void genRangeBoundListByInterval(String lowerBound,
                                                   String upperBound,
                                                   int prePartCntForFuture,
                                                   int postPartCntForPast,
                                                   String ttlTimezone,
                                                   Integer arcPartMode,
                                                   Integer deltaIntervalCount,
                                                   TtlTimeUnit intervalUnit,
                                                   List<String> outputPartBoundList,
                                                   List<String> outputPartNameList) {

        List<LocalDateTime> newRngBoundList = new ArrayList<>();
        List<String> newRngBoundStrList = new ArrayList<>();
        List<String> newRngPartNameList = new ArrayList<>();
        try {
            DateTimeFormatter formatter = TtlJobUtil.ISO_DATETIME_FORMATTER;
//            LocalDateTime startTime = LocalDateTime.parse(lowerBound, formatter);
            LocalDateTime endTime = LocalDateTime.parse(upperBound, formatter);

            /**
             * Prepare the part bound list between the cleanup lower bound and the cleanup upper bound
             */
            LocalDateTime newPostBound = endTime;
            List<LocalDateTime> newPostRngBoundDescList = new ArrayList<>();
            while (true) {
                if (newPostRngBoundDescList.size() >= postPartCntForPast) {
                    break;
                }
                newPostRngBoundDescList.add(newPostBound);
                newPostBound = TtlJobUtil.plusDeltaIntervals(newPostBound, intervalUnit, -1 * deltaIntervalCount);
            }
            List<LocalDateTime> newPostRngBoundAscList = new ArrayList<>();
            for (int i = newPostRngBoundDescList.size() - 1; i >= 0; --i) {
                newPostRngBoundAscList.add(newPostRngBoundDescList.get(i));
            }
            newRngBoundList.addAll(newPostRngBoundAscList);

            /**
             * Prepare the previous part bound list according to  the cleanup upper bound
             */
            LocalDateTime newBound = endTime;
            for (int i = 0; i < prePartCntForFuture; i++) {
                newBound = TtlJobUtil.plusDeltaIntervals(newBound, intervalUnit, deltaIntervalCount);
                newRngBoundList.add(newBound);
            }

            for (int i = 0; i < newRngBoundList.size(); i++) {
                LocalDateTime partBndVal = newRngBoundList.get(i);
                String partBndValStr = partBndVal.format(formatter);
                newRngBoundStrList.add(partBndValStr);
            }

            LocalDateTime lastPartBndVal = null;
            for (int i = 0; i < newRngBoundList.size(); i++) {
                LocalDateTime partBndVal = newRngBoundList.get(i);
                if (i == 0) {
                    lastPartBndVal = TtlJobUtil.plusDeltaIntervals(partBndVal, intervalUnit, -1 * deltaIntervalCount);
                } else {
                    lastPartBndVal = newRngBoundList.get(i - 1);
                }
                String partNameStr = buildPartNameByBoundValue(lastPartBndVal, intervalUnit);
                newRngPartNameList.add(partNameStr);
            }

        } catch (Throwable ex) {
            throw new TtlJobRuntimeException(ex);
        }

        if (outputPartBoundList != null) {
            outputPartBoundList.addAll(newRngBoundStrList);
        }

        if (outputPartNameList != null) {
            outputPartNameList.addAll(newRngPartNameList);
        }

        return;
    }

    protected static String buildPartNameByBoundValue(LocalDateTime partBoundVal, TtlTimeUnit ttlUnit) {
        String partNameFormatterPattern = TtlTaskSqlBuilder.getPartNameFormatterPatternByTtlUnit(ttlUnit);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern(partNameFormatterPattern);
        String partName = "p" + partBoundVal.format(formatter);
        return partName;
    }

    public static boolean checkNeedPerformOptiTblForTtlTbl(BigDecimal primDfPercent, BigDecimal maxTtlTblDfPercent) {
        if (primDfPercent.compareTo(maxTtlTblDfPercent) > 0) {
            return true;
        }
        return false;
    }

    public static TtlScheduledJobStatManager.TtlJobStatInfo getTtlJobStatInfoByJobContext(TtlJobContext jobContext) {
        String dbName = jobContext.getTtlInfo().getTtlInfoRecord().getTableSchema();
        String tbName = jobContext.getTtlInfo().getTtlInfoRecord().getTableName();
        TtlScheduledJobStatManager.TtlJobStatInfo jobStatInfo =
            TtlScheduledJobStatManager.getInstance().getTtlJobStatInfo(dbName, tbName);
        return jobStatInfo;
    }

    public static void updateJobStage(TtlJobContext jobContext, String stageMsg) {
        TtlDefinitionInfo ttlInfo = jobContext.getTtlInfo();
        String dbName = ttlInfo.getTtlInfoRecord().getTableSchema();
        String tbName = ttlInfo.getTtlInfoRecord().getTableName();
        TtlScheduledJobStatManager.TtlJobStatInfo jobStatInfo = TtlScheduledJobStatManager.getInstance()
            .getTtlJobStatInfo(dbName, tbName);
        jobStatInfo.setCurrJobStage(stageMsg);
        jobStatInfo.setCurrJobEndTs(System.currentTimeMillis());

    }

    public static void statTaskTimeCost(TtlJobContext jobContext, long beginTsNano) {
        long endTsNano = System.nanoTime();
        long newTcNano = endTsNano - beginTsNano;

        String dbName = jobContext.getTtlInfo().getTtlInfoRecord().getTableSchema();
        String tbName = jobContext.getTtlInfo().getTtlInfoRecord().getTableName();
        TtlScheduledJobStatManager.TtlJobStatInfo jobStatInfo = TtlScheduledJobStatManager.getInstance()
            .getTtlJobStatInfo(dbName, tbName);
        TtlScheduledJobStatManager.GlobalTtlJobStatInfo globalStatInfo =
            TtlScheduledJobStatManager.getInstance().getGlobalTtlJobStatInfo();

        long tcMs = newTcNano / 1000000;
        jobStatInfo.getCleanupTimeCost().addAndGet(tcMs);
        globalStatInfo.getTotalCleanupTimeCost().addAndGet(tcMs);
        jobStatInfo.calcCleanupSpeed();// unit: bytes/s
        jobStatInfo.calcCleanupRowsSpeed();// unit: rows/s
    }

    public static boolean checkIfInTtlMaintainWindow(ExecutionContext ec) {
        // If the hint TTL_JOB_FOLLOW_MAINTAIN_WINDOW=true is present in ec, the caller is a
        // manually-triggered DDL that explicitly opts-in to maintenance-window enforcement.
        // In that case we skip the global "ignore" flag and always perform the real check.
        if (ec != null && ec.getParamManager().getBoolean(ConnectionParams.TTL_JOB_FOLLOW_MAINTAIN_WINDOW)) {
            return TtlArchivedDataScheduledJob.checkIfScheduledTtlJobInMaintenanceWindow(ec);
        }
        if (TtlConfigUtil.isIgnoreMaintainWindowInTtlJob()) {
            return true;
        }
        return TtlArchivedDataScheduledJob.checkIfScheduledTtlJobInMaintenanceWindow(ec);
    }

    public static int decidePreBuiltPartCnt(int preBuiltPartCntForFuture,
                                            int ttlInterval,
                                            TtlTimeUnit ttlTimeUnit,
                                            int arcPartInterval,
                                            TtlTimeUnit arcTimeUnit
    ) {
        /**
         * The cleanUpUpperBound = normalizedCurrTime - ttlInterval * ttlUnitVal,
         * so normalizedCurrTime <= cleanUpUpperBound + ttlInterval * ttlUnitVal.
         *
         * but the arcTimeUnit maybe is different from tllUnit.
         * so here need convert ttlInterval of ttlUnitVal to new_ttlInterval of arcUnit
         *
         * but here tllUnit and arcUnit are always the same,
         * maybe they are different later.
         */
//        int finalPreBuildPartCnt =
//            preBuiltPartCntForFuture + ttlInterval + 1;// the "+1" is for included curr-time-value bound
//        return finalPreBuildPartCnt;
        return preBuiltPartCntForFuture + 1;
    }

    public static String buildForceLocalIndexExprForTtlCol(TtlDefinitionInfo ttlInfo,
                                                           ExecutionContext ec) {

        String forceIndexExpr = "";
        try {
            String tblSchema = ttlInfo.getTtlInfoRecord().getTableSchema();
            String tblName = ttlInfo.getTtlInfoRecord().getTableName();
            String ttlColName = ttlInfo.getTtlInfoRecord().getTtlCol();
            TableMeta tblMeta = ec.getSchemaManager(tblSchema).getTable(tblName);
            List<IndexMeta> idxMetaList = tblMeta.getIndexes();
            String targetIdxName = "";
            for (int i = 0; i < idxMetaList.size(); i++) {
                IndexMeta idxMeta = idxMetaList.get(i);
                String idxName = idxMeta.getPhysicalIndexName();
                List<ColumnMeta> keyColList = idxMeta.getKeyColumns();
                if (keyColList.get(0).getName().equalsIgnoreCase(ttlColName)) {
                    targetIdxName = idxName;
                    break;
                }
            }
            if (!StringUtils.isEmpty(targetIdxName)) {
                forceIndexExpr = String.format("FORCE INDEX(%s)", targetIdxName);
            }
        } catch (Throwable ex) {
            TtlLoggerUtil.TTL_TASK_LOGGER.warn(ex);
        }
        return forceIndexExpr;
    }

    public static TableMeta getArchiveCciTableMeta(String ttlTblSchema, String ttlTblName,
                                                   ExecutionContext executionContext) {
        TableMeta ttlTblMeta = executionContext.getSchemaManager(ttlTblSchema).getTable(ttlTblName);
        String tblNameOfArcCci = TtlJobUtil.getActualTableNameForArcCci(ttlTblMeta, executionContext);
        String arcTblSchema = ttlTblMeta.getTtlDefinitionInfo().getArchiveTableSchema();
        TableMeta cciTblMeta = executionContext.getSchemaManager(arcTblSchema).getTableWithNull(tblNameOfArcCci);
        return cciTblMeta;
    }

    public static String fetchStringFromQueryValue(List<Map<String, Object>> boundResult, String colName) {
        String expiredLowerBoundStr = null;
        Object expiredLowerBoundObj = boundResult.get(0).get(colName);
        if (expiredLowerBoundObj instanceof io.airlift.slice.Slice) {
            //expiredLowerBoundStr = ((Slice) expiredLowerBoundObj).toString();
            byte[] valBytes = ((io.airlift.slice.Slice) expiredLowerBoundObj).getBytes();
            try {
                expiredLowerBoundStr = new String(valBytes, "utf8");
            } catch (UnsupportedEncodingException e) {
                throw new TtlJobRuntimeException(e);
            }
        } else if (expiredLowerBoundObj instanceof String) {
            expiredLowerBoundStr = (String) expiredLowerBoundObj;
        } else if (expiredLowerBoundObj instanceof OriginalTimestamp) {
            expiredLowerBoundStr =
                ((OriginalTimestamp) expiredLowerBoundObj).getMysqlDateTime().toDatetimeString(0);
        } else if (expiredLowerBoundObj instanceof Long) {
            expiredLowerBoundStr = String.valueOf(expiredLowerBoundObj);
        }

        return expiredLowerBoundStr;
    }

    protected static String formatNumberStringByPartInterval(ExecutionContext ec,
                                                             TtlDefinitionInfo ttlInfo,
                                                             String numberString) {
        ColumnMeta ttlColMeta = ttlInfo.getTtlColMeta(ec);
        DataType ttlColDt = ttlColMeta.getDataType();
        PartitionField partColFld = PartitionFieldBuilder.createField(ttlColDt);
        partColFld.store(numberString, DataTypes.StringType);
        Long numLongVal = partColFld.longValue();
        Long artPartInterval = Long.valueOf(ttlInfo.getTtlInfoRecord().getArcPartInterval());
        Long modRes = numLongVal % artPartInterval;
        Long formatedNumLongVal = numLongVal - modRes;
        String formatedNumLStrVal = String.valueOf(formatedNumLongVal);
        return formatedNumLStrVal;
    }

    public static String formatPivotPointStringByTtlUnitIfNeed(ExecutionContext ec,
                                                               TtlDefinitionInfo ttlInfo,
                                                               String datetimeString) {
        ColumnMeta ttlColMeta = ttlInfo.getTtlColMeta(ec);
        DataType ttlColDt = ttlColMeta.getDataType();
        if (!DataTypeUtil.isDateType(ttlColDt)) {
            return formatNumberStringByPartInterval(ec, ttlInfo, datetimeString);
        }

        /**
         *
         * <pre>
         * set TIME_ZONE='xxx';
         * SELECT
         *  DATE_FORMAT( ?, %s ) as formated_datetime
         * formatter is like %Y-%m-%d 00:00:00.000000
         * </pre>
         *
         */
        String timeZoneStr = ttlInfo.getTtlInfoRecord().getTtlTimezone();
        String tarTimeUnitStr = TtlTimeUnit.of(ttlInfo.getTtlInfoRecord().getArcPartUnit()).getUnitName();
        String formatedDatetimeValueQuery =
            TtlTaskSqlBuilder.buildSelectFormatedDatetimeValueSql(ttlInfo, ec, datetimeString, tarTimeUnitStr);

        final IServerConfigManager serverConfigManager = TtlJobUtil.getServerConfigManager();
        String ttlTblSchemaName = ttlInfo.getTtlInfoRecord().getTableSchema();

        Map<String, Object> sessionVariables = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        sessionVariables.put("time_zone", timeZoneStr);

        String datetimeStringFormatedResult = TtlJobUtil.wrapWithDistributedTrx(
            serverConfigManager,
            ttlTblSchemaName,
            sessionVariables,
            (transConn) -> {
                List<Map<String, Object>> currentDtFormatedByArcPartUnitResult =
                    TtlJobUtil.execLogicalQueryOnInnerConnection(serverConfigManager,
                        ttlTblSchemaName,
                        transConn,
                        ec,
                        formatedDatetimeValueQuery);
                String formatedResult = TtlJobUtil.fetchStringFromQueryValue(currentDtFormatedByArcPartUnitResult,
                    TtlTaskSqlBuilder.COL_NAME_FOR_SELECT_TTL_COL_FORMATED_CURRENT_DATETIME);
                return formatedResult;
            }
        );

        return datetimeStringFormatedResult;
    }

    public static List<Map<String, Object>> execQueryExprSqlAndGetResult(ExecutionContext ec,
                                                                         TtlDefinitionInfo ttlInfo,
                                                                         String queryExprSql) {
        final IServerConfigManager serverConfigManager = TtlJobUtil.getServerConfigManager();
        String ttlTblSchemaName = ttlInfo.getTtlInfoRecord().getTableSchema();
        String ttlTimezoneStr = ttlInfo.getTtlInfoRecord().getTtlTimezone();
        String groupParallelismForConnStr = String.valueOf(TtlConfigUtil.getDefaultGroupParallelismOnDqlConn());
        String charsetEncoding = TtlConfigUtil.defaultCharsetEncodingOnTransConn;
        String sqlModeSetting = TtlConfigUtil.defaultSqlModeOnTransConn;
        Map<String, Object> sessionVariables = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        sessionVariables.put("time_zone", ttlTimezoneStr);
        sessionVariables.put("names", charsetEncoding);
        sessionVariables.put("sql_mode", sqlModeSetting);
        sessionVariables.put("group_parallelism", groupParallelismForConnStr);
        List<Map<String, Object>> resultSet = null;
        resultSet = TtlJobUtil.wrapWithDistributedTrx(
            serverConfigManager,
            ttlTblSchemaName,
            sessionVariables,
            (transConn) -> {
                List<Map<String, Object>> queryResultSet =
                    TtlJobUtil.execLogicalQueryOnInnerConnection(serverConfigManager,
                        ttlTblSchemaName,
                        transConn,
                        ec,
                        queryExprSql);
                return queryResultSet;
            }
        );
        return resultSet;
    }

    /**
     * Check if need add a subjob task of auto refresh the cci view of arc tbl
     */
    public static void addRefreshArcViewSubJobIfNeed(DdlJob ddlJob,
                                                     BaseDdlOperation ddlPlan,
                                                     ExecutionContext ec) {
        List<RefreshArcTblViewContext> targetTtlTblListOutput = new ArrayList<>();
        if (!TtlUtil.needRefreshArcTblView(ddlPlan, ec, targetTtlTblListOutput)) {
            return;
        }
        for (int i = 0; i < targetTtlTblListOutput.size(); i++) {
            RefreshArcTblViewContext ctx = targetTtlTblListOutput.get(i);
            addRefreshArcViewSubJobInner(ec, (ExecutableDdlJob) ddlJob, ctx);
        }
    }

    protected static void addRefreshArcViewSubJobInner(
        ExecutionContext ec,
        ExecutableDdlJob ddlJob,
        RefreshArcTblViewContext ctx) {
        String tableSchema = ctx.getNewTtlTblSchema();
        String newTableName = ctx.getNewTtlTblName();
        String newTableSchema = ctx.getNewTtlTblSchema();
        TtlDefinitionInfo ttlInfo = ctx.getTarTtlInfo();
        if (ttlInfo == null) {
            return;
        }
        String arcTblSchema = ttlInfo.getArchiveTableSchema();
        String arcTblName = ttlInfo.getArchiveTableName();

        String createViewSqlForArcTbl =
            TtlTaskSqlBuilder.buildCreateViewSqlForArcTblByTtlTblName(
                arcTblSchema,
                arcTblName,
                newTableSchema,
                newTableName);
        String dropViewSqlForArcTbl = ""; // ignore rollback
        SubJobTask freshViewSubJobTask =
            new SubJobTask(tableSchema, createViewSqlForArcTbl, dropViewSqlForArcTbl);
        freshViewSubJobTask.setParentAcquireResource(true);

        ExecutableDdlJob executableDdlJob = ddlJob;
//        DdlTask tailTask = executableDdlJob.getTail();
        List<DdlTask> tailNodes =
            executableDdlJob.getAllZeroOutDegreeVertexes().stream().map(o -> o.getObject()).collect(
                Collectors.toList());
        executableDdlJob.addTask(freshViewSubJobTask);
        for (int i = 0; i < tailNodes.size(); i++) {
            DdlTask tailTask = tailNodes.get(i);
            executableDdlJob.addTaskRelationship(tailTask, freshViewSubJobTask);
        }
        executableDdlJob.labelAsTail(freshViewSubJobTask);
    }

    public static String getTtlColStringValueIfUseFuncExpr(TtlDefinitionInfo ttlInfo, ExecutionContext ec,
                                                           TtlPartitionUtil.TtlColValueCalcContext calcContext,
                                                           String cleanupTimeBound) {
        ColumnMeta ttlColMeta = ttlInfo.getTtlColMeta(ec);
        PartKeyLevel partKeyLevel = PartKeyLevel.PARTITION_KEY;
        boolean usePartFunc = false;

        /**
         * For expire after by time interval policy,
         * minBoundToCleanup must be an iso-formated datetime string which is
         * decoded by calculating decoder expr,
         * so if ttl_col use funcExpr encoding,
         * the minBoundToCleanup need convert into its original value which datatype
         * is the datatype in ColumnMeta.
         */

        if (!org.apache.commons.lang.StringUtils.isEmpty(cleanupTimeBound)) {
//                    TtlPartitionUtil.TtlColBoundValue minBoundToCleanupOnTtlColBndVal =
//                        new TtlPartitionUtil.TtlColBoundValue(minBoundToCleanup, ttlColMeta, partKeyLevel, usePartFunc,
//                            ttlInfo);
            /**
             * Build the middle-ware bound type of TtlColBoundValue for minBoundToCleanup
             */
            TtlPartitionUtil.TtlColBoundValue minBoundToCleanupOnTtlColBndVal =
                TtlPartitionUtil.TtlColBoundValue.buildPartBoundValue(cleanupTimeBound,
                    ttlColMeta, partKeyLevel, usePartFunc, ttlInfo, calcContext);
            /**
             * Convert the minBoundToCleanup into its original value of its original datatype.
             * (encoding ttl_col)
             */
            cleanupTimeBound =
                minBoundToCleanupOnTtlColBndVal.getPartBoundValueStringByOriginalPartColDataType();
        }
        return cleanupTimeBound;
    }

    public static final ExecutableDdlJob buildCreateArchiveCciJob(TtlDefinitionInfo ttlDefinitionInfo) {
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        List<DdlTask> taskList = new ArrayList<>();
        String schemaName = ttlDefinitionInfo.getTtlInfoRecord().getTableSchema();
        String logicalTableName = ttlDefinitionInfo.getTtlInfoRecord().getTableName();
        String targetTableName = "hybrid_auto_arc_" + logicalTableName;
        TtlJobContext jobContext = TtlJobContext.buildFromTtlInfo(ttlDefinitionInfo);
        PreparingFormattedCurrDatetimeTask preparingFormattedCurrDatetimeTask =
            new PreparingFormattedCurrDatetimeTask(schemaName, logicalTableName);
        preparingFormattedCurrDatetimeTask.setJobContext(jobContext);
        CheckAndPrepareColumnarIndexPartDefTask prepareCreateCiSqlTask =
            new CheckAndPrepareColumnarIndexPartDefTask(schemaName, logicalTableName, schemaName,
                targetTableName);
        String createCiSqlForArcTblSubJobName =
            TtlTaskSqlBuilder.buildSubJobTaskNameForCreateColumnarIndexBySpecifySubJobStmt();
        String dropCiSqlForArcTbl =
            TtlTaskSqlBuilder.buildDropColumnarIndexSqlForArcTbl(ttlDefinitionInfo, schemaName, targetTableName);
        SubJobTask createCiSubJobTask =
            new SubJobTask(schemaName, createCiSqlForArcTblSubJobName, dropCiSqlForArcTbl);
        createCiSubJobTask.setParentAcquireResource(true);

        taskList.add(preparingFormattedCurrDatetimeTask);
        taskList.add(prepareCreateCiSqlTask);
        taskList.add(createCiSubJobTask);
        executableDdlJob.addSequentialTasks(taskList);
        return executableDdlJob;

    }

    /**
     * Check if the arc cci meta is invalid
     */
    public static boolean checkIfArcCciMetaInvalid(ExecutionContext executionContext, TtlJobContext jobContext) {
        TtlDefinitionInfo ttlInfo = jobContext.getTtlInfo();
        if (!ttlInfo.needPerformExpiredDataArchiving()) {
            return false;
        }
        String ttlTblSchema = ttlInfo.getTtlInfoRecord().getTableSchema();
        String ttlTblName = ttlInfo.getTtlInfoRecord().getTableName();
        SchemaManager schemaManager = executionContext.getSchemaManager(ttlTblSchema);
        if (schemaManager != null) {
            TableMeta ttlTblMeta = schemaManager.getTableWithNull(ttlTblName);
            String actualTblNameOfCci =
                TtlJobUtil.getActualTableNameAndCheckCciTypeForArcCci(ttlTblMeta, executionContext);
            if (org.apache.commons.lang.StringUtils.isEmpty(actualTblNameOfCci)) {
                /**
                 * !!! actualTblNameOfCci is empty, which means the arc cci meta is invalid,
                 * !!! the target arcCci is not archive columnar index.
                 * so give up cleaning the expired data at this task
                 */
                return true;
            }
        }
        return false;
    }
}
