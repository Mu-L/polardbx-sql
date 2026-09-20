package com.alibaba.polardbx.executor.ddl.job.task.ttl;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.CaseInsensitive;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TableMetaChangePreemptiveSyncAction;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.partition.ExtraFieldJSON;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.ttl.TtlInfoAccessor;
import com.alibaba.polardbx.optimizer.config.server.IServerConfigManager;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.PreemptiveTime;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.ttl.TtlConfigUtil;
import com.alibaba.polardbx.optimizer.ttl.TtlDefinitionInfo;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author pangzhaoxing
 */
@Getter
@TaskName(name = "TtlArchiveBoundCommitTask")
public class TtlArchiveBoundCommitTask extends AbstractTtlJobTask {

    final boolean archivedByPartitions;

    @JSONCreator
    public TtlArchiveBoundCommitTask(String schemaName, String logicalTableName, boolean archivedByPartitions) {
        super(schemaName, logicalTableName);
        this.archivedByPartitions = archivedByPartitions;
        onExceptionTryRecoveryThenPause();
    }

    private void commitArchiveBound(ExecutionContext executionContext, Connection metaDbConnection,
                                    ExtraFieldJSON extraFieldJSON) {
        TtlInfoAccessor accessor = new TtlInfoAccessor();
        accessor.setConnection(metaDbConnection);
        accessor.updateArcBoundByByDbAndTb(extraFieldJSON, schemaName, logicalTableName);

    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        super.beforeTransaction(executionContext);
        fetchTtlJobContextFromPreviousTask();
        TtlJobUtil.updateJobStage(this.jobContext, "TtlArchiveBoundCommitTask");
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        super.duringTransaction(metaDbConnection, executionContext);

        TtlDefinitionInfo ttlInfo = this.jobContext.getTtlInfo();
        String arcBound = getCleanUpBound(ttlInfo, executionContext);
        if (arcBound == null) {
            return;
        }

        ExtraFieldJSON extraFieldJSON = ttlInfo.getTtlInfoRecord().getExtra().copy();
        extraFieldJSON.setArcBound(arcBound);

        Map<String, String> refColQueryBoundary = null;
        if (ttlInfo.getTtlInfoRecord().getExtra().getTtlRefColList() != null) {
            refColQueryBoundary = getRefColQueryBoundary(ttlInfo, executionContext, arcBound);
            if (refColQueryBoundary != null) {
                List<String> refColList = ttlInfo.getTtlInfoRecord().getExtra().getTtlRefColList();
                List<String> refColValueList = new ArrayList<>(refColList.size());
                for (String refCol : refColList) {
                    refColValueList.add(refColQueryBoundary.get(refCol));
                }
                extraFieldJSON.setTtlRefColValueList(refColValueList);
            }
        }

        commitArchiveBound(executionContext, metaDbConnection, extraFieldJSON);

        try {
            TableInfoManager.updateTableVersion(schemaName, logicalTableName, metaDbConnection);
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    private Map<String, String> getRefColQueryBoundary(TtlDefinitionInfo ttlInfo, ExecutionContext executionContext,
                                                       String arcBound) {
        List<String> refColList = ttlInfo.getTtlInfoRecord().getExtra().getTtlRefColList();
        if (refColList == null || refColList.isEmpty()) {
            return null;
        }

        StringJoiner sj = new StringJoiner(",");
        for (String refCol : refColList) {
            sj.add("`" + refCol + "`");
        }
        String selectColList = sj.toString();

        ColumnMeta ttlColMeta = ttlInfo.getTtlColMeta(executionContext);
        String ttlCol = ttlColMeta.getName();
        DataType ttlColDataType = ttlColMeta.getDataType();
        String arcBoundSqlValue = DataTypeUtil.isNumberSqlType(ttlColDataType) ? arcBound : "'" + arcBound + "'";

        String selectTtlRefMaxValueInArchiveSQL =
            String.format(TtlTaskSqlBuilder.SELECT_TTL_REF_COL_MAX_VALUE_IN_ARCHIVE,
                selectColList, schemaName, logicalTableName, jobContext.getTtlColForceIndexExpr(), ttlCol,
                arcBoundSqlValue, ttlCol);
        String selectTtlRefMinValueInOnlineSQL = String.format(TtlTaskSqlBuilder.SELECT_TTL_REF_COL_MIN_VALUE_IN_ONLINE,
            selectColList, schemaName, logicalTableName, jobContext.getTtlColForceIndexExpr(), ttlCol, arcBoundSqlValue,
            ttlCol);

        final IServerConfigManager serverConfigManager = TtlJobUtil.getServerConfigManager();
        String ttlTimezoneStr = ttlInfo.getTtlInfoRecord().getTtlTimezone();
        String charsetEncoding = TtlConfigUtil.getDefaultCharsetEncodingOnTransConn();
        String sqlModeSetting = TtlConfigUtil.getDefaultSqlModeOnTransConn();
        String groupParallelismForConnStr = String.valueOf(TtlConfigUtil.getDefaultGroupParallelismOnDqlConn());
        Map<String, Object> sessionVariables = new TreeMap<>(CaseInsensitive.CASE_INSENSITIVE_ORDER);
        sessionVariables.put("time_zone", ttlTimezoneStr);
        sessionVariables.put("names", charsetEncoding);
        sessionVariables.put("sql_mode", sqlModeSetting);
        sessionVariables.put("group_parallelism", groupParallelismForConnStr);

        final AtomicReference<Map<String, Object>> refColQueryBoundaryHolder = new AtomicReference<>();
        TtlJobUtil.wrapWithDistributedTrx(
            serverConfigManager,
            schemaName,
            sessionVariables,
            (transConn) -> {
                List<Map<String, Object>> lowerBoundResult =
                    TtlJobUtil.execLogicalQueryOnInnerConnection(serverConfigManager,
                        schemaName,
                        transConn,
                        executionContext,
                        selectTtlRefMaxValueInArchiveSQL);
                if (lowerBoundResult.size() == 1) {
                    refColQueryBoundaryHolder.set(lowerBoundResult.get(0));
                }
                return 0;
            }
        );

        final AtomicReference<Map<String, Object>> refColQueryBoundaryCheckerHolder = new AtomicReference<>();
        TtlJobUtil.wrapWithDistributedTrx(
            serverConfigManager,
            schemaName,
            sessionVariables,
            (transConn) -> {
                List<Map<String, Object>> upperBoundResult =
                    TtlJobUtil.execLogicalQueryOnInnerConnection(serverConfigManager,
                        schemaName,
                        transConn,
                        executionContext,
                        selectTtlRefMinValueInOnlineSQL);
                if (upperBoundResult.size() == 1) {
                    refColQueryBoundaryCheckerHolder.set(upperBoundResult.get(0));
                }
                return 0;
            }
        );

        if (refColQueryBoundaryHolder.get() != null) {
            if (refColQueryBoundaryCheckerHolder.get() != null) {
                //热分区的最小值一定要大于等于冷分区的最大值
                Map<String, Object> refColQueryBoundary = refColQueryBoundaryHolder.get();
                Map<String, Object> refColQueryBoundaryChecker = refColQueryBoundaryCheckerHolder.get();
                for (String refCol : refColList) {
                    DataType dataType =
                        executionContext.getSchemaManager(schemaName).getTable(logicalTableName).getColumn(refCol)
                            .getDataType();
                    if (dataType.compare(refColQueryBoundary.get(refCol), refColQueryBoundaryChecker.get(refCol)) > 0) {
                        throw new TddlNestableRuntimeException(
                            "the ttl ref col in archive is larger than the ttl ref col in online");
                    }
                }
            }
            Map<String, String> refColQueryBoundary = new HashMap<>(refColQueryBoundaryHolder.get().size());
            for (Map.Entry<String, Object> entry : refColQueryBoundaryHolder.get().entrySet()) {
                refColQueryBoundary.put(entry.getKey(),
                    (String) DataTypes.StringType.convertJavaFrom(entry.getValue()));
            }
            return refColQueryBoundary;
        }
        return null;
    }

    private String getCleanUpBound(TtlDefinitionInfo ttlInfo, ExecutionContext executionContext) {

        ColumnMeta ttlColMeta = ttlInfo.getTtlColMeta(executionContext);
        TtlPartitionUtil.TtlColValueCalcContext calcContext =
            TtlPartitionUtil.TtlColValueCalcContext.buildBoundValueCalcContext(ttlInfo, executionContext);

        if (!archivedByPartitions) {
            ExtraFieldJSON extraFieldJSON = ttlInfo.getTtlInfoRecord().getExtra();
            String oldArcBound = extraFieldJSON.getArcBound();
            if (oldArcBound != null && ttlInfo.isTtlColUseFuncExpr()) {
                oldArcBound =
                    TtlJobUtil.getTtlColStringValueIfUseFuncExpr(ttlInfo, executionContext, calcContext, oldArcBound);
            }

            String minBoundToCleanup = this.jobContext.cleanUpLowerBound;
            if (minBoundToCleanup != null && ttlInfo.isTtlColUseFuncExpr()) {
                minBoundToCleanup = TtlJobUtil.getTtlColStringValueIfUseFuncExpr(ttlInfo, executionContext, calcContext,
                    minBoundToCleanup);
            }

            String newArcBound = this.jobContext.cleanUpUpperBound;
            if (newArcBound != null && ttlInfo.isTtlColUseFuncExpr()) {
                newArcBound =
                    TtlJobUtil.getTtlColStringValueIfUseFuncExpr(ttlInfo, executionContext, calcContext, newArcBound);
            }

            if (oldArcBound == null || minBoundToCleanup == null
                || ttlColMeta.getDataType().compare(oldArcBound, minBoundToCleanup) <= 0) {
                return newArcBound;
            } else {
                return null;
            }
        } else {
            if (jobContext.getNeedDropPartsForTtlTbl()) {
                String newArcBound = this.jobContext.cleanUpUpperBound;
                if (newArcBound != null && ttlInfo.isTtlColUseFuncExpr()) {
                    newArcBound = TtlJobUtil.getTtlColStringValueIfUseFuncExpr(ttlInfo, executionContext, calcContext,
                        newArcBound);
                }
                return newArcBound;
            } else {
                return null;
            }
        }

    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        super.duringRollbackTransaction(metaDbConnection, executionContext);
        TtlDefinitionInfo ttlInfo = this.jobContext.getTtlInfo();
        ExtraFieldJSON extraFieldJSON = ttlInfo.getTtlInfoRecord().getExtra();

        commitArchiveBound(executionContext, metaDbConnection, extraFieldJSON);

        try {
            TableInfoManager.updateTableVersion(schemaName, logicalTableName, metaDbConnection);
        } catch (SQLException e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    @Override
    protected void onRollbackSuccess(ExecutionContext executionContext) {
        //this sync invocation may be deleted in the future
        //CommonMetaChanger.sync(MetaDbDataIdBuilder.getTableDataId(schemaName, logicalTableName));
        PreemptiveTime preemptiveTime = PreemptiveTime.getPreemptiveTimeFromExecutionContext(executionContext,
            ConnectionParams.PREEMPTIVE_MDL_INITWAIT, ConnectionParams.PREEMPTIVE_MDL_INTERVAL);
        if (!StringUtils.isEmpty(logicalTableName)) {
            SyncManagerHelper.syncThrowExceptions(
                new TableMetaChangePreemptiveSyncAction(schemaName, logicalTableName, preemptiveTime), SyncScope.ALL);
        }
    }

    protected void fetchTtlJobContextFromPreviousTask() {

        Class previousTaskClass =
            archivedByPartitions ? CheckAndPrepareDropPartsForTtlTblSqlTask.class : PrepareCleanupIntervalTask.class;
        TtlJobContext jobContext = TtlJobUtil.fetchTtlJobContextFromPreviousTaskByTaskName(
            getJobId(), previousTaskClass,
            getSchemaName(), getLogicalTableName());
        this.jobContext = jobContext;
    }

}
