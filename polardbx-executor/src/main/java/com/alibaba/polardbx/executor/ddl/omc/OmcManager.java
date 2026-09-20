package com.alibaba.polardbx.executor.ddl.omc;

import com.alibaba.polardbx.common.async.AsyncTask;
import com.alibaba.polardbx.common.constants.TransactionAttribute;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.MDC;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.executor.backfill.Throttle;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.executor.changeset.ChangeSetManager;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.ddl.omc.OmcPhyDdlContext.OmcState;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.gms.metadb.GmsSystemTables;
import com.alibaba.polardbx.gms.recyclebin.PhyRecycleBinInfoAccessor;
import com.alibaba.polardbx.gms.recyclebin.PhyRecycleBinInfoRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.Getter;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.commons.lang.StringUtils;
import org.weakref.jmx.internal.guava.util.concurrent.RateLimiter;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import java.util.function.Function;

import static com.alibaba.polardbx.executor.ddl.omc.OmcConcurrentExecuteUtils.executeConcurrently;
import static com.alibaba.polardbx.executor.ddl.omc.OmcPhyDdlContext.RunningState.*;
import static com.alibaba.polardbx.executor.ddl.omc.OmcUtils.ADD_COLUMN_SQL;
import static com.alibaba.polardbx.executor.ddl.omc.OmcUtils.BLOCK_PURGE_SQL;
import static com.alibaba.polardbx.executor.ddl.omc.OmcUtils.CHECK_PROCESSLIST_SQL;
import static com.alibaba.polardbx.executor.ddl.omc.OmcUtils.CREATE_OMC_TABLE_SQL;
import static com.alibaba.polardbx.executor.ddl.omc.OmcUtils.CREATE_RECYCLE_BIN_DB;
import static com.alibaba.polardbx.executor.ddl.omc.OmcUtils.CREATE_SENTRY_TABLE_SQL;
import static com.alibaba.polardbx.executor.ddl.omc.OmcUtils.DROP_COLUMN_SQL;
import static com.alibaba.polardbx.executor.ddl.omc.OmcUtils.DROP_TABLE_SQL;
import static com.alibaba.polardbx.executor.ddl.omc.OmcUtils.GET_CONNECTION_ID;
import static com.alibaba.polardbx.executor.ddl.omc.OmcUtils.LOCK_TABLE_FOR_CHECK_SQL;
import static com.alibaba.polardbx.executor.ddl.omc.OmcUtils.LOCK_TABLE_SQL;
import static com.alibaba.polardbx.executor.ddl.omc.OmcUtils.OMC_HINT_TEMPLATE;
import static com.alibaba.polardbx.executor.ddl.omc.OmcUtils.RENAME_TABLE_RECYCLE_BIN;
import static com.alibaba.polardbx.executor.ddl.omc.OmcUtils.RENAME_TABLE_SQL;
import static com.alibaba.polardbx.executor.ddl.omc.OmcUtils.ROLLBACK_TRANSACTION_SQL;
import static com.alibaba.polardbx.executor.ddl.omc.OmcUtils.SET_LOCK_WAIT_TIMEOUT;
import static com.alibaba.polardbx.executor.ddl.omc.OmcUtils.UNLOCK_TABLE_SQL;
import static com.alibaba.polardbx.executor.ddl.omc.OmcUtils.executeWithConn;
import static com.alibaba.polardbx.executor.ddl.omc.OmcUtils.executeWithNewConn;
import static com.alibaba.polardbx.executor.ddl.omc.OmcUtils.getConnectionId;
import static com.alibaba.polardbx.executor.ddl.omc.OmcUtils.getPhysicalConnection;
import static com.alibaba.polardbx.executor.ddl.omc.OmcUtils.getTsoTimestamp;

/**
 * @author wumu
 */
@Getter
public class OmcManager {
    private final static Logger LOG = SQLRecorderLogger.ddlEngineLogger;

    private final String schemaName;
    private final String tableName;
    private final String phyDdlStmt;
    private final Map<String, List<Pair<String, String>>> sourcePhyTableNames;
    private final Long jobId;
    private final Long taskId;
    private final boolean isGhostDdl;

    public static Map<Long, OmcManager> globalOmcManagerMap = new ConcurrentHashMap<>();

    protected final List<Throwable> phyDdlExceptions = new CopyOnWriteArrayList<>();

    private final List<OmcPhyDdlContext> phyDdlContexts = new ArrayList<>();

    // 流控
    protected Throttle throttle;
    protected volatile RateLimiter rateLimiter;

    // 异常处理相关信息
    private final Map<String, List<Pair<String, String>>> renamedPhyTableNames = new HashMap<>();

    private int totalCount = 0;
    private int successCount = 0;

    public OmcManager(String schemaName, String tableName, String phyDdlStmt,
                      Map<String, List<Pair<String, String>>> sourcePhyTableNames, Long jobId, Long taskId,
                      boolean isGhostDdl) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.phyDdlStmt = phyDdlStmt;
        this.sourcePhyTableNames = sourcePhyTableNames;
        this.jobId = jobId;
        this.taskId = taskId;
        this.isGhostDdl = isGhostDdl;
    }

    public static OmcManager initOmcManager(String schemaName, String tableName, String phyDdlStmt,
                                            Map<String, List<Pair<String, String>>> sourcePhyTableNames,
                                            Long jobId, Long taskId, boolean isGhostDdl) {
        if (globalOmcManagerMap.containsKey(jobId)) {
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN, "omc manager already exists");
        }
        OmcManager omcManager =
            new OmcManager(schemaName, tableName, phyDdlStmt, sourcePhyTableNames, jobId, taskId, isGhostDdl);
        globalOmcManagerMap.put(jobId, omcManager);
        return omcManager;
    }

    public static void cleanUpOmcManager(Long jobId) {
        OmcManager omcManager = globalOmcManagerMap.get(jobId);
        if (omcManager != null) {
            // clean up
            omcManager.close();
            globalOmcManagerMap.remove(jobId);
        }
    }

    public static OmcManager getOmcManager(Long jobId) {
        OmcManager omcManager = globalOmcManagerMap.get(jobId);
        if (null == omcManager) {
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN, "omc manager not exists");
        }
        return omcManager;
    }

    public void close() {
        if (throttle != null) {
            throttle.stop();
        }
        OmcUtils.destroyDataSources(jobId);
    }

    private void initFlowControl(ExecutionContext originEc) {
        ParamManager pm = originEc.getParamManager();
        final long speedMin = pm.getLong(ConnectionParams.OMC_BACKFILL_SPEED_MIN);
        final long speedLimit = pm.getLong(ConnectionParams.OMC_BACKFILL_SPEED_LIMITATION);
        throttle = new Throttle(speedMin, speedLimit, schemaName, 0L);
        rateLimiter = speedLimit <= 0 ? null : RateLimiter.create(speedLimit);
        throttle.setBackFillId(taskId);
    }

    public Map<String, Map<String, Long>> initTableSpaceIdMap(ExecutionContext originEc) {
        // 按物理库批量获取 table space id
        Map<String, Map<String, Long>> dbTableSpaceIdMap = new HashMap<>();
        for (Map.Entry<String, List<Pair<String, String>>> entry : sourcePhyTableNames.entrySet()) {
            String storageInstId = entry.getKey();
            List<Pair<String, String>> phyDbAndTableNames = entry.getValue();

            // 按物理库分组
            Map<String, List<String>> dbToTablesMap = new HashMap<>();
            for (Pair<String, String> phyDbAndTableName : phyDbAndTableNames) {
                String physicalDbName = phyDbAndTableName.getKey();
                String physicalTableName = phyDbAndTableName.getValue();
                dbToTablesMap.computeIfAbsent(physicalDbName, k -> new ArrayList<>()).add(physicalTableName);
            }

            // 批量获取每个物理库的所有表的 table space id
            for (Map.Entry<String, List<String>> dbEntry : dbToTablesMap.entrySet()) {
                String physicalDbName = dbEntry.getKey();
                List<String> tableNames = dbEntry.getValue();
                OmcStorageInfo storageInfo = OmcStorageInfo.fromStorageInstId(storageInstId, physicalDbName, jobId);

                Map<String, Long> tableSpaceIdMap = OmcUtils.batchFetchTableSpaceId(
                    physicalDbName, tableNames, storageInfo, originEc);
                dbTableSpaceIdMap.computeIfAbsent(physicalDbName, k -> new HashMap<>()).putAll(tableSpaceIdMap);
            }
        }
        return dbTableSpaceIdMap;
    }

    /**
     * Get the set of column names for the logical table from the schema manager.
     * Used for validating REBUILD_TABLE_KEEP_FILTER column references.
     */
    private Set<String> getTableColumnNames(ExecutionContext originEc) {
        Set<String> columnNames = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        SchemaManager schemaManager = originEc.getSchemaManager(schemaName);
        TableMeta tableMeta = schemaManager.getTableWithNull(tableName);
        if (tableMeta != null) {
            for (ColumnMeta columnMeta : tableMeta.getAllColumns()) {
                columnNames.add(columnMeta.getName());
            }
        }
        return columnNames;
    }

    public void initOmcTasks(ExecutionContext originEc) {
        if (sourcePhyTableNames == null || sourcePhyTableNames.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                "init omc tasks failed, sourcePhyTableNames is empty");
        }

        // validate
        OmcUtils.validateStmt(phyDdlStmt);

        String keepFilter = originEc.getParamManager().getString(ConnectionParams.REBUILD_TABLE_KEEP_FILTER);
        if (StringUtils.isNotEmpty(keepFilter)) {
            Set<String> columnNames = getTableColumnNames(originEc);
            OmcUtils.validateKeepFilter(keepFilter, columnNames);
        }

        initFlowControl(originEc);

        // 按物理库批量获取 table space id
        Map<String, Map<String, Long>> dbTableSpaceIdMap = initTableSpaceIdMap(originEc);

        // 创建 OmcPhyDdlContext 并使用预获取的 table space id
        for (Map.Entry<String, List<Pair<String, String>>> entry : sourcePhyTableNames.entrySet()) {
            String storageInstId = entry.getKey();
            for (Pair<String, String> phyDbAndTableName : entry.getValue()) {
                String physicalDbName = phyDbAndTableName.getKey();
                String physicalTableName = phyDbAndTableName.getValue();
                OmcStorageInfo storageInfo = OmcStorageInfo.fromStorageInstId(storageInstId, physicalDbName, jobId);

                Long preloadedSpaceId = dbTableSpaceIdMap.get(physicalDbName).get(physicalTableName);
                OmcPhyDdlContext phyDdlContext = new OmcPhyDdlContext(schemaName, tableName,
                    storageInfo, physicalDbName, physicalTableName, phyDdlStmt, jobId, taskId, false, originEc);
                phyDdlContext.doInitWithPreloadedSpaceId(preloadedSpaceId);
                phyDdlContexts.add(phyDdlContext);
            }
        }

        FailPoint.injectExceptionFromHint(FailPointKey.FP_OMC_INIT_TASK_FAILED, originEc);
    }

    public void executeOmcTasks(ExecutionContext originEc) {
        FailPoint.injectExceptionFromHint(FailPointKey.FP_OMC_BEFORE_EXECUTE_TASK_FAILED, originEc);
        executeConcurrently(originEc, schemaName, phyDdlContexts, phyDdlExceptions, phyDdlContext -> {
            this.emitOmcTask(phyDdlContext, phyDdlExceptions);
        });

        throttle.stop();

        if (!phyDdlExceptions.isEmpty()) {
            Throwable e = phyDdlExceptions.get(0);
            LOG.error(String.format("omc task failed, jobId: %s, error: %s", jobId, e.getMessage()));
            String message = e.getCause() == null ? e.getMessage() : e.getCause().getMessage();
            throw new TddlNestableRuntimeException(message);
        }
        FailPoint.injectExceptionFromHint(FailPointKey.FP_OMC_AFTER_EXECUTE_TASK_FAILED, originEc);
    }

    public void initOmcRollbackTasks(ExecutionContext originEc) {
        if (renamedPhyTableNames.isEmpty()) {
            LOG.warn("No physical table need rollback, renamedPhyTableNames is empty");
            return;
        }

        initFlowControl(originEc);

        // 按物理库批量获取 table space id
        Map<String, Map<String, Long>> dbTableSpaceIdMap = initTableSpaceIdMap(originEc);

        for (Map.Entry<String, List<Pair<String, String>>> entry : renamedPhyTableNames.entrySet()) {
            String storageInstId = entry.getKey();
            for (Pair<String, String> phyDbAndTableName : entry.getValue()) {
                String physicalDbName = phyDbAndTableName.getKey();
                String physicalTableName = phyDbAndTableName.getValue();
                OmcStorageInfo storageInfo = OmcStorageInfo.fromStorageInstId(storageInstId, physicalDbName, jobId);
                OmcPhyDdlContext phyDdlContext = new OmcPhyDdlContext(schemaName, tableName,
                    storageInfo, physicalDbName, physicalTableName, phyDdlStmt, jobId, taskId, true, originEc);

                Long preloadedSpaceId = dbTableSpaceIdMap.get(physicalDbName).get(physicalTableName);
                phyDdlContext.doInitWithPreloadedSpaceId(preloadedSpaceId);
                phyDdlContexts.add(phyDdlContext);
            }
        }

        executeConcurrently(originEc, schemaName, phyDdlContexts, phyDdlExceptions, phyDdlContext -> {
            this.emitOmcTask(phyDdlContext, phyDdlExceptions);
        });

        throttle.stop();

        if (!phyDdlExceptions.isEmpty()) {
            Throwable e = phyDdlExceptions.get(0);
            LOG.error(String.format("omc rollback task failed, jobId: %s, error: %s", jobId, e.getMessage()));
            String message = e.getCause() == null ? e.getMessage() : e.getCause().getMessage();
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN, phyDdlExceptions.get(0), message);
        }
    }

    public String buildErrMessage(Throwable t) {
        return "Not all physical omc DDLs have been executed successfully: "
            + totalCount + " expected, " + successCount + " done, "
            + (totalCount - successCount) + " failed. Caused by: "
            + t.getMessage();
    }

    /**
     * 禁止下发的物理 sql 带上 trace，防止被 kill
     */
    public void disableTrace(ExecutionContext originEc) {
        originEc.getExtraCmds().put(ConnectionProperties.OMC_ENABLE_TRACE, false);
    }

    /**
     * 处理异常，需要防止被 pause/rollback ddl kill
     */
    public void handleException(ExecutionContext originEc) {
        LOG.info("omc handle exception, jobId: " + jobId);
        totalCount = 0;
        successCount = 0;
        renamedPhyTableNames.clear();
        if (sourcePhyTableNames == null || sourcePhyTableNames.isEmpty()) {
            LOG.warn("generate omc tasks failed, sourcePhyTableNames is empty");
            return;
        }

        // 按物理库批量获取 table space id
        Map<String, Map<String, Long>> dbTableSpaceIdMap = initTableSpaceIdMap(originEc);

        for (Map.Entry<String, List<Pair<String, String>>> entry : sourcePhyTableNames.entrySet()) {
            String storageInstId = entry.getKey();
            for (Pair<String, String> phyDbAndTableName : entry.getValue()) {
                String physicalDbName = phyDbAndTableName.getKey();
                String physicalTableName = phyDbAndTableName.getValue();
                OmcStorageInfo storageInfo = OmcStorageInfo.fromStorageInstId(storageInstId, physicalDbName, jobId);
                OmcPhyDdlContext phyDdlContext = new OmcPhyDdlContext(schemaName, tableName,
                    storageInfo, physicalDbName, physicalTableName, phyDdlStmt, jobId, taskId, false, originEc);

                Long preloadedSpaceId = dbTableSpaceIdMap.get(physicalDbName).get(physicalTableName);
                phyDdlContext.doInitWithPreloadedSpaceId(preloadedSpaceId);

                totalCount++;

                if (handleLastOmcTask(phyDdlContext)) {
                    // success
                    successCount++;
                    renamedPhyTableNames.computeIfAbsent(storageInstId, k -> new ArrayList<>()).add(phyDbAndTableName);
                }
            }
        }
    }

    private boolean handleLastOmcTask(OmcPhyDdlContext omcDdlContext) {
        LOG.info("handle last omc task , " + omcDdlContext);
        FailPoint.injectExceptionFromHint(FailPointKey.FP_OMC_FAILED_ON_HANDLE_EXCEPTION,
            omcDdlContext.getExecutionContext());
        Long changesetId = omcDdlContext.getChangesetId();
        Long currentTableSpaceId = omcDdlContext.fetchTableSpaceId();
        switch (omcDdlContext.getRunningState()) {
        case SUCCESS:
            return true;
        case INIT:
            if (omcDdlContext.getOmcState() != OmcState.INIT) {
                throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                    String.format("unexpected state, running state: %s, omc state: %s",
                        omcDdlContext.getRunningState(), omcDdlContext.getOmcState()));
            }
            omcDdlContext.updateChangeSetId();
            return false;
        case RUNNING:
        case FAILED:
            // check renamed by tablespace id
            if (!Objects.equals(currentTableSpaceId, omcDdlContext.getSpaceId())) {
                if (omcDdlContext.getOmcState().ordinal() < OmcState.CUTOVER.ordinal()) {
                    throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                        String.format("unexpected tablespace changes, current tableSpaceId: %s, "
                                + "old tableSpaceId: %s, running state: %s, omc state: %s",
                            currentTableSpaceId, omcDdlContext.getSpaceId(),
                            omcDdlContext.getRunningState(), omcDdlContext.getOmcState()));
                }
                // clean up last omc task
                executeWithNewConn(omcDdlContext, String.format(DROP_TABLE_SQL, jobId, taskId, changesetId,
                    SqlIdentifier.surroundWithBacktick(omcDdlContext.phyDbName),
                    SqlIdentifier.surroundWithBacktick(omcDdlContext.lastOmcTableName))
                );
                if (!omcDdlContext.enableRecycleBin || isGhostDdl) {
                    executeWithNewConn(omcDdlContext, String.format(DROP_TABLE_SQL, jobId, taskId, changesetId,
                        SqlIdentifier.surroundWithBacktick(omcDdlContext.phyDbName),
                        SqlIdentifier.surroundWithBacktick(omcDdlContext.lastSentryTableName))
                    );
                    LOG.info(String.format("drop origin physical table %s success", omcDdlContext));
                } else {
                    doRecycleBinTask(omcDdlContext);
                }

                omcDdlContext.updateChangeSetId();
                omcDdlContext.updateAllState(SUCCESS, OmcState.FINISH);
                return true;
            } else {
                // clean up last omc task
                rollbackChangeSet(omcDdlContext);

                executeWithNewConn(omcDdlContext, String.format(DROP_TABLE_SQL, jobId, taskId, changesetId,
                    SqlIdentifier.surroundWithBacktick(omcDdlContext.phyDbName),
                    SqlIdentifier.surroundWithBacktick(omcDdlContext.lastOmcTableName))
                );
                executeWithNewConn(omcDdlContext, String.format(DROP_TABLE_SQL, jobId, taskId, changesetId,
                    SqlIdentifier.surroundWithBacktick(omcDdlContext.phyDbName),
                    SqlIdentifier.surroundWithBacktick(omcDdlContext.lastSentryTableName))
                );

                omcDdlContext.updateChangeSetId();
                omcDdlContext.updateAllState(INIT, OmcState.INIT);
                return false;
            }
        default:
            throw new IllegalStateException("Unexpected value: " + omcDdlContext.getRunningState());
        }
    }

    private boolean handleLastOmcTask(OmcPhyDdlContext omcDdlContext, List<Throwable> exceptions) {
        LOG.info("handle last omc task, " + omcDdlContext);
        try {
            return handleLastOmcTask(omcDdlContext);
        } catch (Throwable e) {
            LOG.warn("check last omc task failed", e);
            exceptions.add(e);
            return true;
        }
    }

    private void emitOmcTask(OmcPhyDdlContext omcDdlContext, List<Throwable> exceptions) {
        LOG.info("emit omc task, " + omcDdlContext);
        if (handleLastOmcTask(omcDdlContext, exceptions)) {
            return;
        }

        Long changesetId = omcDdlContext.getChangesetId();
        String createOmcTableSql = String.format(CREATE_OMC_TABLE_SQL, jobId, taskId, changesetId,
            SqlIdentifier.surroundWithBacktick(omcDdlContext.phyDbName),
            SqlIdentifier.surroundWithBacktick(omcDdlContext.omcTableName),
            SqlIdentifier.surroundWithBacktick(omcDdlContext.phyDbName),
            SqlIdentifier.surroundWithBacktick(omcDdlContext.phyTableName)
        );
        String alterTableSql = generateAlterTableStmt(
            phyDdlStmt,
            omcDdlContext.phyDbName,
            omcDdlContext.omcTableName,
            omcDdlContext.changesetId
        );
        boolean failed = false;
        try {
            omcDdlContext.updateRunningState(RUNNING);
            // 1. create omc table
            omcDdlContext.updateOmcState(OmcState.CREATE_TABLE);
            executeWithNewConn(omcDdlContext, createOmcTableSql);
            // 2. alter table
            omcDdlContext.updateOmcState(OmcState.PHYSICAL_DDL);
            executeWithNewConn(omcDdlContext, alterTableSql);
            // 3. add generated column for check
            omcDdlContext.updateOmcState(OmcState.ADD_GENERATED_COLUMN);
            addGeneratedColumnForCheck(omcDdlContext);
            // 4. start changeset
            omcDdlContext.updateOmcState(OmcState.START_CHANGESET);
            startChangeSet(omcDdlContext);
            // 5. backfill task
            omcDdlContext.updateOmcState(OmcState.BACKFILL);
            copyBaseline(omcDdlContext);
            // 6. checker task
            retryOperationForCheck(this::checkDataWithTso, omcDdlContext);
            // 7. atomic cut-over task
            retryOperation(call -> atomicCutOver(omcDdlContext), omcDdlContext.executionContext);
        } catch (Throwable e) {
            failed = true;
            omcDdlContext.updateAllState(FAILED, OmcState.CLEANUP);
            exceptions.add(e);
            // 8. close changeset on error
            try {
                rollbackChangeSet(omcDdlContext);
            } catch (Throwable x) {
                LOG.error(String.format("rollback changeset %s failed with exception: %s", omcDdlContext, x));
                throw x;
            }
            // 9. drop generated column
            dropGeneratedColumnForCheck(omcDdlContext);
        } finally {
            // 10. cleanup task
            omcDdlContext.updateOmcState(OmcState.CLEANUP);
            executeWithNewConn(omcDdlContext, String.format(DROP_TABLE_SQL, jobId, taskId, changesetId,
                SqlIdentifier.surroundWithBacktick(omcDdlContext.phyDbName),
                SqlIdentifier.surroundWithBacktick(omcDdlContext.omcTableName))
            );
        }

        if (!failed) {
            omcDdlContext.updateAllState(SUCCESS, OmcState.FINISH);
        }
    }

    public void checkDataWithTso(OmcPhyDdlContext omcDdlContext) {
        LOG.info("start check data with tso, " + omcDdlContext);
        // apply changeset before check
        omcDdlContext.updateOmcState(OmcState.CATCHUP);
        changesetCatchUp(omcDdlContext);

        // check with tso
        omcDdlContext.updateOmcState(OmcState.CHECKER);
        Long changesetId = omcDdlContext.changesetId;
        String lockTableSql = String.format(LOCK_TABLE_FOR_CHECK_SQL, jobId, taskId, changesetId,
            SqlIdentifier.surroundWithBacktick(omcDdlContext.phyDbName),
            SqlIdentifier.surroundWithBacktick(omcDdlContext.phyTableName),
            SqlIdentifier.surroundWithBacktick(omcDdlContext.phyDbName),
            SqlIdentifier.surroundWithBacktick(omcDdlContext.omcTableName)
        );
        String blockPurgeSql = String.format(BLOCK_PURGE_SQL, jobId, taskId, changesetId,
            SqlIdentifier.surroundWithBacktick(omcDdlContext.phyDbName),
            SqlIdentifier.surroundWithBacktick(omcDdlContext.sentryTableName));
        String sentryTableSql = String.format(CREATE_SENTRY_TABLE_SQL, jobId, taskId, changesetId,
            SqlIdentifier.surroundWithBacktick(omcDdlContext.phyDbName),
            SqlIdentifier.surroundWithBacktick(omcDdlContext.sentryTableName),
            SqlIdentifier.surroundWithBacktick(omcDdlContext.phyTableName)
        );
        String dropTableSql = String.format(DROP_TABLE_SQL, jobId, taskId, changesetId,
            SqlIdentifier.surroundWithBacktick(omcDdlContext.phyDbName),
            SqlIdentifier.surroundWithBacktick(omcDdlContext.sentryTableName)
        );
        long tsoTimestamp = -1L;
        long start = System.currentTimeMillis();
        AtomicBoolean needKillConnection = new AtomicBoolean(true);
        FutureTask<Void> killTask;
        ExecutionContext ec = omcDdlContext.executionContext;
        try (Connection conn = getPhysicalConnection(omcDdlContext.omcStorageInfo);
            Connection purgeConn = getPhysicalConnection(omcDdlContext.omcStorageInfo)) {
            // 设置一些连接 session 变量
            conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
            conn.setAutoCommit(true);
            OmcUtils.setException(conn);
            OmcUtils.setEncoding(conn, ec.getEncoding());
            OmcUtils.setServerVariables(conn, ec.getServerVariables());
            OmcUtils.setTraceId(conn, ec.getTraceId());
            // 0 create sentry table for block purge
            executeWithNewConn(omcDdlContext, sentryTableSql);
            // 1 get connection id
            Long connId = getConnectionId(conn, String.format(GET_CONNECTION_ID, jobId, taskId, changesetId));
            // 2 set lock wait timeout
            int lockWaitTimeout = OmcUtils.getLockWaitTimeout(ec);
            executeWithConn(conn, String.format(SET_LOCK_WAIT_TIMEOUT, jobId, taskId, changesetId, lockWaitTimeout),
                connId, ec);
            // 3 lock table
            executeWithConn(conn, lockTableSql, connId, ec);
            long locked = System.currentTimeMillis();
            // 4 apply changeset
            killTask = killConnectionWhenTimeout(needKillConnection, omcDdlContext, connId, ec);
            changesetCatchUp(conn, null, omcDdlContext);
            try {
                // 5 block purge
                purgeConn.setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
                purgeConn.setAutoCommit(false);
                OmcUtils.setException(purgeConn);
                OmcUtils.setTraceId(purgeConn, ec.getTraceId());
                executeWithConn(purgeConn, blockPurgeSql, connId, ec);
                // 6 get tso from metadb
                tsoTimestamp = getTsoTimestamp();
                // 7. push max seq memory
                executeWithConn(conn, TransactionAttribute.getPushMaxSeqMemory(tsoTimestamp), connId, ec);
                // fail point
                FailPoint.injectExceptionFromHint(FailPointKey.FP_OMC_FAILED_ON_LOCK_TABLE_CHECK, ec);
                // 8 unlock tables
                executeWithConn(conn, String.format(UNLOCK_TABLE_SQL, jobId, taskId, changesetId), connId, ec);
                long end = System.currentTimeMillis();
                needKillConnection.set(false);
                if (killTask != null) {
                    killTask.cancel(true);
                }
                // record lock table time
                omcDdlContext.updateLockTableTime(end - start, locked - start, end - locked);
                LOG.info(
                    String.format("Get tso for checker success %s, total cost %s ms (lock table %s ms, others %s ms)",
                        omcDdlContext, end - start, locked - start, end - locked));
                EventLogger.log(EventType.OMC_INFO, OmcPhyDdlContext.toJson(omcDdlContext));
                // 9 check with tso
                FailPoint.injectSuspendFromHint(FailPointKey.FP_OMC_CHECK_WITH_TSO_SUSPEND, ec);
                checkWithTso(tsoTimestamp, omcDdlContext);
            } finally {
                // 10 rollback transaction
                needKillConnection.set(false);
                if (killTask != null) {
                    killTask.cancel(true);
                }
                executeWithConn(purgeConn, String.format(ROLLBACK_TRANSACTION_SQL, jobId, taskId, changesetId), connId,
                    ec);
            }
        } catch (Exception e) {
            LOG.error("Failed to check with tso " + tsoTimestamp, e);
            throw GeneralUtil.nestedException(e);
        } finally {
            executeWithNewConn(omcDdlContext, dropTableSql);
        }
    }

    private boolean atomicCutOver(OmcPhyDdlContext omcDdlContext) {
        LOG.info("start atomic cut over, " + omcDdlContext);
        // check renamed first
        if (!Objects.equals(omcDdlContext.fetchTableSpaceId(), omcDdlContext.getSpaceId())) {
            LOG.warn(String.format("Table %s is already cut overed ", omcDdlContext));
            return true;
        }

        // apply changeset before cut over
        omcDdlContext.updateOmcState(OmcState.CATCHUP_BEFORE_CUTOVER);
        changesetCatchUp(omcDdlContext);

        // cut over
        omcDdlContext.updateOmcState(OmcState.CUTOVER);
        Long changesetId = omcDdlContext.changesetId;
        String lockTableSql = String.format(LOCK_TABLE_SQL, jobId, taskId, changesetId,
            SqlIdentifier.surroundWithBacktick(omcDdlContext.phyDbName),
            SqlIdentifier.surroundWithBacktick(omcDdlContext.phyTableName),
            SqlIdentifier.surroundWithBacktick(omcDdlContext.phyDbName),
            SqlIdentifier.surroundWithBacktick(omcDdlContext.omcTableName),
            SqlIdentifier.surroundWithBacktick(omcDdlContext.phyDbName),
            SqlIdentifier.surroundWithBacktick(omcDdlContext.sentryTableName)
        );
        String sentryTableSql = String.format(CREATE_SENTRY_TABLE_SQL, jobId, taskId, changesetId,
            SqlIdentifier.surroundWithBacktick(omcDdlContext.phyDbName),
            SqlIdentifier.surroundWithBacktick(omcDdlContext.sentryTableName),
            SqlIdentifier.surroundWithBacktick(omcDdlContext.phyTableName)
        );
        String dropTableSql = String.format(DROP_TABLE_SQL, jobId, taskId, changesetId,
            SqlIdentifier.surroundWithBacktick(omcDdlContext.phyDbName),
            SqlIdentifier.surroundWithBacktick(omcDdlContext.sentryTableName)
        );
        Long tsoTimestamp = getTsoTimestamp();
        // push max seq memory
        executeWithNewConn(omcDdlContext, TransactionAttribute.getPushMaxSeqMemory(tsoTimestamp));
        LOG.info(String.format("Start to cut over %s, tsoTimestamp %s", omcDdlContext, tsoTimestamp));
        boolean renamed = false;
        AtomicBoolean needKillConnection = new AtomicBoolean(true);
        FutureTask<Void> killTask = null;
        long start = System.currentTimeMillis();
        ExecutionContext ec = omcDdlContext.executionContext;
        FailPoint.injectSuspendFromHint(FailPointKey.FP_OMC_BEFORE_CUTOVER_SUSPEND, ec);
        try {
            try (Connection conn = getPhysicalConnection(omcDdlContext.omcStorageInfo)) {
                // 设置一些连接 session 变量
                conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
                conn.setAutoCommit(true);
                OmcUtils.setException(conn);
                OmcUtils.setEncoding(conn, ec.getEncoding());
                OmcUtils.setServerVariables(conn, ec.getServerVariables());
                OmcUtils.setTraceId(conn, ec.getTraceId());
                // 1 get connection id
                Long connId = getConnectionId(conn, String.format(GET_CONNECTION_ID, jobId, taskId, changesetId));
                // 2 set lock wait timeout
                int lockWaitTimeout = OmcUtils.getLockWaitTimeout(ec);
                executeWithConn(conn, String.format(SET_LOCK_WAIT_TIMEOUT, jobId, taskId, changesetId, lockWaitTimeout),
                    connId, ec);
                // 3 create sentry table
                executeWithNewConn(omcDdlContext, sentryTableSql);
                // 4 lock table
                executeWithConn(conn, lockTableSql, connId, ec);
                long locked = System.currentTimeMillis();
                killTask = killConnectionWhenTimeout(needKillConnection, omcDdlContext, connId, ec);
                // 5 add column to advance omc table snapshot version
                advanceTargetTableSnapshotVersion(conn, omcDdlContext, connId, ec);
                FailPoint.injectExceptionFromHint(FailPointKey.FP_OMC_FAILED_ON_LOCK_TABLE_CUT_OVER_1, ec);
                // 6 apply changeset
                FailPoint.injectSuspendFromHint(FailPointKey.FP_OMC_APPLY_CHANGESET_SUSPEND, ec);
                changesetCatchUp(conn, tsoTimestamp, omcDdlContext);
                // 7 close changeset, after this cut over can not retry
                closeChangeSet(conn, omcDdlContext);
                FailPoint.injectExceptionFromHint(FailPointKey.FP_OMC_FAILED_ON_LOCK_TABLE_CUT_OVER_2, ec);
                // 8 init a new thread to rename table, return conn id
                atomicCutOverRename(omcDdlContext);
                // 9 wait for get session id
                omcDdlContext.waitForGetSessionId();
                // 10 wait for the RENAME to appear in processlist
                retryOperationForCheckRename(call -> checkRenameTableThreadProcess(conn, omcDdlContext), ec);
                FailPoint.injectExceptionFromHint(FailPointKey.FP_OMC_FAILED_ON_LOCK_TABLE_CUT_OVER_3, ec);
                // 11 drop sentry table
                executeWithConn(conn, dropTableSql, connId, ec);
                FailPoint.injectExceptionFromHint(FailPointKey.FP_OMC_FAILED_ON_LOCK_TABLE_CUT_OVER_4, ec);
                // 12 unlock tables
                executeWithConn(conn, String.format(UNLOCK_TABLE_SQL, jobId, taskId, changesetId), connId, ec);
                FailPoint.injectExceptionFromHint(FailPointKey.FP_OMC_FAILED_ON_LOCK_TABLE_CUT_OVER_5, ec);
                // 13 wait renamed
                omcDdlContext.waitForRename();
                renamed = true;
                needKillConnection.set(false);
                if (killTask != null) {
                    killTask.cancel(true);
                }
                // update cut over time
                long end = System.currentTimeMillis();
                omcDdlContext.updateCutOverTime(end - start);
                omcDdlContext.updateLockTableTime(end - start, locked - start, end - locked);
                LOG.info(String.format("Cut over success %s, total cost %s ms (lock table %s ms, others %s ms)",
                    omcDdlContext, end - start, locked - start, end - locked));
                EventLogger.log(EventType.OMC_INFO, OmcPhyDdlContext.toJson(omcDdlContext));
            } catch (Exception e) {
                LOG.error(String.format("Failed to cut over %s", omcDdlContext), e);
                throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN, e,
                    "The omc task failed during atomic cutover");
            }
        } finally {
            needKillConnection.set(false);
            if (killTask != null) {
                killTask.cancel(true);
            }
            omcDdlContext.waitForRenameOnException();
            // 14 drop sentry table
            if (!omcDdlContext.enableRecycleBin || isGhostDdl || !renamed) {
                executeWithNewConn(omcDdlContext, dropTableSql);
                LOG.info(String.format("drop origin physical table %s success", omcDdlContext));
            } else {
                doRecycleBinTask(omcDdlContext);
            }
        }
        return true;
    }

    private void atomicCutOverRename(OmcPhyDdlContext omcDdlContext) {
        ExecutionContext ec = omcDdlContext.executionContext;
        LOG.info("start atomic cut over rename, " + omcDdlContext);
        Long changesetId = omcDdlContext.getChangesetId();
        int lockWaitTimeout = OmcUtils.getLockWaitTimeout(omcDdlContext.executionContext);
        Map mdcContext = MDC.getCopyOfContextMap();
        final long submitTimeMs = System.currentTimeMillis();
        FutureTask<Void> task = new FutureTask<>(
            () -> {
                MDC.setContextMap(mdcContext);
                long scheduleLatencyMs = System.currentTimeMillis() - submitTimeMs;
                LOG.info(String.format(
                    "Async rename task started running, scheduleLatencyMs=%d, %s",
                    scheduleLatencyMs, omcDdlContext));

                String renameTableSql = String.format(RENAME_TABLE_SQL, jobId, taskId, changesetId,
                    SqlIdentifier.surroundWithBacktick(omcDdlContext.phyDbName),
                    SqlIdentifier.surroundWithBacktick(omcDdlContext.phyTableName),
                    SqlIdentifier.surroundWithBacktick(omcDdlContext.phyDbName),
                    SqlIdentifier.surroundWithBacktick(omcDdlContext.sentryTableName),
                    SqlIdentifier.surroundWithBacktick(omcDdlContext.phyDbName),
                    SqlIdentifier.surroundWithBacktick(omcDdlContext.omcTableName),
                    SqlIdentifier.surroundWithBacktick(omcDdlContext.phyDbName),
                    SqlIdentifier.surroundWithBacktick(omcDdlContext.phyTableName)
                );
                Long connId = null;
                try (Connection conn = getPhysicalConnection(omcDdlContext.omcStorageInfo)) {
                    // Inject failure before publishing session id, used to verify that the cutover main
                    // thread no longer hangs in waitForGetSessionId() when the async rename task aborts
                    // before setSessionId() is reached.
                    FailPoint.injectExceptionFromHint(FailPointKey.FP_OMC_FAILED_BEFORE_GET_SESSION_ID, ec);
                    // get connection id
                    connId = getConnectionId(conn, String.format(GET_CONNECTION_ID, jobId, taskId, changesetId));
                    omcDdlContext.setSessionId(connId);
                    LOG.info(String.format(
                        "Async rename task published session id, connId=%s, %s",
                        connId, omcDdlContext));
                    // set lock wait timeout
                    executeWithConn(conn,
                        String.format(SET_LOCK_WAIT_TIMEOUT, jobId, taskId, changesetId, lockWaitTimeout), connId, ec);
                    // execute rename table
                    FailPoint.injectExceptionFromHint(FailPointKey.FP_OMC_FAILED_BEFORE_RENAME, ec);
                    LOG.info(String.format(
                        "Async rename task executing rename sql, connId=%s, %s",
                        connId, omcDdlContext));
                    executeWithConn(conn, renameTableSql, connId, ec);
                    FailPoint.injectExceptionFromHint(FailPointKey.FP_OMC_FAILED_AFTER_RENAME, ec);
                    LOG.info(String.format("Rename table %s success", omcDdlContext));
                } catch (Exception e) {
                    LOG.error(String.format(
                        "Failed to cut over by rename tables, sessionIdPublished=%s, connId=%s, %s",
                        omcDdlContext.renameSessionId != null, connId, omcDdlContext), e);
                    omcDdlContext.getRenameException().set(e);
                } finally {
                    // Make sure the cutover main thread waiting in waitForGetSessionId() is woken up
                    // even if the async task aborted before setSessionId() was reached. Otherwise it
                    // would hang on lock.wait() forever (observed in OMC 3.0 jstack).
                    omcDdlContext.notifyRenameAborted();
                    LOG.info(String.format(
                        "Async rename task finished, sessionIdPublished=%s, hasException=%s, %s",
                        omcDdlContext.renameSessionId != null,
                        omcDdlContext.getRenameException().get() != null,
                        omcDdlContext));
                }
            }, null);
        omcDdlContext.setRenameTask(task);

        ec.getExecutorService().submit(schemaName, ec.getTraceId(), AsyncTask.build(task));
        LOG.info(String.format("Async rename task submitted to executor, traceId=%s, %s",
            ec.getTraceId(), omcDdlContext));
    }

    private FutureTask<Void> killConnectionWhenTimeout(AtomicBoolean needKillConnection, OmcPhyDdlContext omcDdlContext,
                                                       long connId, ExecutionContext ec) {
        long timeout = ec.getParamManager().getLong(ConnectionParams.OMC_CUTOVER_TIMEOUT);
        if (timeout > 0) {
            FutureTask<Void> task = new FutureTask<>(() -> {
                try {
                    Thread.sleep(timeout);
                } catch (InterruptedException e) {
                    // ignore
                }
                if (needKillConnection.get()) {
                    killConnection(connId, omcDdlContext);
                }
            }, null);

            ec.getExecutorService().submit(schemaName, ec.getTraceId(), AsyncTask.build(task));
            return task;
        }
        return null;
    }

    private void killConnection(long connId, OmcPhyDdlContext omcDdlContext) {
        try {
            executeWithNewConn(omcDdlContext, String.format("kill %s", connId));
            LOG.warn(String.format("Kill connection %s success by cut over timeout", connId));
            EventLogger.log(EventType.DDL_INFO,
                String.format(
                    "Online modify column 3.0 atomic cutover failed caused by timeout, schema: [%s], table: [%s], job_id: [%s]",
                    omcDdlContext.schemaName, omcDdlContext.tableName, omcDdlContext.jobId));
        } catch (Exception e) {
            LOG.error(String.format("Failed to kill connection %s by cut over timeout", connId), e);
        }
    }

    public void retryOperation(Function<Void, Boolean> call, ExecutionContext ec) {
        int maxRetries = ec.getParamManager().getInt(ConnectionParams.OMC_MAX_RETRY_COUNT);
        Throwable exception = null;
        for (int i = 0; i < maxRetries; i++) {
            if (CrossEngineValidator.isJobInterrupted(ec) || Thread.currentThread().isInterrupted()) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    "The job '" + ec.getDdlJobId() + "' has been cancelled");
            }
            if (i != 0) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            try {
                if (call.apply(null)) {
                    return;
                }
            } catch (Exception e) {
                exception = e;
                LOG.error(String.format("Failed to retry times[%s]", i + 1), e);
            }
        }
        throw GeneralUtil.nestedException(exception);
    }

    public void retryOperationForCheckRename(Function<Void, Boolean> call, ExecutionContext ec) {
        int maxRetries = ec.getParamManager().getInt(ConnectionParams.OMC_MAX_RETRY_COUNT);
        for (int i = 0; i < maxRetries; i++) {
            if (CrossEngineValidator.isJobInterrupted(ec) || Thread.currentThread().isInterrupted()) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    "The job '" + ec.getDdlJobId() + "' has been cancelled");
            }
            try {
                if (call.apply(null)) {
                    return;
                }
            } catch (Exception e) {
                throw GeneralUtil.nestedException(e);
            }

            LOG.error(String.format("Failed to retry check rename, times[%s]", i + 1));

            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                // ignore
            }
        }
    }

    public void retryOperationForCheck(Consumer<OmcPhyDdlContext> call, OmcPhyDdlContext omcDdlContext) {
        ExecutionContext ec = omcDdlContext.executionContext;
        int maxRetries = ec.getParamManager().getInt(ConnectionParams.OMC_MAX_CHECK_RETRY_COUNT);
        Throwable exception = null;
        for (int i = 0; i < maxRetries; i++) {
            if (CrossEngineValidator.isJobInterrupted(ec) || Thread.currentThread().isInterrupted()) {
                throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                    "The job '" + ec.getDdlJobId() + "' has been cancelled");
            }
            if (i != 0) {
                try {
                    Thread.sleep(1000);
                } catch (InterruptedException e) {
                    // ignore
                }
            }
            try {
                call.accept(omcDdlContext);
                return;
            } catch (Exception e) {
                exception = e;
                LOG.error(String.format("Failed to retry fast checker, times[%s]", i + 1), e);
            }
        }
        throw GeneralUtil.nestedException(exception);
    }

    public void addGeneratedColumnForCheck(OmcPhyDdlContext omcDdlContext) {
        omcDdlContext.addVirtualColumnsMap.forEach((columnName, alterSql) -> {
            // add generated column
            String newColumnName = omcDdlContext.old2NewColumnNameMap.getOrDefault(columnName, columnName);
            try {
                executeWithNewConn(omcDdlContext, alterSql);
                // 如果生成列添加成功，使用校验列进行校验，添加到 check columns 以及 origin columns 中
                String generatedColumn = omcDdlContext.getVirtualColumnNameMap().get(columnName);
                omcDdlContext.sourceOriginColumns.add(columnName);
                omcDdlContext.targetOriginColumns.add(newColumnName);
                omcDdlContext.sourceCheckColumns.add(generatedColumn);
            } catch (Exception e) {
                LOG.error(String.format("Failed to add generated column, sql is %s", alterSql), e);
                // 如果生成列添加失败，则不使用校验列进行校验，补全到 common columns 中
                omcDdlContext.sourceCommonColumns.add(columnName);
                omcDdlContext.targetCommonColumns.add(newColumnName);
            }
        });
    }

    public void dropGeneratedColumnForCheck(OmcPhyDdlContext omcDdlContext) {
        omcDdlContext.dropVirtualColumnsMap.forEach((columnName, alterSql) -> {
            // drop generated column
            try {
                executeWithNewConn(omcDdlContext, alterSql);
            } catch (Exception e) {
                if (StringUtils.containsIgnoreCase(e.getMessage(), "check that column/key exists")) {
                    LOG.error(String.format("Failed to drop generated column, column %s not exists, sql is %s",
                        columnName, alterSql), e);
                } else {
                    LOG.error(String.format("Failed to drop generated column, sql is %s", alterSql), e);
                    throw e;
                }
            }
        });
    }

    public void startChangeSet(OmcPhyDdlContext omcDdlContext) {
        ChangeSetManager changeSetManager = new ChangeSetManager(schemaName);

        // init changeset objects meta
        changeSetManager.getChangeSetMetaManager().getChangeSetReporter().initChangeSetMeta(
            omcDdlContext.changesetId, jobId, -1,
            schemaName, tableName, null, null,
            omcDdlContext.phyDbName, omcDdlContext.phyTableName
        );
        changeSetManager.getChangeSetMetaManager().getChangeSetReporter().loadChangeSetMeta(omcDdlContext.changesetId);
        // start changeset
        changeSetManager.migrateTableStart(
            schemaName, tableName,
            null, omcDdlContext.phyTableName,
            omcDdlContext.phyDbName, omcDdlContext.omcStorageInfo,
            ComplexTaskMetaManager.ComplexTaskType.ONLINE_MODIFY_COLUMN,
            true,
            omcDdlContext.executionContext
        );
    }

    public void closeChangeSet(Connection conn, OmcPhyDdlContext omcDdlContext) {
        ChangeSetManager changeSetManager = new ChangeSetManager(schemaName);
        changeSetManager.getChangeSetMetaManager().getChangeSetReporter().loadChangeSetMeta(omcDdlContext.changesetId);
        changeSetManager.setConnection(conn);
        changeSetManager.migrateTableFinish(
            schemaName, tableName,
            null, omcDdlContext.phyTableName,
            omcDdlContext.phyDbName, omcDdlContext.omcStorageInfo,
            ComplexTaskMetaManager.ComplexTaskType.ONLINE_MODIFY_COLUMN,
            omcDdlContext.executionContext
        );
    }

    public void rollbackChangeSet(OmcPhyDdlContext omcDdlContext) {
        ChangeSetManager changeSetManager = new ChangeSetManager(schemaName);
        changeSetManager.getChangeSetMetaManager().getChangeSetReporter().loadChangeSetMeta(omcDdlContext.changesetId);
        changeSetManager.migrateTableRollback(
            schemaName, tableName,
            null, omcDdlContext.phyTableName,
            omcDdlContext.phyDbName, omcDdlContext.omcStorageInfo,
            ComplexTaskMetaManager.ComplexTaskType.ONLINE_MODIFY_COLUMN,
            omcDdlContext.executionContext
        );
    }

    public void copyBaseline(OmcPhyDdlContext omcDdlContext) {
        ChangeSetManager changeSetManager = new ChangeSetManager(schemaName);
        long successCount = changeSetManager.migrateTableCopyBaseline(
            schemaName, tableName,
            omcDdlContext.phyDbName, omcDdlContext.phyDbName,
            omcDdlContext.omcStorageInfo, omcDdlContext.omcStorageInfo,
            omcDdlContext.phyTableName, omcDdlContext.omcTableName,
            omcDdlContext.sourceTableColumns, omcDdlContext.targetTableColumns,
            omcDdlContext.primaryKeyColumns,
            ComplexTaskMetaManager.ComplexTaskType.ONLINE_MODIFY_COLUMN,
            omcDdlContext.changesetId, taskId, omcDdlContext.executionContext
        );
        omcDdlContext.updateRowCount(successCount);
    }

    public void changesetCatchUp(OmcPhyDdlContext omcDdlContext) {
        changesetCatchUp(null, null, omcDdlContext);
    }

    public void changesetCatchUp(Connection conn, Long tsoTimestamp, OmcPhyDdlContext omcDdlContext) {
        boolean isBeforeChecker = omcDdlContext.getOmcState() == OmcState.CATCHUP;
        boolean isBeforeCutOver = omcDdlContext.getOmcState() == OmcState.CATCHUP_BEFORE_CUTOVER;
        ChangeSetManager changeSetManager = new ChangeSetManager(schemaName);
        changeSetManager.getChangeSetMetaManager().getChangeSetReporter().loadChangeSetMeta(omcDdlContext.changesetId);
        changeSetManager.setConnection(conn);
        changeSetManager.migrateTableCatchup(
            schemaName, tableName,
            omcDdlContext.phyDbName, omcDdlContext.phyDbName,
            omcDdlContext.omcStorageInfo, omcDdlContext.omcStorageInfo,
            omcDdlContext.phyTableName, omcDdlContext.omcTableName,
            omcDdlContext.sourceTableColumns, omcDdlContext.targetTableColumns,
            omcDdlContext.primaryKeyColumns,
            ComplexTaskMetaManager.ComplexTaskType.ONLINE_MODIFY_COLUMN,
            omcDdlContext.changesetId, taskId, tsoTimestamp,
            isBeforeChecker, isBeforeCutOver,
            omcDdlContext.executionContext
        );
    }

    public void checkWithTso(Long tsoTimestamp, OmcPhyDdlContext omcDdlContext) {
        ExecutionContext executionContext = omcDdlContext.executionContext.copy();
        final String keepFilter = executionContext.getParamManager()
            .getString(ConnectionParams.REBUILD_TABLE_KEEP_FILTER);
        OmcTsoChecker omcChecker = new OmcTsoChecker(
            schemaName,
            omcDdlContext.phyDbName,
            omcDdlContext.phyTableName,
            omcDdlContext.omcTableName,
            omcDdlContext.sourceCommonColumns,
            omcDdlContext.targetCommonColumns,
            omcDdlContext.sourceOriginColumns,
            omcDdlContext.targetOriginColumns,
            omcDdlContext.sourceCheckColumns,
            omcDdlContext.primaryKeyColumns,
            omcDdlContext.targetPrimaryKeyColumns,
            omcDdlContext.primaryKeyChanged,
            omcDdlContext.omcStorageInfo,
            tsoTimestamp,
            taskId,
            omcDdlContext.changesetId,
            keepFilter,
            executionContext
        );
        boolean fastCheck = false;
        try {
            fastCheck = omcChecker.tsoCheck();
        } catch (Throwable ex) {
            String msg = "Failed to use omc fastChecker to check data because of throwing exceptions";
            LOG.error(msg, ex);
        }
        // check result
        if (!fastCheck) {
            throw new TddlRuntimeException(ErrorCode.ERR_FAST_CHECKER,
                "online modify column checker found error.");
        }
    }

    private void advanceTargetTableSnapshotVersion(Connection conn, OmcPhyDdlContext omcDdlContext,
                                                   Long connId, ExecutionContext ec) {
        if (!InstanceVersion.isMYSQL80()) {
            // 暂时只对 80 版本做处理
            return;
        }
        String algorithm = "ALGORITHM=INSTANT";
        String columnDef = omcDdlContext.getTempColumnName() + " TINYINT DEFAULT NULL";
        String sql = String.format(ADD_COLUMN_SQL,
            jobId, taskId, omcDdlContext.changesetId,
            SqlIdentifier.surroundWithBacktick(omcDdlContext.phyDbName),
            SqlIdentifier.surroundWithBacktick(omcDdlContext.omcTableName),
            columnDef,
            algorithm);
        try {
            executeWithConn(conn, sql, connId, ec);
        } catch (Exception e) {
            LOG.error(String.format("Failed to add column to advance omc table snapshot version, sql is %s", sql), e);
        }

        sql = String.format(DROP_COLUMN_SQL,
            jobId, taskId, omcDdlContext.changesetId,
            SqlIdentifier.surroundWithBacktick(omcDdlContext.phyDbName),
            SqlIdentifier.surroundWithBacktick(omcDdlContext.omcTableName),
            omcDdlContext.getTempColumnName(),
            algorithm);
        try {
            executeWithConn(conn, sql, connId, ec);
        } catch (Exception e) {
            LOG.error(String.format("Failed to add column to advance omc table snapshot version, sql is %s", sql), e);
        }
    }

    private void doRecycleBinTask(OmcPhyDdlContext omcDdlContext) {
        Long changesetId = omcDdlContext.getChangesetId();

        // createRecycleBinDbIfNotExists
        executeWithNewConn(omcDdlContext, String.format(CREATE_RECYCLE_BIN_DB, jobId, taskId, changesetId,
            SqlIdentifier.surroundWithBacktick(PhyRecycleBinInfoRecord.PHY_DB_NAME)));

        // insertRecycleBinMeta
        try (Connection conn = MetaDbUtil.getConnection()) {
            conn.setAutoCommit(false);
            PhyRecycleBinInfoAccessor recycleBinInfoAccessor = new PhyRecycleBinInfoAccessor();
            recycleBinInfoAccessor.setConnection(conn);
            List<PhyRecycleBinInfoRecord> records = new ArrayList<>();
            PhyRecycleBinInfoRecord record = new PhyRecycleBinInfoRecord();
            record.setJobId(this.getTaskId());
            record.setStorageInstId(omcDdlContext.getOmcStorageInfo().getStorageId());
            record.setCurDbName(PhyRecycleBinInfoRecord.PHY_DB_NAME);
            record.setCurTbName(omcDdlContext.recycleBinTableName);
            record.setOriginDbName(omcDdlContext.phyDbName);
            record.setOriginTbName(omcDdlContext.phyTableName);
            record.setStatus(PhyRecycleBinInfoRecord.STATUS_INIT);
            record.setType(PhyRecycleBinInfoRecord.NORM_DLL_TYPE);
            records.add(record);
            recycleBinInfoAccessor.insert(records);
            conn.commit();
        } catch (Exception ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                " fail to add meta into " + GmsSystemTables.PHY_RECYCLE_BIN_INFO, ex);
        }

        // rename table
        executeWithNewConn(omcDdlContext, String.format(RENAME_TABLE_RECYCLE_BIN, jobId, taskId, changesetId,
            SqlIdentifier.surroundWithBacktick(omcDdlContext.phyDbName),
            SqlIdentifier.surroundWithBacktick(omcDdlContext.sentryTableName),
            SqlIdentifier.surroundWithBacktick(PhyRecycleBinInfoRecord.PHY_DB_NAME),
            SqlIdentifier.surroundWithBacktick(omcDdlContext.recycleBinTableName))
        );
        LOG.info(String.format("rename origin physical table %s for recycle bin", omcDdlContext));

        // update meta
        try (Connection conn = MetaDbUtil.getConnection()) {
            conn.setAutoCommit(false);
            PhyRecycleBinInfoAccessor recycleBinInfoAccessor = new PhyRecycleBinInfoAccessor();
            recycleBinInfoAccessor.setConnection(conn);
            List<PhyRecycleBinInfoRecord> records = new ArrayList<>();
            recycleBinInfoAccessor.updateByStorageAndTb(
                omcDdlContext.getOmcStorageInfo().getStorageId(),
                omcDdlContext.recycleBinTableName,
                PhyRecycleBinInfoRecord.STATUS_RENAME
            );
            recycleBinInfoAccessor.insert(records);
            conn.commit();
        } catch (Exception ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                " fail to add meta into " + GmsSystemTables.PHY_RECYCLE_BIN_INFO, ex);
        }
    }

    private boolean checkRenameTableThreadProcess(Connection conn, OmcPhyDdlContext omcDdlContext) {
        Long connId = getConnectionId(conn, String.format(CHECK_PROCESSLIST_SQL, jobId, taskId,
            omcDdlContext.changesetId, omcDdlContext.getRenameSessionId(), "%metadata lock%", "%rename%"));
        return Objects.equals(connId, omcDdlContext.getRenameSessionId());
    }

    public String generateAlterTableStmt(String phyDdlStmt, String physicalDbName, String phyTableName,
                                         Long changesetId) {
        String hint = String.format(OMC_HINT_TEMPLATE, jobId, taskId, changesetId);
        SQLAlterTableStatement alterTable = (SQLAlterTableStatement) FastsqlUtils.parseSql(phyDdlStmt).get(0);
        alterTable.setTableSource(new SQLExprTableSource(new SQLPropertyExpr(
            SqlIdentifier.surroundWithBacktick(physicalDbName),
            SqlIdentifier.surroundWithBacktick(phyTableName))));
        return hint + alterTable;
    }
}
