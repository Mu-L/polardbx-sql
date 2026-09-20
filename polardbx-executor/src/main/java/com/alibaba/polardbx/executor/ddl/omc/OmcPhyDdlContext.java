package com.alibaba.polardbx.executor.ddl.omc;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson.serializer.SimplePropertyPreFilter;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.version.InstanceVersion;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableAddColumn;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableDropColumnItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableItem;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLColumnDefinition;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLNotNullConstraint;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLTableElement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.MysqlForeignKey;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableChangeColumn;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlAlterTableModifyColumn;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlCreateTableStatement;
import com.alibaba.polardbx.druid.util.JdbcConstants;
import com.alibaba.polardbx.executor.changeset.ChangeSetManager;
import com.alibaba.polardbx.executor.ddl.newengine.cross.CrossEngineValidator;
import com.alibaba.polardbx.executor.gsi.GsiUtils;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.executor.utils.failpoint.FailPointKey;
import com.alibaba.polardbx.gms.metadb.misc.OmcAccessor;
import com.alibaba.polardbx.gms.metadb.misc.OmcRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnsInfoSchemaRecord;
import com.alibaba.polardbx.gms.metadb.table.IndexesInfoSchemaRecord;
import com.alibaba.polardbx.optimizer.config.table.TableColumnUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import lombok.Data;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang.StringUtils;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.FutureTask;
import java.util.concurrent.atomic.AtomicReference;

import static com.alibaba.polardbx.executor.ddl.omc.OmcUtils.SELECT_PHY_PARTITION_NAMES;
import static com.alibaba.polardbx.executor.ddl.omc.OmcUtils.SELECT_TABLE_SPACE_ID_57;
import static com.alibaba.polardbx.executor.ddl.omc.OmcUtils.SELECT_TABLE_SPACE_ID_80;
import static org.apache.calcite.sql.SqlIdentifier.surroundWithBacktick;

/**
 * @author wumu
 */
@Data
public class OmcPhyDdlContext {
    private final static Logger LOG = SQLRecorderLogger.ddlEngineLogger;

    public final String schemaName;
    public final String tableName;
    public final OmcStorageInfo omcStorageInfo;
    public final String phyDbName;
    public final String phyTableName;
    public String omcTableName;
    public String sentryTableName;
    public String recycleBinTableName;
    public final String phyDdlStmt;
    public final boolean isRollback;
    public final Long jobId;
    public final Long taskId;
    public final Long changesetId;
    public ExecutionContext executionContext;

    public Long totalRowCount;

    // 幂等校验 tablespace id
    public Long spaceId;

    // 残留表清理
    public Long lastChangesetId;
    public String lastOmcTableName;
    public String lastSentryTableName;

    // 数据回填列
    public List<String> sourceTableColumns = new ArrayList<>();
    public List<String> targetTableColumns = new ArrayList<>();

    // 列映射
    public Map<String, String> old2NewColumnNameMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    // 主键列
    public boolean primaryKeyChanged = false;
    public List<String> primaryKeyColumns = new ArrayList<>();
    public List<String> targetPrimaryKeyColumns = new ArrayList<>();

    // 校验列
    public List<String> sourceCommonColumns = new ArrayList<>();
    public List<String> targetCommonColumns = new ArrayList<>();
    public List<String> sourceOriginColumns = new ArrayList<>();
    public List<String> targetOriginColumns = new ArrayList<>();
    public List<String> sourceCheckColumns = new ArrayList<>();

    // 校验生成列相关
    public Map<String, String> virtualColumnNameMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    public Map<String, String> addVirtualColumnsMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    public Map<String, String> dropVirtualColumnsMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);

    // 用于目标表版本推进的临时列
    public String tempColumnName;

    // 切换表
    public Long renameSessionId;
    public FutureTask<Void> renameTask;
    public AtomicReference<Exception> renameException = new AtomicReference<>();

    private final Object lock = new Object();

    // 时间记录
    public Long lockTableTime;
    public Long mdlTime;
    public Long othersTime;

    // 回收站
    public boolean enableRecycleBin = false;

    // 状态记录
    public enum RunningState {
        INIT,
        RUNNING,
        SUCCESS,
        FAILED
    }

    public enum OmcState {
        INIT,
        CREATE_TABLE,
        PHYSICAL_DDL,
        ADD_GENERATED_COLUMN,
        START_CHANGESET,
        BACKFILL,
        CATCHUP,
        CHECKER,
        CATCHUP_BEFORE_CUTOVER,
        CUTOVER,
        CLEANUP,
        FINISH
    }

    private RunningState runningState = RunningState.INIT;
    private OmcState omcState = OmcState.INIT;

    public OmcPhyDdlContext(String schemaName, String tableName, OmcStorageInfo omcStorageInfo,
                            String phyDbName, String phyTableName, String phyDdlStmt,
                            Long jobId, Long taskId, boolean isRollback,
                            ExecutionContext executionContext) {
        this.schemaName = schemaName;
        this.tableName = tableName;
        this.omcStorageInfo = omcStorageInfo;
        this.phyDbName = phyDbName;
        this.phyTableName = phyTableName;
        this.phyDdlStmt = phyDdlStmt;
        this.isRollback = isRollback;
        this.jobId = jobId;
        this.taskId = taskId;
        this.executionContext = executionContext;
        this.changesetId = ChangeSetManager.getChangeSetId();
        this.omcTableName = OmcUtils.getOmcTableName(phyTableName, changesetId, false);
        this.sentryTableName = OmcUtils.getSentryTableName(phyTableName, changesetId, false);
        this.recycleBinTableName = OmcUtils.getOmcBinTableName(changesetId);
    }

    public void doInitWithPreloadedSpaceId(Long preloadedSpaceId) {
        // reload last omc task
        boolean initialized = reloadFromMetaDb();

        // prepare
        prepareData(initialized, preloadedSpaceId);

        // update omc record
        if (!initialized) {
            initOmcRecord();
        }
    }

    public void prepareData(boolean initialized, Long preloadedSpaceId) {
        // recycleBin
        enableRecycleBin = executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_PHY_RECYCLEBIN);

        // prepare tablespace id
        if (preloadedSpaceId != null) {
            spaceId = preloadedSpaceId;
        } else {
            spaceId = fetchTableSpaceId();
        }

        // prepare row count
        totalRowCount = OmcUtils.getTableRowsCount(omcStorageInfo, phyDbName, phyTableName);

        // get physical table stmt
        String createTableSql = OmcUtils.getCreateTableSql(omcStorageInfo, phyDbName, phyTableName);
        final List<SQLStatement> statementList =
            SQLUtils.parseStatementsWithDefaultFeatures(createTableSql, JdbcConstants.MYSQL);
        final MySqlCreateTableStatement stmt = (MySqlCreateTableStatement) statementList.get(0);

        // get alter table stmt
        final List<SQLStatement> alterStatement =
            SQLUtils.parseStatementsWithDefaultFeatures(phyDdlStmt, JdbcConstants.MYSQL);
        final SQLAlterTableStatement alterStmt = (SQLAlterTableStatement) alterStatement.get(0);

        Set<String> dropColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        Map<String, SQLColumnDefinition> old2NewColumnDefMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        Set<String> allColumns = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);

        for (SQLAlterTableItem sqlAlterTableItem : alterStmt.getItems()) {
            String colName;
            SQLColumnDefinition newColumnDefinition;
            if (sqlAlterTableItem instanceof MySqlAlterTableModifyColumn) {
                MySqlAlterTableModifyColumn modifyColumn = (MySqlAlterTableModifyColumn) sqlAlterTableItem;
                newColumnDefinition = modifyColumn.getNewColumnDefinition();
                colName = SQLUtils.normalizeNoTrim(newColumnDefinition.getColumnName());
                old2NewColumnDefMap.put(colName, newColumnDefinition);
            } else if (sqlAlterTableItem instanceof MySqlAlterTableChangeColumn) {
                MySqlAlterTableChangeColumn changeColumn = (MySqlAlterTableChangeColumn) sqlAlterTableItem;
                newColumnDefinition = changeColumn.getNewColumnDefinition();
                colName = SQLUtils.normalizeNoTrim(changeColumn.getColumnName().getSimpleName());
                String newColumnName = SQLUtils.normalizeNoTrim(newColumnDefinition.getColumnName());
                old2NewColumnNameMap.put(colName.toLowerCase(), newColumnName.toLowerCase());
                old2NewColumnDefMap.put(colName, newColumnDefinition);
                allColumns.add(newColumnName);
            } else if (sqlAlterTableItem instanceof SQLAlterTableDropColumnItem) {
                SQLAlterTableDropColumnItem dropColumnItem = (SQLAlterTableDropColumnItem) sqlAlterTableItem;
                colName = SQLUtils.normalizeNoTrim(dropColumnItem.getColumns().get(0).getSimpleName());
                dropColumns.add(colName.toLowerCase());
            } else if (sqlAlterTableItem instanceof SQLAlterTableAddColumn) {
                SQLAlterTableAddColumn addColumn = (SQLAlterTableAddColumn) sqlAlterTableItem;
                newColumnDefinition = addColumn.getColumns().get(0);
                colName = SQLUtils.normalizeNoTrim(newColumnDefinition.getColumnName());
                allColumns.add(colName);
            }
        }

        // 获取当前所有列名
        for (SQLTableElement tableElement : stmt.getTableElementList()) {
            if (tableElement instanceof SQLColumnDefinition) {
                final SQLColumnDefinition columnDefinition = (SQLColumnDefinition) tableElement;
                // check fk
                final String columnName = SQLUtils.normalizeNoTrim(columnDefinition.getName().getSimpleName());
                allColumns.add(columnName);
            } else if (tableElement instanceof MysqlForeignKey) {
                throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                    "Do not support online modify column when table has foreign key");
            }
        }

        // 生成临时列
        tempColumnName = generateRandomColumnName(allColumns, "omc_tmp");

        // 校验外键
        if (OmcUtils.hasReferencedForeignKey(phyDbName, phyTableName, omcStorageInfo)) {
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                "Do not support online modify column when table is referenced by foreign key");
        }

        // 生成校验列
        if (!initialized) {
            for (SQLTableElement tableElement : stmt.getTableElementList()) {
                if (tableElement instanceof SQLColumnDefinition) {
                    final SQLColumnDefinition columnDefinition = (SQLColumnDefinition) tableElement;
                    final String columnName = SQLUtils.normalizeNoTrim(columnDefinition.getName().getSimpleName());
                    boolean autoIncrement = columnDefinition.isAutoIncrement();

                    if (old2NewColumnDefMap.containsKey(columnName)) {
                        SQLColumnDefinition newColumnDefinition = old2NewColumnDefMap.get(columnName);
                        if (!autoIncrement && !newColumnDefinition.isAutoIncrement()) {
                            // for checker prepare
                            String colNameStr = columnName.toLowerCase();
                            virtualColumnNameMap.put(colNameStr, generateRandomColumnName(allColumns, colNameStr));
                        }
                    }
                }
            }
        }

        // prepare primary key
        List<IndexesInfoSchemaRecord> primaryKeyInfo = OmcUtils.getPrimaryKeyInfo(omcStorageInfo, phyTableName);
        if (primaryKeyInfo == null || primaryKeyInfo.isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                "Do not support online modify column when table has no primary key");
        }
        for (IndexesInfoSchemaRecord record : primaryKeyInfo) {
            String columnName = record.columnName;
            primaryKeyColumns.add(columnName);
            if (dropColumns.contains(columnName)) {
                primaryKeyChanged = true;
                continue;
            }
            if (old2NewColumnNameMap.containsKey(columnName)) {
                primaryKeyChanged = true;
                String newColumnName = old2NewColumnNameMap.get(columnName);
                targetPrimaryKeyColumns.add(newColumnName);
            }
            targetPrimaryKeyColumns.add(columnName);
        }

        // prepare table columns
        List<ColumnsInfoSchemaRecord> columnsInfo = OmcUtils.getColumnsInfo(omcStorageInfo, phyTableName);
        for (ColumnsInfoSchemaRecord record : columnsInfo) {
            if (StringUtils.containsIgnoreCase(record.extra, "VIRTUAL GENERATED")) {
                continue;
            }
            if (StringUtils.containsIgnoreCase(record.extra, "STORED GENERATED")) {
                continue;
            }
            if (dropColumns.contains(record.columnName)) {
                continue;
            }
            // prepare backfill columns
            sourceTableColumns.add(record.columnName);
            String newColumn = old2NewColumnNameMap.getOrDefault(record.columnName.toLowerCase(), record.columnName);
            targetTableColumns.add(newColumn);
            // prepare common check columns
            if (MapUtils.isEmpty(virtualColumnNameMap) || !virtualColumnNameMap.containsKey(record.columnName)) {
                sourceCommonColumns.add(record.columnName);
                targetCommonColumns.add(newColumn);
            }
        }

        // prepareGeneratedColumn4Check
        if (MapUtils.isNotEmpty(virtualColumnNameMap)) {
            String algorithm = InstanceVersion.isMYSQL80() ? "ALGORITHM=INSTANT" : "ALGORITHM=INPLACE, LOCK=NONE";
            virtualColumnNameMap.forEach((colName, virColName) -> {
                SQLColumnDefinition columnDefinition = old2NewColumnDefMap.get(colName);
                String definition = TableColumnUtils.getDataDefFromColumnDefNoDefault(columnDefinition);
                boolean notNull =
                    columnDefinition.getConstraints().stream().anyMatch(e -> e instanceof SQLNotNullConstraint);
                String addSql = String.format(
                    "ALTER TABLE %s.%s ADD COLUMN %s %s GENERATED ALWAYS AS (ALTER_TYPE(%s)) VIRTUAL %s COMMENT 'omc check column', %s",
                    surroundWithBacktick(phyDbName),
                    surroundWithBacktick(phyTableName),
                    surroundWithBacktick(virColName), definition, surroundWithBacktick(colName),
                    notNull ? "NOT NULL" : "",
                    algorithm);
                String dropSql = String.format("ALTER TABLE %s.%s DROP COLUMN %s, %s",
                    surroundWithBacktick(phyDbName),
                    surroundWithBacktick(phyTableName),
                    surroundWithBacktick(virColName),
                    algorithm);
                addVirtualColumnsMap.put(colName, addSql);
                dropVirtualColumnsMap.put(colName, dropSql);
            });
        }

        LOG.info("prepare data finished sourceTableName: " + phyTableName + ", targetTableName: " + omcTableName);
    }

    private String generateRandomColumnName(Set<String> currentColumns, String columnName) {
        String newColumnName = GsiUtils.generateRandomGsiName(columnName);
        while (currentColumns.contains(newColumnName)) {
            newColumnName = GsiUtils.generateRandomGsiName(columnName);
        }
        return newColumnName;
    }

    public void waitForRename() {
        if (renameTask != null) {
            try {
                renameTask.get();
            } catch (Exception e) {
                if (renameException.get() == null) {
                    renameException.set(e);
                }
            } finally {
                renameTask = null;
            }
        }
        if (renameException.get() != null) {
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN, renameException.get(),
                "rename tables failed");
        }
    }

    public void waitForRenameOnException() {
        if (renameTask != null) {
            try {
                renameTask.get();
            } catch (Exception e) {
                // ignore
            } finally {
                renameTask = null;
            }
        }
    }

    /**
     * Wait until the async rename task publishes its session id, fails fast on async failure or job
     * cancellation. The legacy implementation used unconditional {@code lock.wait()} which would hang
     * forever if the async rename task threw before calling {@link #setSessionId(Long)} (e.g. failed
     * to acquire the physical connection or to fetch connection id), see jstack reports of OMC 3.0
     * cutover threads stuck in {@code Object.wait()}.
     */
    public void waitForGetSessionId() throws InterruptedException {
        // wake up periodically to check abort/cancel even if the async task missed notify().
        final long pollIntervalMs = 1000L;
        synchronized (lock) {
            while (renameSessionId == null && renameException.get() == null) {
                if (CrossEngineValidator.isJobInterrupted(executionContext)
                    || Thread.currentThread().isInterrupted()) {
                    Long ddlJobId = executionContext == null ? null : executionContext.getDdlJobId();
                    throw new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
                        "The job '" + ddlJobId + "' has been cancelled");
                }
                lock.wait(pollIntervalMs);
            }
            if (renameSessionId == null && renameException.get() != null) {
                throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN, renameException.get(),
                    "rename tables failed before session id was published");
            }
        }
    }

    public void setSessionId(Long sessionId) {
        synchronized (lock) {
            renameSessionId = sessionId;
            lock.notifyAll();
        }
    }

    /**
     * Wake up {@link #waitForGetSessionId()} when the async rename task aborts before publishing
     * its session id. Safe to call multiple times and on the success path - the waiter uses a
     * predicate to filter spurious wake-ups.
     */
    public void notifyRenameAborted() {
        synchronized (lock) {
            lock.notifyAll();
        }
    }

    public Long fetchTableSpaceId() {
        // get local partition info
        String sql = String.format(SELECT_PHY_PARTITION_NAMES, phyTableName, phyDbName);
        List<Map<Integer, ParameterContext>> partitionInfo =
            OmcUtils.queryWithNewConn(executionContext, omcStorageInfo, sql);

        boolean hasNoPhyPart = true;
        List<String> partNameList = new ArrayList<>();
        if (GeneralUtil.isNotEmpty(partitionInfo)) {
            hasNoPhyPart = false;
            for (Map<Integer, ParameterContext> parameterContextMap : partitionInfo) {
                partNameList.add((String) parameterContextMap.get(1).getValue());
            }
        }

        // get space id
        Map<String, Pair<String, String>> tableInfo = OmcUtils.getSourceTableInfo(
            phyDbName.toLowerCase(),
            phyTableName.toLowerCase(),
            partNameList,
            hasNoPhyPart,
            omcStorageInfo
        );

        long spaceId = 0L;
        for (Map.Entry<String, Pair<String, String>> entry : tableInfo.entrySet()) {
            String name = entry.getValue().getKey();

            sql =
                String.format(InstanceVersion.isMYSQL80() ? SELECT_TABLE_SPACE_ID_80 : SELECT_TABLE_SPACE_ID_57, name);
            List<Map<Integer, ParameterContext>> result =
                OmcUtils.queryWithNewConn(executionContext, omcStorageInfo, sql);

            if (GeneralUtil.isEmpty(result) || result.size() > 1) {
                throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                    String.format("Failed to get space id, sql is %s, result is %s", sql, result));
            }

            Map<Integer, ParameterContext> parameterContextMap = result.get(0);
            Long currentSpaceId = (Long) parameterContextMap.get(1).getValue();
            if (currentSpaceId == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                    String.format("Failed to get space id, sql is %s, space id is null", sql));
            }

            LOG.info(String.format("Get space id, name: %s, spaceId: %s", name, currentSpaceId));

            spaceId += currentSpaceId;
        }

        return spaceId;
    }

    public boolean reloadFromMetaDb() {
        OmcRecord omcRecord = getOmcRecordFromMetaDb();
        if (omcRecord == null) {
            return false;
        }

        // last space id
        this.spaceId = omcRecord.getSpaceId();
        // last changeset id
        this.lastChangesetId = omcRecord.getChangesetId();
        this.lastOmcTableName = OmcUtils.getOmcTableName(phyTableName, lastChangesetId, false);
        this.lastSentryTableName = OmcUtils.getSentryTableName(phyTableName, lastChangesetId, false);
        // last running state
        this.runningState = RunningState.values()[omcRecord.getStatus()];
        this.omcState = OmcState.values()[omcRecord.getOmcStatus()];
        // last generated column map
        this.virtualColumnNameMap = JSON.parseObject(
            omcRecord.getGeneratedColumnMap(),
            new TypeReference<Map<String, String>>() {
            }
        );
        return true;
    }

    public void initOmcRecord() {
        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

        OmcRecord omcRecord = new OmcRecord();
        omcRecord.setJobId(jobId);
        omcRecord.setTaskId(taskId);
        omcRecord.setChangesetId(changesetId);
        omcRecord.setTableSchema(schemaName);
        omcRecord.setTableName(tableName);
        omcRecord.setStorageInstId(omcStorageInfo.storageId);
        omcRecord.setPhysicalDb(phyDbName);
        omcRecord.setPhysicalTable(phyTableName);
        omcRecord.setPhysicalDdlSql(phyDdlStmt);
        omcRecord.setTotalRowCount(totalRowCount);
        omcRecord.setRollback(isRollback);
        omcRecord.setSpaceId(spaceId);
        omcRecord.setStatus(RunningState.INIT.ordinal());
        omcRecord.setOmcStatus(OmcState.INIT.ordinal());
        omcRecord.setStartTime(now.format(formatter));
        omcRecord.setEndTime("");
        omcRecord.setCutOverTime(-1L);
        omcRecord.setGeneratedColumnMap(JSON.toJSONString(virtualColumnNameMap));
        omcRecord.setExtra("");

        new OmcRecordDelegate<Integer>(new OmcAccessor()) {
            @Override
            protected Integer invoke() {
                return omcAccessor.insert(Collections.singletonList(omcRecord));
            }
        }.execute();
    }

    public OmcRecord getOmcRecordFromMetaDb() {
        return new OmcRecordDelegate<OmcRecord>(new OmcAccessor()) {
            @Override
            protected OmcRecord invoke() {
                return omcAccessor.query(jobId, taskId, omcStorageInfo.storageId, phyDbName, phyTableName, isRollback);
            }
        }.execute();
    }

    public void updateRunningState(RunningState newRunningState) {
        int affRows = new OmcRecordDelegate<Integer>(new OmcAccessor()) {
            @Override
            protected Integer invoke() {
                return omcAccessor.updateStatus(
                    jobId,
                    taskId,
                    omcStorageInfo.storageId,
                    phyDbName,
                    phyTableName,
                    isRollback,
                    newRunningState.ordinal()
                );
            }
        }.execute();

        LOG.info(String.format("update running state from %s to %s, affRows: %s, %s",
            runningState.name(), newRunningState.name(), affRows, this));

        this.runningState = newRunningState;
    }

    public void updateOmcState(OmcState newOmcState) {
        int affRows = new OmcRecordDelegate<Integer>(new OmcAccessor()) {
            @Override
            protected Integer invoke() {
                return omcAccessor.updateOmcStatus(
                    jobId,
                    taskId,
                    omcStorageInfo.storageId,
                    phyDbName,
                    phyTableName,
                    isRollback,
                    newOmcState.ordinal()
                );
            }
        }.execute();

        LOG.info(String.format("update omc state from %s to %s, affRows: %s, %s",
            this.omcState.name(), newOmcState.name(), affRows, this));

        this.omcState = newOmcState;

        FailPoint.injectFromHint(FailPointKey.FP_OMC_FAILED_ON_OMC_STATUS, executionContext, (k, v) -> {
            OmcState filedOmcState = OmcState.valueOf(v);
            if (filedOmcState == newOmcState) {
                throw new RuntimeException(
                    "injected failure from " + FailPointKey.FP_OMC_FAILED_ON_OMC_STATUS + " values is " + v);
            }
        });
    }

    public void updateAllState(RunningState newRunningState, OmcState newOmcState) {
        int affRows = new OmcRecordDelegate<Integer>(new OmcAccessor()) {
            @Override
            protected Integer invoke() {
                return omcAccessor.updateAllStatus(
                    jobId,
                    taskId,
                    omcStorageInfo.storageId,
                    phyDbName,
                    phyTableName,
                    isRollback,
                    newRunningState.ordinal(),
                    newOmcState.ordinal()
                );
            }
        }.execute();

        LOG.info(String.format("update all state from %s,%s to %s,%s, affRows: %s, %s",
            runningState.name(), omcState.name(),
            newRunningState.name(), newOmcState.name(), affRows, this)
        );

        this.runningState = newRunningState;
        this.omcState = newOmcState;
    }

    public void updateChangeSetId() {
        int affRows = new OmcRecordDelegate<Integer>(new OmcAccessor()) {
            @Override
            protected Integer invoke() {
                return omcAccessor.updateChangeSetId(
                    jobId,
                    taskId,
                    omcStorageInfo.storageId,
                    phyDbName,
                    phyTableName,
                    isRollback,
                    changesetId
                );
            }
        }.execute();

        LOG.info(String.format("update changeset id from %s to %s, affRows: %s, %s",
            lastChangesetId, changesetId, affRows, this));
    }

    public void updateCutOverTime(long cutOverTime) {
        int affRows = new OmcRecordDelegate<Integer>(new OmcAccessor()) {
            @Override
            protected Integer invoke() {
                return omcAccessor.updateCutOverTime(
                    jobId,
                    taskId,
                    omcStorageInfo.storageId,
                    phyDbName,
                    phyTableName,
                    isRollback,
                    cutOverTime
                );
            }
        }.execute();

        LOG.info(String.format("update cut over time %s, affRows: %s, %s", cutOverTime, affRows, this));
    }

    public void updateRowCount(long rowCount) {
        int affRows = new OmcRecordDelegate<Integer>(new OmcAccessor()) {
            @Override
            protected Integer invoke() {
                return omcAccessor.updateRowCount(
                    jobId,
                    taskId,
                    omcStorageInfo.storageId,
                    phyDbName,
                    phyTableName,
                    isRollback,
                    rowCount
                );
            }
        }.execute();
        LOG.info(String.format("update row count %s after backfill, affRows: %s, %s", rowCount, affRows, this));
    }

    public void updateLockTableTime(long lockTableTime, long mdlTime, long othersTime) {
        this.lockTableTime = lockTableTime;
        this.mdlTime = mdlTime;
        this.othersTime = othersTime;
    }

    @Override
    public String toString() {
        return "OmcPhyDdlContext{"
            + "jobId=" + jobId
            + ", taskId=" + taskId
            + ", changesetId=" + changesetId
            + ", schemaName='" + schemaName + '\''
            + ", tableName='" + tableName + '\''
            + ", phyDbName='" + phyDbName + '\''
            + ", phyTableName='" + phyTableName + '\''
            + ", totalRowCount='" + totalRowCount + '\''
            + ", isRollback=" + isRollback
            + ", spaceId=" + spaceId
            + ", runningState=" + runningState
            + ", omcState=" + omcState
            + '}';
    }

    public static String toJson(OmcPhyDdlContext obj) {
        if (obj == null) {
            return "";
        }
        SimplePropertyPreFilter includeFilter = new SimplePropertyPreFilter();
        includeFilter.getIncludes().add("jobId");
        includeFilter.getIncludes().add("changesetId");
        includeFilter.getIncludes().add("schemaName");
        includeFilter.getIncludes().add("tableName");
        includeFilter.getIncludes().add("phyDbName");
        includeFilter.getIncludes().add("phyTableName");
        includeFilter.getIncludes().add("totalRowCount");
        includeFilter.getIncludes().add("isRollback");
        includeFilter.getIncludes().add("omcState");
        includeFilter.getIncludes().add("lockTableTime");
        includeFilter.getIncludes().add("mdlTime");
        includeFilter.getIncludes().add("othersTime");

        return JSON.toJSONString(obj, includeFilter);
    }
}
