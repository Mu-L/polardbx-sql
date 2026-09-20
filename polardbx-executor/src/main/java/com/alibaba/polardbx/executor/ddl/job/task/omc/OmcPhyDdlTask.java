package com.alibaba.polardbx.executor.ddl.job.task.omc;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.ddl.newengine.DdlTaskState;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterTableStatement;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLExprTableSource;
import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.spec.AlterTableRollbacker;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.omc.OmcManager;
import com.alibaba.polardbx.executor.ddl.omc.OmcUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import lombok.Getter;
import org.apache.calcite.sql.SqlIdentifier;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author wumu
 */
@TaskName(name = "OmcPhyDdlTask")
@Getter
public class OmcPhyDdlTask extends AlterTablePhyDdlTask {
    final private String tableName;
    final private String phyDdlStmt;
    final private String originSql;
    // Map<storageInstId, Pair<physicalDbName, physicalTableName>>
    final private Map<String, List<Pair<String, String>>> sourcePhyTableNames;
    final private boolean isGhostDdl;
    final private String cleanupKeepFilter;

    public OmcPhyDdlTask(String schemaName, String tableName, String phyDdlStmt, String originSql,
                         Map<String, List<Pair<String, String>>> sourcePhyTableNames, boolean isGhostDdl) {
        this(schemaName, tableName, phyDdlStmt, originSql, sourcePhyTableNames, isGhostDdl, null);
    }

    @JSONCreator
    public OmcPhyDdlTask(String schemaName, String tableName, String phyDdlStmt, String originSql,
                         Map<String, List<Pair<String, String>>> sourcePhyTableNames, boolean isGhostDdl,
                         String cleanupKeepFilter) {
        super(schemaName, tableName, null);
        this.tableName = tableName;
        this.phyDdlStmt = phyDdlStmt;
        this.originSql = originSql;
        this.sourcePhyTableNames = sourcePhyTableNames;
        this.isGhostDdl = isGhostDdl;
        this.cleanupKeepFilter = cleanupKeepFilter;
        onExceptionTryRollback();
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        executeImpl(executionContext);
    }

    @Override
    public void executeImpl(ExecutionContext executionContext) {
        if (cleanupKeepFilter != null) {
            executionContext.getParamManager().getProps()
                .put(ConnectionProperties.REBUILD_TABLE_KEEP_FILTER, cleanupKeepFilter);
        }
        EventLogger.log(EventType.DDL_INFO,
            String.format("Online modify column 3.0 start, schema: [%s], table: [%s], job_id: [%s], sql: [%s]",
                schemaName, tableName, jobId, originSql));

        // 1. init omc manager
        OmcManager omcManager = OmcManager.initOmcManager(
            schemaName,
            tableName,
            phyDdlStmt,
            sourcePhyTableNames,
            jobId,
            taskId,
            isGhostDdl
        );
        // 2. init omc task
        try {
            omcManager.initOmcTasks(executionContext);
            try {
                // 3. update task state
                updateTaskStateInNewTxn(DdlTaskState.DIRTY);
                // 4. execute omc task
                omcManager.executeOmcTasks(executionContext);
                // 5. update supported commands
                if (!AlterTableRollbacker.checkIfRollbackable(phyDdlStmt)) {
                    updateSupportedCommands(true, false, null);
                }
            } catch (Throwable t) {
                // 6. handle exception
                omcManager.disableTrace(executionContext);
                omcManager.handleException(executionContext);
                if (omcManager.getSuccessCount() == 0L) {
                    enableRollback(this);
                } else {
                    if (!AlterTableRollbacker.checkIfRollbackable(phyDdlStmt)) {
                        updateSupportedCommands(true, false, null);
                    }
                }
                String errMsg = omcManager.buildErrMessage(t);
                EventLogger.log(EventType.DDL_INFO, String.format(
                    "Online modify column 3.0 failed, schema: [%s], table: [%s], job_id: [%s], err_msg: [%s]",
                    schemaName, tableName, jobId, errMsg));
                throw new TddlNestableRuntimeException(errMsg);
            }
        } finally {
            // 6. clean up
            OmcManager.cleanUpOmcManager(jobId);
        }

        EventLogger.log(EventType.DDL_INFO,
            String.format("Online modify column 3.0 success, schema: [%s], table: [%s], job_id: [%s], sql: [%s]",
                schemaName, tableName, jobId, originSql));
    }

    @Override
    public void rollbackImpl(ExecutionContext executionContext) {
        String reversedSql;
        SQLAlterTableStatement alterTableStmt = (SQLAlterTableStatement) FastsqlUtils.parseSql(phyDdlStmt).get(0);
        if (AlterTableRollbacker.checkIfRollbackable(alterTableStmt)) {
            reversedSql = genReversedAlterTableStmt(alterTableStmt);
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_ONLINE_MODIFY_COLUMN,
                "The DDL job is not rollbackable because the DDL includes some operations that doesn't support rollback");
        }

        try {
            OmcManager omcManager =
                OmcManager.initOmcManager(schemaName, tableName, reversedSql, sourcePhyTableNames, jobId, taskId,
                    isGhostDdl);
            omcManager.handleException(executionContext);
            omcManager.initOmcRollbackTasks(executionContext);
        } finally {
            OmcManager.cleanUpOmcManager(jobId);
        }

        EventLogger.log(EventType.DDL_INFO, String.format(
            "Online modify column 3.0 rollback success, schema: [%s], table: [%s], job_id: [%s], sql: [%s]",
            schemaName, tableName, jobId, originSql));
    }

    @Override
    public List<String> explainInfo(ExecutionContext ec) {
        List<String> result = new ArrayList<>();
        result.add(String.format(
            OmcUtils.CREATE_OMC_TABLE_SQL, "", "", "", schemaName, tableName + "_omc", schemaName, tableName));

        SQLAlterTableStatement alterTable = (SQLAlterTableStatement) FastsqlUtils.parseSql(phyDdlStmt).get(0);
        alterTable.setTableSource(new SQLExprTableSource(new SQLPropertyExpr(
            SqlIdentifier.surroundWithBacktick(schemaName),
            SqlIdentifier.surroundWithBacktick(tableName + "_omc"))));
        String alter = SQLUtils.toSQLString(alterTable, DbType.mysql, new SQLUtils.FormatOption(true, false));
        result.add(alter.toLowerCase());

        result.add(String.format(
            OmcUtils.CREATE_SENTRY_TABLE_SQL, "", "", "", schemaName, tableName + "_del", tableName));
        result.add(String.format(
            OmcUtils.LOCK_TABLE_SQL, "", "", "", schemaName, tableName, schemaName, tableName + "_omc", schemaName,
            tableName + "_del"));
        result.add(String.format(OmcUtils.RENAME_TABLE_SQL, "", "", "", schemaName, tableName, schemaName,
            tableName + "_del", schemaName, tableName + "_omc", schemaName, tableName));
        result.add(String.format(OmcUtils.UNLOCK_TABLE_SQL, "", "", ""));
        return result;
    }
}
