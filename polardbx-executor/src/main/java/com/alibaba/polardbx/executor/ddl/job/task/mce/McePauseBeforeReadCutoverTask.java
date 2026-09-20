package com.alibaba.polardbx.executor.ddl.job.task.mce;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineDagExecutor;
import com.alibaba.polardbx.executor.ddl.newengine.DdlEngineDagExecutorMap;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineAccessor;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.gms.metadb.misc.MceColumnStateAccessor;
import com.alibaba.polardbx.gms.metadb.misc.MceColumnStateRecord;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

/**
 * Persistent user-confirmation gate between the completed MCE checker and READ_ADDR cutover.
 *
 * <p>A nullable override captures a statement hint or session value when the job is created. If
 * there is no job-local override, the task reads the latest instance-global value when it reaches
 * the gate. The DDL framework persists this task as SUCCESS in the same transaction used below,
 * so the root job can never commit PAUSED while this task remains READY.
 */
@Getter
@TaskName(name = "McePauseBeforeReadCutoverTask")
public class McePauseBeforeReadCutoverTask extends BaseDdlTask {

    private static final Logger MCE_LOGGER = LoggerFactory.getLogger("EXT_COLUMN");

    private final String tableName;
    private final String columnName;
    private final String addrColumnName;
    private final Boolean pauseBeforeReadCutoverOverride;

    private transient boolean pauseCommitted;
    private transient String pauseConfigSource;

    @JSONCreator
    public McePauseBeforeReadCutoverTask(String schemaName, String tableName, String columnName,
                                         Boolean pauseBeforeReadCutoverOverride) {
        super(schemaName);
        this.tableName = tableName;
        this.columnName = columnName;
        this.addrColumnName = columnName + "_addr_";
        this.pauseBeforeReadCutoverOverride = pauseBeforeReadCutoverOverride;
    }

    @Override
    protected void duringTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        pauseCommitted = false;
        pauseConfigSource = null;
        boolean shouldPause;
        if (pauseBeforeReadCutoverOverride != null) {
            shouldPause = pauseBeforeReadCutoverOverride;
            pauseConfigSource = "HINT_OR_SESSION";
        } else {
            shouldPause = InstConfUtil.getBool(ConnectionParams.MCE_PAUSE_BEFORE_READ_CUTOVER);
            pauseConfigSource = "GLOBAL";
        }
        if (!shouldPause) {
            return;
        }

        long currentJobId = requireJobId(getJobId(), "current");
        long rootJobId = requireJobId(getRootJobId() == null ? getJobId() : getRootJobId(), "root");

        // Keep the lock order aligned with MceChangeReadModeTask: root DDL job first, then MCE row.
        DdlEngineAccessor ddlAccessor = new DdlEngineAccessor();
        ddlAccessor.setConnection(metaDbConnection);
        DdlEngineRecord rootJob = ddlAccessor.queryForUpdate(rootJobId);
        requireRunningRootJob(rootJob, rootJobId);

        MceColumnStateAccessor mceAccessor = new MceColumnStateAccessor();
        mceAccessor.setConnection(metaDbConnection);
        MceControlStateHelper.requireState(mceAccessor, currentJobId, schemaName, tableName, columnName,
            addrColumnName, MceColumnStateRecord.STATE_DUAL_WRITE);

        DdlEngineRecord supportedCommands = new DdlEngineRecord();
        supportedCommands.setSupportContinue();
        supportedCommands.setSupportCancel();
        requireAffectedOne("enable CONTINUE/CANCEL",
            ddlAccessor.updateSupportedCommands(rootJobId, supportedCommands.supportedCommands));
        requireAffectedOne("set manual pause policy",
            ddlAccessor.updatePausedPolicy(rootJobId, DdlState.PAUSED, DdlState.ROLLBACK_PAUSED));
        requireAffectedOne("pause RUNNING root job",
            ddlAccessor.compareAndSetDdlState(rootJobId, DdlState.PAUSED, DdlState.RUNNING));
        MceTaskFailPoint.failOnce(MceTaskFailPoint.FP_MCE_PAUSE_GATE_FAIL_ONCE,
            currentJobId, getTaskId() == null ? 0L : getTaskId(), executionContext);
        pauseCommitted = true;
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        if (!pauseCommitted) {
            return;
        }
        MceTaskFailPoint.clearFailOnce(MceTaskFailPoint.FP_MCE_PAUSE_GATE_FAIL_ONCE,
            getJobId() == null ? 0L : getJobId(), getTaskId() == null ? 0L : getTaskId());
        long rootJobId = getRootJobId() == null ? getJobId() : getRootJobId();
        MCE_LOGGER.info(String.format(
            "[MCE] paused before READ_ADDR cutover: jobId=%d, table=%s.%s, column=%s, state=DUAL_WRITE, "
                + "checker=completed, configSource=%s. Confirm with CONTINUE DDL %d or restore the ordinary table "
                + "with ROLLBACK DDL %d",
            rootJobId, schemaName, tableName, columnName, pauseConfigSource, rootJobId, rootJobId));

        DdlEngineDagExecutor executor = DdlEngineDagExecutorMap.get(schemaName, rootJobId);
        if (executor != null) {
            executor.interrupt();
        }
    }

    @Override
    protected void duringRollbackTransaction(Connection metaDbConnection, ExecutionContext executionContext) {
        // The gate owns no MCE metadata. Existing reverse tasks restore the ordinary-table layout.
    }

    private void requireRunningRootJob(DdlEngineRecord record, long rootJobId) {
        if (record == null
            || record.jobId != rootJobId
            || !DdlState.RUNNING.name().equalsIgnoreCase(record.state)
            || record.schemaName == null
            || !record.schemaName.equalsIgnoreCase(schemaName)
            || record.objectName == null
            || !record.objectName.equalsIgnoreCase(tableName)) {
            throw error("root job must be RUNNING and own the target table, rootJobId=" + rootJobId);
        }
    }

    private long requireJobId(Long jobId, String kind) {
        if (jobId == null || jobId <= 0L) {
            throw error(kind + " job ID is missing");
        }
        return jobId;
    }

    private void requireAffectedOne(String action, int affected) {
        if (affected != 1) {
            throw error(action + " affected " + affected + " rows");
        }
    }

    private TddlRuntimeException error(String reason) {
        return new TddlRuntimeException(ErrorCode.ERR_DDL_JOB_ERROR,
            "[MCE] cannot pause before READ_ADDR for " + schemaName + "." + tableName + "." + columnName
                + ": " + reason);
    }

    @Override
    protected String remark() {
        return String.format("|MCE pause gate before READ_ADDR, table=%s, column=%s->%s",
            tableName, columnName, addrColumnName);
    }
}
