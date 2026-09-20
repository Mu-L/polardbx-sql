package com.alibaba.polardbx.executor.ddl.job.task.mce;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.misc.MceColumnStateAccessor;
import com.alibaba.polardbx.gms.metadb.misc.MceColumnStateRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnStatus;
import com.alibaba.polardbx.gms.metadb.table.ExternalizedColumnInfo;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;
import java.util.Collections;

/**
 * Internalize Task: stop maintaining the addr column. Transitions control state
 * DUAL_WRITE → NONE and demotes the addr column to ABSENT (its record and mapping stay until the
 * final drop-addr task removes them, matching the forward NONE layout expected by
 * {@code MceColumnStateResolver}).
 * <p>
 * From the following TableSync on, DML writes only the plaintext content column; the addr column
 * receives no new BlobRefs, so the table's staging traffic for this column stops and the final
 * drop-addr task can require a drained staging.
 */
@Getter
@TaskName(name = "MceInternalizeChangeWriteContentOnlyTask")
public class MceInternalizeChangeWriteContentOnlyTask extends BaseGmsTask {

    /**
     * MCE state-transition timeline goes to tddl.log (SLS-collected) instead of the
     * ddl-engine log, so migration troubleshooting survives log collection.
     */
    private static final Logger MCE_LOGGER = LoggerFactory.getLogger("EXT_COLUMN");

    private final String tableName;
    private final String columnName;
    private final String addrColumnName;

    @JSONCreator
    public MceInternalizeChangeWriteContentOnlyTask(String schemaName, String tableName, String columnName) {
        super(schemaName, tableName);
        this.tableName = tableName;
        this.columnName = columnName;
        this.addrColumnName = ExternalizedColumnInfo.toAddrColumnName(columnName);
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        // No super.beforeTransaction(): version check fails mid-pipeline (see MceAddAddrColumnTask).
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        MCE_LOGGER.info(
            String.format("[MCE] MceInternalizeChangeWriteContentOnlyTask START table=%s.%s (stop writing addr)",
                schemaName, tableName));

        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        tableInfoManager.updateColumnStatus(schemaName, tableName,
            Collections.singletonList(addrColumnName), ColumnStatus.ABSENT.getValue());

        MceColumnStateAccessor mceAccessor = new MceColumnStateAccessor();
        mceAccessor.setConnection(metaDbConnection);
        MceControlStateHelper.transition(mceAccessor, getJobId() == null ? 0L : getJobId(),
            schemaName, tableName, columnName, addrColumnName,
            MceColumnStateRecord.STATE_DUAL_WRITE,
            MceColumnStateRecord.STATE_NONE);

        MCE_LOGGER.info(String.format("[MCE] MceInternalizeChangeWriteContentOnlyTask DONE table=%s.%s → NONE",
            schemaName, tableName));
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        // Not rollbackable after the read cutover; forward-only.
    }

    @Override
    protected String remark() {
        return String.format("|MCE internalize DUAL_WRITE→NONE (stop writing addr), table=%s, column=%s",
            tableName, columnName);
    }
}
