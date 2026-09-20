package com.alibaba.polardbx.executor.ddl.job.task.mce;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.gms.metadb.misc.MceColumnStateAccessor;
import com.alibaba.polardbx.gms.metadb.misc.MceColumnStateRecord;
import com.alibaba.polardbx.gms.metadb.table.ColumnsAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnsRecord;
import com.alibaba.polardbx.gms.metadb.table.ExternalizedColumnInfo;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

/**
 * Internalize Task: cut reads over to the restored plaintext column — the point of no return of
 * the reverse pipeline. Transitions control state READ_ADDR → DUAL_WRITE.
 * <p>
 * In one MetaDB transaction: disable CANCEL, clear the content column's externalized flag and its
 * mapping (so reads stop injecting FETCH_BLOB and hit the plaintext column directly), and move the
 * control state. The resulting layout equals the forward DUAL_WRITE shape — content (PUBLIC, no
 * flag, no mapping) + addr (WRITE_ONLY, mapping→content) — so writes keep dual-writing both sides
 * until the next task stops maintaining the addr column.
 * <p>
 * After this transaction commits the job is forward-only (PAUSE/CONTINUE); rollback would put a
 * read path on a column whose backfill guarantees no longer hold.
 */
@Getter
@TaskName(name = "MceInternalizeChangeReadModeTask")
public class MceInternalizeChangeReadModeTask extends BaseGmsTask {

    /**
     * MCE state-transition timeline goes to tddl.log (SLS-collected) instead of the
     * ddl-engine log, so migration troubleshooting survives log collection.
     */
    private static final Logger MCE_LOGGER = LoggerFactory.getLogger("EXT_COLUMN");

    private final String tableName;
    private final String columnName;
    private final String addrColumnName;

    @JSONCreator
    public MceInternalizeChangeReadModeTask(String schemaName, String tableName, String columnName) {
        super(schemaName, tableName);
        this.tableName = tableName;
        this.columnName = columnName;
        this.addrColumnName = ExternalizedColumnInfo.toAddrColumnName(columnName);
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        // No super.beforeTransaction(): version check fails mid-pipeline (see MceAddAddrColumnTask).
        injectFailPointBeforeInternalizeChangeReadMode(executionContext);
    }

    private static void injectFailPointBeforeInternalizeChangeReadMode(ExecutionContext executionContext) {
        MceTaskFailPoint.pauseWhileEnabled(MceTaskFailPoint.FP_MCE_INTERNALIZE_BEFORE_CHANGE_READ_MODE,
            executionContext);
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        long t0 = System.currentTimeMillis();
        MCE_LOGGER.info(String.format("[MCE] MceInternalizeChangeReadModeTask START table=%s.%s",
            schemaName, tableName));

        // The read cutover back to plaintext is the reverse pipeline's point of no return. Disable
        // CANCEL in the same MetaDB transaction as the flag/mapping/control cutover.
        updateSupportedCommands(true, false, metaDbConnection);

        TableInfoManager tableInfoManager = new TableInfoManager();
        tableInfoManager.setConnection(metaDbConnection);
        ColumnsAccessor columnsAccessor = new ColumnsAccessor();
        columnsAccessor.setConnection(metaDbConnection);

        columnsAccessor.resetColumnFlag(schemaName, tableName, columnName,
            ColumnsRecord.FLAG_EXTERNALIZED_COLUMN);
        tableInfoManager.updateColumnMappingName(schemaName, tableName, columnName, null);

        MceColumnStateAccessor mceAccessor = new MceColumnStateAccessor();
        mceAccessor.setConnection(metaDbConnection);
        MceControlStateHelper.transition(mceAccessor, getJobId() == null ? 0L : getJobId(),
            schemaName, tableName, columnName, addrColumnName,
            MceColumnStateRecord.STATE_READ_ADDR,
            MceColumnStateRecord.STATE_DUAL_WRITE);

        MCE_LOGGER.info(
            String.format("[MCE] MceInternalizeChangeReadModeTask DONE table=%s.%s → DUAL_WRITE (cost %sms)",
                schemaName, tableName, System.currentTimeMillis() - t0));
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        // Not rollbackable after the read cutover back to plaintext.
    }

    @Override
    protected String remark() {
        return String.format("|MCE internalize READ_ADDR→DUAL_WRITE (read cutover to plaintext), table=%s, column=%s",
            tableName, columnName);
    }
}
