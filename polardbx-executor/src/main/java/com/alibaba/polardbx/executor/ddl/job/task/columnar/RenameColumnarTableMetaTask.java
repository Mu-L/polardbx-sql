package com.alibaba.polardbx.executor.ddl.job.task.columnar;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

import java.sql.Connection;

@Getter
@TaskName(name = "RenameColumnarTableMetaTask")
public class RenameColumnarTableMetaTask extends BaseGmsTask {
    private final String primaryTableName;
    private final String newPrimaryTableName;
    private final long versionId;

    @JSONCreator
    public RenameColumnarTableMetaTask(String schemaName, String primaryTableName, String newPrimaryTableName,
                                       long versionId) {
        super(schemaName, primaryTableName);
        this.primaryTableName = primaryTableName;
        this.newPrimaryTableName = newPrimaryTableName;
        this.versionId = versionId;
        onExceptionTryRecoveryThenRollback();
    }

    @Override
    public void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        FailPoint.injectRandomExceptionFromHint(executionContext);
        FailPoint.injectRandomSuspendFromHint(executionContext);
        TableMetaChanger.renameColumnarTableMeta(metaDbConnection, schemaName, primaryTableName, newPrimaryTableName,
            versionId, jobId);
    }

    @Override
    public void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        TableMetaChanger.renameColumnarTableMeta(metaDbConnection, schemaName, newPrimaryTableName, primaryTableName,
            versionId, jobId);
    }

    @Override
    protected void updateTableVersion(Connection metaDbConnection) {
        // 不修改版本，后续 qihua 会废弃该 task，和主表元数据一同提交修改，保证原子性
    }
}
