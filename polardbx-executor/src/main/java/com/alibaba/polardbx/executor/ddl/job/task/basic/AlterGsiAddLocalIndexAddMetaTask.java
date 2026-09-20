package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.fastjson.annotation.JSONCreator;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.meta.GsiMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.meta.TableMetaChanger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseGmsTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.executor.sync.TableMetaChangePreemptiveSyncAction;
import com.alibaba.polardbx.gms.metadb.table.LackLocalIndexStatus;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.config.table.PreemptiveTime;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;
import org.apache.commons.lang3.StringUtils;

import java.sql.Connection;
import java.util.List;

@TaskName(name = "AlterGsiAddLocalIndexAddMetaTask")
@Getter
public class AlterGsiAddLocalIndexAddMetaTask extends BaseGmsTask {
    String schemaName;

    String logicalTableName;

    List<String> indexes;

    String dbIndex;

    String indexName;

    String phyTableName;

    @JSONCreator
    public AlterGsiAddLocalIndexAddMetaTask(String schemaName,
                                            String logicalTableName,
                                            String indexName,
                                            List<String> indexes,
                                            String dbIndex,
                                            String phyTableName) {
        super(schemaName, logicalTableName);
        this.schemaName = schemaName;
        this.logicalTableName = logicalTableName;
        this.indexName = indexName;
        this.indexes = indexes;
        this.dbIndex = dbIndex;
        this.phyTableName = phyTableName;
    }

    @Override
    protected void executeImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        // Add column
        TableMetaChanger.addIndexesMetaOnly(metaDbConnection, schemaName, indexName, indexes, dbIndex, phyTableName);
        GsiMetaChanger.updateLackingLocalIndex(metaDbConnection, schemaName, logicalTableName, indexName,
            LackLocalIndexStatus.NO_LACKIING);
    }

    @Override
    protected void rollbackImpl(Connection metaDbConnection, ExecutionContext executionContext) {
        GsiMetaChanger.updateLackingLocalIndex(metaDbConnection, schemaName, logicalTableName, indexName,
            LackLocalIndexStatus.LACKING);
        TableMetaChanger.removeIndexesMetaOnly(metaDbConnection, schemaName, logicalTableName, indexes, dbIndex,
            phyTableName);
    }

    @Override
    protected void onExecutionSuccess(ExecutionContext executionContext) {
        PreemptiveTime preemptiveTime = PreemptiveTime.getPreemptiveTimeFromExecutionContext(executionContext,
            ConnectionParams.PREEMPTIVE_MDL_INITWAIT, ConnectionParams.PREEMPTIVE_MDL_INTERVAL);
        if (!StringUtils.isEmpty(logicalTableName)) {
            SyncManagerHelper.syncThrowExceptions(
                new TableMetaChangePreemptiveSyncAction(schemaName, logicalTableName, preemptiveTime), SyncScope.ALL);
        }
    }
}
