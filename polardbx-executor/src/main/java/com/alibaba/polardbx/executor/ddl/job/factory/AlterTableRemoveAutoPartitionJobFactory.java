package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.executor.ddl.job.task.basic.RemoveAutoPartitionChangeMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcRepartitionMarkTask;
import com.alibaba.polardbx.executor.ddl.job.validator.GsiValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.job.validator.ddl.RepartitionValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.OnlineDdlInfo;
import com.alibaba.polardbx.executor.ddl.newengine.job.OnlineDdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import org.apache.calcite.sql.SqlKind;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;

import static com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility.Protected;

/**
 * @author wumu
 */
public class AlterTableRemoveAutoPartitionJobFactory extends OnlineDdlJobFactory {

    private final String schemaName;
    private final String primaryTableName;
    private final ExecutionContext executionContext;

    public AlterTableRemoveAutoPartitionJobFactory(String schemaName, String primaryTableName,
                                                   ExecutionContext executionContext) {
        super(executionContext, OnlineDdlInfo.DdlAlgorithm.INSTANT);
        this.schemaName = schemaName;
        this.primaryTableName = primaryTableName;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {
        TableValidator.validateTableExistence(schemaName, primaryTableName, executionContext);
        GsiValidator.validateAllowDdlOnTable(schemaName, primaryTableName, executionContext);
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        //validate
        boolean isAutoPartition = RepartitionValidator.validateRemoveAutoPartition(
            schemaName,
            primaryTableName
        );

        if (!isAutoPartition) {
            return new TransientDdlJob();
        }

        RemoveAutoPartitionChangeMetaTask removeAutoPartitionChangeMetaTask = new RemoveAutoPartitionChangeMetaTask(
            schemaName,
            primaryTableName
        );
        TableSyncTask tableSyncTask = new TableSyncTask(schemaName, primaryTableName);
        CdcRepartitionMarkTask cdcDdlMarkTask = new CdcRepartitionMarkTask(
            schemaName, primaryTableName, SqlKind.ALTER_TABLE, Protected);

        ExecutableDdlJob result = new ExecutableDdlJob();
        result.addSequentialTasks(Arrays.asList(removeAutoPartitionChangeMetaTask, tableSyncTask, cdcDdlMarkTask));

        return result;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, primaryTableName));

        // lock table group of primary table
        OptimizerContext oc =
            Objects.requireNonNull(OptimizerContext.getContext(schemaName), schemaName + " corrupted");

        PartitionInfo partitionInfo = oc.getPartitionInfoManager().getPartitionInfo(primaryTableName);
        if (partitionInfo != null && partitionInfo.getTableGroupId() != -1) {
            TableGroupConfig tableGroupConfig =
                oc.getTableGroupInfoManager().getTableGroupConfigById(partitionInfo.getTableGroupId());
            String tgName = tableGroupConfig.getTableGroupRecord().getTg_name();
            resources.add(concatWithDot(schemaName, tgName));
        }
    }

    @Override
    protected void sharedResources(Set<String> resources) {

    }
}
