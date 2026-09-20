package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.executor.ddl.job.task.basic.*;
import com.alibaba.polardbx.executor.ddl.job.task.exchangepartition.ExchangePartitionChangeMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupsSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.OnlineDdlInfo;
import com.alibaba.polardbx.executor.ddl.newengine.job.OnlineDdlJobFactory;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableExchangePartitionPreparedData;
import com.google.common.collect.Lists;

import java.util.*;

/**
 * @author luoyanxin
 */
public class AlterTableExchangePartitionJobFactory extends OnlineDdlJobFactory {

    final AlterTableExchangePartitionPreparedData preparedData;
    final ExecutionContext executionContext;
    protected static final String SET_NEW_TABLE_GROUP = "alter table `%s` set tablegroup=''";

    public AlterTableExchangePartitionJobFactory(AlterTableExchangePartitionPreparedData preparedData,
                                                 ExecutionContext executionContext) {
        super(executionContext, OnlineDdlInfo.DdlAlgorithm.META_ONLY);
        this.preparedData = preparedData;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        if (preparedData.isExclusiveTargetTableGroup() && preparedData.isExclusiveSrcTableGroup()) {
            return doExchangeInOriginTableGroup();
        } else {
            return doExchangeInNewTableGroup();
        }
    }

    private ExecutableDdlJob doExchangeInOriginTableGroup() {
        SchemaManager schemaManager = executionContext.getSchemaManager(preparedData.getSchemaName());
        TableMeta srcTableMeta = schemaManager.getTable(preparedData.getSourceTableName());
        TableMeta targetTableMeta = schemaManager.getTable(preparedData.getTargetTableName());
        Map<String, Long> srcTableVersion = new HashMap<>();
        srcTableVersion.put(srcTableMeta.getTableName(), srcTableMeta.getVersion());
        Map<String, Long> targetTableVersion = new HashMap<>();
        targetTableVersion.put(targetTableMeta.getTableName(), targetTableMeta.getVersion());
        DdlTask validateSrcTableTask =
            new AlterTableGroupValidateTask(preparedData.getSchemaName(), preparedData.getSrcTableGroupName(),
                srcTableVersion, true, null, false);
        DdlTask validateTargetTableTask =
            new AlterTableGroupValidateTask(preparedData.getSchemaName(), preparedData.getTargetTableGroupName(),
                targetTableVersion, true, null, false);

        ExchangePartitionChangeMetaTask changeMetaTask =
            new ExchangePartitionChangeMetaTask(preparedData.getSchemaName(),
                preparedData.getSourceTableName(),
                preparedData.getTargetTableName(),
                preparedData.getSourcePartitionNames(),
                preparedData.getTargetPartitionNames());
        List<String> tableNames = Lists.newArrayList(preparedData.getSourceTableName(),
            preparedData.getTargetTableName());
        List<String> tableGroupNames = Lists.newArrayList(preparedData.getSrcTableGroupName(),
            preparedData.getTargetTableGroupName());
        TablesSyncTask syncTableTask = new TablesSyncTask(preparedData.getSchemaName(), tableNames);
        TableGroupsSyncTask tableGroupsSyncTask =
            new TableGroupsSyncTask(preparedData.getSchemaName(), tableGroupNames);
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addSequentialTasks(
            Arrays.asList(validateSrcTableTask, validateTargetTableTask, changeMetaTask, syncTableTask,
                tableGroupsSyncTask));
        return executableDdlJob;
    }

    private ExecutableDdlJob doExchangeInNewTableGroup() {
        SchemaManager schemaManager = executionContext.getSchemaManager(preparedData.getSchemaName());
        TableMeta srcTableMeta = schemaManager.getTable(preparedData.getSourceTableName());
        TableMeta targetTableMeta = schemaManager.getTable(preparedData.getTargetTableName());
        Map<String, Long> srcTableVersion = new HashMap<>();
        srcTableVersion.put(srcTableMeta.getTableName(), srcTableMeta.getVersion());
        Map<String, Long> targetTableVersion = new HashMap<>();
        targetTableVersion.put(targetTableMeta.getTableName(), targetTableMeta.getVersion());
        List<DdlTask> ddlTasks = new ArrayList<>();
        if (!preparedData.isExclusiveSrcTableGroup()) {
            SubJobTask subJobsrcTableToNewGroup =
                new SubJobTask(preparedData.getSchemaName(),
                    String.format(SET_NEW_TABLE_GROUP, preparedData.getSourceTableName()), null);
            subJobsrcTableToNewGroup.setParentAcquireResource(true);
            ddlTasks.add(subJobsrcTableToNewGroup);
        }
        if (!preparedData.isExclusiveTargetTableGroup()) {
            SubJobTask subJobTrgetTableToNewGroup =
                new SubJobTask(preparedData.getSchemaName(),
                    String.format(SET_NEW_TABLE_GROUP, preparedData.getTargetTableName()), null);
            subJobTrgetTableToNewGroup.setParentAcquireResource(true);
            ddlTasks.add(subJobTrgetTableToNewGroup);
        }
        SubJobTask subJobMoveTable = new SubJobTask(preparedData.getSchemaName(), preparedData.getSourceSql(), null);
        ddlTasks.add(subJobMoveTable);
        subJobMoveTable.setParentAcquireResource(true);
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        executableDdlJob.addSequentialTasks(ddlTasks);
        return executableDdlJob;
    }

    public static ExecutableDdlJob create(AlterTableExchangePartitionPreparedData preparedData,
                                          ExecutionContext executionContext) {
        return new AlterTableExchangePartitionJobFactory(preparedData, executionContext).create();
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getSourceTableName()));
        resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getTargetTableName()));
        resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getSrcTableGroupName()));
        resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getTargetTableGroupName()));
    }

    @Override
    protected void sharedResources(Set<String> resources) {

    }

}
