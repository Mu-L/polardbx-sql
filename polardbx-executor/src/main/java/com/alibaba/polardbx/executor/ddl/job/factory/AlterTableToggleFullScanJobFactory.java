package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.common.cdc.CdcDdlMarkVisibility;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.AlterTableToggleFullScanTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.CreatePhyTableWithRollbackCheckTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TablesSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.UpdateTablesVersionTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcAlterTableSetTableGroupMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcTableGroupDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupAddMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableGroupValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableSetGroupAddSubTaskMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.AlterTableSetTableGroupChangeMetaOnlyTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.CleanupEmptyTableGroupTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.JoinGroupValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.ReloadTableMetaAfterChangeTableGroupTask;
import com.alibaba.polardbx.executor.ddl.job.task.tablegroup.TableGroupSyncTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.OnlineDdlInfo;
import com.alibaba.polardbx.executor.ddl.newengine.job.TransientDdlJob;
import com.alibaba.polardbx.executor.partitionmanagement.AlterTableGroupUtils;
import com.alibaba.polardbx.executor.scaleout.ScaleOutUtils;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ComplexTaskMetaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableSetTableGroupPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableToggleFullScanPreparedData;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoUtil;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.tablegroup.AlterTableGroupSnapShotUtils;
import com.alibaba.polardbx.optimizer.tablegroup.TableGroupInfoManager;
import com.alibaba.polardbx.rule.TableRule;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.DDL;
import org.apache.calcite.sql.SqlKind;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;

/**
 * @author luoyanxin
 */
public class AlterTableToggleFullScanJobFactory extends DdlJobFactory {

    @Deprecated
    private final DDL ddl;
    private final AlterTableToggleFullScanPreparedData preparedData;
    protected final ExecutionContext executionContext;

    public AlterTableToggleFullScanJobFactory(DDL ddl, AlterTableToggleFullScanPreparedData preparedData,
                                              ExecutionContext executionContext) {
        this.preparedData = preparedData;
        this.ddl = ddl;
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();
        boolean isNewPart = DbInfoManager.getInstance().isNewPartitionDb(preparedData.getSchemaName());
        TableMeta tableMeta =
            executionContext.getSchemaManager(preparedData.getSchemaName()).getTable(preparedData.getTableName());
        if (isNewPart) {
            PartitionInfo partitionInfo = tableMeta.getPartitionInfo();
            if (partitionInfo.isBlockFullTableScan() && !preparedData.isEnable()) {
                return new TransientDdlJob();
            } else if (!partitionInfo.isBlockFullTableScan() && preparedData.isEnable()) {
                return new TransientDdlJob();
            }
        } else {
            TableRule tableRule =
                OptimizerContext.getContext(tableMeta.getSchemaName())
                    .getRuleManager()
                    .getTableRule(tableMeta.getTableName());
            if (!tableRule.isAllowFullTableScan() && !preparedData.isEnable()) {
                return new TransientDdlJob();
            } else if (tableRule.isAllowFullTableScan() && preparedData.isEnable()) {
                return new TransientDdlJob();
            }
        }
        AlterTableToggleFullScanTask toggleTask =
            new AlterTableToggleFullScanTask(preparedData.getSchemaName(), preparedData.getTableName(),
                preparedData.isEnable());
        TableSyncTask syncTask = new TableSyncTask(preparedData.getSchemaName(), preparedData.getTableName());
        executableDdlJob.addSequentialTasks(ImmutableList.of(toggleTask, syncTask));
        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(preparedData.getSchemaName(), preparedData.getTableName()));
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

}
