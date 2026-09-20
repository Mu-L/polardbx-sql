/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ddl.job.converter.PhysicalPlanData;
import com.alibaba.polardbx.executor.ddl.job.factory.util.FactoryUtils;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropEntitySecurityAttrTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropTableHideTableMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropTablePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropTableRemoveMetaTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.DropTableValidateTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.StoreTableLocalityTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.cdc.CdcDdlMarkTask;
import com.alibaba.polardbx.executor.ddl.job.task.columnar.CleanBlobCacheForTableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.columnar.DropBlobColumnMappingTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJobFactory;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.wrapper.ExecutableDdlJob4DropTable;
import com.alibaba.polardbx.gms.lbac.LBACSecurityEntity;
import com.alibaba.polardbx.gms.lbac.LBACSecurityManager;
import com.alibaba.polardbx.gms.lbac.LBACSecurityLabel;
import com.alibaba.polardbx.gms.lbac.LBACSecurityPolicy;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.ScaleOutPlanUtil;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.google.common.collect.Lists;

import java.util.*;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.common.cdc.ICdcManager.DEFAULT_DDL_VERSION_ID;

public class DropTableJobFactory extends DdlJobFactory {

    protected final PhysicalPlanData physicalPlanData;
    protected final String schemaName;
    protected final String logicalTableName;
    protected final ExecutionContext executionContext;

    public DropTableJobFactory(PhysicalPlanData physicalPlanData, ExecutionContext executionContext) {
        this.physicalPlanData = physicalPlanData;
        this.schemaName = physicalPlanData.getSchemaName();
        this.logicalTableName = physicalPlanData.getLogicalTableName();
        this.executionContext = executionContext;
    }

    @Override
    protected void validate() {

    }

    @Override
    protected ExecutableDdlJob doCreate() {
        DropTableValidateTask validateTask = new DropTableValidateTask(schemaName, logicalTableName);
        DropTableRemoveMetaTask removeMetaTask = new DropTableRemoveMetaTask(schemaName, logicalTableName, true);
        StoreTableLocalityTask storeTableLocalityTask =
            new StoreTableLocalityTask(schemaName, logicalTableName, "", true);
        TableSyncTask tableSyncTask = new TableSyncTask(schemaName, logicalTableName);
        TableSyncTask localityTableSyncTask = new TableSyncTask(schemaName, logicalTableName);
        boolean recycleBinEnable = ScaleOutPlanUtil.isPhyRecyclebinEnable(executionContext);
        DdlTask phyDdlTask;
        List<DdlTask> dropForeignKeyTasksBeforeRename = new ArrayList<>();
        TableMeta tableMeta = null;
        boolean hasForeignKey = false;
        try {
            tableMeta = executionContext.getSchemaManager(schemaName).getTable(logicalTableName);
            hasForeignKey = tableMeta.hasForeignKey();
        } catch (Exception ex) {
            // pass
        }
        boolean hasExternalColumnLifecycle = tableMeta != null
            && (tableMeta.hasExternalizedColumn() || tableMeta.hasColumnInMceLifecycle());
        if (recycleBinEnable && !hasForeignKey) {
            Map<String, Set<String>> newTopology = new HashMap<>();
            Map<String, List<List<String>>> topology = physicalPlanData.getTableTopology();
            topology.forEach((k, v) ->
                newTopology.computeIfAbsent(k,
                    i -> v.stream().map(l -> l.get(0)).collect(Collectors.toSet()))
            );
            phyDdlTask =
                ComplexTaskFactory.createRenameUselessPhyTableTask(schemaName, logicalTableName, newTopology, null,
                    dropForeignKeyTasksBeforeRename, false, executionContext);
        } else {
            phyDdlTask = new DropTablePhyDdlTask(schemaName, physicalPlanData);
        }
        DropEntitySecurityAttrTask desaTask = createDESATask();
        CdcDdlMarkTask cdcDdlMarkTask =
            new CdcDdlMarkTask(schemaName, physicalPlanData, false, false, DEFAULT_DDL_VERSION_ID);
        ExecutableDdlJob4DropTable executableDdlJob = new ExecutableDdlJob4DropTable();

        List<DdlTask> taskList;
        if (hasExternalColumnLifecycle) {
            taskList = Lists.newArrayList(
                validateTask,
                storeTableLocalityTask,
                localityTableSyncTask,
                new DropTableHideTableMetaTask(schemaName, logicalTableName),
                new DropBlobColumnMappingTask(schemaName, logicalTableName),
                new TableSyncTask(schemaName, logicalTableName));
        } else {
            taskList = Lists.newArrayList(
                validateTask,
                storeTableLocalityTask,
                localityTableSyncTask,
                removeMetaTask,
                tableSyncTask);
        }
        if (GeneralUtil.isNotEmpty(dropForeignKeyTasksBeforeRename)) {
            taskList.addAll(dropForeignKeyTasksBeforeRename);
        }
        taskList.add(phyDdlTask);
        taskList.add(desaTask);
        taskList.add(cdcDdlMarkTask);
        if (hasExternalColumnLifecycle) {
            taskList.add(removeMetaTask);
            taskList.add(new CleanBlobCacheForTableSyncTask(schemaName, logicalTableName));
            taskList.add(tableSyncTask);
        }

        // sync foreign key table meta
        taskList.addAll(FactoryUtils.getFkTableSyncTasks(schemaName, logicalTableName));

        executableDdlJob.addSequentialTasks(taskList.stream().filter(Objects::nonNull).collect(Collectors.toList()));

        //labels should be replaced by fields in ExecutableDdlJob4DropTable
        executableDdlJob.labelAsHead(validateTask);
        executableDdlJob.labelAsTail(tableSyncTask);

        executableDdlJob.setValidateTask(validateTask);
        executableDdlJob.setRemoveMetaTask(removeMetaTask);
        executableDdlJob.setTableSyncTaskAfterRemoveMeta(tableSyncTask);
        executableDdlJob.setPhyDdlTask(phyDdlTask);
        executableDdlJob.setCdcDdlMarkTask(cdcDdlMarkTask);
        if (hasExternalColumnLifecycle) {
            executableDdlJob.setDropGsiTaskAnchor(tableSyncTask);
        }

        return executableDdlJob;
    }

    @Override
    protected void excludeResources(Set<String> resources) {
        resources.add(concatWithDot(schemaName, logicalTableName));

        // exclude foreign key tables
        FactoryUtils.getFkTableExcludeResources(schemaName, logicalTableName, resources);
    }

    @Override
    protected void sharedResources(Set<String> resources) {
    }

    protected DropEntitySecurityAttrTask createDESATask() {
        List<LBACSecurityEntity> esaList = new ArrayList<>();
        LBACSecurityPolicy policy = LBACSecurityManager.getInstance().getTablePolicy(schemaName, logicalTableName);
        if (policy == null) {
            return null;
        }
        esaList.add(new LBACSecurityEntity(
            LBACSecurityEntity.EntityKey.createTableKey(schemaName, logicalTableName),
            LBACSecurityEntity.EntityType.TABLE,
            policy.getPolicyName()));
        List<ColumnMeta> columnMetas =
            OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTable(logicalTableName).getAllColumns();
        for (ColumnMeta columnMeta : columnMetas) {
            LBACSecurityLabel
                label =
                LBACSecurityManager.getInstance().getColumnLabel(schemaName, logicalTableName, columnMeta.getName());
            if (label == null) {
                continue;
            }
            esaList.add(new LBACSecurityEntity(
                LBACSecurityEntity.EntityKey.createColumnKey(schemaName, logicalTableName, columnMeta.getName()),
                LBACSecurityEntity.EntityType.COLUMN,
                label.getLabelName()
            ));
        }
        return esaList.isEmpty() ? null : new DropEntitySecurityAttrTask(schemaName, logicalTableName, esaList);
    }

}
