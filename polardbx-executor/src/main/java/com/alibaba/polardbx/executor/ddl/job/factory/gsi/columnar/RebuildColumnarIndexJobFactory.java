/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.executor.ddl.job.factory.gsi.columnar;

import com.alibaba.polardbx.common.ColumnarOptions;
import com.alibaba.polardbx.common.properties.ColumnarConfig;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TableSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.basic.TablesSyncTask;
import com.alibaba.polardbx.executor.ddl.job.task.columnar.IgnoreCciTask;
import com.alibaba.polardbx.executor.ddl.job.task.columnar.RebuildCciCutOverTask;
import com.alibaba.polardbx.executor.ddl.job.task.gsi.ValidateTableVersionTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.OnlineDdlInfo;
import com.alibaba.polardbx.executor.ddl.newengine.job.OnlineDdlJobFactory;
import com.alibaba.polardbx.executor.utils.DdlUtils;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.CreateGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.DropGlobalIndexPreparedData;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.gsi.RebuildCciPreparedData;
import org.apache.calcite.sql.SqlAddIndex;
import org.apache.calcite.sql.SqlAlterTable;
import org.apache.calcite.sql.SqlCreateTable;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexDefinition;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

public class RebuildColumnarIndexJobFactory extends OnlineDdlJobFactory {

    protected ExecutionContext executionContext;
    RebuildCciPreparedData preparedData;
    Pair<String, SqlCreateTable> primaryTableInfo;
    LogicalAlterTable logicalAlterTable;

    public RebuildColumnarIndexJobFactory(LogicalAlterTable logicalAlterTable,
                                          Pair<String, SqlCreateTable> primaryTableInfo,
                                          ExecutionContext executionContext) {
        super(executionContext, OnlineDdlInfo.DdlAlgorithm.OSC);
        this.logicalAlterTable = logicalAlterTable;
        this.preparedData = logicalAlterTable.getAlterTableWithGsiPreparedData().getRebuildCciPreparedData();
        this.primaryTableInfo = primaryTableInfo;
        this.executionContext = executionContext;
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob executableDdlJob = new ExecutableDdlJob();

        String schemaName = preparedData.getSchemaName();
        String tableName = preparedData.getTableName();
        String cciName = preparedData.getCciName();
        String randomCciName = preparedData.getRandomName();
        SqlIndexDefinition def = preparedData.getIndexDefinition();

        // add cci job
        final SqlIdentifier tempIndexName = new SqlIdentifier(randomCciName, SqlParserPos.ZERO);
        final SqlAddIndex sqlAddIndex =
            new SqlAddIndex(SqlParserPos.ZERO, tempIndexName, def);

        sqlAddIndex.getIndexDef().setPrimaryTableDefinition(primaryTableInfo.getKey());
        sqlAddIndex.getIndexDef().setPrimaryTableNode(primaryTableInfo.getValue());

        SqlAlterTable sqlAlterTable = (SqlAlterTable) logicalAlterTable.relDdl.getSqlNode();
        sqlAlterTable.getAlters().set(0, sqlAddIndex);

        CreateGlobalIndexPreparedData createGlobalIndexPreparedData =
            logicalAlterTable.prepareCreateGsiData(randomCciName, sqlAddIndex);
        long versionId = DdlUtils.generateVersionId(executionContext);
        createGlobalIndexPreparedData.setDdlVersionId(versionId);
        createGlobalIndexPreparedData.setMarkByHint(true);

        ExecutableDdlJob addCciJob =
            buildCreateCciJob(logicalAlterTable, createGlobalIndexPreparedData, executionContext);
        executableDdlJob.appendJob2(addCciJob);

        Map<String, String> cciMap = new HashMap<>();
        cciMap.put(cciName, randomCciName);

        // cut over
        RebuildCciCutOverTask cutOverTask = new RebuildCciCutOverTask(schemaName, tableName, cciMap);
        executableDdlJob.appendTask(cutOverTask);
        DdlTask syncCciTask = new TableSyncTask(schemaName, cciName);
        executableDdlJob.appendTask(syncCciTask);
        DdlTask syncTask = new TableSyncTask(schemaName, tableName);
        executableDdlJob.appendTask(syncTask);

        String columnarType = def.getColumnarOptions().get(ColumnarOptions.TYPE);
        // do not drop cci if it is snapshot
        if (columnarType != null && columnarType.equalsIgnoreCase(ColumnarConfig.SNAPSHOT)) {
            // ignore remained cci
            executableDdlJob.appendTask(new IgnoreCciTask(schemaName, tableName,
                Collections.singletonList(randomCciName)));
            executableDdlJob.appendTask(new TablesSyncTask(schemaName, Collections.singletonList(randomCciName)));
            executableDdlJob.appendTask(new TableSyncTask(schemaName, tableName));
        } else {
            // drop cci jobs
            DropGlobalIndexPreparedData dropGlobalIndexPreparedData = new DropGlobalIndexPreparedData(
                schemaName,
                tableName,
                randomCciName,
                false);
            dropGlobalIndexPreparedData.setOriginalIndexName(randomCciName);
            versionId = DdlUtils.generateVersionId(executionContext);
            dropGlobalIndexPreparedData.setDdlVersionId(versionId);
            dropGlobalIndexPreparedData.setMarkByHint(true);
            ExecutableDdlJob dropCciJob = buildDropCciJob(dropGlobalIndexPreparedData, executionContext);

            executableDdlJob.appendJob2(dropCciJob);
        }
        return executableDdlJob;
    }

    private ExecutableDdlJob buildCreateCciJob(LogicalAlterTable logicalAlterTable,
                                               CreateGlobalIndexPreparedData cciPreparedData,
                                               ExecutionContext executionContext) {

        return CreateColumnarIndexJobFactory.create4CreateCci(
            logicalAlterTable.relDdl,
            cciPreparedData,
            executionContext);
    }

    private ExecutableDdlJob buildDropCciJob(DropGlobalIndexPreparedData preparedData,
                                             ExecutionContext executionContext) {
        final Map<String, Long> tableVersions = new HashMap<>();
        tableVersions.put(preparedData.getPrimaryTableName(), preparedData.getTableVersion());
        final ValidateTableVersionTask validateTableVersionTask =
            new ValidateTableVersionTask(preparedData.getSchemaName(), tableVersions);

        ExecutableDdlJob cciJob = DropColumnarIndexJobFactory.create(preparedData, executionContext, false, true);
        cciJob.addTask(validateTableVersionTask);
        cciJob.addTaskRelationship(validateTableVersionTask, cciJob.getHead());

        return cciJob;
    }

    @Override
    protected void validate() {
        TableMeta tableMeta = OptimizerContext.getContext(preparedData.getSchemaName()).getLatestSchemaManager()
            .getTable(preparedData.getCciName());
        if (tableMeta.isColumnarArchive()) {
            throw new UnsupportedOperationException("Do not support rebuild archive columnar index");
        }
        if (!executionContext.getParamManager().getBoolean(ConnectionParams.ENABLE_REBUILD_SNAPSHOT_CCI)
            && tableMeta.isColumnarSnapshot()) {
            throw new UnsupportedOperationException("Do not support rebuild snapshot columnar index");
        }
    }

    @Override
    protected void excludeResources(Set<String> resources) {

    }

    @Override
    protected void sharedResources(Set<String> resources) {

    }

    public static ExecutableDdlJob rebuildCci(LogicalAlterTable logicalAlterTable,
                                              Pair<String, SqlCreateTable> primaryTableInfo,
                                              ExecutionContext ec) {
        return new RebuildColumnarIndexJobFactory(logicalAlterTable, primaryTableInfo, ec).create();
    }
}
