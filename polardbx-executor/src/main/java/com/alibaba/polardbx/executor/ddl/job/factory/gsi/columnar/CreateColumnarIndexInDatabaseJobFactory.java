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

import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.sql.ast.SQLCommentHint;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.job.validator.IndexValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.OnlineDdlInfo;
import com.alibaba.polardbx.executor.ddl.newengine.job.OnlineDdlJobFactory;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateIndexInDatabase;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.CreateIndexInDatabasePreparedData;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import org.apache.calcite.sql.SqlNodeList;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class CreateColumnarIndexInDatabaseJobFactory extends OnlineDdlJobFactory {

    protected final String dbName;
    protected final boolean isColumnar;
    protected ExecutionContext executionContext;

    protected LogicalCreateIndexInDatabase logicalCreateIndexInDatabase;

    private final List<Pair<String, String>> addCciSql;
    private final List<Pair<String, String>> tableNameAndIndexNameList;

    public CreateColumnarIndexInDatabaseJobFactory(CreateIndexInDatabasePreparedData preparedData,
                                                   LogicalCreateIndexInDatabase logicalCreateIndexInDatabase,
                                                   ExecutionContext executionContext) {
        this(
            executionContext,
            logicalCreateIndexInDatabase,
            preparedData.getDbName(),
            preparedData.isColumnar(),
            preparedData.getAddCciSql(),
            preparedData.getTableNameAndIndexNameList()
        );
    }

    public CreateColumnarIndexInDatabaseJobFactory(ExecutionContext executionContext,
                                                   LogicalCreateIndexInDatabase logicalCreateIndexInDatabase,
                                                   String dbName,
                                                   boolean isColumnar,
                                                   List<Pair<String, String>> addCciSql,
                                                   List<Pair<String, String>> tableNameAndIndexNameList) {
        super(executionContext, OnlineDdlInfo.DdlAlgorithm.OSC);
        this.executionContext = executionContext;
        this.logicalCreateIndexInDatabase = logicalCreateIndexInDatabase;
        this.dbName = dbName;
        this.isColumnar = isColumnar;
        this.addCciSql = addCciSql;
        this.tableNameAndIndexNameList = tableNameAndIndexNameList;
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob createColumnarIndexInDatabaseJob = new ExecutableDdlJob();

        SqlNodeList hints = logicalCreateIndexInDatabase.getSqlCreateIndexInDatabase().getHints();
        String hintString = "";
        if (hints != null && hints.size() != 0) {
            List<SQLCommentHint> headHints =
                FastsqlUtils.parseSql(executionContext.getSql()).get(0).getHeadHintsDirect();
            if (headHints != null && headHints.size() == 1) {
                hintString = headHints.get(0).toString();
            } else {
                throw new UnsupportedOperationException(
                    "Only support one hint in CREATE INDEX FOR TABLES IN/FROM DATABASE.");
            }
        }

        List<SubJobTask> addCciSubJobTasks = new ArrayList<>();
        for (Pair<String, String> sql : addCciSql) {
            SubJobTask addCciSubJobTask =
                new SubJobTask(dbName, hintString + sql.getKey(), hintString + sql.getValue());
            addCciSubJobTask.setParentAcquireResource(true);
            addCciSubJobTasks.add(addCciSubJobTask);
        }

        for (int i = 0; i < addCciSubJobTasks.size(); i++) {
            if (i == 0) {
                createColumnarIndexInDatabaseJob.appendTask(addCciSubJobTasks.get(i));
            } else {
                createColumnarIndexInDatabaseJob.addTaskRelationship(addCciSubJobTasks.get(i - 1),
                    addCciSubJobTasks.get(i));
            }
        }

        return createColumnarIndexInDatabaseJob;
    }

    @Override
    protected void validate() {
        validateColumnar();
        validateEmptyTask();
        validatePrimaryKeys();
        validateCciLimitOnSameTable();
        validateSameIndex();
    }

    private void validateColumnar() {
        if (!isColumnar) {
            throw new TddlNestableRuntimeException(
                "Non-Columnar indexes are not allowed to use the [CREATE INDEX FOR TABLES IN/FROM DATABASE] syntax.");
        }
    }

    private void validateEmptyTask() {
        if (addCciSql.isEmpty()) {
            throw new TddlNestableRuntimeException(
                "All tables in database '" + dbName + "' have Clustered Columnar Indexes.");
        }
    }

    private void validatePrimaryKeys() {
        List<TableMeta> allTables = OptimizerContext.getContext(dbName).getLatestSchemaManager().getAllUserTables();
        for (TableMeta tableMeta : allTables) {
            if (!executionContext.getParamManager()
                .getBoolean(ConnectionParams.ENABLE_CCI_ON_TABLE_WITH_IMPLICIT_PK) &&
                !GlobalIndexMeta.hasExplicitPrimaryKey(tableMeta)) {
                throw new TddlNestableRuntimeException(
                    "Do not support create Clustered Columnar Index on table without primary key on table '"
                        + tableMeta.getTableName() + "'");
            }
        }
    }

    private void validateCciLimitOnSameTable() {
        for (Pair<String, String> tableNameAndIndexName : tableNameAndIndexNameList) {
            String tableName = tableNameAndIndexName.getKey();
            IndexValidator.validateColumnarIndexNumLimit(dbName, tableName,
                executionContext.getParamManager().getLong(ConnectionParams.MAX_CCI_COUNT));
        }
    }

    private void validateSameIndex() {
        for (Pair<String, String> tableNameAndIndexName : tableNameAndIndexNameList) {
            String tableName = tableNameAndIndexName.getKey();
            String indexName = tableNameAndIndexName.getValue();
            IndexValidator.validateIndexNonExistence(dbName, tableName, indexName);
        }
    }

    @Override
    protected void excludeResources(Set<String> resources) {

    }

    @Override
    protected void sharedResources(Set<String> resources) {

    }

    public static ExecutableDdlJob create(LogicalCreateIndexInDatabase logicalCreateIndexInDatabase,
                                          ExecutionContext executionContext) {
        return new CreateColumnarIndexInDatabaseJobFactory(
            logicalCreateIndexInDatabase.getCreateIndexInDatabasePreparedData(),
            logicalCreateIndexInDatabase,
            executionContext).create();
    }
}
