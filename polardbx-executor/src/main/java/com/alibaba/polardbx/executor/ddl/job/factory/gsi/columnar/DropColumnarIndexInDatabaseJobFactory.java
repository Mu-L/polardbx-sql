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
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.druid.sql.ast.SQLCommentHint;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.OnlineDdlInfo;
import com.alibaba.polardbx.executor.ddl.newengine.job.OnlineDdlJobFactory;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropIndexInDatabase;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.DropIndexInDatabasePreparedData;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import org.apache.calcite.sql.SqlNodeList;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class DropColumnarIndexInDatabaseJobFactory extends OnlineDdlJobFactory {
    protected final String dbName;
    protected final boolean isColumnar;
    protected ExecutionContext executionContext;

    protected LogicalDropIndexInDatabase logicalDropIndexInDatabase;

    private final List<Pair<String, String>> dropCciSql;
    private final List<Pair<String, String>> tableNameAndIndexNameList;

    public DropColumnarIndexInDatabaseJobFactory(DropIndexInDatabasePreparedData preparedData,
                                                 LogicalDropIndexInDatabase logicalDropIndexInDatabase,
                                                 ExecutionContext executionContext) {
        this(
            executionContext,
            logicalDropIndexInDatabase,
            preparedData.getDbName(),
            preparedData.isColumnar(),
            preparedData.getDropCciSql(),
            preparedData.getTableNameAndIndexNameList()
        );
    }

    public DropColumnarIndexInDatabaseJobFactory(ExecutionContext executionContext,
                                                 LogicalDropIndexInDatabase logicalDropIndexInDatabase,
                                                 String dbName,
                                                 boolean isColumnar,
                                                 List<Pair<String, String>> dropCciSql,
                                                 List<Pair<String, String>> tableNameAndIndexNameList) {
        super(executionContext, OnlineDdlInfo.DdlAlgorithm.OSC);
        this.executionContext = executionContext;
        this.logicalDropIndexInDatabase = logicalDropIndexInDatabase;
        this.dbName = dbName;
        this.isColumnar = isColumnar;
        this.dropCciSql = dropCciSql;
        this.tableNameAndIndexNameList = tableNameAndIndexNameList;
    }

    @Override
    protected ExecutableDdlJob doCreate() {
        ExecutableDdlJob createColumnarIndexInDatabaseJob = new ExecutableDdlJob();

        SqlNodeList hints = logicalDropIndexInDatabase.getSqlDropIndexInDatabase().getHints();
        String hintString = "";
        if (hints != null && hints.size() != 0) {
            List<SQLCommentHint> headHints =
                FastsqlUtils.parseSql(executionContext.getSql()).get(0).getHeadHintsDirect();
            if (headHints != null && headHints.size() == 1) {
                hintString = headHints.get(0).toString();
            } else {
                throw new UnsupportedOperationException(
                    "Only support one hint in DROP INDEX FOR TABLES IN/FROM DATABASE.");
            }
        }

        List<SubJobTask> addCciSubJobTasks = new ArrayList<>();
        for (Pair<String, String> sql : dropCciSql) {
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
    }

    private void validateColumnar() {
        if (!isColumnar) {
            throw new TddlNestableRuntimeException(
                "Non-Columnar indexes are not allowed to use the [CREATE INDEX FOR TABLES IN/FROM DATABASE] syntax.");
        }
    }

    private void validateEmptyTask() {
        if (dropCciSql.isEmpty()) {
            throw new TddlNestableRuntimeException(
                "No table in database '" + dbName + "' has Clustered Columnar Indexes.");
        }
    }

    @Override
    protected void excludeResources(Set<String> resources) {

    }

    @Override
    protected void sharedResources(Set<String> resources) {

    }

    public static ExecutableDdlJob create(LogicalDropIndexInDatabase logicalDropIndexInDatabase,
                                          ExecutionContext executionContext) {
        return new DropColumnarIndexInDatabaseJobFactory(
            logicalDropIndexInDatabase.getDropIndexInDatabasePreparedData(),
            logicalDropIndexInDatabase,
            executionContext).create();
    }
}
