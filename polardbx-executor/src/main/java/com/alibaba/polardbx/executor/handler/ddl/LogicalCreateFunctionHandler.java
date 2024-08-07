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

package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.executor.ddl.job.factory.CreateFunctionJobFactory;
import com.alibaba.polardbx.executor.ddl.job.validator.TableValidator;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateFunction;
import org.apache.calcite.sql.SqlCreateFunction;

public class LogicalCreateFunctionHandler extends LogicalCommonDdlHandler {

    public LogicalCreateFunctionHandler(IRepository repo) {
        super(repo);
    }

    @Override
    protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        final LogicalCreateFunction logicalCreateFunction = (LogicalCreateFunction) logicalDdlPlan;
        return buildCreateFunctionJob(logicalCreateFunction, executionContext);
    }

    @Override
    protected boolean validatePlan(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
        // Notice, since we reused the logic of new ddl engine, so we should validate function name as table name
        String functionName = getObjectName(logicalDdlPlan);
        TableValidator.validateTableName(functionName);
        TableValidator.validateTableNameLength(functionName);
        return false;
    }

    private DdlJob buildCreateFunctionJob(LogicalCreateFunction logicalCreateFunction,
                                          ExecutionContext executionContext) {
        ExecutableDdlJob functionJob = CreateFunctionJobFactory.createFunction(
            logicalCreateFunction,
            executionContext);
        return functionJob;
    }

    @Override
    protected String getObjectName(BaseDdlOperation logicalDdlPlan) {
        SqlCreateFunction sqlCreateFunction = (SqlCreateFunction) logicalDdlPlan.getNativeSqlNode();
        return sqlCreateFunction.getFunctionName();
    }
}
