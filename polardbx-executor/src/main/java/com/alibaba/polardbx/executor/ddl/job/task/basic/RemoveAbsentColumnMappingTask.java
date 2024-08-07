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

package com.alibaba.polardbx.executor.ddl.job.task.basic;

import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.util.TaskName;
import com.alibaba.polardbx.executor.ddl.newengine.meta.SchemaEvolutionAccessorDelegate;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import lombok.Getter;

@TaskName(name = "RemoveAbsentColumnMappingTask")
@Getter
public class RemoveAbsentColumnMappingTask extends BaseDdlTask {

    protected String logicalTableName;

    public RemoveAbsentColumnMappingTask(String schemaName, String logicalTableName) {
        super(schemaName);
        this.logicalTableName = logicalTableName;
    }

    @Override
    protected void beforeTransaction(ExecutionContext executionContext) {
        new SchemaEvolutionAccessorDelegate<Integer>() {
            @Override
            protected Integer invoke() {
                columnMappingAccessor.deleteAbsent(schemaName, logicalTableName);
                return 0;
            }
        }.execute();
    }
}
