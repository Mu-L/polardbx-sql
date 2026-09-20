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

package com.alibaba.polardbx.executor.ddl.job.validator;

import com.alibaba.polardbx.common.ddl.newengine.DdlConstants;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.executor.ddl.job.task.BaseDdlTask;
import com.alibaba.polardbx.executor.ddl.job.task.BasePhyDdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.meta.DdlJobManager;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalAlterTableAddPartition;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateDatabase;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateIndex;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalCreateTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropDatabase;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalDropTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalOptimizeTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalRenameTable;
import com.alibaba.polardbx.optimizer.core.rel.ddl.LogicalTruncateTable;

import java.util.List;
import java.util.stream.Collectors;

import static com.alibaba.polardbx.gms.topology.SystemDbHelper.CDC_DB_NAME;

public class CommonValidator {

    public static void validateDdlJob(String schemaName, String logicalTableName, DdlJob ddlJob, Logger logger,
                                      ExecutionContext executionContext) {
        if (ddlJob == null) {
            throw DdlHelper.logAndThrowError(logger, "Invalid job: The Job is null");
        }
        if (!ddlJob.isValid()) {
            throw DdlHelper.logAndThrowError(logger, "Invalid job in which the task topology has cycles");
        }
        if (ddlJob.getTaskCount() == 0) {
            throw DdlHelper.logAndThrowError(logger, "Invalid job: The Job has no task");
        }

        // Check the capacity of the DDL Engine system table.
        validateDdlJobCapacity(schemaName);
    }

    public static int MAX_PHYSICAL_TASK_NUM = 10;
    public static int EXPECTED_PHYSICAL_DDL_TASK_NUM = 1;

    public static void blockLogicalDdlJob(String schemaName, BaseDdlOperation logicalDdlPlan, DdlJob ddlJob,
                                          ExecutionContext executionContext) {
        Boolean blockLogicalDdl = executionContext.getParamManager().getBoolean(ConnectionParams.BLOCK_LOGICAL_DDL);
        if (blockLogicalDdl && !schemaName.equalsIgnoreCase(CDC_DB_NAME)) {
            if (logicalDdlPlan instanceof LogicalCreateDatabase || logicalDdlPlan instanceof LogicalDropDatabase
                || logicalDdlPlan instanceof LogicalCreateTable || logicalDdlPlan instanceof LogicalDropTable) {
                return;
            }
            if (logicalDdlPlan instanceof LogicalAlterTable || logicalDdlPlan instanceof LogicalRenameTable
                || logicalDdlPlan instanceof LogicalOptimizeTable || logicalDdlPlan instanceof LogicalTruncateTable
                || logicalDdlPlan instanceof LogicalCreateIndex) {
                List<DdlTask> ddlTaskList = ddlJob.getAllTasks();
                if (ddlTaskList.size() <= MAX_PHYSICAL_TASK_NUM) {
                    List<DdlTask> phyDdlTaskList =
                        ddlTaskList.stream().filter(ddlTask -> ddlTask instanceof BasePhyDdlTask).collect(
                            Collectors.toList());
                    if (phyDdlTaskList.size() == EXPECTED_PHYSICAL_DDL_TASK_NUM) {
                        return;
                    }
                }
            }
            throw DdlHelper.logAndThrowError(null,
                "We don't support block the logical DDL, which is controlled by the parameter BLOCK_LOGICAL_DDL");
        }
    }

    private static void validateDdlJobCapacity(String schemaName) {
        DdlJobManager engineManager = new DdlJobManager();
        int leftJobCount = engineManager.countAll(schemaName);
        if (leftJobCount > DdlConstants.MAX_LEFT_DDL_JOBS_IN_SYS_TABLE) {
            throw new TddlRuntimeException(ErrorCode.ERR_PENDING_DDL_JOBS_EXCEED_LIMIT,
                String.valueOf(DdlConstants.MAX_LEFT_DDL_JOBS_IN_SYS_TABLE));
        }
    }

}
