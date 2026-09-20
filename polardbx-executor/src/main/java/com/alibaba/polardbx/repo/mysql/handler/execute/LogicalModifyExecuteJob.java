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

package com.alibaba.polardbx.repo.mysql.handler.execute;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.utils.RowSet;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.dml.BroadcastWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableModify;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author lijiu.lzw
 * LogicalModifyHandler 的execute
 */
public class LogicalModifyExecuteJob extends ExecuteJob {
    private final LogicalModify modify;
    private final List<ColumnMeta> returnColumns;

    public LogicalModifyExecuteJob(ExecutionContext executionContext, ParallelExecutor parallelExecutor,
                                   LogicalModify modify, List<ColumnMeta> returnColumns) {
        super(executionContext, parallelExecutor);
        this.modify = modify;
        this.returnColumns = returnColumns;

        this.schemaName = modify.getSchemaName();
        if (StringUtils.isEmpty(this.schemaName)) {
            this.schemaName = executionContext.getSchemaName();
        }
        PhyTableOperationUtil.enableIntraGroupParallelism(this.schemaName, this.executionContext);

    }

    /**
     * 类似execute(DistinctWriter writer,...)
     */
    @Override
    public void execute(List<List<Object>> values, long memorySize) throws Exception {
        RowSet rowSet = new RowSet(values, returnColumns);

        List<DistinctWriter> dWriters = modify.getPrimaryModifyWriters();
        List<LogicalJob> jobs = new ArrayList<>();
        final boolean skipUnchangedRow =
            executionContext.getParamManager().getBoolean(ConnectionParams.DML_RELOCATE_SKIP_UNCHANGED_ROW);
        final boolean checkJsonByStringCompare =
            executionContext.getParamManager().getBoolean(ConnectionParams.DML_CHECK_JSON_BY_STRING_COMPARE);
        List<RelNode> allGsiPhyPlan = new ArrayList<>();
        for (DistinctWriter writer : dWriters) {
            Function<DistinctWriter, List<List<Object>>> rowGenerator =
                rowSet::distinctRowSetWithoutNull;
            if (skipUnchangedRow && modify.getNeedCompareWriters().containsKey(writer)) {
                // 跳过没有变化的行从而：
                // 1. 避免没有变化的情况下更新 ON UPDATE TIMESTAMP 列
                // 2. 减少下发的物理 SQL 数
                // affectRows 根据参数判断是否需要加上没有修改的行
                Integer tableIndex = modify.getNeedCompareWriters().get(writer);
                rowGenerator = new Function<DistinctWriter, List<List<Object>>>() {
                    @Override
                    public List<List<Object>> apply(DistinctWriter distinctWriter) {
                        return rowSet.distinctRowSetWithoutNullThenRemoveSameRow(distinctWriter,
                            modify.getSetColumnTargetMappings().get(tableIndex),
                            modify.getSetColumnSourceMappings().get(tableIndex),
                            modify.getSetColumnMetas().get(tableIndex), checkJsonByStringCompare);
                    }
                };
            }

            List<RelNode> inputs = writer.getInput(executionContext, rowGenerator);
            final List<RelNode> primaryPhyPlan =
                inputs.stream().filter(o -> ((BaseQueryOperation) o).isPrimaryWriteRelNode()).collect(
                    Collectors.toList());
            final List<RelNode> allPhyPlan = new ArrayList<>(primaryPhyPlan);
            final List<RelNode> replicatePlans =
                inputs.stream().filter(o -> ((BaseQueryOperation) o).isReplicateRelNode()).collect(
                    Collectors.toList());
            allPhyPlan.addAll(replicatePlans);
            if (allPhyPlan.isEmpty()) {
                continue;
            }
            boolean isBroadcast = writer instanceof BroadcastWriter;
            boolean multiWriteWithoutBroadcast = GeneralUtil.isNotEmpty(replicatePlans) && !isBroadcast;
            boolean multiWriteWithBroadcast = GeneralUtil.isNotEmpty(replicatePlans) && isBroadcast;

            ExecutionContext modifyEc = executionContext.copy();
            LogicalJob job = new LogicalJob(parallelExecutor, modifyEc);
            if (multiWriteWithoutBroadcast || multiWriteWithBroadcast) {
                List<Object> params = new ArrayList<>();
                params.add(primaryPhyPlan);
                params.add(multiWriteWithoutBroadcast);
                params.add(multiWriteWithBroadcast);
                job.setAffectRowsFunction(ExecuteJob::specialAffectRows);
                job.setAffectRowsFunctionParams(params);
            }

            // handle insert for update
            if (modify.getOperation() == TableModify.Operation.UPDATE) {
                Integer tableIndex = modify.getPrimaryWriterToPrimaryIndex().get(writer);
                Function<DistinctWriter, List<List<Object>>> finalRowGenerator = rowGenerator;
                List<RelNode> gsiPhyPlan = new ArrayList<>();
                if (modify.getGsiModifyWritersMap().containsKey(tableIndex)) {
                    modify.getGsiModifyWritersMap()
                        .get(tableIndex)
                        .stream()
                        .flatMap(gsiWriter -> gsiWriter.getInput(executionContext, finalRowGenerator).stream())
                        .forEach(gsiPhyPlan::add);
                }
                allGsiPhyPlan.addAll(gsiPhyPlan);
            }
            job.setAllPhyPlan(allPhyPlan);
            jobs.add(job);
        }
        //将所有GSI表的物理执行计划整合到一个job
        //handle insert for delete
        if (modify.getOperation() == TableModify.Operation.DELETE) {
            for (DistinctWriter writer : modify.getGsiModifyWriters()) {
                List<RelNode> inputs = writer.getInput(executionContext, rowSet::distinctRowSetWithoutNull);
                allGsiPhyPlan.addAll(inputs);
            }
        }
        if (!allGsiPhyPlan.isEmpty()) {
            ExecutionContext modifyEc = executionContext.copy();
            LogicalJob job = new LogicalJob(parallelExecutor, modifyEc);
            job.setAllPhyPlan(allGsiPhyPlan);
            job.setAffectRowsFunction(ExecuteJob::noAffectRows);
            jobs.add(job);
        }
        if (!jobs.isEmpty()) {
            //最后一个任务完成才能释放内存
            jobs.get(jobs.size() - 1).setMemorySize(memorySize);
            for (LogicalJob job : jobs) {
                scheduleJob(job);
            }
        }
    }

}
