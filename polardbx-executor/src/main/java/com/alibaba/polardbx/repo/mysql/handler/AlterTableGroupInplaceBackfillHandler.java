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

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.TddlNestableRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.gsi.CheckerManager;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.backfill.Loader;
import com.alibaba.polardbx.executor.corrector.Checker;
import com.alibaba.polardbx.executor.corrector.Reporter;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.fastchecker.FastChecker;
import com.alibaba.polardbx.executor.handler.HandlerCommon;
import com.alibaba.polardbx.executor.partitionmanagement.BackfillExecutor;
import com.alibaba.polardbx.executor.partitionmanagement.corrector.AlterTableGroupChecker;
import com.alibaba.polardbx.executor.partitionmanagement.corrector.AlterTableGroupReporter;
import com.alibaba.polardbx.executor.partitionmanagement.fastchecker.AlterTableGroupFastChecker;
import com.alibaba.polardbx.statistics.SQLRecorderLogger;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.AlterTableGroupInplaceBackfill;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import com.alibaba.polardbx.optimizer.utils.QueryConcurrencyPolicy;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlSelect;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.executor.utils.ExecUtils.getQueryConcurrencyPolicy;

/**
 * 支持存储层下推的AlterTableGroup回填处理器
 * 使用polardbx_hasher函数在存储层计算路由信息，避免数据在CN节点传输
 */
public class AlterTableGroupInplaceBackfillHandler extends HandlerCommon {

    public AlterTableGroupInplaceBackfillHandler(IRepository repo) {
        super(repo);
    }

    @Override
    public Cursor handle(RelNode logicalPlan, ExecutionContext executionContext) {
        AlterTableGroupInplaceBackfill backfill = (AlterTableGroupInplaceBackfill) logicalPlan;
        String schemaName = backfill.getSchemaName();
        String logicalTable = backfill.getLogicalTableName();
        final boolean useBinary = executionContext.getParamManager().getBoolean(ConnectionParams.BACKFILL_USING_BINARY);

        BackfillExecutor backfillExecutor = new BackfillExecutor((List<RelNode> inputs,
                                                                  ExecutionContext executionContext1) -> {
            QueryConcurrencyPolicy queryConcurrencyPolicy = getQueryConcurrencyPolicy(executionContext1);
            if (Loader.canUseBackfillReturning(executionContext1, schemaName)) {
                queryConcurrencyPolicy = QueryConcurrencyPolicy.GROUP_CONCURRENT_BLOCK;
            }
            List<Cursor> inputCursors = new ArrayList<>(inputs.size());
            executeWithConcurrentPolicy(executionContext1, inputs, queryConcurrencyPolicy, inputCursors, schemaName);
            return inputCursors;
        });

        executionContext = clearSqlMode(executionContext.copy());

        if (!executionContext.getParamManager().getBoolean(ConnectionParams.BACKFILL_USING_BINARY)) {
            upgradeEncoding(executionContext, schemaName, logicalTable);
        }

        PhyTableOperationUtil.disableIntraGroupParallelism(schemaName, executionContext);

        Map<String, Set<String>> sourcePhyTables = backfill.getSourcePhyTables();
        Map<String, Set<String>> targetPhyTables = backfill.getTargetPhyTables();

        boolean useChangeSet = backfill.isUseChangeSet();

        int affectRows = 0;
        if (!sourcePhyTables.isEmpty()) {
            // 使用存储层下推的回填执行器
            affectRows = backfillExecutor
                .inplaceBackfill(schemaName, logicalTable, backfill.getSourcePhyTables(),
                    backfill.getSrcTargetTableMap(),
                    backfill.getTargetTablePartitionBounds(),
                    backfill.getSourceTablePartitionBounds(),
                    backfill.getActivePartitionKeys(), useChangeSet,
                    useBinary, executionContext);
        }

        return new AffectRowCursor(affectRows);
    }
} 