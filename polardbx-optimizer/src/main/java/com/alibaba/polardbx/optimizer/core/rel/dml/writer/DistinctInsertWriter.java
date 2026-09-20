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

package com.alibaba.polardbx.optimizer.core.rel.dml.writer;

import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableInsertSharder;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableModifyBuilder;
import com.alibaba.polardbx.optimizer.core.rel.dml.DistinctWriter;
import com.alibaba.polardbx.optimizer.core.rel.dml.DmlWriteContext;
import com.alibaba.polardbx.optimizer.core.rel.dml.RoutedModifyInput;
import com.alibaba.polardbx.optimizer.utils.BuildPlanUtils;
import com.google.common.base.Preconditions;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.mapping.Mapping;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

/**
 * Insert writer, support deduplicate input rows
 *
 * @author chenmo.cm
 */
public class DistinctInsertWriter extends InsertWriter implements DistinctWriter {

    private final Mapping deduplicateMapping;

    public DistinctInsertWriter(RelOptTable targetTable, LogicalInsert insert, Mapping deduplicateMapping) {
        super(targetTable, insert);
        Preconditions.checkNotNull(insert);

        this.deduplicateMapping = deduplicateMapping;
    }

    @Override
    public Mapping getGroupingMapping() {
        return deduplicateMapping;
    }

    /**
     * Build physical INSERT plans from the rows selected for this distinct writer.
     *
     * <p>Ordinary DML goes directly through the original insert-plan builder. A statement-scoped
     * {@link DmlWriteContext}, when present, is inserted into the same flow at three stable boundaries: prepare the
     * writer rows, materialize route-dependent state after sharding, and attach companion plans after the primary
     * plans have been built.</p>
     */
    @Override
    public List<RelNode> getInput(ExecutionContext ec, Function<DistinctWriter, List<List<Object>>> rowGenerator) {
        // The caller applies this writer's grouping mapping and supplies the deduplicated logical INSERT rows.
        final List<List<Object>> logicalRows = rowGenerator.apply(this);
        final DmlWriteContext writeContext = ec.getDmlWriteContext();

        // Preserve the original Writer path for ordinary DML. Empty input also needs no route-dependent callback.
        if (writeContext == null || logicalRows.isEmpty()) {
            return PhyTableModifyBuilder.buildInsert(insert, logicalRows, ec, false);
        }

        // Let the statement context prepare the exact rows consumed by this leaf Writer. For example, an
        // externalized DML context copies target rows, reuses known addresses and records a pending materializer.
        final List<List<Object>> distinctRows = writeContext.prepareModifyRows(this, logicalRows, ec);

        // Rebuild routing parameters from the prepared rows. The parameters on the original ExecutionContext may
        // still describe the logical rows and must not be used to route a rewritten physical row representation.
        final List<Map<Integer, ParameterContext>> batchParams = BuildPlanUtils.buildInsertBatchParam(distinctRows);
        final Parameters routingParameters = new Parameters(batchParams);
        final ExecutionContext routingEc = ec.copy(routingParameters);

        // Compute the physical owner of every row once. PhyTableShardResult keeps the value indices needed to map
        // each prepared row back to its target group and physical table.
        final PhyTableInsertSharder insertPartitioner = new PhyTableInsertSharder(insert, routingParameters,
            SequenceAttribute.getAutoValueOnZero(ec.getSqlMode()));
        final List<PhyTableInsertSharder.PhyTableShardResult> shardResults = new ArrayList<>(
            insertPartitioner.shardValues(insert.getInput(), insert.getLogicalTableName(), routingEc));

        // Route-dependent materialization must happen after the owner route is known but before business plans are
        // built. The row-index map also guarantees that companion state is bound to the same primary INSERT leaf.
        writeContext.beforeModifyPlans(this, new RoutedModifyInput(distinctRows,
            RoutedModifyInput.buildInsertRoutes(insert.getSchemaName(), shardResults, distinctRows.size())), ec);

        // Build primary INSERT plans with the same prepared rows and shard results used above; do not route again.
        final List<RelNode> primaryPlans = PhyTableModifyBuilder.buildInsertByShardResults(insert, distinctRows, ec,
            insertPartitioner.getSqlTemplate(), shardResults);

        // The context may prepend companion plans (such as transactional staging writes) to their primary plans.
        return writeContext.afterModifyPlans(this, primaryPlans, ec);
    }

}
