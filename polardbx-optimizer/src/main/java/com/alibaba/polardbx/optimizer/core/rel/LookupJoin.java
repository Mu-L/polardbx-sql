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

package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.optimizer.config.meta.CostModelWeight;
import com.alibaba.polardbx.optimizer.config.meta.TableScanIOEstimator;
import com.alibaba.polardbx.optimizer.index.Index;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.util.Util;

/**
 * @author dylan
 */
public interface LookupJoin {
    // cost for join from TableLookup and TableLookup as lookup side of join
    static RelOptCost getCostIfJoinFromTableLookup(RelMetadataQuery mq, Join joinOfTableLookup) {

        // get the index scan side of look up join
        //     Right Join
        //     /      \
        //   LookUp   rel
        //    /
        // IndexScan
        //
        // ---------------------------
        //
        //                BKA Right Join
        //                  /      \
        //   IndexScan(join=rel)    rel
        //
        // note that in the case, the cost of BKA Right Join can't be ignored
        RelNode node = joinOfTableLookup.getLeft();
        if (joinOfTableLookup.getJoinType() == JoinRelType.RIGHT) {
            node = joinOfTableLookup.getRight();
        }

        if (node instanceof RelSubset) {
            node = Util.first(((RelSubset) node).getBest(), ((RelSubset) node).getOriginal());
        }

        if (node instanceof Gather) {
            node = ((Gather) node).getInput();
        }

        if (node instanceof LogicalIndexScan) {
            LogicalIndexScan logicalIndexScan = (LogicalIndexScan) node;
            if (logicalIndexScan.isLookupTable()) {
                // This TableLookup is lookup side of lookup join such as (BKA, Materialized Semi Join)
                Index index = logicalIndexScan.getLookupInfo().getLookupIndex();
                RelNode mysqlRelNode = logicalIndexScan.getMysqlNode();
                RelOptCost scanCost = mq.getCumulativeCost(mysqlRelNode);

                final double rows;
                final double cpu;
                final double memory;
                final double io;

                if (index != null) {
                    double selectivity = index.getTotalSelectivity();
                    rows = Math.max(logicalIndexScan.getTable().getRowCount() * selectivity, 1);
                    cpu = scanCost.getCpu() * selectivity;
                    memory = scanCost.getMemory() * selectivity;
                    double size =
                        TableScanIOEstimator.estimateRowSize(logicalIndexScan.getTable().getRowType())
                            * logicalIndexScan.getTable().getRowCount() * selectivity;
                    io = Math.ceil(size / CostModelWeight.RAND_IO_PAGE_SIZE);
                } else {
                    rows = scanCost.getRows();
                    cpu = scanCost.getCpu();
                    memory = scanCost.getMemory();
                    io = scanCost.getIo();
                }

                RelOptPlanner planner = logicalIndexScan.getCluster().getPlanner();
                // plus the TableLookup right side by multi by 2
                RelOptCost indexCost = planner.getCostFactory().makeCost(rows, cpu, memory, io, 0.5)
                    .multiplyBy(2);
                return indexCost;
            }
        }
        return null;
    }

    // the relNode must is the lookupSide of LookupJoin(BKA, SemiBKA, Materialized)
    // 1. LogicalView -> cpu, mem, io, net = 0
    // 2. TableLookup (before expand) -> cpu, mem, io, net = 1
    // 3. LogicalProject + Join (after expand) -> cpu, mem, io, net = 1
    static RelOptCost getLookupCost(RelMetadataQuery mq, RelNode relNode) {
        RelNode lookupNode = relNode;
        if (relNode instanceof RelSubset) {
            lookupNode = Util.first(((RelSubset) relNode).getBest(), ((RelSubset) relNode).getOriginal());
        }

        // the expand table lookup
        if (lookupNode instanceof LogicalProject) {
            RelNode input = ((LogicalProject) lookupNode).getInput();
            if (input instanceof RelSubset) {
                input = Util.first(((RelSubset) input).getBest(), ((RelSubset) input).getOriginal());
            }
            if (input instanceof Join) {
                RelOptCost cost = getCostIfJoinFromTableLookup(mq, (Join) input);
                if (cost != null) {
                    return cost;
                }
            }
            // else: FETCH_BLOB Project wrapping a LogicalView, fall through to default cost
        }

        // tableLookup (before expand) or logicalview
        return mq.getCumulativeCost(lookupNode);
    }

    RelOptCost getLookupCost(RelMetadataQuery mq);

    Index getLookupIndex();

    void deepVisitLookupJoin();
}
