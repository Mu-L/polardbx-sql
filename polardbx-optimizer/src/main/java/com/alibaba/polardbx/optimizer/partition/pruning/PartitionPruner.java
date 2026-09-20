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

package com.alibaba.polardbx.optimizer.partition.pruning;

import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.gms.partition.TablePartitionRecord;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModifyView;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.partition.FullScanTableBlackListManager;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.sharding.result.RelShardInfo;
import org.apache.calcite.plan.RelOptPredicateList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSimplify;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.Collections;
import java.util.List;

/**
 * @author chenghui.lch
 */
public class PartitionPruner {

    //=========== Build Phase ============

    /**
     * Methods for generating PartitionPruneStep
     */
    public static PartitionPruneStep generatePartitionPrueStepInfo(PartitionInfo partInfo,
                                                                   RelNode relPlan,
                                                                   RexNode partPredInfo,
                                                                   ExecutionContext ec) {
        return PartitionPruneStepBuilder.generatePartitionPruneStepInfo(partInfo,
            relPlan == null ? null : relPlan.getRowType(), partPredInfo, ec);
    }

    /**
     * Methods for generating TupleRouteInfo by LogicalInsert
     */
    public static PartitionTupleRouteInfo generatePartitionTupleRoutingInfo(LogicalInsert insert,
                                                                            PartitionInfo partitionInfo) {
        return PartitionTupleRouteInfoBuilder.genPartTupleRoutingInfo(insert, partitionInfo);
    }

    /**
     * Methods for generating TupleRouteInfo by special row values
     *
     * @param tupleValRowType tupleValRowType will contain the datatype of all partition/subpartition columns
     * @param tupleValAst tupleValAst will contain the dynamic params of all partition/subpartition columns
     */
    public static PartitionTupleRouteInfo generatePartitionTupleRoutingInfo(String schemaName,
                                                                            String logTbName,
                                                                            PartitionInfo specificPartInfo,
                                                                            RelDataType tupleValRowType,
                                                                            List<List<SqlNode>> tupleValAst,
                                                                            ExecutionContext ec) {
        return PartitionTupleRouteInfoBuilder
            .genPartTupleRoutingInfo(schemaName, logTbName, specificPartInfo, tupleValRowType, tupleValAst, ec);
    }

    //========== Pruning Phase =============

    /**
     * Methods for pruning by stepInfo ( for query with condition )
     */
    public static PartPrunedResult doPruningByStepInfo(PartitionPruneStep stepInfo,
                                                       ExecutionContext context) {
        return doPruningByStepInfoWithExtraInfo(stepInfo, context, null);
    }

    public static PartPrunedResult doPruningByStepInfoWithExtraInfo(PartitionPruneStep stepInfo,
                                                                    ExecutionContext context,
                                                                    PartPruneStepPruningExtraInfo pruningExtraInfo) {
        PartPruneStepPruningContext pruningCtx = PartPruneStepPruningContext.initPruningContext(context);
        pruningCtx.setExtraInfo(pruningExtraInfo);
        boolean enablePartPruning = context.getParamManager().getBoolean(ConnectionParams.ENABLE_PARTITION_PRUNING);
        if (stepInfo.getPartitionInfo().isNoPartitionKeyTable() && enablePartPruning) {
            enablePartPruning = false;
        }
        if (!enablePartPruning) {
            PartitionInfo partInfo = stepInfo.getPartitionInfo();
            PartitionPruneStep fullScanStep =
                PartitionPruneStepBuilder.genFullScanPruneStepInfoInner(partInfo,
                    partInfo.getPartitionBy().getPhysicalPartLevel(), true);
            pruningCtx.setRootStep(fullScanStep);
            PartPrunedResult prunedResult = fullScanStep.prunePartitions(context, pruningCtx, null);
            PartitionPrunerUtils.logStepExplainInfo(context, prunedResult.getPartInfo(), pruningCtx);
            return prunedResult;
        }
        pruningCtx.setRootStep(stepInfo);
        PartPrunedResult prunedResult = stepInfo.prunePartitions(context, pruningCtx, null);
        invalidPartitionFilter(stepInfo.getPartitionInfo(), prunedResult);
        PartitionPrunerUtils.logStepExplainInfo(context, prunedResult.getPartInfo(), pruningCtx);
        return prunedResult;
    }

    public static void invalidPartitionFilter(PartitionInfo partitionInfo, PartPrunedResult prunedResult) {
        if (!prunedResult.getPartBitSet().isEmpty()) {
            List<PartitionSpec> phyPartSpecs = partitionInfo.getPartitionBy().getPhysicalPartitions();
            int partCnt = phyPartSpecs.size();
            for (int i = prunedResult.getPartBitSet().nextSetBit(0); i >= 0;
                 i = prunedResult.getPartBitSet().nextSetBit(i + 1)) {
                if (i >= partCnt) {
                    throw new TddlRuntimeException(ErrorCode.ERR_PARTITION_MANAGEMENT,
                        "Find pruned partition error");
                }
                PartitionSpec phySpec = phyPartSpecs.get(i);
                if (phySpec.getStatus() != null
                    && phySpec.getStatus() == TablePartitionRecord.PARTITION_STATUS_PARTITION_OFFLINE) {
                    prunedResult.getPartBitSet().clear(i);
                }
            }
        }
    }

    /**
     * Methods for pruning by tupleRouteInfo ( for insert / replace )
     */
    public static PartPrunedResult doPruningByTupleRouteInfo(PartitionTupleRouteInfo tupleRouteInfo, int tupleIndex,
                                                             ExecutionContext context) {
        PartPruneStepPruningContext pruningCtx = PartPruneStepPruningContext.initPruningContext(context);
        pruningCtx.setPruningByTuple(true);
        /**
         * disable const expr eval cache
         */
        pruningCtx.setEnableConstExprEvalCache(false);
        PartPrunedResult rs = tupleRouteInfo.routeTuple(tupleIndex, context, pruningCtx);
        pruningCtx.setRootTuple(tupleRouteInfo);
        invalidPartitionFilter(tupleRouteInfo.getPartInfo(), rs);
        PartitionPrunerUtils.logStepExplainInfo(context, rs.getPartInfo(), pruningCtx);
        return rs;
    }

    /**
     * Methods for calculating partition func expression by tupleRouteInfo
     */
    public static List<SearchDatumInfo> doCalcSearchDatumByTupleRouteInfo(PartitionTupleRouteInfo tupleRouteInfo,
                                                                          int tupleIndex,
                                                                          ExecutionContext context) {
        PartPruneStepPruningContext pruningCtx = PartPruneStepPruningContext.initPruningContext(context);
        pruningCtx.setPruningByTuple(true);
        /**
         * disable const expr eval cache
         */
        pruningCtx.setEnableConstExprEvalCache(false);
        return tupleRouteInfo.calcSearchDatum(tupleIndex, context, pruningCtx);
    }

    /**
     * Do pruning by plan and context, only used by LogicalView
     */
    public static List<PartPrunedResult> prunePartitions(RelNode relPlan, ExecutionContext context) {
        List<PartitionPruneStep> usedSteps = new ArrayList<>();
        List<PartPrunedResult> usedResults = new ArrayList<>();
        List<PartPrunedResult> rs = prunePartitionsInner(relPlan, context, usedSteps, usedResults);
        applyFullScanBlackListCheck(relPlan, usedSteps, usedResults, context);
        return rs;
    }

    /**
     * Decide whether each accessed table performs a real full table scan and
     * trigger the blacklist throw if so.
     *
     * <p>The decision is made by recursively inspecting the partition prune
     * step shape: a step "gives up pruning" iff it is forceFullScan (and not
     * a zero-scan conflict) at the operator level, or it is a Combine whose
     * sub-steps contain a give-up branch, or it is a SubPartStepAnd whose
     * BOTH levels give up. A predicate that the partition pruner can really
     * make use of (e.g. RANGE bound predicate, equality on a sub-partition
     * key) does NOT count as full scan even when the resulting physical
     * BitSet happens to cover all partitions.
     *
     * <p>Always-false filters (e.g. {@code WHERE 1=2}) are excluded as a
     * second-stage bypass since the empty result set is guaranteed regardless
     * of how the pruner reports it.
     */
    private static void applyFullScanBlackListCheck(RelNode relPlan,
                                                    List<PartitionPruneStep> usedSteps,
                                                    List<PartPrunedResult> usedResults,
                                                    ExecutionContext context) {
        if (usedSteps == null || usedSteps.isEmpty()) {
            return;
        }
        /**
         * If the SQL explicitly chose partitions via PARTITION(...) syntax,
         * the user already knows what they are scanning, so do not block by
         * the full-scan blacklist.
         */
        if (relPlan instanceof LogicalView) {
            if (((LogicalView) relPlan).useSelectPartitions()) {
                return;
            }
        }

        /**
         * Quick check: a table is doing a real full table scan iff
         *   (a) the step shape says the pruner gave up pruning (forceFullScan
         *       on the partition-key path); OR
         *   (b) the table is single-level HASH-like (HASH/KEY/DIRECT_HASH/
         *       CO_HASH/UDF_HASH) and its physical-partition BitSet covers
         *       every physical partition. HASH-like strategies cannot prune
         *       range predicates, so a full BitSet implies a real full scan
         *       (e.g. {@code a > 1} on a KEY-partitioned table).
         * For sub-partitioned tables we rely solely on step shape because
         * the BitSet may be full just because two equality values happen to
         * hash into different sub-partition buckets, which is still a
         * legitimate predicate-driven query.
         */
        int n = Math.min(usedSteps.size(), usedResults == null ? 0 : usedResults.size());
        boolean anyRealFullScan = false;
        for (int i = 0; i < n; i++) {
            if (isRealFullScan(usedSteps.get(i), usedResults.get(i))) {
                anyRealFullScan = true;
                break;
            }
        }
        if (!anyRealFullScan) {
            return;
        }

        /**
         * At least one step would trigger the blacklist. Before throwing,
         * check if the pushed plan contains an always-false filter (e.g.
         * WHERE 1=2). This is only done when needed to avoid the cost of
         * RexSimplify on every query.
         */
        if (relPlan instanceof LogicalView) {
            if (containsAlwaysFalseFilter(((LogicalView) relPlan).getPushedRelNode())) {
                return;
            }
        }

        for (int i = 0; i < n; i++) {
            PartitionPruneStep step = usedSteps.get(i);
            PartPrunedResult result = usedResults.get(i);
            if (!isRealFullScan(step, result)) {
                continue;
            }
            FullScanTableBlackListManager.getInstance()
                .throwIfNotAllowFullScan(result.getPartInfo(), context);
        }
    }

    /**
     * Returns true iff the given (step, prunedResult) pair represents a real
     * full table scan that should be subject to the blacklist throw.
     */
    private static boolean isRealFullScan(PartitionPruneStep step, PartPrunedResult result) {
        if (step == null || result == null) {
            return false;
        }
        if (stepGivesUpPruning(step)) {
            return true;
        }
        // Step shape says some real pruning happened, but HASH/KEY strategies
        // cannot prune range predicates. The pruner expands ranges into
        // per-bucket ops that look like real pruning but actually scan all
        // physical partitions. Use the physical BitSet as a fallback check.
        PartitionInfo partInfo = result.getPartInfo();
        if (partInfo == null || partInfo.getPartitionBy() == null) {
            return false;
        }
        BitSet phyBitSet = result.getPhysicalPartBitSet();
        if (phyBitSet == null) {
            return false;
        }
        java.util.List<PartitionSpec> phyParts = partInfo.getPartitionBy().getPhysicalPartitions();
        if (phyParts == null || phyParts.isEmpty()) {
            return false;
        }
        if (phyBitSet.cardinality() < phyParts.size()) {
            // Not scanning all physical partitions — not a full scan.
            return false;
        }
        // Physical BitSet covers ALL partitions. Decide by partition strategy:
        PartitionStrategy firstLevelStrategy = partInfo.getPartitionBy().getStrategy();
        if (partInfo.getPartitionBy().getSubPartitionBy() == null) {
            // Single-level table: only hash-like strategies produce false
            // "pruned" steps for range predicates.
            return isNonRangePrunableStrategy(firstLevelStrategy);
        }
        // Sub-partitioned table. BLOCK when:
        //   (a) first-level strategy is hash-like (range predicates on hash
        //       key expand to all buckets without forceFullScan), OR
        //   (b) first-level step gives up pruning (forceFullScan, e.g.
        //       predicate only on sub-partition key with no first-level filter)
        // This ensures RANGE first-level with a legitimate range predicate
        // (e.g. id>=1 that happens to cover all partitions) is still ALLOWED.
        if (isNonRangePrunableStrategy(firstLevelStrategy)) {
            return true;
        }
        // First-level is RANGE/LIST: check if its step actually gave up.
        if (step instanceof PartitionPruneSubPartStepAnd) {
            PartitionPruneSubPartStepAnd subAnd = (PartitionPruneSubPartStepAnd) step;
            PartitionPruneStep firstLevelStep = subAnd.getPartStep();
            return stepGivesUpPruning(firstLevelStep);
        }
        return false;
    }

    /**
     * Returns true if the partition strategy cannot effectively prune range
     * predicates. For these strategies, a full physical BitSet implies a real
     * full table scan even when the pruner does not mark forceFullScan.
     *
     * <p>Includes:
     * <ul>
     *   <li>HASH/KEY/DIRECT_HASH/CO_HASH/UDF_HASH: hash-based strategies
     *       expand ranges into per-bucket enumeration without real pruning.</li>
     *   <li>LIST/LIST_COLUMNS: value-list-based strategies; range predicates
     *       require brute-force enumeration against each partition's value set.
     *       With a DEFAULT partition, ranges almost always hit all partitions.</li>
     * </ul>
     *
     * <p>RANGE/RANGE_COLUMNS are excluded because they natively prune ranges.
     */
    private static boolean isNonRangePrunableStrategy(PartitionStrategy s) {
        if (s == null) {
            return false;
        }
        switch (s) {
        case HASH:
        case KEY:
        case DIRECT_HASH:
        case CO_HASH:
        case UDF_HASH:
        case LIST:
        case LIST_COLUMNS:
            return true;
        default:
            return false;
        }
    }

    /**
     * Returns true if the partition prune step represents a query that has
     * effectively given up pruning on (at least) the partitioning key, and
     * therefore amounts to a full table scan.
     *
     * <p>Specifically:
     * <ul>
     *   <li>A leaf {@link PartitionPruneStepOp} gives up iff it is
     *       forceFullScan AND not a conflict (zero-scan) step.</li>
     *   <li>A {@link PartitionPruneStepCombine} (AND/OR of sibling steps)
     *       gives up iff ANY of its sub-steps gives up. This catches cases
     *       like {@code id = 1 OR id > 10} on a KEY-partitioned table where
     *       the second branch cannot be pruned and forces a full scan.</li>
     *   <li>A {@link PartitionPruneSubPartStepAnd} (sub-partitioned table)
     *       gives up only when BOTH the first-level step and the sub-level
     *       step give up. If either level can really prune, the table is
     *       NOT considered full-scanned.</li>
     * </ul>
     */
    private static boolean stepGivesUpPruning(PartitionPruneStep step) {
        if (step instanceof PartitionPruneStepOp) {
            PartitionPruneStepOp op = (PartitionPruneStepOp) step;
            return op.isForceFullScan() && !op.isConflict();
        }
        // SubPartStepAnd extends PartitionPruneStepCombine; check it first.
        if (step instanceof PartitionPruneSubPartStepAnd) {
            PartitionPruneSubPartStepAnd subAnd = (PartitionPruneSubPartStepAnd) step;
            PartitionPruneStep firstLevel = subAnd.getPartStep();
            if (!stepGivesUpPruning(firstLevel)) {
                return false;
            }
            PartitionPruneStep subOr = subAnd.getSubPartStepOr();
            if (subOr instanceof PartitionPruneSubPartStepOr) {
                PartitionPruneStep subTmpl =
                    ((PartitionPruneSubPartStepOr) subOr).getSubPartStepTemp();
                return stepGivesUpPruning(subTmpl);
            }
            return true;
        }
        if (step instanceof PartitionPruneStepCombine) {
            for (PartitionPruneStep sub : ((PartitionPruneStepCombine) step).getSubSteps()) {
                if (stepGivesUpPruning(sub)) {
                    return true;
                }
            }
            return false;
        }
        return false;
    }

    /**
     * Walks the given RelNode tree and returns true if any Filter node has a
     * condition that simplifies to constant FALSE.
     *
     * <p>Note: a raw RexCall like {@code 1=2} is NOT directly recognized as
     * always-false by {@link RexNode#isAlwaysFalse()} unless it has been
     * simplified into a literal. So we proactively run the condition through
     * {@link RexSimplify} before testing.
     */
    private static boolean containsAlwaysFalseFilter(RelNode root) {
        if (root == null) {
            return false;
        }
        final boolean[] found = new boolean[] {false};
        new RelVisitor() {
            @Override
            public void visit(RelNode node, int ordinal, RelNode parent) {
                if (found[0]) {
                    return;
                }
                if (node instanceof Filter) {
                    RexNode cond = ((Filter) node).getCondition();
                    if (isAlwaysFalsePredicate(node, cond)) {
                        found[0] = true;
                        return;
                    }
                }
                super.visit(node, ordinal, parent);
            }
        }.go(root);
        return found[0];
    }

    private static boolean isAlwaysFalsePredicate(RelNode node, RexNode cond) {
        if (cond == null) {
            return false;
        }
        if (cond.isAlwaysFalse()) {
            return true;
        }
        try {
            RexBuilder rexBuilder = node.getCluster() != null ? node.getCluster().getRexBuilder() : null;
            if (rexBuilder == null) {
                return false;
            }
            RexSimplify simplify =
                new RexSimplify(rexBuilder, RelOptPredicateList.EMPTY, true, RexUtil.EXECUTOR);
            RexNode simplified = simplify.simplify(cond);
            return simplified != null && simplified.isAlwaysFalse();
        } catch (Throwable t) {
            // best-effort: if simplification fails for any reason, fall back to
            // the conservative default of not bypassing the blacklist.
            return false;
        }
    }

    private static List<PartPrunedResult> prunePartitionsInner(RelNode relPlan,
                                                               ExecutionContext context,
                                                               List<PartitionPruneStep> usedSteps,
                                                               List<PartPrunedResult> usedResults) {

        if (relPlan instanceof LogicalView) {
            LogicalView logicalView = (LogicalView) relPlan;

            boolean useSelectPartitions = logicalView.useSelectPartitions();
            if (logicalView.isJoin()) {
                boolean containAnyReplicasTables = logicalView.containAnyReplicasTables(context);
                if (containAnyReplicasTables) {
                    List<PartPrunedResult> allTbPrunedResultsOfReplicasTable =
                        prunePartitionsForReplicasTables(context, logicalView, useSelectPartitions);
                    return allTbPrunedResultsOfReplicasTable;
                }

                List<PartPrunedResult> allTbPrunedResults = new ArrayList<>();
                List<PartitionPruneStep> stepsForJoin = new ArrayList<>();
                for (int i = 0; i < logicalView.getTableNames().size(); i++) {

                    PartitionPruneStep pruneStepInfo = null;
                    RelShardInfo relShardInfo = logicalView.getRelShardInfo(i, context);
                    if (!useSelectPartitions)  {
                        pruneStepInfo = relShardInfo.getPartPruneStepInfo();
                    } else {
                        PartitionInfo partInfo = relShardInfo.getPartPruneStepInfo().getPartitionInfo();
                        pruneStepInfo = PartitionPruneStepBuilder.genFullScanPruneStepInfoInner(partInfo,
                            partInfo.getPartitionBy().getPhysicalPartLevel(), true);
                    }

                    /**
                     * do pruning partitions by context
                     */
                    PartPrunedResult tbPrunedResult =
                        PartitionPruner.doPruningByStepInfo(pruneStepInfo, context);
                    allTbPrunedResults.add(tbPrunedResult);
                    stepsForJoin.add(pruneStepInfo);
                }

                boolean bitSetSame = checkIfBitSetTheSameForPrunedResults(allTbPrunedResults);
                if (!bitSetSame) {
                    /**
                     * <pre>
                     *     When bitSetSame = false, that means:
                     *     a. LogicalView contains a Pushed Join with at least 2 tables;
                     *     b. the predicates of left tbl of join and
                     *        the predicates of right tbl of join are NOT the same,
                     *        so the pruning bitset of left tbl and right tbl are different;
                     *     c. As building the physical of the pushed join of logicalVew must
                     *        be sure that the pruning result of left tbl and right tbl
                     *        are the same, so here have to generate full scan for both
                     *        left tbl and right tbl ignoring there actual pruning result.
                     * <pre/>
                     *
                     *
                     */
                    List<PartPrunedResult> fullScanResults = new ArrayList<>();
                    for (int i = 0; i < logicalView.getTableNames().size(); i++) {
                        String tblName = logicalView.getTableNames().get(i);
                        String schemaName = logicalView.getSchemaName();
                        PartitionInfo partInfo = null;
                        if (context != null) {
                            partInfo = context.getSchemaManager(schemaName).getTable(tblName).getPartitionInfo();
                        }
                        PartitionPruneStep partitionPruneStep =
                            PartitionPruneStepBuilder.genFullScanPruneStepInfoInner(partInfo,
                                partInfo.getPartitionBy().getPhysicalPartLevel(), true);
                        PartPrunedResult tbPrunedResult =
                            PartitionPruner.doPruningByStepInfo(partitionPruneStep, context);
                        fullScanResults.add(tbPrunedResult);
                    }
                    /**
                     * Use ORIGINAL per-table predicate steps and pruning
                     * results (not the synthetic full-scan ones that are only
                     * generated to make join bitsets line up). The synthetic
                     * full-scan result would otherwise have a BitSet covering
                     * all physical partitions and unconditionally trigger the
                     * blacklist even when the per-table predicate did prune.
                     */
                    if (usedSteps != null) {
                        usedSteps.addAll(stepsForJoin);
                    }
                    if (usedResults != null) {
                        usedResults.addAll(allTbPrunedResults);
                    }
                    return fullScanResults;
                }

                if (usedSteps != null) {
                    usedSteps.addAll(stepsForJoin);
                }
                if (usedResults != null) {
                    usedResults.addAll(allTbPrunedResults);
                }
                return allTbPrunedResults;
            } else {

                boolean containAnyReplicasTables = logicalView.containAnyReplicasTables(context);
                if (containAnyReplicasTables) {
                    List<PartPrunedResult> allTbPrunedResultsOfReplicasTable =
                        prunePartitionsForReplicasTables(context, logicalView, useSelectPartitions);
                    return allTbPrunedResultsOfReplicasTable;
                }

                PartPrunedResult tbPrunedResult = null;
                PartitionPruneStep usedStep = null;
                RelShardInfo relShardInfo = logicalView.getRelShardInfo(0, context);
                if (!useSelectPartitions) {
                    PartitionPruneStep pruneStepInfo = relShardInfo.getPartPruneStepInfo();
                    /**
                     * do pruning partitions by context
                     */
                    tbPrunedResult = PartitionPruner.doPruningByStepInfo(pruneStepInfo, context);
                    usedStep = pruneStepInfo;
                } else {
                    PartitionInfo partInfo = relShardInfo.getPartPruneStepInfo().getPartitionInfo();
                    PartitionPruneStep fullScanStep = PartitionPruneStepBuilder.genFullScanPruneStepInfoInner(partInfo,
                        partInfo.getPartitionBy().getPhysicalPartLevel(), true);
                    tbPrunedResult = PartitionPruner.doPruningByStepInfo(fullScanStep, context);
                    usedStep = fullScanStep;
                }
                List<PartPrunedResult> allTbPrunedResults = new ArrayList<>();
                allTbPrunedResults.add(tbPrunedResult);
                if (usedSteps != null) {
                    usedSteps.add(usedStep);
                }
                if (usedResults != null) {
                    usedResults.add(tbPrunedResult);
                }
                return allTbPrunedResults;
            }
        } else {
            throw GeneralUtil.nestedException(new NotSupportException("Not support to non logical view"));
        }
    }

    private static @NotNull List<PartPrunedResult> prunePartitionsForReplicasTables(ExecutionContext context,
                                                                                    LogicalView logicalView,
                                                                                    boolean useSelectPartitions) {
        String tableSchema = logicalView.getSchemaName();
        List<String> tableNames = logicalView.getTableNames();
        PartPruneStepPruningExtraInfo pruningExtraInfo =
            PartitionPrunerUtils.preparePruningExtraInfoIfNeed(context, tableSchema, tableNames);
        boolean usingLock = logicalView.getLockMode() != SqlSelect.LockMode.UNDEF;
        boolean isModifyView = logicalView instanceof LogicalModifyView;
        if (pruningExtraInfo != null) {
            boolean containLockMode = usingLock || isModifyView;
            pruningExtraInfo.initReplicasTableRandomReading(context, containLockMode);
        }
        List<PartPrunedResult> allTbPrunedResults = new ArrayList<>();
        for (int i = 0; i < logicalView.getTableNames().size(); i++) {
            PartitionPruneStep pruneStepInfo = null;
            RelShardInfo relShardInfo = logicalView.getRelShardInfo(i, context);
            if (!useSelectPartitions) {
                pruneStepInfo = relShardInfo.getPartPruneStepInfo();
            } else {
                PartitionInfo partInfo = relShardInfo.getPartPruneStepInfo().getPartitionInfo();
                pruneStepInfo = PartitionPruneStepBuilder.genFullScanPruneStepInfoInner(partInfo,
                    partInfo.getPartitionBy().getPhysicalPartLevel(), true);
            }

            /**
             * do pruning partitions by context
             */
            PartPrunedResult tbPrunedResult =
                PartitionPruner.doPruningByStepInfoWithExtraInfo(pruneStepInfo, context, pruningExtraInfo);
            allTbPrunedResults.add(tbPrunedResult);
        }
        return allTbPrunedResults;
    }

    private static boolean checkIfBitSetTheSameForPrunedResults(List<PartPrunedResult> allTbPrunedResults) {
        BitSet bitSet = null;
        boolean bitSetSame = true;
        for (PartPrunedResult partPrunedResult : allTbPrunedResults) {
            PartitionInfo partitionInfo = partPrunedResult.getPartInfo();
            if (partitionInfo.isBroadcastTable()) {
                continue;
            }
            if (partitionInfo.isReplicasTable()) {
                continue;
            }

            if (bitSet == null) {
                bitSet = partPrunedResult.getPartBitSet();
            } else if (!bitSet.equals(partPrunedResult.getPartBitSet())) {
                bitSetSame = false;
                break;
            }
        }
        return bitSetSame;
    }

    public static List<PartPrunedResult> pruneCciPartitions(RelNode relPlan, ExecutionContext context,
                                                            PartitionInfo cciPartInfo) {
        if (!(relPlan instanceof OSSTableScan)) {
            throw GeneralUtil.nestedException(new NotSupportException("Not support to non OSS table scan"));
        }
        OSSTableScan ossTableScan = (OSSTableScan) relPlan;
        boolean useSelectPartitions = ossTableScan.useSelectPartitions();
        PartPrunedResult tbPrunedResult;
        if (!useSelectPartitions) {
            RelShardInfo relShardInfo = ossTableScan.getCciRelShardInfo(context, cciPartInfo);
            tbPrunedResult = PartitionPruner.doPruningByStepInfo(relShardInfo.getPartPruneStepInfo(), context);
            if (relShardInfo.getPartitions() != null) {
                PartitionPrunerUtils.filterPartitionsBySelectedPartition(tbPrunedResult, relShardInfo.getPartitions());
            }
        } else {
            PartitionPruneStep fullScanStep = PartitionPruneStepBuilder.genFullScanPruneStepInfoInner(cciPartInfo,
                cciPartInfo.getPartitionBy().getPartLevel(), true);
            tbPrunedResult = PartitionPruner.doPruningByStepInfo(fullScanStep, context);
        }
        return Collections.singletonList(tbPrunedResult);
    }
}
