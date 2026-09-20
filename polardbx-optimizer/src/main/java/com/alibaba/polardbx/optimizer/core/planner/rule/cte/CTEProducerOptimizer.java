package com.alibaba.polardbx.optimizer.core.planner.rule.cte;

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.PushFilterRule;
import com.alibaba.polardbx.optimizer.core.planner.rule.PushProjectRule;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.CTEAnchor;
import org.apache.calcite.rel.core.CTEConsumer;
import org.apache.calcite.rel.core.CTEProducer;
import org.apache.calcite.rel.logical.LogicalCTEAnchor;
import org.apache.calcite.rel.logical.LogicalCTEConsumer;
import org.apache.calcite.rel.logical.LogicalCTEProducer;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.rel.rules.FilterProjectTransposeRule;
import org.apache.calcite.rel.rules.FilterSetOpTransposeRule;
import org.apache.calcite.rel.rules.ProjectJoinTransposeRule;
import org.apache.calcite.rel.rules.ProjectMergeRule;
import org.apache.calcite.rel.rules.ProjectSetOpTransposeRule;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.mapping.Mapping;
import org.apache.calcite.util.mapping.MappingType;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * For non-inlined (reused) CTEs:
 * Phase 1: Push filters into CTE consumers (via HepPlanner rules), recursively into innerRel,
 * then apply pre-filter to producers
 * Phase 2: Iteratively push projects into CTE consumers and apply column pruning to producers,
 * repeating until no further column pruning is possible
 * <p>
 * Consumer-absorbed conditions/projects are kept inside CTEConsumer and propagated
 * to PhysicalCTEConsumer during conversion, so no explicit Filter/Project materialization
 * is needed.
 */
public class CTEProducerOptimizer {

    private static final int MAX_ITERATIONS = 64;

    public static RelNode optimize(RelNode input, PlannerContext plannerContext) {
        CTEContext cteContext = plannerContext.getCteContext();
        // reCollect ensures the context is up-to-date (caller may have already checked hasCTE,
        // but the state could have changed after inlining; re-sync is cheap)
        cteContext.reCollect(input);

        // CTE IDs that have already had filters pushed down to their producers.
        Set<Integer> filterPushedCteIds = Sets.newHashSet();
        RelNode current = input;
        // Phase 1: Push filters into CTE consumers and apply pre-filter to producers
        for (int iter = 0; iter < MAX_ITERATIONS; iter++) {
            current = pushFilterIntoCTEConsumers(current, plannerContext);
            cteContext.reCollect(current);
            RelNode pruned = processAllAnchorsForFilter(current, cteContext, filterPushedCteIds);
            if (pruned == current) {
                // No column pruning happened, iteration is stable
                break;
            }
            current = pruned;
        }

        // Phase 2: Iteratively push projects and prune columns until no more column pruning
        for (int iter = 0; iter < MAX_ITERATIONS; iter++) {
            current = pushProjectIntoCTEConsumers(current, plannerContext);
            cteContext.reCollect(current);
            RelNode pruned = processAllAnchorsForColumnPrune(current, cteContext);
            if (pruned == current) {
                // No column pruning happened, iteration is stable
                break;
            }
            current = pruned;
        }

        return current;
    }

    /**
     * Use HepPlanner with filter push rules to propagate Filter
     * through anchors, unions, joins and into CTE consumers.
     */
    private static RelNode pushFilterIntoCTEConsumers(
        RelNode input, PlannerContext plannerContext) {
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addGroupBegin();
        builder.addRuleInstance(FilterCTEAnchorTransposeRule.INSTANCE);
        builder.addRuleInstance(FilterSetOpTransposeRule.INSTANCE);
        builder.addRuleInstance(FilterJoinRule.FILTER_ON_JOIN);
        builder.addRuleInstance(FilterProjectTransposeRule.INSTANCE);
        builder.addRuleInstance(PushFilterToCTEConsumerRule.INSTANCE);
        builder.addRuleInstance(PushFilterRule.LOGICALVIEW);
        builder.addGroupEnd();

        HepPlanner planner = new HepPlanner(builder.build(), plannerContext);
        planner.setRoot(input);
        RelNode result = planner.findBestExp();

        // Recursively optimize inside each CTEConsumer's innerRel
        result = result.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(LogicalCTEConsumer consumer) {
                RelNode innerRel = consumer.getInnerRel();
                RelNode optimizedInnerRel = pushFilterIntoCTEConsumers(innerRel, plannerContext);
                if (optimizedInnerRel != innerRel) {
                    return consumer.copy(consumer.getTraitSet(), optimizedInnerRel);
                }
                return consumer;
            }
        });

        return result;
    }

    /**
     * Use HepPlanner with project push rules to propagate Project
     * through anchors, unions, joins and into CTE consumers.
     */
    private static RelNode pushProjectIntoCTEConsumers(
        RelNode input, PlannerContext plannerContext) {
        HepProgramBuilder builder = new HepProgramBuilder();
        builder.addGroupBegin();
        builder.addRuleInstance(ProjectCTEAnchorTransposeRule.INSTANCE);
        builder.addRuleInstance(ProjectSetOpTransposeRule.INSTANCE);
        builder.addRuleInstance(ProjectJoinTransposeRule.INSTANCE);
        builder.addRuleInstance(ProjectMergeRule.INSTANCE);
        builder.addRuleInstance(FilterProjectTransposeRule.INSTANCE);
        builder.addRuleInstance(PushProjectToCTEConsumerRule.INSTANCE);
        builder.addRuleInstance(PushProjectRule.INSTANCE);
        builder.addGroupEnd();

        HepPlanner planner = new HepPlanner(builder.build(), plannerContext);
        planner.setRoot(input);
        RelNode result = planner.findBestExp();

        // Recursively optimize inside each CTEConsumer's innerRel
        result = result.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(LogicalCTEConsumer consumer) {
                RelNode innerRel = consumer.getInnerRel();
                RelNode optimizedInnerRel = pushProjectIntoCTEConsumers(innerRel, plannerContext);
                if (optimizedInnerRel != innerRel) {
                    return consumer.copy(consumer.getTraitSet(), optimizedInnerRel);
                }
                return consumer;
            }
        });

        return result;
    }

    /**
     * Walk the tree bottom-up and apply pre-filter to each non-inlined CTE producer.
     */
    private static RelNode processAllAnchorsForFilter(RelNode input, CTEContext cteContext,
                                                      Set<Integer> filterPushedCteIds) {
        return input.accept(new RelShuttleImpl() {
            @Override
            public RelNode visit(RelNode other) {
                RelNode visited = visitChildren(other);
                if (visited instanceof LogicalCTEAnchor) {
                    LogicalCTEAnchor anchor = (LogicalCTEAnchor) visited;
                    if (!cteContext.shouldInline(anchor.getCteId())) {
                        return processNonInlinedCTEFilter(anchor, cteContext, filterPushedCteIds);
                    }
                }
                return visited;
            }
        });
    }

    /**
     * Walk the tree bottom-up and apply column pruning to each non-inlined CTE producer.
     */
    private static RelNode processAllAnchorsForColumnPrune(RelNode input, CTEContext cteContext) {
        return input.accept(new RelShuttleImpl() {
            public RelNode visit(LogicalCTEConsumer cteConsumer) {
                LogicalCTEConsumer current = (LogicalCTEConsumer) super.visit(cteConsumer);
                if (current != cteConsumer) {
                    cteContext.replaceCteConsumer(cteConsumer, current);
                }
                return current;
            }

            @Override
            public RelNode visit(RelNode other) {
                RelNode visited = visitChildren(other);
                if (visited instanceof LogicalCTEAnchor) {
                    LogicalCTEAnchor anchor = (LogicalCTEAnchor) visited;
                    if (!cteContext.shouldInline(anchor.getCteId())) {
                        return processNonInlinedCTEColumnPrune(anchor, cteContext);
                    }
                }
                return visited;
            }
        });
    }

    /**
     * Apply pre-filter to a non-inlined CTE producer based on consumers' conditions.
     * Column indices are unchanged since only a Filter is added.
     */
    private static RelNode processNonInlinedCTEFilter(LogicalCTEAnchor anchor, CTEContext cteContext,
                                                      Set<Integer> filterPushedCteIds) {
        LogicalCTEProducer producer = (LogicalCTEProducer) anchor.getLeft();
        final int cteId = anchor.getCteId();

        // Skip if filter has already been pushed for this cteId
        if (filterPushedCteIds.contains(cteId)) {
            return anchor;
        }

        Collection<LogicalCTEConsumer> consumers = cteContext.getCteConsumers(cteId);
        if (consumers.isEmpty()) {
            return anchor;
        }

        // All consumers must have conditions to create a pre-filter
        boolean allHaveConditions = consumers.stream().allMatch(c -> !c.getConditions().isEmpty());
        if (!allHaveConditions) {
            return anchor;
        }

        consumers = consumers.stream().sorted(
                Comparator.comparingInt((LogicalCTEConsumer x) -> x.getCteId()).thenComparingInt(CTEConsumer::getSn))
            .collect(Collectors.toList());
        // Compute pre-filter (OR of all consumers' conditions)
        RexBuilder rexBuilder = producer.getCluster().getRexBuilder();
        List<RexNode> orOperands = new ArrayList<>();
        for (LogicalCTEConsumer consumer : consumers) {
            RexNode consumerAnd = RexUtil.composeConjunction(
                rexBuilder, consumer.getConditions(), false);
            orOperands.add(consumerAnd);
        }
        RexNode producerFilter = RexUtil.composeDisjunction(rexBuilder, orOperands, false);

        // Apply pre-filter to producer input (column indices unchanged)
        RelNode newProducerInput = LogicalFilter.create(producer.getInput(), producerFilter);
        LogicalCTEProducer newProducer = new LogicalCTEProducer(
            producer.getCluster(), producer.getTraitSet(), newProducerInput,
            producer.getCteId(), producer.getRowType());

        // Mark this cteId as filter-pushed to avoid redundant processing
        filterPushedCteIds.add(cteId);

        return new LogicalCTEAnchor(anchor.getCluster(), anchor.getTraitSet(),
            newProducer, anchor.getRight(), anchor.getCteId(), anchor.getRight().getRowType());
    }

    /**
     * Apply column pruning to a non-inlined CTE producer based on consumers' projects.
     * Remaps consumers' projects/conditions through the column mapping.
     */
    private static RelNode processNonInlinedCTEColumnPrune(LogicalCTEAnchor anchor, CTEContext cteContext) {
        LogicalCTEProducer producer = (LogicalCTEProducer) anchor.getLeft();
        final int cteId = anchor.getCteId();
        final int producerFieldCount = producer.getRowType().getFieldCount();

        Collection<LogicalCTEConsumer> consumers = cteContext.getCteConsumers(cteId);
        if (consumers.isEmpty()) {
            return anchor;
        }

        // Compute required columns (union across all consumers)
        ImmutableBitSet.Builder requiredColsBuilder = ImmutableBitSet.builder();
        for (LogicalCTEConsumer consumer : consumers) {
            if (consumer.getProjects().isEmpty()) {
                requiredColsBuilder.set(0, producerFieldCount);
                break;
            }
            for (RexNode proj : consumer.getProjects()) {
                requiredColsBuilder.addAll(RelOptUtil.InputFinder.bits(proj));
            }
            for (RexNode cond : consumer.getConditions()) {
                requiredColsBuilder.addAll(RelOptUtil.InputFinder.bits(cond));
            }
        }
        if (requiredColsBuilder.cardinality() == 0) {
            requiredColsBuilder.set(0);
        }
        ImmutableBitSet requiredCols = requiredColsBuilder.build();
        boolean needColumnPrune = requiredCols.cardinality() < producerFieldCount;

        if (!needColumnPrune) {
            return anchor;
        }

        // Build column mapping
        Mapping columnMapping = Mappings.create(
            MappingType.INVERSE_SURJECTION, producerFieldCount, requiredCols.cardinality());
        int targetIdx = 0;
        for (int srcIdx : requiredCols) {
            columnMapping.set(srcIdx, targetIdx++);
        }

        // Add column pruning Project to producer
        RexBuilder rexBuilder = producer.getCluster().getRexBuilder();
        RelNode newProducerInput = producer.getInput();
        List<RexNode> projectExprs = new ArrayList<>();
        List<String> fieldNames = new ArrayList<>();
        RelDataType inputType = newProducerInput.getRowType();
        for (int idx : requiredCols) {
            projectExprs.add(rexBuilder.makeInputRef(
                inputType.getFieldList().get(idx).getType(), idx));
            fieldNames.add(inputType.getFieldList().get(idx).getName());
        }

        RelDataType newProducerRowType = producer.getCluster().getTypeFactory()
            .createStructType(
                projectExprs.stream().map(RexNode::getType).collect(Collectors.toList()),
                fieldNames);
        newProducerInput = LogicalProject.create(
            newProducerInput, projectExprs, newProducerRowType);

        // Create new Producer
        LogicalCTEProducer newProducer = new LogicalCTEProducer(
            producer.getCluster(), producer.getTraitSet(), newProducerInput,
            producer.getCteId(), newProducerRowType);

        return reconstructCTEAnchor(anchor, newProducer, columnMapping, cteId, cteContext);
    }

    /**
     * Reconstruct the CTE anchor: update each consumer with remapped projects/conditions
     * after producer column pruning. Also updates cteContext cache with new objects.
     */
    private static RelNode reconstructCTEAnchor(
        LogicalCTEAnchor anchor,
        LogicalCTEProducer newProducer,
        Mapping columnMapping,
        int cteId,
        CTEContext cteContext) {
        // Collect new consumers during reconstruction to update cteContext cache
        Set<LogicalCTEConsumer> newConsumers = Sets.newHashSet();

        // Reconstruct each consumer in the right (usage) subtree
        RelNode right = anchor.getRight();
        if (columnMapping != null) {
            right = right.accept(new RelShuttleImpl() {
                @Override
                public RelNode visit(LogicalCTEConsumer consumer) {
                    LogicalCTEConsumer current = (LogicalCTEConsumer) super.visit(consumer);
                    if (current.getCteId() == cteId) {
                        current = reconstructConsumer(current, columnMapping);
                        newConsumers.add(current);
                        return current;
                    }
                    return current;
                }
            });
        }

        // Update cteContext cache with new producer and consumers
        cteContext.updateAfterReconstruct(cteId, newProducer, newConsumers);

        return new LogicalCTEAnchor(anchor.getCluster(), anchor.getTraitSet(),
            newProducer, right, anchor.getCteId(), right.getRowType());
    }

    /**
     * Validate that no CTE-related operators (CTEAnchor, CTEProducer, CTEConsumer)
     * exist inside any LogicalCTEConsumer's innerRel throughout the plan tree.
     * Throws AssertionError if a violation is found.
     */
    public static void validateConsumerInnerRelNoCTE(RelNode root, PlannerContext plannerContext) {
        CTEContext cteContext = plannerContext.getCteContext();
        cteContext.forceInit(root, plannerContext);
//        if (!cteContext.hasCTE()) {
//            return;
//        }
//        root.accept(new RelShuttleImpl() {
//            @Override
//            public RelNode visit(LogicalCTEConsumer consumer) {
//                // Check that this consumer's innerRel does not contain CTE operators
//                consumer.getInnerRel().accept(new SubqueryAwareRelShuttle() {
//                    @Override
//                    public RelNode visit(LogicalCTEConsumer cteConsumer) {
//                        throw new AssertionError(
//                            "Found CTE operator " + cteConsumer.getClass().getSimpleName()
//                                + " inside innerRel of CTE consumer "
//                                + consumer.getCteId() + "_" + consumer.getSn());
//                    }
//
//                    @Override
//                    public RelNode visit(RelNode other) {
//                        if (other instanceof CTEAnchor
//                            || other instanceof CTEProducer) {
//                            throw new AssertionError(
//                                "Found CTE operator " + other.getClass().getSimpleName()
//                                    + " inside innerRel of CTE consumer "
//                                    + consumer.getCteId() + "_" + consumer.getSn());
//                        }
//                        return visitChildren(other);
//                    }
//                });
//                return consumer;
//            }
//
//            @Override
//            public RelNode visit(RelNode other) {
//                return visitChildren(other);
//            }
//        });
    }

    private static LogicalCTEConsumer reconstructConsumer(
        LogicalCTEConsumer consumer,
        Mapping columnMapping) {
        if (CollectionUtils.isEmpty(consumer.getConditions()) && CollectionUtils.isEmpty(consumer.getProjects())) {
            return consumer;
        }
        // Remap conditions through column mapping if needed
        List<RexNode> newConditions = consumer.getConditions();
        if (!CollectionUtils.isEmpty(consumer.getConditions())) {
            newConditions = Lists.newArrayList();
            for (RexNode cond : consumer.getConditions()) {
                newConditions.add(RexUtil.apply(columnMapping, cond));
            }
        }

        // Remap projects through column mapping if needed
        List<RexNode> newProjects = consumer.getProjects();
        if (!CollectionUtils.isEmpty(consumer.getProjects())) {
            newProjects = Lists.newArrayList();
            for (RexNode proj : consumer.getProjects()) {
                newProjects.add(RexUtil.apply(columnMapping, proj));
            }
        }

        // Skip if the project is identical to the input (all columns in order, no actual pruning)
        boolean isIdentity = newProjects.size() == columnMapping.size();
        if (isIdentity) {
            for (int i = 0; i < newProjects.size(); i++) {
                RexNode expr = newProjects.get(i);
                if (!(expr instanceof RexInputRef) || ((RexInputRef) expr).getIndex() != i) {
                    isIdentity = false;
                    break;
                }
            }
        }
        if (isIdentity) {
            newProjects = ImmutableList.of();
        }

        // Keep remapped projects/conditions inside the consumer
        return new LogicalCTEConsumer(consumer.getCluster(), consumer.getTraitSet(), consumer.getInnerRel(),
            consumer.getCteId(), consumer.getSn(), consumer.getInnerRel().getRowType(),
            newProjects, newConditions);
    }

}
