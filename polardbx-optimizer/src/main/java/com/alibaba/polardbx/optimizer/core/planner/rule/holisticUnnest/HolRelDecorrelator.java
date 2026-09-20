package com.alibaba.polardbx.optimizer.core.planner.rule.holisticUnnest;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.optimizer.core.TddlOperatorTable;
import com.alibaba.polardbx.optimizer.core.planner.rule.cte.CTEContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.Strong;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCTEAnchor;
import org.apache.calcite.rel.logical.LogicalCTEConsumer;
import org.apache.calcite.rel.logical.LogicalCTEProducer;
import org.apache.calcite.rel.logical.LogicalColCorrelate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSemiJoin;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.logical.LogicalWindow;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexCorrelVariable;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.util.ImmutableBitSet;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.ReflectUtil;
import org.apache.calcite.util.ReflectiveVisitor;
import org.apache.calcite.util.mapping.Mappings;
import org.apache.commons.collections.CollectionUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * HolRelDecorrelator replaces all correlated expressions (corExp) in a relational
 * expression (RelNode) tree with non-correlated expressions that are produced
 * from joining the RelNode that produces the corExp with the RelNode that
 * references it.
 * <p>
 * An implementation of paper 'Improving Unnesting of Complex Queries '
 */
public class HolRelDecorrelator implements ReflectiveVisitor {
    //~ Instance fields --------------------------------------------------------

    protected final RelBuilder relBuilder;

    protected final RexBuilder rexBuilder;

    private final CTEContext cteContext;
    // map built during translation
    protected CorelMap cm;

    protected SubtreeChecker subtreeChecker;

    protected Map<Integer, Unnesting> cteIdUnnestMap;

    @SuppressWarnings("method.invocation.invalid")
    protected final ReflectUtil.MethodDispatcher<Frame> dispatcher =
        ReflectUtil.createMethodDispatcher(
            Frame.class, getVisitor().getClass(), "decorrelateRel",
            RelNode.class,
            Unnesting.class,
            Set.class);

    /**
     * Built during decorrelation, of rel to all the newly created correlated
     * variables in its output, and to map old input positions to new input
     * positions. This is from the view point of the parent rel of a new rel.
     */
    protected final Map<RelNode, Frame> map = new HashMap<>();

    //~ Constructors -----------------------------------------------------------

    protected HolRelDecorrelator(
        CorelMap cm,
        RelBuilder relBuilder,
        CTEContext cteContext) {
        this.cm = cm;
        this.relBuilder = relBuilder;
        this.rexBuilder = relBuilder.getRexBuilder();
        this.cteContext = cteContext;
        this.cteIdUnnestMap = Maps.newHashMap();
    }

    //~ Methods ----------------------------------------------------------------

    /**
     * Decorrelates a query.
     *
     * <p>This is the main entry point to {@code HolRelDecorrelator}.
     *
     * @param rootRel Root node of the query
     * @param relBuilder Builder for relational expressions
     * @return Equivalent query with all
     * {@link Correlate} instances removed
     */
    public static RelNode decorrelateQuery(RelNode rootRel, RelBuilder relBuilder, CTEContext cteContext) {
        final CorelMap corelMap = new CorelMapBuilder().build(rootRel);
        final HolRelDecorrelator decorrelator = new HolRelDecorrelator(corelMap, relBuilder, cteContext);
        RelNode newRootRel = decorrelator.removeCorrelationViaRule(rootRel);
        if (corelMap.hasCorrelation()) {
            newRootRel = decorrelator.decorrelate(newRootRel);
        }
        return newRootRel;
    }

    protected RelNode decorrelate(RelNode root) {
        // Necessary to update cm (CorelMap) since CorrelateProjectExtractor above may modify the plan
        this.cm = new CorelMapBuilder().build(root);
        this.map.clear();
        this.subtreeChecker = SubtreeChecker.build(root);

        final Frame frame = getInvoke(root, null, null);
        if (frame != null) {
            // Check if the frame has more fields than the original and discard the extra ones
            RelNode result = frame.r;
            int fields = frame.r.getRowType().getFieldCount();
            if (fields > frame.oldToNewOutputs.size()) {
                relBuilder.push(result);
                List<Map.Entry<Integer, Integer>> entries = Lists.newArrayList(frame.oldToNewOutputs.entrySet());
                entries.sort(Map.Entry.comparingByKey());
                final List<RexNode> exprList = Lists.newArrayList();
                entries.forEach(entry -> exprList.add(relBuilder.field(entry.getValue())));
                relBuilder.project(exprList);
                result = relBuilder.build();
            } else {
                Litmus.THROW.check(fields == frame.oldToNewOutputs.size(),
                    "Produced relation has fewer columns than the original relation");
            }
            return result;
        }
        return root;
    }

    /**
     * Remove some instances of {@link Correlate} from a query plan
     * by applying a default set of rules (only some of the
     * {@link Correlate}s might be removable in such way).
     */
    public RelNode removeCorrelationViaRule(RelNode root) {
        // TODO : use RBO for fast path
        return root;
    }

    protected RexNode decorrelateExpr(RelNode currentRel, Map<RelNode, Frame> map, RexNode exp) {
        DecorrelateRexShuttle shuttle = new DecorrelateRexShuttle(currentRel, map);
        return exp.accept(shuttle);
    }

    public Frame getInvoke(RelNode r, Unnesting unnest, Set<RelNode> accessing) {
        Frame frame;
        if (!decorrelating(unnest)) {
            frame = dispatcher.invoke(getVisitor(), r, null, null);
        } else if (!shouldPushCorrelate(unnest, accessing)) {
            Frame rightFrame = dispatcher.invoke(getVisitor(), r, null, null);
            Map<CorDef, Integer> corDefOutputs = dominatingAllEquality(unnest, r);
            if (corDefOutputs != null) {
                List<RexNode> projects = Lists.newArrayList();
                RelNode newInput = rightFrame.r;
                int newInputFieldCount = newInput.getRowType().getFieldCount();
                for (int i = 0; i < newInputFieldCount; i++) {
                    projects.add(RexInputRef.of(i, newInput.getRowType()));
                }
                NavigableMap<CorDef, Integer> corDefOutputsForProject = Maps.newTreeMap();
                for (Map.Entry<CorDef, Integer> entry : corDefOutputs.entrySet()) {
                    int newOutput = rightFrame.oldToNewOutputs.get(entry.getValue());
                    corDefOutputsForProject.put(entry.getKey(), projects.size());
                    projects.add(RexInputRef.of(newOutput, newInput.getRowType()));
                }
                RelNode finalNode = relBuilder.push(newInput).project(projects).build();
                frame = new Frame(finalNode, corDefOutputsForProject, rightFrame.oldToNewOutputs);
            } else {
                Frame leftFrame = buildDominatingSet(unnest);
                RelNode newLeft = leftFrame.r;
                int newLeftFieldCount = newLeft.getRowType().getFieldCount();
                RelNode newRight = rightFrame.r;

                // add right info
                final Map<Integer, Integer> mapOldToNewOutputs = new HashMap<>();
                for (Map.Entry<Integer, Integer> entry : rightFrame.oldToNewOutputs.entrySet()) {
                    mapOldToNewOutputs.put(entry.getKey(), entry.getValue() + newLeftFieldCount);
                }
                // discard corDef from right side
                RelNode finalNode = relBuilder.push(newLeft).push(newRight)
                    .join(JoinRelType.INNER, ImmutableList.of()).build();
                frame = new Frame(finalNode, leftFrame.corDefOutputs, mapOldToNewOutputs);
            }
        } else {
            frame = dispatcher.invoke(getVisitor(), r, unnest, accessing);
        }
        if (frame == null) {
            frame = new Frame(r.copy(r.getTraitSet(), r.getInputs()), ImmutableSortedMap.of(),
                identityMap(r.getRowType().getFieldCount()));
        }
        map.put(r, frame);
        return frame;
    }

    Map<CorDef, Integer> dominatingAllEquality(Unnesting unnest, RelNode relNode) {
        Map<CorDef, Integer> corDefOutputs = Maps.newTreeMap();
        Equality equality = unnest.getEquality();
        for (CorDef corDef : unnest.getInfo().getOuterRefs()) {
            Integer corId = equality.getId(corDef);
            if (corId == null) {
                return null;
            }
            for (int i = 0; i < relNode.getRowType().getFieldCount(); i++) {
                Integer colId = equality.getId(i);
                if (colId == null) {
                    continue;
                }
                if (equality.getUnionFind().isConnected(colId, corId)) {
                    corDefOutputs.put(corDef, i);
                    break;
                }
            }
            if (!corDefOutputs.containsKey(corDef)) {
                return null;
            }
        }
        return corDefOutputs;
    }

    /**
     * build dominating set D for left side of correlate
     */
    Frame buildDominatingSet(Unnesting unnest) {
        Correlate correlate = unnest.getInfo().getJoin();
        Frame frame = getOrCreateFrame((correlate).getLeft());
        RelNode copiedLeft = CBOUtil.RelCopied.copy(frame.r);
        if (copiedLeft == null) {
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER, "failed to copy relNode during decorrelation");
        }
        Integer cteId = unnest.getInfo().getCteId();
        if (cteId != null) {
            copiedLeft = new LogicalCTEConsumer(copiedLeft.getCluster(),
                copiedLeft.getTraitSet(), copiedLeft, cteId, cteContext.getNextCteConsumerSn(cteId),
                copiedLeft.getRowType());
            cteContext.registerCteConsumer((LogicalCTEConsumer) copiedLeft);
        }
        // build D from outerRefs
        ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
        for (CorDef corDef : unnest.getInfo().getOuterRefs()) {
            if (corDef.corr.equals(correlate.getCorrelationId())) {
                builder.set(frame.oldToNewOutputs.get(corDef.field));
            } else {
                builder.set(frame.corDefOutputs.get(corDef));
            }
        }
        ImmutableBitSet groupSet = builder.build();
        BitSetRanker ranker = new BitSetRanker(groupSet);

        // build corDefOutputs
        final NavigableMap<CorDef, Integer> corDefOutputs = Maps.newTreeMap();
        for (CorDef corDef : unnest.getInfo().getOuterRefs()) {
            int loc;
            if (corDef.corr.equals(correlate.getCorrelationId())) {
                loc = frame.oldToNewOutputs.get(corDef.field);
            } else {
                loc = frame.corDefOutputs.get(corDef);
            }
            corDefOutputs.put(corDef, ranker.getRank(loc));
        }
        return new Frame(
            LogicalAggregate.create(copiedLeft, groupSet, ImmutableList.of(), ImmutableList.of()),
            corDefOutputs, Maps.newHashMap());
    }

    /**
     * Fallback if none of the other {@code decorrelateRel} methods match.
     */
    public Frame decorrelateRel(RelNode rel, Unnesting unnest, Set<RelNode> accessing) {
        if (!rel.getInputs().isEmpty()) {
            throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER, "unsupported rel type " + rel.getClass());
        }
        RelNode newRel = rel.copy(rel.getTraitSet(), rel.getInputs());
        return register(rel, newRel, identityMap(rel.getRowType().getFieldCount()),
            ImmutableSortedMap.of());
    }

    public Frame decorrelateRel(LogicalInsert rel, Unnesting unnest, Set<RelNode> accessing) {
        final RelNode oldInput = rel.getInput();
        if (oldInput == null) {
            return null;
        }

        Frame frame = getInvoke(oldInput, unnest, accessing);
        RelNode newInput = frame.r;
        if (!HolRelDecorrelator.isIdentityMap(frame.oldToNewOutputs)
            || !frame.corDefOutputs.isEmpty()) {
            List<RexNode> projects = Lists.newArrayList();
            RelDataType frameRowtype = frame.r.getRowType();
            for (Map.Entry<Integer, Integer> entry : frame.oldToNewOutputs.entrySet()) {
                projects.add(RexInputRef.of(entry.getValue(), frameRowtype));
            }
            newInput = relBuilder.push(frame.r).project(projects).build();
        }

        RelNode newRel = rel.copy(rel.getTraitSet(), ImmutableList.of(newInput));
        return register(rel, newRel, identityMap(rel.getRowType().getFieldCount()),
            ImmutableSortedMap.of());
    }

    public Frame decorrelateRel(LogicalView rel, Unnesting unnest, Set<RelNode> accessing) {
        LogicalView newRel = rel.copy(rel.getTraitSet());
        return register(rel, newRel, identityMap(rel.getRowType().getFieldCount()), ImmutableSortedMap.of());
    }

    public Frame decorrelateRel(LogicalProject rel, Unnesting unnest, Set<RelNode> accessing) {
        removeRel(accessing, rel);

        // change column ref
        if (decorrelating(unnest)) {
            Map<Integer, Integer> shift = Maps.newHashMap();
            for (int i = 0; i < rel.getProjects().size(); i++) {
                RexNode node = rel.getProjects().get(i);
                if (node instanceof RexInputRef) {
                    shift.put(i, ((RexInputRef) node).getIndex());
                }
            }
            unnest.getEquality().shiftColRef(shift);
        }

        final RelNode oldInput = rel.getInput();
        Frame frame = getInvoke(oldInput, unnest, accessing);
        final List<RexNode> oldProjects = rel.getProjects();
        final List<RelDataTypeField> relOutput = rel.getRowType().getFieldList();

        // Project projects the original expressions,
        // plus any correlated variables the input wants to pass along.
        final List<RexNode> projects = Lists.newArrayList();
        final List<String> projectsName = Lists.newArrayList();

        // Project projects the original expressions
        final Map<Integer, Integer> mapOldToNewOutputs = Maps.newHashMap();
        int newPos;
        for (newPos = 0; newPos < oldProjects.size(); newPos++) {
            projects.add(decorrelateExpr(rel, map, oldProjects.get(newPos)));
            projectsName.add(relOutput.get(newPos).getName());
            mapOldToNewOutputs.put(newPos, newPos);
        }

        // Project any correlated variables the input wants to pass along.
        final NavigableMap<CorDef, Integer> corDefOutputs = Maps.newTreeMap();
        for (Map.Entry<CorDef, Integer> entry : frame.corDefOutputs.entrySet()) {
            final RelDataTypeField field = frame.r.getRowType().getFieldList().get(entry.getValue());
            projects.add(new RexInputRef(entry.getValue(), field.getType()));
            projectsName.add(field.getName());
            corDefOutputs.put(entry.getKey(), newPos);
            newPos++;
        }

        RelNode newProject = relBuilder.push(frame.r)
            .projectNamed(projects, projectsName, true)
            .build();

        return register(rel, newProject, mapOldToNewOutputs, corDefOutputs);
    }

    public Frame decorrelateRel(LogicalFilter rel, Unnesting unnest, Set<RelNode> accessing) {
        removeRel(accessing, rel);
        final RelNode oldInput = rel.getInput();

        // add equality
        if (decorrelating(unnest)) {
            for (RexNode condition : RelOptUtil.conjunctions(rel.getCondition())) {
                if (condition instanceof RexCall &&
                    (condition.getKind() == SqlKind.EQUALS || condition.getKind() == SqlKind.IS_NOT_DISTINCT_FROM)) {
                    unnest.getEquality().union(((RexCall) condition).getOperands().get(0),
                        ((RexCall) condition).getOperands().get(1));
                }
            }
        }

        Frame frame = getInvoke(oldInput, unnest, accessing);
        relBuilder.push(frame.r).filter(decorrelateExpr(rel, map, rel.getCondition()));
        return register(rel, relBuilder.build(), frame.oldToNewOutputs,
            frame.corDefOutputs);
    }

    public Frame decorrelateRel(LogicalSort rel, Unnesting unnest, Set<RelNode> accessing) {
        removeRel(accessing, rel);
        final RelNode oldInput = rel.getInput();
        if (decorrelating(unnest)) {
            unnest.getEquality().shiftColRef(Maps.newHashMap());
        }

        Frame frame = getInvoke(oldInput, unnest, accessing);
        Mappings.TargetMapping mapping = Mappings.target(frame.oldToNewOutputs,
            rel.getRowType().getFieldCount(), frame.r.getRowType().getFieldCount());
        RelCollation newCollation = rel.getCollation().apply(mapping);
        if ((!rel.withLimit()) || !decorrelating(unnest)) {
            RelNode newNode = rel.copy(rel.getTraitSet().replace(newCollation), frame.r, newCollation);
            return register(rel, newNode, frame.oldToNewOutputs, frame.corDefOutputs);
        }
        // with limit, use window
        //
        // select *
        // from S
        // order by X limit L offset O
        //
        // select *
        // from ( select *, row_number() as RN OVER (PARTITION BY corr order by X) from S)
        // where RN between O+1 and O+L
        RelDataType newInputRowType = frame.r.getRowType();
        // build agg
        AggregateCall aggCall = AggregateCall.create(SqlStdOperatorTable.ROW_NUMBER,
            false,
            false,
            ImmutableIntList.of(),
            -1,
            rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BIGINT),
            "RN");
        ImmutableBitSet groupSet = ImmutableBitSet.builder().addAll(frame.corDefOutputs.values()).build();
        final Window.RexWinAggCall winAggCall =
            new Window.RexWinAggCall(
                aggCall.getAggregation(),
                aggCall.getType(),
                ImmutableList.of(),
                0,
                false);

        // build window group
        Window.Group windowGroup =
            new Window.Group(groupSet, false, RexWindowBound.UNBOUNDED_PRECEDING,
                RexWindowBound.CURRENT_ROW, newCollation, ImmutableList.of(winAggCall));

        final List<Map.Entry<String, RelDataType>> fieldList = Lists.newArrayList(newInputRowType.getFieldList());
        fieldList.add(org.apache.calcite.util.Pair.of("RN", winAggCall.getType()));
        final RelDataType windowRowType = rexBuilder.getTypeFactory().createStructType(fieldList);

        LogicalWindow newWindow = LogicalWindow.create(rel.getTraitSet().replace(newCollation), frame.r,
            ImmutableList.of(),
            windowRowType,
            ImmutableList.of(windowGroup));

        RexNode condition;
        if (rel.offset == null) {
            condition = rexBuilder.makeCall(
                TddlOperatorTable.LESS_THAN_OR_EQUAL,
                RexInputRef.of(newInputRowType.getFieldCount(), newWindow.getRowType()),
                rel.fetch);
        } else {
            condition = rexBuilder.makeCall(
                TddlOperatorTable.BETWEEN,
                RexInputRef.of(newInputRowType.getFieldCount(), newWindow.getRowType()),
                rexBuilder.makeCall(TddlOperatorTable.PLUS,
                    rel.offset, rexBuilder.makeBigIntLiteral(1L)),
                rexBuilder.makeCall(TddlOperatorTable.PLUS,
                    rel.offset, rel.fetch));
        }
        RelNode finalNode = relBuilder.push(newWindow).filter(condition).build();
        return register(rel, finalNode, frame.oldToNewOutputs, frame.corDefOutputs);
    }

    public Frame decorrelateRel(LogicalAggregate rel, Unnesting unnest, Set<RelNode> accessing) {
        boolean withoutGroupBy = rel.getGroupCount() == 0;
        removeRel(accessing, rel);
        ImmutableBitSet oldGroupSet = rel.getGroupSet();
        // change column ref for agg
        if (decorrelating(unnest)) {
            Map<Integer, Integer> shift = Maps.newHashMap();
            for (int i = oldGroupSet.nextSetBit(0), cnt = 0; i >= 0; i = oldGroupSet.nextSetBit(i + 1), cnt++) {
                shift.put(i, cnt);
            }
            unnest.getEquality().shiftColRef(shift);
        }
        final RelNode oldInput = rel.getInput();
        Frame frame = getInvoke(oldInput, unnest, accessing);
        // build group set
        ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
        for (int loc = rel.getGroupSet().nextSetBit(0); loc >= 0; loc = rel.getGroupSet().nextSetBit(loc + 1)) {
            builder.set(frame.oldToNewOutputs.get(loc));
        }
        frame.corDefOutputs.values().forEach(builder::set);
        ImmutableBitSet newGroupSet = builder.build();

        // build group Sets
        List<ImmutableBitSet> newGroupSets = Lists.newArrayList();
        if (CollectionUtils.isNotEmpty(rel.getGroupSets())) {
            List<ImmutableBitSet> groupSets = rel.getGroupSets();
            for (ImmutableBitSet subGroupSet : groupSets) {
                ImmutableBitSet.Builder newGroupSetBuilder = ImmutableBitSet.builder();
                for (int loc = subGroupSet.nextSetBit(0); loc >= 0; loc = subGroupSet.nextSetBit(loc + 1)) {
                    newGroupSetBuilder.set(frame.oldToNewOutputs.get(loc));
                }
                frame.corDefOutputs.values().forEach(newGroupSetBuilder::set);
                newGroupSets.add(newGroupSetBuilder.build());
            }
        }

        List<AggregateCall> newAggCalls = Lists.newArrayList();
        for (AggregateCall aggCall : rel.getAggCallList()) {
            List<Integer> newArgList =
                aggCall.getArgList().stream().map(frame.oldToNewOutputs::get).collect(Collectors.toList());
            newAggCalls.add(aggCall.copy(newArgList, aggCall.filterArg));
        }

        // build new agg
        LogicalAggregate newAgg =
            rel.copy(rel.getTraitSet(), frame.r, rel.indicator, newGroupSet, newGroupSets, newAggCalls);
        BitSetRanker ranker = new BitSetRanker(newGroupSet);

        final Map<Integer, Integer> mapOldToNewOutputsForAgg = Maps.newHashMap();
        final NavigableMap<CorDef, Integer> corDefOutputsForAgg = Maps.newTreeMap();
        for (int loc = oldGroupSet.nextSetBit(0); loc >= 0; loc = oldGroupSet.nextSetBit(loc + 1)) {
            int newInputLoc = frame.oldToNewOutputs.get(loc);
            int newOutputLoc = ranker.getRank(newInputLoc);
            mapOldToNewOutputsForAgg.put(mapOldToNewOutputsForAgg.size(), newOutputLoc);
        }
        int newGroupCount = newAgg.getGroupCount();
        for (int i = 0; i < rel.getAggCallList().size(); i++) {
            mapOldToNewOutputsForAgg.put(mapOldToNewOutputsForAgg.size(), newGroupCount + i);
        }
        for (Map.Entry<CorDef, Integer> entry : frame.corDefOutputs.entrySet()) {
            int newInputLoc = entry.getValue();
            int newOutputLoc = ranker.getRank(newInputLoc);
            corDefOutputsForAgg.put(entry.getKey(), newOutputLoc);
        }

        // with group by, return directly
        if (!withoutGroupBy || !decorrelating(unnest)) {
            return register(rel, newAgg, mapOldToNewOutputsForAgg, corDefOutputsForAgg);
        }

        //    project (corr from left + agg function)
        //        |
        //        |
        //    left join
        //      /    \
        //     /      \
        //  dominate  agg(corr + agg function)
        //             |
        //          input

        // build left join
        Frame dominateFrame = buildDominatingSet(unnest);
        RelNode dominate = dominateFrame.r;
        int leftFieldCount = dominate.getRowType().getFieldCount();
        List<RexNode> conditions = Lists.newArrayList();
        List<RelDataTypeField> newRightFieldList = newAgg.getRowType().getFieldList();
        for (Map.Entry<CorDef, Integer> entry : dominateFrame.corDefOutputs.entrySet()) {
            int newLeftPos = entry.getValue();
            int newRightPos = corDefOutputsForAgg.get(entry.getKey());
            conditions.add(rexBuilder.makeCall(
                SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                RexInputRef.of(newLeftPos, dominate.getRowType()),
                new RexInputRef(leftFieldCount + newRightPos, newRightFieldList.get(newRightPos).getType())));
        }
        RelNode node = relBuilder.push(dominate).push(newAgg).join(JoinRelType.LEFT, conditions).build();

        // build project
        final Map<Integer, Integer> mapOldToNewOutputsForProject = Maps.newHashMap();
        final NavigableMap<CorDef, Integer> corDefOutputsForProject = Maps.newTreeMap();
        RelDataType joinedRowType = node.getRowType();
        List<RexNode> projects = Lists.newArrayList();
        // add correlated column
        for (Map.Entry<CorDef, Integer> entry : dominateFrame.corDefOutputs.entrySet()) {
            corDefOutputsForProject.put(entry.getKey(), projects.size());
            projects.add(RexInputRef.of(entry.getValue(), joinedRowType));
        }

        // add agg column
        for (int i = 0, cnt = rel.getGroupCount(); i < rel.getAggCallList().size(); i++, cnt++) {
            int projectLoc = mapOldToNewOutputsForAgg.get(cnt) + leftFieldCount;
            mapOldToNewOutputsForProject.put(cnt, projects.size());
            if (rel.getAggCallList().get(i).getAggregation().getKind() != SqlKind.COUNT) {
                projects.add(RexInputRef.of(projectLoc, joinedRowType));
            } else {
                RexNode condition = rexBuilder.makeCall(SqlStdOperatorTable.IS_NULL,
                    RexInputRef.of(projectLoc, joinedRowType));
                RexNode thenClause = rexBuilder.makeBigIntLiteral(0L);
                RexNode elseClause = RexInputRef.of(projectLoc, joinedRowType);
                projects.add(rexBuilder.makeCall(SqlStdOperatorTable.CASE, condition, thenClause, elseClause));
            }
        }

        RelNode finalNode = relBuilder.push(node).project(projects).build();
        return register(rel, finalNode, mapOldToNewOutputsForProject, corDefOutputsForProject);
    }

    public Frame decorrelateRel(LogicalWindow rel, Unnesting unnest, Set<RelNode> accessing) {
        removeRel(accessing, rel);
        final RelNode oldInput = rel.getInput();
        int oldInputColumn = oldInput.getRowType().getFieldCount();
        // change column ref for window
        if (decorrelating(unnest)) {
            unnest.getEquality().shiftColRef(identityMap(oldInputColumn));
        }

        Frame frame = getInvoke(oldInput, unnest, accessing);
        int newInputColumn = frame.r.getRowType().getFieldCount();
        final Map<Integer, Integer> mapOldToNewOutputs = Maps.newHashMap(frame.oldToNewOutputs);
        final NavigableMap<CorDef, Integer> corDefOutputs = Maps.newTreeMap(frame.corDefOutputs);

        final RexShuttle indexAdjustment = new RexShuttle() {
            @Override
            public RexNode visitInputRef(RexInputRef inputRef) {
                final int newIndex =
                    getAdjustedIndexForWindow(inputRef.getIndex(), frame, newInputColumn, oldInputColumn);
                return new RexInputRef(newIndex, inputRef.getType());
            }

            @Override
            public RexNode visitCall(final RexCall call) {
                if (call instanceof Window.RexWinAggCall) {
                    boolean[] update = {false};
                    final List<RexNode> clonedOperands = visitList(call.operands, update);
                    if (update[0]) {
                        return new Window.RexWinAggCall(
                            (SqlAggFunction) call.getOperator(),
                            call.getType(),
                            clonedOperands,
                            ((Window.RexWinAggCall) call).ordinal,
                            ((Window.RexWinAggCall) call).distinct);
                    } else {
                        return call;
                    }
                } else {
                    return super.visitCall(call);
                }
            }
        };

        // build new window
        final List<Window.Group> groups = Lists.newArrayList();
        final RelDataTypeFactory.Builder outputBuilder = rel.getCluster().getTypeFactory().builder();
        frame.r.getRowType().getFieldList().forEach(outputBuilder::add);

        for (int i = 0, cnt = 0; i < rel.groups.size(); i++) {
            Window.Group windowGroup = rel.groups.get(i);
            // Adjust keys
            ImmutableBitSet oldGroupSet = windowGroup.keys;
            ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
            for (int loc = oldGroupSet.nextSetBit(0); loc >= 0; loc = oldGroupSet.nextSetBit(loc + 1)) {
                builder.set(getAdjustedIndexForWindow(loc, frame, newInputColumn, oldInputColumn));
            }
            frame.corDefOutputs.values().forEach(builder::set);
            ImmutableBitSet newGroupSet = builder.build();

            // Adjust orderKeys
            final List<RelFieldCollation> newOrderKeys = new ArrayList<>();
            for (RelFieldCollation relFieldCollation : windowGroup.orderKeys.getFieldCollations()) {
                final int index = relFieldCollation.getFieldIndex();
                newOrderKeys.add(relFieldCollation.copy(
                    getAdjustedIndexForWindow(index, frame, newInputColumn, oldInputColumn)));
            }

            // Adjust Window Functions
            List<Window.RexWinAggCall> newAggCalls = Lists.newArrayList();
            for (Window.RexWinAggCall aggCall : windowGroup.aggCalls) {
                newAggCalls.add((Window.RexWinAggCall) aggCall.accept(indexAdjustment));
                mapOldToNewOutputs.put(oldInputColumn + cnt, newInputColumn + cnt);
                outputBuilder.add(rel.getRowType().getFieldList().get(oldInputColumn + cnt));
                cnt++;
            }
            groups.add(new Window.Group(newGroupSet, windowGroup.isRows, windowGroup.lowerBound,
                windowGroup.upperBound, RelCollations.of(newOrderKeys), newAggCalls));
        }
        final LogicalWindow newLogicalWindow =
            LogicalWindow.create(rel.getTraitSet().replace(RelCollations.EMPTY), frame.r,
                rel.constants, outputBuilder.build(), groups);
        return register(rel, newLogicalWindow, mapOldToNewOutputs, corDefOutputs);
    }

    public Frame decorrelateRel(LogicalColCorrelate rel, Unnesting unnest, Set<RelNode> accessing) {
        final RelNode oldLeft = rel.getInput(0);
        final RelNode oldRight = rel.getInput(1);
        final Frame leftFrame;
        final Frame rightFrame;

        // unnest left
        Set<RelNode> accLeft = Sets.newHashSet();
        if (accessing != null) {
            accessing.stream().filter(r -> subtreeChecker.isInSubtree(r, oldLeft)).forEach(accLeft::add);
        }
        leftFrame = getInvoke(oldLeft, unnest, accLeft);

        UnnestingInfo newInfo = new UnnestingInfo(rel, unnest, cteContext.nextCteId());
        Unnesting newUnnest = new Unnesting(newInfo);
        Set<RelNode> acc = Sets.newHashSet(cm.getAccessing(rel));
        if (accessing != null) {
            accessing.stream().filter(r -> subtreeChecker.isInSubtree(r, oldRight)).forEach(acc::add);
        }
        rightFrame = getInvoke(oldRight, newUnnest, acc);

        RelNode leftNode = leftFrame.r;
        // wrap left node with CTEConsumer if enable cte reuse
        Integer cteId = newInfo.getCteId();
        if (cteId != null) {
            RelNode copiedLeft = CBOUtil.RelCopied.copy(leftNode);
            if (copiedLeft == null) {
                throw new TddlRuntimeException(ErrorCode.ERR_OPTIMIZER, "failed to copy relNode during decorrelation");
            }
            leftNode = new LogicalCTEConsumer(leftNode.getCluster(),
                leftNode.getTraitSet(), copiedLeft, cteId, cteContext.getNextCteConsumerSn(cteId),
                leftNode.getRowType());
            cteContext.registerCteConsumer((LogicalCTEConsumer) leftNode);
        }
        // Change correlate rel into a join.
        // Join all the correlated variables produced by this correlate rel
        // with the values generated and propagated from the right input
        final List<RexNode> conditions = new ArrayList<>();
        final List<RelDataTypeField> newLeftOutput = leftFrame.r.getRowType().getFieldList();
        int newLeftFieldCount = newLeftOutput.size();
        final List<RelDataTypeField> newRightOutput = rightFrame.r.getRowType().getFieldList();
        for (Map.Entry<CorDef, Integer> rightOutput : rightFrame.corDefOutputs.entrySet()) {
            final CorDef corDef = rightOutput.getKey();
            final int newLeftPos;
            if (corDef.corr.equals(rel.getCorrelationId())) {
                newLeftPos = leftFrame.oldToNewOutputs.get(corDef.field);
            } else {
                newLeftPos = leftFrame.corDefOutputs.get(corDef);
            }
            final int newRightPos = rightOutput.getValue();

            // Using `equals` instead of `IS NOT DISTINCT FROM` is an optimization
            // for non-nullable fields. However, `IS NOT DISTINCT FROM` is always
            // the correct choice in all cases.
            conditions.add(rexBuilder.makeCall(
                isFieldNotNull(rightFrame.r, newRightPos)
                    ? SqlStdOperatorTable.EQUALS : SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                RexInputRef.of(newLeftPos, newLeftOutput),
                new RexInputRef(newLeftFieldCount + newRightPos,
                    newRightOutput.get(newRightPos).getType())));
        }
        RelNode newJoin;
        if (rel.getJoinType() == SemiJoinType.SEMI || rel.getJoinType() == SemiJoinType.ANTI) {
            newJoin = relBuilder.push(leftNode).push(rightFrame.r).logicalSemiJoin(conditions,
                    rel.getJoinType().toJoinType(), new SqlNodeList(SqlParserPos.ZERO), false)
                .build();
        } else {
            newJoin = relBuilder.push(leftNode).push(rightFrame.r).join(rel.getJoinType().toJoinType(), conditions)
                .build();
        }

        final Map<Integer, Integer> mapOldToNewOutputs = new HashMap<>();
        final NavigableMap<CorDef, Integer> corDefOutputs = new TreeMap<>();
        List<RexNode> projects = Lists.newArrayList();
        for (Map.Entry<Integer, Integer> entry : leftFrame.oldToNewOutputs.entrySet()) {
            mapOldToNewOutputs.put(entry.getKey(), projects.size());
            projects.add(RexInputRef.of(entry.getValue(), newJoin.getRowType()));
        }
        if (!(rel.getJoinType() == SemiJoinType.SEMI || rel.getJoinType() == SemiJoinType.ANTI)) {
            int oldLeftFieldCount = oldLeft.getRowType().getFieldCount();
            for (Map.Entry<Integer, Integer> entry : rightFrame.oldToNewOutputs.entrySet()) {
                mapOldToNewOutputs.put(entry.getKey() + oldLeftFieldCount, projects.size());
                projects.add(RexInputRef.of(entry.getValue() + newLeftFieldCount, newJoin.getRowType()));
            }
        }
        if (decorrelating(unnest)) {
            CorrelationId target = rel.getCorrelationId();
            for (Map.Entry<CorDef, Integer> entry : leftFrame.corDefOutputs.entrySet()) {
                final CorDef corDef = entry.getKey();
                if (!corDef.corr.equals(target)) {
                    projects.add(RexInputRef.of(entry.getValue(), newJoin.getRowType()));
                    corDefOutputs.put(corDef, projects.size() - 1);
                }
            }
        }

        RelNode finalNode = relBuilder.push(newJoin).project(projects).build();
        if (cteId != null) {
            // wrap final node with CTEAnchor and CTEProducer
            LogicalCTEProducer producer = new LogicalCTEProducer(leftFrame.r.getCluster(),
                leftFrame.r.getTraitSet(), leftFrame.r, cteId, leftFrame.r.getRowType());
            cteContext.registerCteProducer(producer);
            finalNode = new LogicalCTEAnchor(finalNode.getCluster(), finalNode.getTraitSet(),
                producer, finalNode, cteId, finalNode.getRowType());
        }
        return register(rel, finalNode, mapOldToNewOutputs, corDefOutputs);
    }

    public Frame decorrelateRel(LogicalUnion rel, Unnesting unnest, Set<RelNode> accessing) {
        removeRel(accessing, rel);
        List<Set<RelNode>> childAcc = childAccessingSplit(accessing, rel);
        Frame firstFrame = getInvoke(rel.getInput(0), Unnesting.copy(unnest), childAcc.get(0));
        List<RelNode> newInputs = Lists.newArrayList();
        newInputs.add(firstFrame.r);
        final Map<Integer, Integer> oldToNewOutputs = firstFrame.oldToNewOutputs;
        final NavigableMap<CorDef, Integer> corDefOutputs = firstFrame.corDefOutputs;
        for (int i = 1; i < rel.getInputs().size(); i++) {
            Frame childFrame = getInvoke(rel.getInput(i), Unnesting.copy(unnest), childAcc.get(i));
            RelNode child = childFrame.r;
            // project columns to match fist frame
            Map<Integer, Integer> oldToNewOutputsForProject = Maps.newTreeMap();
            for (Map.Entry<Integer, Integer> entry : oldToNewOutputs.entrySet()) {
                oldToNewOutputsForProject.put(entry.getValue(), childFrame.oldToNewOutputs.get(entry.getKey()));
            }
            for (Map.Entry<CorDef, Integer> entry : corDefOutputs.entrySet()) {
                oldToNewOutputsForProject.put(entry.getValue(), childFrame.corDefOutputs.get(entry.getKey()));
            }
            List<RexNode> projects = Lists.newArrayList();
            for (Map.Entry<Integer, Integer> entry : oldToNewOutputsForProject.entrySet()) {
                projects.add(RexInputRef.of(entry.getValue(), child.getRowType()));
            }
            newInputs.add(relBuilder.push(childFrame.r).project(projects).build());
        }
        final RelNode finalNode = rel.copy(rel.getTraitSet(), newInputs, rel.all);
        return register(rel, finalNode, oldToNewOutputs, corDefOutputs);
    }

    public Frame decorrelateRel(LogicalJoin rel, Unnesting unnest, Set<RelNode> accessing) {
        final RelNode oldLeft = rel.getInput(0);
        final RelNode oldRight = rel.getInput(1);
        final Frame leftFrame;
        final Frame rightFrame;
        Unnesting unnestLeft = null;
        Unnesting unnestRight = null;
        boolean leftPushed = false;
        // build accessing list
        List<Set<RelNode>> childAcc = childAccessingSplit(accessing, rel);
        Set<RelNode> accLeft = childAcc.get(0);
        Set<RelNode> accRight = childAcc.get(1);
        int oldLeftFieldCount = oldLeft.getRowType().getFieldCount();

        // build new unnest
        List<RexNode> conditions = Lists.newArrayList();
        if (decorrelating(unnest)) {
            for (RexNode condition : RelOptUtil.conjunctions(rel.getCondition())) {
                if (condition instanceof RexCall &&
                    (condition.getKind() == SqlKind.EQUALS || condition.getKind() == SqlKind.IS_NOT_DISTINCT_FROM)) {
                    unnest.getEquality().union(((RexCall) condition).getOperands().get(0),
                        ((RexCall) condition).getOperands().get(1));
                }
            }
            unnestLeft = new Unnesting(unnest.info, unnest.equality.copy());
            unnestLeft.getEquality().shiftColRef(identityMap(oldLeftFieldCount));

            unnestRight = new Unnesting(unnest.info, unnest.equality.copy());
            Map<Integer, Integer> shift = Maps.newHashMap();
            for (int i = 0; i < oldRight.getRowType().getFieldCount(); i++) {
                shift.put(i + oldLeftFieldCount, i);
            }
            unnestRight.getEquality().shiftColRef(shift);
        }

        if (CollectionUtils.isEmpty(accRight) && !rel.getJoinType().generatesNullsOnLeft()) {
            leftFrame = getInvoke(oldLeft, unnestLeft, accLeft);
            rightFrame = getInvoke(oldRight, null, null);
            leftPushed = true;
        } else if (CollectionUtils.isEmpty(accLeft) && !rel.getJoinType().generatesNullsOnRight()) {
            leftFrame = getInvoke(oldLeft, null, null);
            rightFrame = getInvoke(oldRight, unnestRight, accRight);
        } else {
            leftFrame = getInvoke(oldLeft, unnestLeft, accLeft);
            rightFrame = getInvoke(oldRight, unnestRight, accRight);
            leftPushed = true;
            int newLeftFieldCount = leftFrame.r.getRowType().getFieldCount();
            for (CorDef corDef : unnest.getInfo().outerRefs) {
                int newRightPos = rightFrame.corDefOutputs.get(corDef);
                conditions.add(rexBuilder.makeCall(
                    SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                    RexInputRef.of(leftFrame.corDefOutputs.get(corDef), leftFrame.r.getRowType()),
                    new RexInputRef(newLeftFieldCount + newRightPos,
                        rightFrame.r.getRowType().getFieldList().get(newRightPos).getType())));
            }
        }
        conditions.add(decorrelateExpr(rel, map, rel.getCondition()));
        RelNode newJoin = rel.copy(rel.getTraitSet(), RexUtil.composeConjunction(rexBuilder, conditions, false),
            leftFrame.r, rightFrame.r, rel.getJoinType(), rel.isSemiJoinDone());
        RelDataType joinedRowType = newJoin.getRowType();
        final Map<Integer, Integer> mapOldToNewOutputs = new HashMap<>();
        final NavigableMap<CorDef, Integer> corDefOutputs = new TreeMap<>();
        List<RexNode> projects = Lists.newArrayList();
        int newLeftFieldCount = leftFrame.r.getRowType().getFieldCount();
        for (Map.Entry<Integer, Integer> entry : leftFrame.oldToNewOutputs.entrySet()) {
            mapOldToNewOutputs.put(entry.getKey(), projects.size());
            projects.add(RexInputRef.of(entry.getValue(), joinedRowType));
        }
        for (Map.Entry<Integer, Integer> entry : rightFrame.oldToNewOutputs.entrySet()) {
            mapOldToNewOutputs.put(entry.getKey() + oldLeftFieldCount, projects.size());
            projects.add(RexInputRef.of(entry.getValue() + newLeftFieldCount, joinedRowType));
        }
        if (decorrelating(unnest)) {
            for (CorDef corDef : unnest.getInfo().outerRefs) {
                corDefOutputs.put(corDef, projects.size());
                if (leftPushed && !rel.getJoinType().generatesNullsOnLeft()) {
                    projects.add(RexInputRef.of(leftFrame.corDefOutputs.get(corDef), joinedRowType));
                } else {
                    projects.add(
                        RexInputRef.of(rightFrame.corDefOutputs.get(corDef) + newLeftFieldCount, joinedRowType));
                }
            }
        }
        RelNode finalNode = relBuilder.push(newJoin).project(projects).build();
        return register(rel, finalNode, mapOldToNewOutputs, corDefOutputs);
    }

    public Frame decorrelateRel(LogicalSemiJoin rel, Unnesting unnest, Set<RelNode> accessing) {
        final RelNode oldLeft = rel.getInput(0);
        final RelNode oldRight = rel.getInput(1);
        final Frame leftFrame;
        final Frame rightFrame;
        Unnesting unnestLeft = null;
        Unnesting unnestRight = null;

        List<Set<RelNode>> childAcc = childAccessingSplit(accessing, rel);
        Set<RelNode> accLeft = childAcc.get(0);
        Set<RelNode> accRight = childAcc.get(1);

        // build new unnest
        List<RexNode> conditions = Lists.newArrayList();
        if (decorrelating(unnest)) {
            for (RexNode condition : RelOptUtil.conjunctions(rel.getCondition())) {
                if (condition instanceof RexCall &&
                    (condition.getKind() == SqlKind.EQUALS
                        || condition.getKind() == SqlKind.IS_NOT_DISTINCT_FROM)) {
                    unnest.getEquality().union(((RexCall) condition).getOperands().get(0),
                        ((RexCall) condition).getOperands().get(1));
                }
            }
            int oldLeftFieldCount = oldLeft.getRowType().getFieldCount();
            unnestLeft = new Unnesting(unnest.info, unnest.equality.copy());
            unnestLeft.getEquality().shiftColRef(identityMap(oldLeftFieldCount));

            unnestRight = new Unnesting(unnest.info, unnest.equality.copy());
            Map<Integer, Integer> shift = Maps.newHashMap();
            for (int i = 0; i < oldRight.getRowType().getFieldCount(); i++) {
                shift.put(i + oldLeftFieldCount, i);
            }
            unnestRight.getEquality().shiftColRef(shift);
        }

        if (CollectionUtils.isEmpty(accRight)) {
            leftFrame = getInvoke(oldLeft, unnestLeft, accLeft);
            rightFrame = getInvoke(oldRight, null, null);
        } else {
            leftFrame = getInvoke(oldLeft, unnestLeft, accLeft);
            rightFrame = getInvoke(oldRight, unnestRight, accRight);
            for (CorDef corDef : unnest.getInfo().outerRefs) {
                int newRightPos = rightFrame.corDefOutputs.get(corDef);
                conditions.add(rexBuilder.makeCall(
                    SqlStdOperatorTable.IS_NOT_DISTINCT_FROM,
                    RexInputRef.of(leftFrame.corDefOutputs.get(corDef), leftFrame.r.getRowType()),
                    new RexInputRef(leftFrame.r.getRowType().getFieldCount() + newRightPos,
                        rightFrame.r.getRowType().getFieldList().get(newRightPos).getType())));
            }
        }
        conditions.add(decorrelateExpr(rel, map, rel.getCondition()));
        RelNode newJoin = rel.copy(rel.getTraitSet(), RexUtil.composeConjunction(rexBuilder, conditions, false),
            leftFrame.r, rightFrame.r, rel.getJoinType(), rel.isSemiJoinDone());
        final Map<Integer, Integer> mapOldToNewOutputs = new HashMap<>();
        final NavigableMap<CorDef, Integer> corDefOutputs = new TreeMap<>();
        RelNode finalNode = originColumnFirst(unnest, leftFrame, newJoin, mapOldToNewOutputs, corDefOutputs);
        return register(rel, finalNode, mapOldToNewOutputs, corDefOutputs);
    }

    public Frame decorrelateRel(LogicalCTEAnchor rel, Unnesting unnest, Set<RelNode> accessing) {
        removeRel(accessing, rel);
        final RelNode oldLeft = rel.getLeft();
        final RelNode oldRight = rel.getRight();
        final Frame leftFrame;
        final Frame rightFrame;

        Unnesting unnestLeft = null;
        Unnesting unnestRight = unnest;
        List<Set<RelNode>> childAcc = childAccessingSplit(accessing, rel);
        Set<RelNode> accLeft = childAcc.get(0);
        Set<RelNode> accRight = childAcc.get(1);

        if (decorrelating(unnest)) {
            unnestLeft = new Unnesting(unnest.info, unnest.equality.copy());
            unnestLeft.getEquality().shiftColRef(Maps.newHashMap());
        }

        if (CollectionUtils.isEmpty(accLeft)) {
            rightFrame = getInvoke(oldRight, unnestRight, accRight);
            leftFrame = getInvoke(oldLeft, null, null);
        } else {
            rightFrame = getInvoke(oldRight, unnestRight, accRight);
            leftFrame = getInvoke(oldLeft, unnestLeft, accLeft);
        }

        LogicalCTEAnchor newRel = new LogicalCTEAnchor(rel.getCluster(), rel.getTraitSet(), leftFrame.r, rightFrame.r,
            rel.getCteId(), rightFrame.r.getRowType());
        return register(rel, newRel, rightFrame.oldToNewOutputs, rightFrame.corDefOutputs);
    }

    public Frame decorrelateRel(LogicalCTEProducer rel, Unnesting unnest, Set<RelNode> accessing) {
        removeRel(accessing, rel);
        final RelNode oldInput = rel.getInput();
        Frame frame = getInvoke(oldInput, unnest, accessing);
        final Map<Integer, Integer> mapOldToNewOutputs = new HashMap<>();
        final NavigableMap<CorDef, Integer> corDefOutputs = new TreeMap<>();
        RelNode newProject = originColumnFirst(unnest, frame, frame.r, mapOldToNewOutputs, corDefOutputs);

        LogicalCTEProducer newRel = new LogicalCTEProducer(rel.getCluster(), rel.getTraitSet(),
            newProject, rel.getCteId(), newProject.getRowType());
        return register(rel, newRel, mapOldToNewOutputs, corDefOutputs);
    }

    public Frame decorrelateRel(LogicalCTEConsumer rel, Unnesting unnest, Set<RelNode> accessing) {
        removeRel(accessing, rel);
        final RelNode oldInput = rel.getInnerRel();

        if (decorrelating(unnest)) {
            cteIdUnnestMap.computeIfAbsent(rel.getCteId(), (x) -> new Unnesting(unnest.info, unnest.equality.copy()));
        }
        Frame frame = getInvoke(oldInput, unnest, accessing);
        final Map<Integer, Integer> mapOldToNewOutputs = new HashMap<>();
        final NavigableMap<CorDef, Integer> corDefOutputs = new TreeMap<>();
        RelNode newProject = originColumnFirst(unnest, frame, frame.r, mapOldToNewOutputs, corDefOutputs);

        LogicalCTEConsumer newRel = rel.copy(rel.getTraitSet(), newProject);
        return register(rel, newRel, frame.oldToNewOutputs, frame.corDefOutputs);
    }

    public RelNode originColumnFirst(Unnesting unnest,
                                     Frame frame,
                                     RelNode newRel,
                                     Map<Integer, Integer> mapOldToNewOutputs,
                                     NavigableMap<CorDef, Integer> corDefOutputs) {
        List<RexNode> projects = Lists.newArrayList();
        RelDataType frameRowtype = newRel.getRowType();
        for (Map.Entry<Integer, Integer> entry : frame.oldToNewOutputs.entrySet()) {
            mapOldToNewOutputs.put(entry.getKey(), projects.size());
            projects.add(RexInputRef.of(entry.getValue(), frameRowtype));
        }
        if (decorrelating(unnest)) {
            for (CorDef corDef : unnest.getInfo().outerRefs) {
                corDefOutputs.put(corDef, projects.size());
                projects.add(RexInputRef.of(frame.corDefOutputs.get(corDef), frameRowtype));
            }
        }
        return relBuilder.push(newRel).project(projects).build();
    }

    List<Set<RelNode>> childAccessingSplit(Set<RelNode> accessing, RelNode r) {
        List<Set<RelNode>> split = Lists.newArrayList();
        for (RelNode child : r.getInputs()) {
            if (CollectionUtils.isEmpty(accessing)) {
                split.add(null);
                continue;
            }
            Set<RelNode> childAcc = Sets.newHashSet();
            for (RelNode acc : accessing) {
                if (subtreeChecker.isInSubtree(acc, child)) {
                    childAcc.add(acc);
                }
            }
            split.add(childAcc);
        }
        return split;
    }

    private int getAdjustedIndexForWindow(final int initIndex,
                                          final Frame frame, final int newInputColumn, final int oldInputColumn) {
        if (initIndex >= oldInputColumn) {
            return newInputColumn + (initIndex - oldInputColumn);
        } else {
            return frame.oldToNewOutputs.get(initIndex);
        }
    }

    private Frame getOrCreateFrame(RelNode r) {
        final Frame frame = getFrame(r);
        if (frame == null) {
            return new Frame(r, ImmutableSortedMap.of(),
                identityMap(r.getRowType().getFieldCount()));
        }
        return frame;
    }

    private Frame getFrame(RelNode r) {
        return map.get(r);
    }

    private boolean decorrelating(Unnesting unnesting) {
        return unnesting != null;
    }

    private void removeRel(Set<RelNode> accessing, RelNode rel) {
        if (accessing != null) {
            accessing.remove(rel);
        }
    }

    private boolean shouldPushCorrelate(Unnesting unnesting, Set<RelNode> accessing) {
        return unnesting != null && CollectionUtils.isNotEmpty(accessing);
    }

    private static RexInputRef getNewForOldInputRef(RelNode currentRel,
                                                    Map<RelNode, Frame> map,
                                                    RexInputRef oldInputRef) {
        requireNonNull(currentRel, "currentRel");

        int oldOrdinal = oldInputRef.getIndex();
        int newOrdinal = 0;

        // determine which input rel oldOrdinal references, and adjust
        // oldOrdinal to be relative to that input rel
        RelNode oldInput = null;

        for (RelNode oldInput0 : currentRel.getInputs()) {
            RelDataType oldInputType = oldInput0.getRowType();
            int n = oldInputType.getFieldCount();
            if (oldOrdinal < n) {
                oldInput = oldInput0;
                break;
            }
            RelNode newInput =
                requireNonNull(map.get(oldInput0),
                    () -> "map.get(oldInput0) for " + oldInput0).r;
            newOrdinal += newInput.getRowType().getFieldCount();
            oldOrdinal -= n;
        }

        requireNonNull(oldInput, "oldInput");
        final Frame frame = requireNonNull(map.get(oldInput));

        // now oldOrdinal is relative to oldInput
        int oldLocalOrdinal = oldOrdinal;

        // figure out the newLocalOrdinal, relative to the newInput.
        int newLocalOrdinal = oldLocalOrdinal;

        if (!frame.oldToNewOutputs.isEmpty()) {
            newLocalOrdinal = requireNonNull(frame.oldToNewOutputs.get(oldLocalOrdinal));
        }

        newOrdinal += newLocalOrdinal;

        return new RexInputRef(newOrdinal,
            frame.r.getRowType().getFieldList().get(newLocalOrdinal).getType());
    }

    /* Returns an immutable map with the identity [0: 0, .., count-1: count-1]. */
    static Map<Integer, Integer> identityMap(int count) {
        ImmutableMap.Builder<Integer, Integer> builder = ImmutableMap.builder();
        for (int i = 0; i < count; i++) {
            builder.put(i, i);
        }
        return builder.build();
    }

    static private boolean isIdentityMap(Map<Integer, Integer> map) {
        if (map == null) {
            return false;
        }
        for (int i = 0; i < map.size(); i++) {
            Integer val = map.get(i);
            if (val == null || val != i) {
                return false;
            }
        }
        return true;
    }

    /**
     * Registers a relational expression and the relational expression it became
     * after decorrelation.
     */
    Frame register(RelNode rel, RelNode newRel,
                   Map<Integer, Integer> oldToNewOutputs,
                   NavigableMap<CorDef, Integer> corDefOutputs) {
        final Frame frame = new Frame(newRel, corDefOutputs, oldToNewOutputs);
        map.put(rel, frame);
        return frame;
    }

    //~ Inner Classes ----------------------------------------------------------

    /**
     * Shuttle that decorrelates.
     */
    private static class DecorrelateRexShuttle extends RexShuttle {
        private final RelNode currentRel;
        private final Map<RelNode, Frame> map;

        private DecorrelateRexShuttle(RelNode currentRel,
                                      Map<RelNode, Frame> map) {
            this.currentRel = requireNonNull(currentRel, "currentRel");
            this.map = requireNonNull(map, "map");
        }

        @Override
        public RexNode visitFieldAccess(RexFieldAccess fieldAccess) {
            final RexNode ref = fieldAccess.getReferenceExpr();
            if (!(ref instanceof RexCorrelVariable)) {
                return fieldAccess;
            }
            final RexCorrelVariable corVar = (RexCorrelVariable) ref;
            int newInputOutputOffset = 0;
            for (RelNode input : currentRel.getInputs()) {
                final Frame frame = map.get(input);
                if (frame != null) {
                    // try to find in this input rel the position of corVar
                    CorDef corDef = CorDef.create(corVar.getId(), fieldAccess.getField().getIndex());
                    Integer newInputPos = frame.corDefOutputs.get(corDef);
                    if (newInputPos != null) {
                        // This input does produce the corVar referenced.
                        return new RexInputRef(newInputPos + newInputOutputOffset,
                            frame.r.getRowType().getFieldList().get(newInputPos).getType());
                    }

                    // this input does not produce the corVar needed
                    newInputOutputOffset += frame.r.getRowType().getFieldCount();
                } else {
                    // this input is not rewritten
                    newInputOutputOffset += input.getRowType().getFieldCount();
                }
            }
            return fieldAccess;
        }

        @Override
        public RexNode visitInputRef(RexInputRef inputRef) {
            final RexInputRef ref = getNewForOldInputRef(currentRel, map, inputRef);
            if (ref.getIndex() == inputRef.getIndex() && ref.getType() == inputRef.getType()) {
                return inputRef; // re-use old object, to prevent needless expr cloning
            }
            return ref;
        }
    }

    static class BitSetRanker {
        private final int[] ranks;

        public BitSetRanker(ImmutableBitSet bitSet) {
            ranks = new int[bitSet.length()];
            int ser = -1;
            for (int i = 0; i < bitSet.length(); i++) {
                if (bitSet.get(i)) {
                    ser++;
                }
                ranks[i] = ser;
            }
        }

        public int getRank(int loc) {
            return ranks[loc];
        }
    }

    /**
     * A map of the locations of
     * {@link Correlate}
     * in a tree of {@link RelNode}s.
     *
     * <p>It is used to drive the decorrelation process.
     * Treat it as immutable; rebuild if you modify the tree.
     *
     * </ol>
     */
    protected static class CorelMap {
        private final Map<RelNode, Set<RelNode>> mapAccessing;

        private CorelMap(Map<RelNode, Set<CorDef>> mapRefRelToCorRef,
                         NavigableMap<CorrelationId, RelNode> mapCorToCorRel) {
            this.mapAccessing = Maps.newHashMap();
            for (Map.Entry<RelNode, Set<CorDef>> entry : mapRefRelToCorRef.entrySet()) {
                for (CorDef corDef : entry.getValue()) {
                    RelNode correlate = mapCorToCorRel.get(corDef.corr);
                    if (correlate != null) {
                        Set<RelNode> set = this.mapAccessing.computeIfAbsent(correlate, k -> Sets.newHashSet());
                        set.add(entry.getKey());
                    }
                }
            }
        }

        @Override
        public String toString() {
            return "accessing=" + mapAccessing;
        }

        @SuppressWarnings("UndefinedEquals")
        @Override
        public boolean equals(Object obj) {
            return obj == this
                || obj instanceof CorelMap
                && mapAccessing.equals(((CorelMap) obj).mapAccessing);
        }

        @Override
        public int hashCode() {
            return Objects.hash(mapAccessing);
        }

        public boolean hasCorrelation() {
            return !mapAccessing.isEmpty();
        }

        public Set<RelNode> getAccessing(RelNode node) {
            return mapAccessing.get(node);
        }

    }

    /**
     * Builds a {@link CorelMap}.
     */
    public static class CorelMapBuilder extends RelHomogeneousShuttle {
        final NavigableMap<CorrelationId, RelNode> mapCorToCorRel = Maps.newTreeMap();

        final Map<RelNode, Set<CorDef>> mapRefRelToCorDef = Maps.newHashMap();

        /**
         * Creates a CorelMap by iterating over a {@link RelNode} tree.
         */
        public CorelMap build(RelNode... rels) {
            for (RelNode rel : rels) {
                CBOUtil.stripHep(rel).accept(this);
            }
            return new CorelMap(mapRefRelToCorDef, mapCorToCorRel);
        }

        @Override
        public RelNode visit(RelNode other) {
            if (other instanceof Join) {
                Join join = (Join) other;
                try {
                    stack.push(join);
                    join.getCondition().accept(rexVisitor(join));
                } finally {
                    stack.pop();
                }
            } else if (other instanceof Correlate) {
                Correlate correlate = (Correlate) other;
                mapCorToCorRel.put(correlate.getCorrelationId(), correlate);
            } else if (other instanceof Filter) {
                Filter filter = (Filter) other;
                try {
                    stack.push(filter);
                    filter.getCondition().accept(rexVisitor(filter));
                } finally {
                    stack.pop();
                }
            } else if (other instanceof Project) {
                Project project = (Project) other;
                try {
                    stack.push(project);
                    for (RexNode node : project.getProjects()) {
                        node.accept(rexVisitor(project));
                    }
                } finally {
                    stack.pop();
                }
            }
            return super.visit(other);
        }

        @Override
        protected RelNode visitChild(RelNode parent, int i,
                                     RelNode input) {
            return super.visitChild(parent, i, CBOUtil.stripHep(input));
        }

        private RexVisitorImpl<Void> rexVisitor(final RelNode rel) {
            return new RexVisitorImpl<Void>(true) {
                @Override
                public Void visitFieldAccess(RexFieldAccess fieldAccess) {
                    final RexNode ref = fieldAccess.getReferenceExpr();
                    if (ref instanceof RexCorrelVariable) {
                        final RexCorrelVariable var = (RexCorrelVariable) ref;
                        Set<CorDef> corDefSet =
                            mapRefRelToCorDef.computeIfAbsent(rel, k -> new HashSet<>());
                        CorDef corDef = CorDef.create(var.getId(), fieldAccess.getField().getIndex());
                        corDefSet.add(corDef);
                    }
                    return super.visitFieldAccess(fieldAccess);
                }

                @Override
                public Void visitSubQuery(RexSubQuery subQuery) {
                    subQuery.rel.accept(CorelMapBuilder.this);
                    return super.visitSubQuery(subQuery);
                }
            };
        }
    }

    /**
     * Frame describing the relational expression after decorrelation
     * and where to find the output fields and correlation variables
     * among its output fields.
     */
    public static class Frame {
        final RelNode r;
        final ImmutableSortedMap<CorDef, Integer> corDefOutputs;
        final ImmutableSortedMap<Integer, Integer> oldToNewOutputs;

        Frame(RelNode r, NavigableMap<CorDef, Integer> corDefOutputs,
              Map<Integer, Integer> oldToNewOutputs) {
            this.r = requireNonNull(r, "r");
            this.corDefOutputs = ImmutableSortedMap.copyOf(corDefOutputs);
            this.oldToNewOutputs = ImmutableSortedMap.copyOf(oldToNewOutputs);
        }
    }

    /**
     * Check if the field at the given index is non-nullable.
     *
     * <p>This method performs a basic check for `null` values in the field. However, a
     * `false` result does not necessarily mean that the field contains `null` values.
     * It only guarantees that if the result is `true`, the field contains no `null` values.
     */
    private static boolean isFieldNotNull(RelNode rel, int index) {
        RelDataType type = rel.getRowType().getFieldList().get(index).getType();
        return !type.isNullable() || isFieldNotNullRecursive(rel, index);
    }

    private static boolean isFieldNotNullRecursive(RelNode rel, int index) {
        if (rel instanceof Project) {
            Project project = (Project) rel;

            RexNode expr = project.getProjects().get(index);
            if (!(expr instanceof RexInputRef)) {
                return false;
            }
            return isFieldNotNullRecursive(project.getInput(), ((RexInputRef) expr).getIndex());
        } else if (rel instanceof Aggregate) {
            Aggregate agg = (Aggregate) rel;
            ImmutableBitSet groupSet = agg.getGroupSet();

            if (index >= groupSet.size()) {
                return false;
            }
            return isFieldNotNullRecursive(agg.getInput(), groupSet.asList().get(index));
        } else if (rel instanceof Filter) {
            Filter filter = (Filter) rel;
            if (Strong.isNotTrue(filter.getCondition(), ImmutableBitSet.of(index))) {
                return true;
            }
            return isFieldNotNullRecursive(filter.getInput(), index);
        } else if (rel instanceof Join) {
            Join join = (Join) rel;
            int leftFieldCnt = join.getLeft().getRowType().getFieldCount();
            if (index < join.getLeft().getRowType().getFieldCount()) {
                if (!join.getJoinType().generatesNullsOnLeft()) {
                    return Strong.isNotTrue(join.getCondition(), ImmutableBitSet.of(index))
                        || isFieldNotNullRecursive(join.getLeft(), index);
                }
            } else {
                if (!join.getJoinType().generatesNullsOnRight()) {
                    return Strong.isNotTrue(join.getCondition(), ImmutableBitSet.of(index))
                        || isFieldNotNullRecursive(join.getRight(), index - leftFieldCnt);
                }
            }
            return false;
        } else {
            return false;
        }
    }

    // -------------------------------------------------------------------------
    //  Getter/Setter
    // -------------------------------------------------------------------------

    /**
     * Returns the {@code visitor} on which the {@code MethodDispatcher} dispatches
     * each {@code decorrelateRel} method, the default implementation returns this instance,
     * if you got a sub-class, override this method to replace the {@code visitor} as the
     * sub-class instance.
     */
    protected HolRelDecorrelator getVisitor() {
        return this;
    }
}
