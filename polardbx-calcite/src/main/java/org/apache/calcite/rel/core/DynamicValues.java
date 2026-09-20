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

package org.apache.calcite.rel.core;

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.AbstractRelNode;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlExplainLevel;

import java.util.List;

public class DynamicValues extends AbstractRelNode {

    /**
     * Raw-string execution mode of a single-tuple DynamicValues backed by RawString parameters.
     * COLUMN_ARRAY: one dynamic param per column, rows are produced by zipping the column arrays.
     * ROW_LIST: one bare dynamic param whose elements are row lists.
     */
    public enum RawStringMode {
        COLUMN_ARRAY,
        ROW_LIST
    }

    private static final Function<ImmutableList<RexNode>, Object> F =
        new Function<ImmutableList<RexNode>, Object>() {
            @Override
            public Object apply(ImmutableList<RexNode> tuple) {
                String s = tuple.toString();
                return "{ " + s.substring(1, s.length() - 1) + " }";
            }
        };

    public ImmutableList<ImmutableList<RexNode>> tuples;

    // Raw-string execution mode, maintained by the optimizer at every construction / conversion
    // site that may produce a raw-string shape. Never mutated after construction: SPM-cached
    // plans are shared static objects.
    private final RawStringMode rawStringMode;

    protected DynamicValues(RelOptCluster cluster, RelTraitSet traits, RelDataType rowType,
                            ImmutableList<ImmutableList<RexNode>> tuples) {
        this(cluster, traits, rowType, tuples, null);
    }

    protected DynamicValues(RelOptCluster cluster, RelTraitSet traits, RelDataType rowType,
                            ImmutableList<ImmutableList<RexNode>> tuples, RawStringMode rawStringMode) {
        super(cluster, traits);
        this.rowType = rowType;
        this.tuples = tuples;
        this.rawStringMode = rawStringMode;
    }

    public DynamicValues(RelInput input) {
        super(input.getCluster(), input.getTraitSet());
        this.rowType = input.getRowType("type");
        this.tuples = input.getDynamicTuples("tuples");
        // Absent in plans serialized by older versions; consumers re-detect via
        // LogicalDynamicValues.detectRawStringMode when needed.
        this.rawStringMode = input.getEnum("rawStringMode", RawStringMode.class, null);
    }

    public static DynamicValues create(RelOptCluster cluster, RelDataType rowType,
                                       ImmutableList<ImmutableList<RexNode>> tuples) {
        return new DynamicValues(cluster, cluster.traitSet(), rowType, tuples);
    }

    public static DynamicValues create(RelOptCluster cluster, RelTraitSet traits, RelDataType rowType,
                                       ImmutableList<ImmutableList<RexNode>> tuples) {
        return new DynamicValues(cluster, traits, rowType, tuples);
    }

    public static DynamicValues create(RelOptCluster cluster, RelTraitSet traits, RelDataType rowType,
                                       ImmutableList<ImmutableList<RexNode>> tuples, RawStringMode rawStringMode) {
        return new DynamicValues(cluster, traits, rowType, tuples, rawStringMode);
    }

    @Override
    protected RelDataType deriveRowType() {
        return rowType;
    }

    public ImmutableList<ImmutableList<RexNode>> getTuples() {
        return tuples;
    }

    /**
     * The raw-string mode maintained by the optimizer at construction / conversion time. Null
     * when the shape is not marked (or the plan was deserialized from an older version).
     */
    public RawStringMode getRawStringMode() {
        return rawStringMode;
    }

    @Override
    public RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        double dRows = mq.getRowCount(this);

        // Assume CPU is negligible since values are precomputed.
        double dCpu = 1;
        double dMemory = 0;
        double dIo = 0;
        double dNet = 0;
        return planner.getCostFactory().makeCost(dRows, dCpu, dMemory, dIo, dNet);
    }

    @Override
    public double estimateRowCount(RelMetadataQuery mq) {
        return tuples.size();
    }

    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }

    @Override
    public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "DynamicValues");
        pw.item("tuples", Lists.transform(tuples, F));
        if (rawStringMode != null) {
            pw.item("rawStringMode", rawStringMode.name());
        }
        return pw;
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
        RawStringMode mode = getRawStringMode();
        return super.explainTerms(pw)
            .itemIf("type", rowType,
                pw.getDetailLevel() == SqlExplainLevel.DIGEST_ATTRIBUTES)
            .itemIf("type", rowType.getFieldList(), true)
            .itemIf("tuples", tuples, true)
            // Serialized as a name string: some RelJson writers reject unknown enum types.
            .itemIf("rawStringMode", mode == null ? null : mode.name(), mode != null);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        assert inputs.isEmpty();
        return new DynamicValues(getCluster(), traitSet, rowType, tuples, rawStringMode);
    }
}
