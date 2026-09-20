package org.apache.calcite.rel.logical;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.Correlate;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.externalize.RelDrdsWriter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.SemiJoinType;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.ImmutableBitSet;

import static java.util.Objects.requireNonNull;

public class LogicalColCorrelate extends Correlate {
    //~ Instance fields --------------------------------------------------------

    //~ Constructors -----------------------------------------------------------

    /**
     * Creates a LogicalColCorrelate.
     *
     * @param cluster cluster this relational expression belongs to
     * @param left left input relational expression
     * @param right right input relational expression
     * @param correlationId variable name for the row of left input
     * @param requiredColumns Required columns
     * @param joinType join type
     */
    public LogicalColCorrelate(
        RelOptCluster cluster,
        RelTraitSet traitSet,
        RelNode left,
        RelNode right,
        CorrelationId correlationId,
        ImmutableBitSet requiredColumns,
        SemiJoinType joinType) {
        super(
            cluster,
            traitSet,
            left,
            right,
            correlationId,
            requiredColumns,
            joinType);
    }

    /**
     * Creates a LogicalColCorrelate by parsing serialized output.
     */
    public LogicalColCorrelate(RelInput input) {
        this(input.getCluster(), input.getTraitSet(),
            input.getInputs().get(0),
            input.getInputs().get(1),
            new CorrelationId(
                (Integer) requireNonNull(input.get("correlation"), "correlation")),
            input.getBitSet("requiredColumns"),
            requireNonNull(input.getEnum("joinType", SemiJoinType.class), "joinType"));
    }

    /**
     * Creates a LogicalColCorrelate.
     */
    public static LogicalColCorrelate create(RelNode left, RelNode right,
                                             CorrelationId correlationId, ImmutableBitSet requiredColumns,
                                             SemiJoinType joinType) {
        final RelOptCluster cluster = left.getCluster();
        final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
        return new LogicalColCorrelate(cluster, traitSet, left, right, correlationId,
            requiredColumns, joinType);
    }

    //~ Methods ----------------------------------------------------------------

    @Override
    public LogicalColCorrelate copy(RelTraitSet traitSet,
                                    RelNode left, RelNode right, CorrelationId correlationId,
                                    ImmutableBitSet requiredColumns, SemiJoinType joinType) {
        assert traitSet.containsIfApplicable(Convention.NONE);
        return new LogicalColCorrelate(getCluster(), traitSet, left, right,
            correlationId, requiredColumns, joinType);
    }

    @Override protected RelDataType deriveRowType() {
        switch (joinType) {
        case LEFT:
        case INNER:
            return SqlValidatorUtil.deriveJoinRowType(left.getRowType(),
                right.getRowType(), joinType.toJoinType(),
                getCluster().getTypeFactory(), null,
                ImmutableList.of());
        case ANTI:
        case SEMI:
            return left.getRowType();
        default:
            throw new IllegalStateException("Unknown join type " + joinType);
        }
    }

    @Override public RelWriter explainTermsForDisplay(RelWriter pw) {
        pw.item(RelDrdsWriter.REL_NAME, "ColCorrelate");
        pw.item("cor", correlationId);
        pw.item("type", joinType);
        return pw;
    }
    @Override
    public RelNode accept(RelShuttle shuttle) {
        return shuttle.visit(this);
    }
}
