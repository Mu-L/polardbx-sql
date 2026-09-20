package org.apache.calcite.rel.logical;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelInput;
import org.apache.calcite.rel.RelNode;

import java.util.List;

/**
 * @author pangzhaoxing
 */
public class LogicalHybridUnion extends LogicalUnion {

    public LogicalHybridUnion(RelOptCluster cluster, RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        super(cluster, traitSet, inputs, all);
    }

    /**
     * Creates a LogicalUnion by parsing serialized output.
     */
    public LogicalHybridUnion(RelInput input) {
        super(input);
    }


    /** Creates a LogicalUnion. */
    public static LogicalHybridUnion create(List<RelNode> inputs, boolean all) {
        final RelOptCluster cluster = inputs.get(0).getCluster();
        final RelTraitSet traitSet = cluster.traitSetOf(Convention.NONE);
        return new LogicalHybridUnion(cluster, traitSet, inputs, all);
    }

    //~ Methods ----------------------------------------------------------------

    public LogicalHybridUnion copy(
            RelTraitSet traitSet, List<RelNode> inputs, boolean all) {
        return new LogicalHybridUnion(getCluster(), traitSet, inputs, all);
    }
}
