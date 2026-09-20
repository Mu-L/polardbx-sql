package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.optimizer.core.DrdsConvention;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.LogicalExternalInsert;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelTraitSet;

/**
 * Convert rule for {@link LogicalExternalInsert}.
 * <p>
 * Mirrors the structure of {@link DrdsInsertConvertRule}: two static instances
 * cover the SMP ({@code DrdsConvention}) and columnar ({@code ColConvention})
 * pipelines respectively.
 */
public class DrdsExternalInsertConvertRule extends RelOptRule {

    public static final DrdsExternalInsertConvertRule SMP_INSTANCE =
        new DrdsExternalInsertConvertRule(DrdsConvention.INSTANCE);

    public static final DrdsExternalInsertConvertRule COL_INSTANCE =
        new DrdsExternalInsertConvertRule(CBOUtil.getColConvention());

    private final Convention outConvention;

    DrdsExternalInsertConvertRule(Convention outConvention) {
        super(operand(LogicalExternalInsert.class, Convention.NONE, any()),
            "DrdsExternalInsertConvertRule:" + outConvention.getName());
        this.outConvention = outConvention;
    }

    public Convention getOutConvention() {
        return outConvention;
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final LogicalExternalInsert insert = call.rel(0);
        RelTraitSet traitSet = insert.getTraitSet().simplify().replace(outConvention);
        call.transformTo(insert.copy(traitSet, convertList(insert.getInputs(), outConvention)));
    }
}
