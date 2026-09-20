package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.rel.Limit;
import com.alibaba.polardbx.optimizer.core.rel.MemSort;
import com.alibaba.polardbx.optimizer.core.rel.TopN;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;

public class SMPMergeLimitSortRule extends RelOptRule {

    public static final SMPMergeLimitSortRule INSTANCE = new SMPMergeLimitSortRule();

    public SMPMergeLimitSortRule() {
        super(operand(Limit.class, operand(MemSort.class, any())),
                "MergeLimitSortRule");
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        PlannerContext plannerContext = PlannerContext.getPlannerContext(call);
        if (!plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_MERGE_LIMIT_SORT)) {
            return false;
        }
        return super.matches(call);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        Limit limit = (Limit) call.rels[0];
        MemSort sort = (MemSort) call.rels[1];

        if (sort.withLimit() || !sort.withOrderBy()) {
            return;
        }
        if (!limit.withLimit() || limit.withOrderBy()) {
            return;
        }
        if (!limit.getTraitSet().equals(sort.getTraitSet())) {
            return;
        }

        call.transformTo(
                TopN.create(sort.getTraitSet(), sort.getInput(), sort.getCollation(), limit.offset, limit.fetch)
        );
    }
}
