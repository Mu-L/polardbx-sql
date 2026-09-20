package com.alibaba.polardbx.optimizer.core.planner.rule.holisticUnnest;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.cte.SubqueryAwareRelShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.util.Util;

public class UnnestUtil {

    /**
     * Check whether the given plan contains any sub-query in the ON condition of a join.
     * Traverses embedded sub-queries as well via {@link SubqueryAwareRelShuttle}.
     * Fast-fail: returns false directly when
     * {@link ConnectionParams#ENABLE_JOIN_SUBQUERY_UNNEST} is turned off (default).
     */
    public static boolean containsJoinConditionSubQuery(RelNode input, PlannerContext plannerContext) {
        if (input == null) {
            return false;
        }
        if (plannerContext == null) {
            return false;
        }
        if (!plannerContext.getParamManager().getBoolean(ConnectionParams.ENABLE_JOIN_SUBQUERY_UNNEST)) {
            return false;
        }
        SubqueryAwareRelShuttle shuttle = new SubqueryAwareRelShuttle() {
            @Override
            public RelNode visit(LogicalJoin join) {
                RexUtil.RexSubqueryListFinder finder = new RexUtil.RexSubqueryListFinder();
                join.getCondition().accept(finder);
                if (!finder.getSubQueries().isEmpty()) {
                    throw Util.FoundOne.NULL;
                }
                return super.visit(join);
            }
        };
        try {
            input.accept(shuttle);
            return false;
        } catch (Util.FoundOne e) {
            return true;
        }
    }
}
