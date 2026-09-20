package com.alibaba.polardbx.optimizer.core.planner.rule.util;

import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.LookupJoin;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.volcano.RelSubset;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.rel.RelNode;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

public class CheapestPlanReplacer {
    VolcanoPlanner planner;

    public CheapestPlanReplacer(VolcanoPlanner planner) {
        this.planner = planner;
    }

    public RelNode visit(
        RelNode p) {
        if (p instanceof RelSubset) {
            RelSubset subset = (RelSubset) p;
            RelNode cheapest = subset.getBest();

            if (cheapest == null) {
                // Dump the planner's expression pool so we can figure
                // out why we reached impasse.
                StringWriter sw = new StringWriter();
                final PrintWriter pw = new PrintWriter(sw);
                pw.println("Node [" + subset.getDescription()
                    + "] could not be implemented; planner state:\n");
                planner.dump(pw);
                pw.flush();
                final String dump = sw.toString();
                RuntimeException e =
                    new RelOptPlanner.CannotPlanException(dump);
                throw e;
            }
            p = cheapest;
        }

        if (p instanceof LogicalView) {
            LogicalView lv = (LogicalView) p;
            if (lv.isLookupTable()) {
                lv = lv.copy(p.getTraitSet());
                return lv;
            }
        }
        List<RelNode> oldInputs = p.getInputs();
        List<RelNode> inputs = new ArrayList<>();
        for (int i = 0; i < oldInputs.size(); i++) {
            RelNode oldInput = oldInputs.get(i);
            RelNode input = visit(oldInput);
            inputs.add(input);
        }
        if (!inputs.equals(oldInputs)) {
            p = p.copy(p.getTraitSet(), inputs);
        }
        if (p instanceof LookupJoin) {
            ((LookupJoin) p).deepVisitLookupJoin();
        }
        return p;
    }
}