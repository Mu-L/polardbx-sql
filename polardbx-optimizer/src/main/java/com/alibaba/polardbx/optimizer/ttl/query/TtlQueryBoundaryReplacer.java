package com.alibaba.polardbx.optimizer.ttl.query;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.ttl.TtlUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexNode;

public class TtlQueryBoundaryReplacer extends RelShuttleImpl {

    private final ExecutionContext ec;

    public TtlQueryBoundaryReplacer(ExecutionContext ec) {
        this.ec = ec;

    }

    @Override
    public RelNode visit(LogicalFilter filter) {
        RexNode condition = filter.getCondition();
        if (condition != null) {
            RexNode newCondition = condition.accept(new TtlQueryBoundaryRexReplacer(ec, filter.getCluster()));
            if (newCondition != condition) {
                return filter.copy(filter.getTraitSet(), filter.getInput(), newCondition);
            }
        }
        return super.visit(filter);
    }

    @Override
    public RelNode visit(TableScan scan) {
        if (scan instanceof LogicalView) {
            LogicalView logicalView = (LogicalView) scan;
            if (logicalView.getPushedRelNode() == null) {
                return scan;
            }
            RelNode newPushedNode = logicalView.getPushedRelNode().accept(this);
            if (newPushedNode != logicalView.getPushedRelNode()) {
                return ((LogicalView) scan).copy(scan.getTraitSet(), newPushedNode);
            }
        }
        return super.visit(scan);
    }
}
