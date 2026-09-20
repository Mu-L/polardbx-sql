package com.alibaba.polardbx.optimizer.core.planner.rule.cte;

import com.alibaba.polardbx.optimizer.core.rel.PhysicalCTEConsumer;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CTEAnchor;
import org.apache.calcite.rel.core.CTEConsumer;
import org.apache.calcite.rel.core.CTEProducer;
import org.apache.calcite.rel.logical.LogicalCTEConsumer;
import org.apache.calcite.util.Util;

public class CTEUtil {
    public static void collectCte(RelNode root, CTEContext cteContext) {
        if (root == null) {
            return;
        }
        if (!cteContext.isEnableCteReuse()) {
            return;
        }
        SubqueryAwareRelShuttle shuttle = new SubqueryAwareRelShuttle() {
            @Override
            public RelNode visit(LogicalCTEConsumer cteConsumer) {
                cteContext.registerCteConsumer(cteConsumer);
                return super.visit(cteConsumer);
            }

            @Override
            public RelNode visit(RelNode other) {
                if (other instanceof CTEProducer) {
                    cteContext.registerCteProducer((CTEProducer) other);
                } else if (other instanceof PhysicalCTEConsumer) {
                    cteContext.registerCteConsumer((CTEConsumer) other);
                }
                return visitChildren(other);
            }
        };
        root.accept(shuttle);
    }

    public static boolean findCte(RelNode root) {
        SubqueryAwareRelShuttle shuttle = new SubqueryAwareRelShuttle() {
            @Override
            public RelNode visit(LogicalCTEConsumer cteConsumer) {
                throw Util.FoundOne.NULL;
            }

            @Override
            public RelNode visit(RelNode other) {
                if (other instanceof CTEAnchor
                    || other instanceof CTEProducer
                    || other instanceof CTEConsumer) {
                    throw Util.FoundOne.NULL;
                }
                return visitChildren(other);
            }
        };
        try {
            root.accept(shuttle);
            return false;
        } catch (Util.FoundOne e) {
            return true;
        }
    }
}
