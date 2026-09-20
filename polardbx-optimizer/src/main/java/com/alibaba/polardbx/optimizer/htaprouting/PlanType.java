package com.alibaba.polardbx.optimizer.htaprouting;

import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsert;
import com.alibaba.polardbx.optimizer.core.rel.LogicalModify;
import com.alibaba.polardbx.optimizer.core.rel.LogicalRelocate;
import com.alibaba.polardbx.optimizer.core.rel.LogicalView;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.planmanager.LogicalViewWithSubqueryFinder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexDynamicParam;

public enum PlanType {
    // pure row plan
    ROW,

    // pure columnar plan
    COLUMNAR,

    // row-columnar plan
    ROW_COLUMNAR,

    // insert into row select from columnar
    INSERT_ROW_SELECT_COLUMNAR;

    public static boolean containColumnar(PlanType planType) {
        if (planType == null) {
            return false;
        }
        return planType != ROW;
    }

    public static boolean isColumnar(PlanType planType) {
        return planType == COLUMNAR;
    }

    /**
     * determine the plan schedule type
     *
     * @param input a physical plan optimized
     * @param optimizerType optimizer used to generate the plan
     * @return optimizer to be used for the sql
     */
    public static PlanType determinePlanType(RelNode input,
                                             OptimizerType optimizerType,
                                             PlannerContext pc) {
        if (input instanceof BaseQueryOperation) {
            HtapTrace.tracePlanType(pc.getHtapTrace(), PlanType.ROW, "base query operation");
            return ROW;
        }
        if (optimizerType == OptimizerType.COLUMNAR) {
            if (CBOUtil.isInsertSelectFromColumnar(input)) {
                HtapTrace.tracePlanType(pc.getHtapTrace(), PlanType.INSERT_ROW_SELECT_COLUMNAR,
                    "insert select for columnar");
                return INSERT_ROW_SELECT_COLUMNAR;
            }
        }

        if (input instanceof LogicalInsert || input instanceof LogicalRelocate || input instanceof LogicalModify) {
            // TODO: remove this
            HtapTrace.tracePlanType(pc.getHtapTrace(), PlanType.ROW,
                "to remove the limitation for insert");
            return ROW;
        }

        boolean containRow = false;
        boolean containColumnar = false;
        LogicalViewWithSubqueryFinder logicalViewWithSubqueryFinder = new LogicalViewWithSubqueryFinder();
        input.accept(logicalViewWithSubqueryFinder);
        for (LogicalView lv : logicalViewWithSubqueryFinder.getResult()) {
            if (lv instanceof OSSTableScan) {
                if (((OSSTableScan) lv).isColumnarIndex()) {
                    containColumnar = true;
                    continue;
                }
            }
            if (lv.getScalarList() != null) {
                for (RexDynamicParam dynamicParam : lv.getScalarList()) {
                    LogicalViewWithSubqueryFinder dynamicParamLvFinder = new LogicalViewWithSubqueryFinder();
                    dynamicParam.getRel().accept(dynamicParamLvFinder);
                    for (LogicalView subLv : dynamicParamLvFinder.getResult()) {
                        if (subLv instanceof OSSTableScan) {
                            if (((OSSTableScan) subLv).isColumnarIndex()) {
                                containColumnar = true;
                            }
                        }
                    }
                }
            }
            containRow = true;
        }
        if (!containColumnar) {
            HtapTrace.tracePlanType(pc.getHtapTrace(), PlanType.ROW, "no columnar index found");
            return ROW;
        }
        if (!containRow) {
            HtapTrace.tracePlanType(pc.getHtapTrace(), PlanType.COLUMNAR, "only columnar index found");
            return COLUMNAR;
        }
        HtapTrace.tracePlanType(pc.getHtapTrace(), PlanType.ROW_COLUMNAR, "both columnar index and row table found");
        return ROW_COLUMNAR;
    }

    public static boolean compatibleWithRoutingType(PlanType planType, RoutingType routingType) {
        if (routingType == null) {
            return true;
        }
        switch (routingType) {
        case ROW:
        case FOLLOWER:
        case STALE:
            return planType == ROW;
        case COLUMNAR:
            return planType == COLUMNAR || planType == ROW_COLUMNAR || planType == INSERT_ROW_SELECT_COLUMNAR;
        case HTAP:
            return true;
        default:
            return false;
        }

    }
}
