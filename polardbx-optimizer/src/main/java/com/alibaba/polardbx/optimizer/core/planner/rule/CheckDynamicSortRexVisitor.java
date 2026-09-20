package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import org.apache.calcite.rex.RexDynamicParam;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;

import java.util.ArrayList;
import java.util.List;

public class CheckDynamicSortRexVisitor extends RexShuttle {

    private List<RexDynamicParam> dynamicSortValues = new ArrayList<>();

    @Override
    public RexNode visitDynamicParam(RexDynamicParam dynamicParam) {
        if (dynamicParam.getIndex() == PlannerUtils.DYNAMIC_SORT_PARAM_INDEX) {
            dynamicSortValues.add(dynamicParam);
        }
        return dynamicParam;
    }

    public List<RexDynamicParam> getDynamicSortValues() {
        return dynamicSortValues;
    }
}
