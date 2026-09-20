package com.alibaba.polardbx.optimizer.core.planner.rule.holisticUnnest;

import com.google.common.collect.Lists;
import org.apache.calcite.rel.core.Correlate;

import java.util.List;

public class UnnestingInfo {
    final Correlate join;
    final Unnesting parent;
    final List<CorDef> outerRefs;
    final Integer cteId;

    public UnnestingInfo(Correlate join, Unnesting parent, Integer cteId) {
        this.join = join;
        this.parent = parent;
        this.outerRefs = Lists.newArrayList();
        if (parent != null) {
            this.outerRefs.addAll(parent.getInfo().outerRefs);
        }
        for (int i = join.getRequiredColumns().nextSetBit(0); i >= 0;
             i = join.getRequiredColumns().nextSetBit(i + 1)) {
            this.outerRefs.add(new CorDef(join.getCorrelationId(), i));
        }
        this.cteId = cteId;
    }

    public Correlate getJoin() {
        return join;
    }

    public Unnesting getParent() {
        return parent;
    }

    public List<CorDef> getOuterRefs() {
        return outerRefs;
    }

    public Integer getCteId() {
        return cteId;
    }
}
