package com.alibaba.polardbx.optimizer.core.planner.rule.holisticUnnest;

import org.apache.calcite.rel.core.CorrelationId;

import java.util.Objects;

public class CorDef implements Comparable<CorDef> {
    public final CorrelationId corr;
    public final int field;

    public CorDef(CorrelationId corr, int field) {
        this.corr = corr;
        this.field = field;
    }

    @Override
    public String toString() {
        return corr.getName() + '.' + field;
    }

    @Override
    public int hashCode() {
        return Objects.hash(corr, field);
    }

    @Override
    public boolean equals(Object o) {
        return this == o
            || (o instanceof CorDef
            && corr == ((CorDef) o).corr && field == ((CorDef) o).field);
    }

    @Override
    public int compareTo(CorDef o) {
        int c = corr.compareTo(o.corr);
        if (c != 0) {
            return c;
        }
        return Integer.compare(field, o.field);
    }

    public static CorDef create(CorrelationId corr, int field) {
        return new CorDef(corr, field);
    }
}
