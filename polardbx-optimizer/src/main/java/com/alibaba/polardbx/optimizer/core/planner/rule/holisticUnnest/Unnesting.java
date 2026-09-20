package com.alibaba.polardbx.optimizer.core.planner.rule.holisticUnnest;

public class Unnesting {
    final UnnestingInfo info;
    final Equality equality;

    public Unnesting(UnnestingInfo info) {
        this.info = info;
        this.equality = new Equality();
    }

    public Unnesting(UnnestingInfo info, Equality equality) {
        this.info = info;
        this.equality = equality;
    }

    public UnnestingInfo getInfo() {
        return info;
    }

    public Equality getEquality() {
        return equality;
    }

    public static Unnesting copy(Unnesting unnest) {
        if (unnest == null) {
            return null;
        }
        return new Unnesting(unnest.getInfo(), unnest.getEquality().copy());
    }
}
