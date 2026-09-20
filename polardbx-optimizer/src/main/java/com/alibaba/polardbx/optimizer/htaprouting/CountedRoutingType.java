package com.alibaba.polardbx.optimizer.htaprouting;

import java.util.concurrent.atomic.LongAdder;

public class CountedRoutingType {
    LongAdder counter;
    RoutingType routingType;

    public CountedRoutingType(LongAdder counter, RoutingType routingType) {
        this.counter = counter;
        this.routingType = routingType;
    }

    public void add() {
        counter.increment();
    }

    public RoutingType getRoutingType() {
        return routingType;
    }

    boolean isFollowerRouting() {
        return RoutingType.isFollower(routingType);
    }

    public static CountedRoutingType getHigherRoutingType(CountedRoutingType a, CountedRoutingType b) {
        if (a == null) {
            return b;
        }
        if (b == null) {
            return a;
        }

        return (a.getRoutingType().ordinal() <= b.getRoutingType().ordinal()) ? a : b;
    }

    public static void addCounter(CountedRoutingType a) {
        if (a != null) {
            a.add();
        }
    }
}
