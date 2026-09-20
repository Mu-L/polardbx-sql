package com.alibaba.polardbx.optimizer.external.connector;

public class ConnectorColumnStatistics {

    private final long ndv;
    private final double nullFraction;
    private final Comparable<?> minValue;
    private final Comparable<?> maxValue;
    private final long avgSize;

    public ConnectorColumnStatistics(long ndv, double nullFraction,
                                     Comparable<?> minValue, Comparable<?> maxValue,
                                     long avgSize) {
        this.ndv = ndv;
        this.nullFraction = nullFraction;
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.avgSize = avgSize;
    }

    public long getNdv() {
        return ndv;
    }

    public double getNullFraction() {
        return nullFraction;
    }

    public Comparable<?> getMinValue() {
        return minValue;
    }

    public Comparable<?> getMaxValue() {
        return maxValue;
    }

    public long getAvgSize() {
        return avgSize;
    }
}
