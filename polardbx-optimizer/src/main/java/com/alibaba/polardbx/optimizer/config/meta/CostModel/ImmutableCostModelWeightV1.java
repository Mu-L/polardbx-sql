package com.alibaba.polardbx.optimizer.config.meta.CostModel;

public class ImmutableCostModelWeightV1 implements ICostModelWeightService {
    private final double memoryWeight = 0.001;
    private final double ioWeight = 5000;
    private final double colNetWeight = 5000000;
    private final double netWeight = 5000000;
    private final double buildWeight = 2;
    private final double probeWeight = 1.1;
    private final double reverseSemiProbeWeight = 1.6;
    private final double reverseAntiProbeWeight = 1.85;
    private final double mergeWeight = 1.2;
    private final double nlWeight = 1.1;
    private final double hashAggWeight = 1.5;
    private final double sortAggWeight = 1.0;
    private final double sortWindowWeight = 1.2;
    private final double sortWeight = 1.05;
    private final double avgTupleMatch = 10;
    private final double shardWeight = 0.25;
    private final double startUpWeight = 1.01;

    public ImmutableCostModelWeightV1() {
    }

    public double getMemoryWeight() {
        return memoryWeight;
    }

    public double getIoWeight() {
        return ioWeight;
    }

    public double getColNetWeight() {
        return colNetWeight;
    }

    public double getNetWeight() {
        return netWeight;
    }

    public double getBuildWeight() {
        return buildWeight;
    }

    public double getProbeWeight() {
        return probeWeight;
    }

    public double getReverseSemiProbeWeight() {
        return reverseSemiProbeWeight;
    }

    public double getReverseAntiProbeWeight() {
        return reverseAntiProbeWeight;
    }

    public double getMergeWeight() {
        return mergeWeight;
    }

    public double getHashAggWeight() {
        return hashAggWeight;
    }

    public double getSortAggWeight() {
        return sortAggWeight;
    }

    public double getSortWindowWeight() {
        return sortWindowWeight;
    }

    public double getSortWeight() {
        return sortWeight;
    }

    public double getAvgTupleMatch() {
        return avgTupleMatch;
    }

    public double getShardWeight() {
        return shardWeight;
    }

    public double getNlWeight() {
        return nlWeight;
    }

    public double getStartUpWeight() {
        return startUpWeight;
    }
}
