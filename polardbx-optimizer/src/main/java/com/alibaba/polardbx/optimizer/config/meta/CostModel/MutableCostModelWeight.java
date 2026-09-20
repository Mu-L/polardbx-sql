package com.alibaba.polardbx.optimizer.config.meta.CostModel;

public class MutableCostModelWeight implements ICostModelWeightService {
    private double memoryWeight = -1;
    private double ioWeight = -1;
    private double colNetWeight = -1;
    private double netWeight = -1;
    private double buildWeight = -1;
    private double probeWeight = -1;
    private double reverseSemiProbeWeight = -1;
    private double reverseAntiProbeWeight = -1;
    private double mergeWeight = -1;
    private double nlWeight = -1;
    private double hashAggWeight = -1;
    private double sortAggWeight = -1;
    private double sortWindowWeight = -1;
    private double sortWeight = -1;
    private double avgTupleMatch = -1;
    private double shardWeight = -1;
    private double startUpWeight = -1;

    MutableCostModelWeight() {
    }

    public double getMemoryWeight() {
        return memoryWeight;
    }

    public void setMemoryWeight(double memoryWeight) {
        this.memoryWeight = memoryWeight;
    }

    public double getIoWeight() {
        return ioWeight;
    }

    public void setIoWeight(double ioWeight) {
        this.ioWeight = ioWeight;
    }

    public double getColNetWeight() {
        return colNetWeight;
    }

    public double getNetWeight() {
        return netWeight;
    }

    public void setNetWeight(double netWeight) {
        this.netWeight = netWeight;
    }

    public double getBuildWeight() {
        return buildWeight;
    }

    public void setBuildWeight(double buildWeight) {
        this.buildWeight = buildWeight;
    }

    public double getProbeWeight() {
        return probeWeight;
    }

    public void setProbeWeight(double probeWeight) {
        this.probeWeight = probeWeight;
    }

    public double getReverseSemiProbeWeight() {
        return reverseSemiProbeWeight;
    }

    public void setReverseSemiProbeWeight(double reverseSemiProbeWeight) {
        this.reverseSemiProbeWeight = reverseSemiProbeWeight;
    }

    public double getReverseAntiProbeWeight() {
        return reverseAntiProbeWeight;
    }

    public void setReverseAntiProbeWeight(double reverseAntiProbeWeight) {
        this.reverseAntiProbeWeight = reverseAntiProbeWeight;
    }

    public double getMergeWeight() {
        return mergeWeight;
    }

    public void setMergeWeight(double mergeWeight) {
        this.mergeWeight = mergeWeight;
    }

    public double getHashAggWeight() {
        return hashAggWeight;
    }

    public void setHashAggWeight(double hashAggWeight) {
        this.hashAggWeight = hashAggWeight;
    }

    public double getSortAggWeight() {
        return sortAggWeight;
    }

    public void setSortAggWeight(double sortAggWeight) {
        this.sortAggWeight = sortAggWeight;
    }

    public double getSortWindowWeight() {
        return sortWindowWeight;
    }

    public void setSortWindowWeight(double sortWindowWeight) {
        this.sortWindowWeight = sortWindowWeight;
    }

    public double getSortWeight() {
        return sortWeight;
    }

    public void setSortWeight(double sortWeight) {
        this.sortWeight = sortWeight;
    }

    public double getAvgTupleMatch() {
        return avgTupleMatch;
    }

    public void setAvgTupleMatch(double avgTupleMatch) {
        this.avgTupleMatch = avgTupleMatch;
    }

    public double getShardWeight() {
        return shardWeight;
    }

    public void setShardWeight(double shardWeight) {
        this.shardWeight = shardWeight;
    }

    public double getNlWeight() {
        return nlWeight;
    }

    public void setNlWeight(double nlWeight) {
        this.nlWeight = nlWeight;
    }

    public double getStartUpWeight() {
        return startUpWeight;
    }

    public void setStartUpWeight(double startUpWeight) {
        this.startUpWeight = startUpWeight;
    }
}
