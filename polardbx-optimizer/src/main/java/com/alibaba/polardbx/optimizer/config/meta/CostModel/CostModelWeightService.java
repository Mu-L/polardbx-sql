package com.alibaba.polardbx.optimizer.config.meta.CostModel;

public class CostModelWeightService implements ICostModelWeightService {

    private ICostModelWeightService immutableImpl;

    private final ICostModelWeightService mutableImpl;

    public CostModelWeightService(ICostModelWeightService immutableImpl) {
        this.mutableImpl = new MutableCostModelWeight();
        this.immutableImpl = immutableImpl;
    }

    public void setImmutable(ICostModelWeightService immutableImpl) {
        this.immutableImpl = immutableImpl;
    }

    private double getValueOrDefault(double manualValue, double defaultValue) {
        return manualValue > 0 ? manualValue : defaultValue;
    }

    public double getMemoryWeight() {
        return getValueOrDefault(mutableImpl.getMemoryWeight(), immutableImpl.getMemoryWeight());
    }

    public void setMemoryWeight(double memoryWeight) {
        mutableImpl.setMemoryWeight(memoryWeight);
    }

    public double getIoWeight() {
        return getValueOrDefault(mutableImpl.getIoWeight(), immutableImpl.getIoWeight());
    }

    public void setIoWeight(double ioWeight) {
        mutableImpl.setIoWeight(ioWeight);
    }

    public double getColNetWeight() {
        return getValueOrDefault(mutableImpl.getColNetWeight(), immutableImpl.getColNetWeight());
    }

    public void setColNetWeight(double colNetWeight) {
        mutableImpl.setNetWeight(colNetWeight);
    }

    public double getNetWeight() {
        return getValueOrDefault(mutableImpl.getNetWeight(), immutableImpl.getNetWeight());
    }

    public void setNetWeight(double netWeight) {
        mutableImpl.setNetWeight(netWeight);
    }

    public double getBuildWeight() {
        return getValueOrDefault(mutableImpl.getBuildWeight(), immutableImpl.getBuildWeight());
    }

    public void setBuildWeight(double buildWeight) {
        mutableImpl.setBuildWeight(buildWeight);
    }

    public double getProbeWeight() {
        return getValueOrDefault(mutableImpl.getProbeWeight(), immutableImpl.getProbeWeight());
    }

    public void setProbeWeight(double probeWeight) {
        mutableImpl.setProbeWeight(probeWeight);
    }

    public double getReverseSemiProbeWeight() {
        return getValueOrDefault(mutableImpl.getReverseSemiProbeWeight(), immutableImpl.getReverseSemiProbeWeight());
    }

    public void setReverseSemiProbeWeight(double reverseSemiProbeWeight) {
        mutableImpl.setReverseSemiProbeWeight(reverseSemiProbeWeight);
    }

    public double getReverseAntiProbeWeight() {
        return getValueOrDefault(mutableImpl.getReverseAntiProbeWeight(), immutableImpl.getReverseAntiProbeWeight());
    }

    public void setReverseAntiProbeWeight(double reverseAntiProbeWeight) {
        mutableImpl.setReverseAntiProbeWeight(reverseAntiProbeWeight);
    }

    public double getMergeWeight() {
        return getValueOrDefault(mutableImpl.getMergeWeight(), immutableImpl.getMergeWeight());
    }

    public void setMergeWeight(double mergeWeight) {
        mutableImpl.setMergeWeight(mergeWeight);
    }

    public double getHashAggWeight() {
        return getValueOrDefault(mutableImpl.getHashAggWeight(), immutableImpl.getHashAggWeight());
    }

    public void setHashAggWeight(double hashAggWeight) {
        mutableImpl.setHashAggWeight(hashAggWeight);
    }

    public double getSortAggWeight() {
        return getValueOrDefault(mutableImpl.getSortAggWeight(), immutableImpl.getSortAggWeight());
    }

    public void setSortAggWeight(double sortAggWeight) {
        mutableImpl.setSortAggWeight(sortAggWeight);
    }

    public double getSortWindowWeight() {
        return getValueOrDefault(mutableImpl.getSortWindowWeight(), immutableImpl.getSortWindowWeight());
    }

    public void setSortWindowWeight(double sortWindowWeight) {
        mutableImpl.setSortWindowWeight(sortWindowWeight);
    }

    public double getSortWeight() {
        return getValueOrDefault(mutableImpl.getSortWeight(), immutableImpl.getSortWeight());
    }

    public void setSortWeight(double sortWeight) {
        mutableImpl.setSortWeight(sortWeight);
    }

    public double getAvgTupleMatch() {
        return getValueOrDefault(mutableImpl.getAvgTupleMatch(), immutableImpl.getAvgTupleMatch());
    }

    public void setAvgTupleMatch(double avgTupleMatch) {
        mutableImpl.setAvgTupleMatch(avgTupleMatch);
    }

    public double getShardWeight() {
        return getValueOrDefault(mutableImpl.getShardWeight(), immutableImpl.getShardWeight());
    }

    public void setShardWeight(double shardWeight) {
        mutableImpl.setShardWeight(shardWeight);
    }

    public double getNlWeight() {
        return getValueOrDefault(mutableImpl.getNlWeight(), immutableImpl.getNlWeight());
    }

    public void setNlWeight(double nlWeight) {
        mutableImpl.setNlWeight(nlWeight);
    }

    public double getStartUpWeight() {
        return getValueOrDefault(mutableImpl.getStartUpWeight(), immutableImpl.getStartUpWeight());
    }

    public void setStartUpWeight(double startUpWeight) {
        mutableImpl.setStartUpWeight(startUpWeight);
    }
}
