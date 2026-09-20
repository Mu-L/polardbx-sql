package com.alibaba.polardbx.optimizer.config.meta.CostModel;

public interface ICostModelWeightService {
    // 权重获取方法
    double getMemoryWeight();

    double getIoWeight();

    double getColNetWeight();

    double getNetWeight();

    double getBuildWeight();

    double getProbeWeight();

    double getReverseSemiProbeWeight();

    double getReverseAntiProbeWeight();

    double getMergeWeight();

    double getHashAggWeight();

    double getSortAggWeight();

    double getSortWindowWeight();

    double getSortWeight();

    double getAvgTupleMatch();

    double getShardWeight();

    double getNlWeight();

    double getStartUpWeight();

    // 权重设置方法（默认不可变）

    default void setMemoryWeight(double memoryWeight) {
        throw new UnsupportedOperationException("Weight is immutable.");
    }

    default void setIoWeight(double ioWeight) {
        throw new UnsupportedOperationException("Weight is immutable.");
    }

    default void setColNetWeight(double colNetWeight) {
        throw new UnsupportedOperationException("Weight is immutable.");
    }

    default void setNetWeight(double netWeight) {
        throw new UnsupportedOperationException("Weight is immutable.");
    }

    default void setBuildWeight(double buildWeight) {
        throw new UnsupportedOperationException("Weight is immutable.");
    }

    default void setProbeWeight(double probeWeight) {
        throw new UnsupportedOperationException("Weight is immutable.");
    }

    default void setReverseSemiProbeWeight(double reverseSemiProbeWeight) {
        throw new UnsupportedOperationException("Weight is immutable.");
    }

    default void setReverseAntiProbeWeight(double reverseAntiProbeWeight) {
        throw new UnsupportedOperationException("Weight is immutable.");
    }

    default void setMergeWeight(double mergeWeight) {
        throw new UnsupportedOperationException("Weight is immutable.");
    }

    default void setHashAggWeight(double hashAggWeight) {
        throw new UnsupportedOperationException("Weight is immutable.");
    }

    default void setSortAggWeight(double sortAggWeight) {
        throw new UnsupportedOperationException("Weight is immutable.");
    }

    default void setSortWindowWeight(double sortWindowWeight) {
        throw new UnsupportedOperationException("Weight is immutable.");
    }

    default void setSortWeight(double sortWeight) {
        throw new UnsupportedOperationException("Weight is immutable.");
    }

    default void setAvgTupleMatch(double avgTupleMatch) {
        throw new UnsupportedOperationException("Weight is immutable.");
    }

    default void setShardWeight(double shardWeight) {
        throw new UnsupportedOperationException("Weight is immutable.");
    }

    default void setNlWeight(double nlWeight) {
        throw new UnsupportedOperationException("Weight is immutable.");
    }

    default void setStartUpWeight(double startUpWeight) {
        throw new UnsupportedOperationException("Weight is immutable.");
    }
}
