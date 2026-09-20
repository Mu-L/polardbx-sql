package com.alibaba.polardbx.executor.mpp.metadata;

public enum SplitType {
    JDBC,
    DYNAMIC_JDBC,
    STREAM_JDBC,
    ORC,
    CSV,
    REMOTE
}