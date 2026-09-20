package com.alibaba.polardbx.optimizer.external.connector;

public enum PushdownCapability {
    PROJECT,
    FILTER,
    SORT,
    AGG
}
