package com.alibaba.polardbx.gms.metadb.misc;

public enum ReadWriteLockAcquireResult {
    ALREADY_HELD,
    NEWLY_GRANTED,
    WAITING
}
