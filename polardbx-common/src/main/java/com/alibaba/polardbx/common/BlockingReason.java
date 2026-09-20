package com.alibaba.polardbx.common;

public enum BlockingReason {
    NOT_BLOCKED,
    WAIT_FOR_CONSUMER,
    WAIT_FOR_PRE_PREPROCESSOR,
    WAIT_FOR_SCAN_IO,
    WAIT_FOR_SPLIT,

    WAIT_PIPELINE_DEPENDENCY,
    WAIT_DRIVER_CONSUMER_FINISHED,

    WAIT_FOR_MEMORY,
    WAIT_FOR_MEMORY_REVOKE,
    WAIT_FOR_EXCHANGE_CLIENT,

    WAIT_FOR_NO_MORE_SPLIT,

    LOCAL_BUFFER_NOT_EMPTY,
    LOCAL_BUFFER_NOT_FULL,

    /// Some operators can get blocked due to the producer(s) (they are currently
    /// waiting data from) not having anything produced. Used by LocalExchange,
    /// LocalMergeExchange, Exchange, PartialAgg and MergeExchange operators.
    WAIT_FOR_PRODUCER,

    WAIT_FOR_BLOOM_FILTER,

    WAIT_FOR_SPILL_WRITE,
    WAIT_FOR_SPILL_READ,

    WAIT_FOR_PARALLEL_BUILD,
}
