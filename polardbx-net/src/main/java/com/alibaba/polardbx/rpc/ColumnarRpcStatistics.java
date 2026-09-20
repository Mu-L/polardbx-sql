package com.alibaba.polardbx.rpc;

import java.util.concurrent.atomic.AtomicLong;

public class ColumnarRpcStatistics {
    private final AtomicLong bytesRead = new AtomicLong();
    private final AtomicLong reqCounts = new AtomicLong();
    private final AtomicLong reqHits = new AtomicLong();
    private final AtomicLong reqFileNotFound = new AtomicLong();
    private final AtomicLong reqDataCorrupted = new AtomicLong();
    private final AtomicLong reqRateLimited = new AtomicLong();
    private final AtomicLong reqInvalidArgs = new AtomicLong();
    private final AtomicLong reqUnknownError = new AtomicLong();

    public void incBytesRead(long bytes) {
        bytesRead.addAndGet(bytes);
    }

    public void incReqCounts(long count) {
        reqCounts.addAndGet(count);
    }

    public void incReqHits(long hits) {
        reqHits.addAndGet(hits);
    }

    public void incReqFileNotFound(long count) {
        reqFileNotFound.addAndGet(count);
    }

    public void incReqDataCorrupted(long count) {
        reqDataCorrupted.addAndGet(count);
    }

    public void incReqRateLimited(long count) {
        reqRateLimited.addAndGet(count);
    }

    public void incReqInvalidArgs(long count) {
        reqInvalidArgs.addAndGet(count);
    }

    public void incReqUnknownError(long count) {
        reqUnknownError.addAndGet(count);
    }

    public long getBytesRead() {
        return bytesRead.get();
    }

    public long getReqCounts() {
        return reqCounts.get();
    }

    public long getReqHits() {
        return reqHits.get();
    }

    public long getReqFileNotFound() {
        return reqFileNotFound.get();
    }

    public long getReqDataCorrupted() {
        return reqDataCorrupted.get();
    }

    public long getReqRateLimited() {
        return reqRateLimited.get();
    }

    public long getReqInvalidArgs() {
        return reqInvalidArgs.get();
    }

    public long getReqUnknownError() {
        return reqUnknownError.get();
    }
}