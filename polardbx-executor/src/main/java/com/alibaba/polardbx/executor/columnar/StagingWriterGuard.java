package com.alibaba.polardbx.executor.columnar;

import java.util.concurrent.CompletableFuture;

/**
 * RAII guard for staging writer counter.
 *
 * <p>Usage:
 * <pre>{@code
 * try (StagingWriterGuard guard = StagingTableManager.getInstance().acquireWriter()) {
 *     int seqId = guard.getSeqId();
 *     CompletableFuture<Void> future = ...;
 *     guard.transferTo(future);  // normal path: release when future completes
 * }
 * // exception path: guard.close() auto-releases
 * }</pre>
 *
 * <p>Guarantees: exactly one releaseWriter() call per acquire, regardless of
 * whether the block exits normally or via exception.
 */
public class StagingWriterGuard implements AutoCloseable {

    private final int seqId;
    private volatile boolean transferred = false;

    StagingWriterGuard(int seqId) {
        this.seqId = seqId;
    }

    public int getSeqId() {
        return seqId;
    }

    /**
     * Transfer release responsibility to the given future's whenComplete.
     * After this call, {@link #close()} becomes a no-op.
     */
    public void transferTo(CompletableFuture<Void> future) {
        transferred = true;
        future.whenComplete((v, ex) ->
            StagingTableManager.getInstance().releaseWriter(seqId));
    }

    @Override
    public void close() {
        if (!transferred) {
            StagingTableManager.getInstance().releaseWriter(seqId);
        }
    }
}
