package com.alibaba.polardbx.executor.columnar;

import com.alibaba.polardbx.common.oss.blob.BlobObjectId;
import com.alibaba.polardbx.common.oss.blob.BlobPageFormat;
import com.alibaba.polardbx.gms.metadb.table.BlobFileIdAllocator;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Allocates logical slots for values buffered in a staging sequence.
 *
 * <p>One mutable allocation state is maintained per {@code (seqId, tableId)}. The state is only
 * an admission optimization: the full slot address is persisted in the staging row, so losing the
 * in-memory state on a CN restart only leaves a partially filled Page and never makes an existing
 * value unreachable.
 */
final class BlobPageSlotAllocator {

    static final long TARGET_RAW_PAGE_BYTES = BlobPageFormat.DEFAULT_TARGET_PAGE_RAW_BYTES;

    private static final BlobPageSlotAllocator INSTANCE = new BlobPageSlotAllocator();

    private final Map<PageOwner, PageState> activePages = new ConcurrentHashMap<>();

    static BlobPageSlotAllocator getInstance() {
        return INSTANCE;
    }

    private BlobPageSlotAllocator() {
    }

    long allocate(int seqId, long tableId, int rawLength) {
        if (seqId <= 0) {
            throw new IllegalArgumentException("staging seqId must be positive: " + seqId);
        }
        if (rawLength < 0) {
            throw new IllegalArgumentException("rawLength must not be negative: " + rawLength);
        }

        PageOwner owner = new PageOwner(seqId, tableId);
        PageState state = activePages.computeIfAbsent(owner, ignored -> new PageState());
        synchronized (state) {
            if (state.needsNewPage(rawLength)) {
                state.startNewPage();
            }
            int slotId = state.nextSlotId++;
            state.rawBytes += rawLength;
            return BlobObjectId.encode(state.pageId, slotId);
        }
    }

    void releaseSeq(int seqId) {
        activePages.keySet().removeIf(owner -> owner.seqId == seqId);
    }

    private static final class PageState {
        private long pageId;
        private int nextSlotId;
        private long rawBytes;

        private boolean needsNewPage(int nextRawLength) {
            if (pageId == 0 || nextSlotId >= BlobObjectId.MAX_SLOT_ID + 1) {
                return true;
            }
            return nextSlotId > 0 && rawBytes + (long) nextRawLength > TARGET_RAW_PAGE_BYTES;
        }

        private void startNewPage() {
            pageId = BlobFileIdAllocator.getInstance().allocate();
            nextSlotId = 0;
            rawBytes = 0;
        }
    }

    private static final class PageOwner {
        private final int seqId;
        private final long tableId;

        private PageOwner(int seqId, long tableId) {
            this.seqId = seqId;
            this.tableId = tableId;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof PageOwner)) {
                return false;
            }
            PageOwner that = (PageOwner) obj;
            return seqId == that.seqId && tableId == that.tableId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(seqId, tableId);
        }
    }
}
