package com.alibaba.polardbx.executor.operator.util.topnutils;

import com.alibaba.polardbx.executor.operator.util.HeapIndexRow;
import lombok.Getter;

import static com.alibaba.polardbx.util.MoreObjects.toStringHelper;

/**
 * The class is a pointer to a row in a page.
 * The actual position in the page is mutable because as pages are compacted, the position will change.
 */
@Getter
public class IndexRow implements HeapIndexRow {
    public final int pageId;
    public int position;

    public IndexRow(int pageId, int position) {
        this.pageId = pageId;
        reset(position);
    }

    public void reset(int position) {
        this.position = position;
    }

    @Override
    public String toString() {
        return toStringHelper(this)
            .add("pageId", pageId)
            .add("position", position)
            .toString();
    }
}
