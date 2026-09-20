package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.google.common.base.Preconditions;

import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class PartialHashAggResultIterator implements AggResultIterator {

    private final BlockingQueue<Chunk> groupChunks;
    private final BlockingQueue<Chunk> valueChunks;
    private Chunk END;
    public PartialHashAggResultIterator() {
        this.groupChunks = new LinkedBlockingQueue<>();
        this.valueChunks = new LinkedBlockingQueue<>();
    }
    public PartialHashAggResultIterator(List<Chunk> groupChunks, List<Chunk> valueChunks) {
        Preconditions.checkArgument(groupChunks.size() == valueChunks.size());
        this.groupChunks = new LinkedBlockingQueue<>(groupChunks);
        this.valueChunks = new LinkedBlockingQueue<>(valueChunks);
    }

    public void mergeOther(PartialHashAggResultIterator other) {
        this.groupChunks.addAll(other.getGroupChunks());
        this.valueChunks.addAll(other.getValueChunks());
    }

    public void addEndChunk(Chunk endChunk) {
        //must update END first
        END = endChunk;
        this.groupChunks.add(endChunk);
        this.valueChunks.add(endChunk);
    }

    public BlockingQueue<Chunk> getGroupChunks() {
        return groupChunks;
    }

    public BlockingQueue<Chunk> getValueChunks() {
        return valueChunks;
    }

    @Override
    public boolean isEmpty() {
        return groupChunks.isEmpty() || valueChunks.isEmpty();
    }

    @Override
    public Chunk nextChunk() {
        if (groupChunks.isEmpty() || valueChunks.isEmpty()) {
            return null;
        }
        Chunk groupChunk = groupChunks.poll();
        Chunk valueChunk = valueChunks.poll();

        if (groupChunk == END && valueChunk == END) {
            Assert.assertTrue(groupChunks.isEmpty() && valueChunks.isEmpty(),
                "groupChunk and valueChunk must be empty after END");
            return END;
        }

        int valueBlockCount = valueChunk.getBlockCount();
        int groupBlockCount = groupChunk.getBlockCount();

        Block[] blocks = new Block[groupBlockCount + valueBlockCount];
        for (int i = 0; i < groupBlockCount; i++) {
            blocks[i] = groupChunk.getBlock(i);
        }
        for (int i = 0; i < valueBlockCount; i++) {
            blocks[i + groupBlockCount] = valueChunk.getBlock(i);
        }
        return new Chunk(blocks);
    }
}
