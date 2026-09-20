package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.ChunkConverter;
import com.alibaba.polardbx.executor.chunk.LongBlock;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.CountV2;
import com.google.common.base.Preconditions;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TransparentPreAggResultIterator implements AggResultIterator {
    private final ArrayDeque<Chunk> resultChunk;
    private final BlockingQueue<Chunk> cachedChunk;
    private final BlockingQueue<Chunk> cachedKeyChunk;
    private final ChunkConverter[] valueConverters;
    private final List<Aggregator> aggregators;

    private Chunk END;
    private ExecutionContext context;
    public TransparentPreAggResultIterator(List<Chunk> inputChunk, List<Chunk> inputKeyChunk,
                                           ChunkConverter[] valueConverters, List<Aggregator> aggregators,
                                           ExecutionContext context) {
        cachedChunk = new LinkedBlockingQueue<>();
        cachedKeyChunk = new LinkedBlockingQueue<>();
        resultChunk = new ArrayDeque<>();
        cachedChunk.addAll(inputChunk);
        cachedKeyChunk.addAll(inputKeyChunk);
        this.valueConverters = valueConverters;
        this.aggregators = aggregators;
        this.context = context;

    }

    public void addChunk(Chunk inputChunk, Chunk inputKeyChunk) {
        cachedChunk.add(inputChunk);
        cachedKeyChunk.add(inputKeyChunk);
    }

    public void addEndChunk(Chunk endChunk) {
        END = endChunk;
        cachedChunk.add(endChunk);
        cachedKeyChunk.add(endChunk);
    }

    @Override
    public boolean isEmpty() {
        return resultChunk.isEmpty() && (cachedChunk.isEmpty() || cachedKeyChunk.isEmpty());
    }

    @Override
    public Chunk nextChunk() {
        Chunk ret = resultChunk.poll();
        if (ret == null && cachedChunk != null) {
            //no need to read the newest version
            int size = Math.min(cachedChunk.size(), cachedKeyChunk.size());
            resultChunk.addAll(convertPlainChunk(cachedChunk, cachedKeyChunk, size));
            ret = resultChunk.poll();
        }
        return ret;
    }

    public List<Chunk> convertPlainChunk(BlockingQueue<Chunk> cachedChunkList, BlockingQueue<Chunk> cachedKeyChunkList, int size) {
        List<Chunk> retChunks = new ArrayList<>();
        LongBlock dummy = null;
        while (size != 0) {
            size -= 1;
            Chunk groupChunk = cachedKeyChunkList.poll();
            Chunk valueChunk = cachedChunkList.poll();
            if (groupChunk == END && valueChunk == END) {
                retChunks.add(END);
                Assert.assertTrue(cachedChunkList.isEmpty(),
                    "groupChunk and valueChunk must be empty after END");
                continue;
            }
            int groupBlockCount = groupChunk.getBlockCount();

            Block[] blocks = new Block[groupBlockCount + aggregators.size()];
            for (int i = 0; i < groupBlockCount; i++) {
                blocks[i] = groupChunk.getBlock(i);
            }
            for (int i = 0; i < aggregators.size(); i++) {
                Preconditions.checkArgument(valueConverters[i].columnWidth() <= 1);
                if (valueConverters[i].columnWidth() == 0) {
                    Preconditions.checkArgument(aggregators.get(i) instanceof CountV2);
                    blocks[i + groupBlockCount] = generateLongBlockWithValue1(valueChunk.getPositionCount(), dummy);
                } else {
                    blocks[i + groupBlockCount] = valueConverters[i].apply(valueChunk).getBlock(0);
                }
            }
            retChunks.add(new Chunk(blocks));
        }

        return retChunks;
    }

    private LongBlock generateLongBlockWithValue1(int len, LongBlock dummy) {
        if (dummy == null) {
            boolean[] valueIsNull = new boolean[len];
            long[] longValues = new long[len];
            for (int i = 0; i < len; i++) {
                longValues[i] = 1;
                valueIsNull[i] = false;
            }
            dummy = new LongBlock(0, len, valueIsNull, longValues);
        }
        return dummy;
    }

}
