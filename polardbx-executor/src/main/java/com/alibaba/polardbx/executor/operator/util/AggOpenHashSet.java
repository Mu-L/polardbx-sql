package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.collection.MemoryCountableIntArrayList;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.utils.memory.SizeOf;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.executor.operator.scan.impl.DictionaryMapping;
import com.alibaba.polardbx.executor.operator.scan.impl.DictionaryMappingImpl;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.IntegerType;
import com.alibaba.polardbx.optimizer.core.datatype.LongType;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import com.alibaba.polardbx.optimizer.memory.OperatorMemoryAllocatorCtx;
import com.google.common.base.Preconditions;
import it.unimi.dsi.fastutil.HashCommon;
import it.unimi.dsi.fastutil.ints.Int2BooleanOpenHashMap;
import it.unimi.dsi.fastutil.ints.MemoryCountableInt2BooleanOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2BooleanOpenHashMap;
import it.unimi.dsi.fastutil.longs.MemoryCountableLong2BooleanOpenHashMap;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

import java.util.Arrays;
import java.util.BitSet;
import java.util.List;

import static com.alibaba.polardbx.executor.operator.util.AggOpenHashMap.SERIALIZED_MASK;
import static it.unimi.dsi.fastutil.Hash.DEFAULT_LOAD_FACTOR;

/**
 * @author liugaoji
 * AggOpenHashSet is used for DISTINCT Agg.
 * The difference between AggOpenHashSet and AggOpenHashMap is HashSet do not maintain any information about GroupID
 */
public class AggOpenHashSet implements AggHashMap {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(AggOpenHashSet.class).instanceSize();
    private static final int INT_BATCH_INSTANCE_SIZE = ClassLayout.parseClass(IntBatchGroupBy.class).instanceSize();
    private static final int LONG_BATCH_INSTANCE_SIZE = ClassLayout.parseClass(LongBatchGroupBy.class).instanceSize();
    private static final int SLICE_INT_BATCH_INSTANCE_SIZE = ClassLayout.parseClass(SliceIntBatchGroupBy.class).instanceSize();
    private static final int INT_INT_BATCH_INSTANCE_SIZE = ClassLayout.parseClass(IntIntBatchGroupBy.class).instanceSize();
    private static final int LONG_LONG_BATCH_INSTANCE_SIZE = ClassLayout.parseClass(LongLongBatchGroupBy.class).instanceSize();
    private static final int DEFAULT_INSTANCE_SIZE = ClassLayout.parseClass(DefaultGroupBy.class).instanceSize();
    private static final int INT_128_ARRAY_INSTANCE_SIZE = ClassLayout.parseClass(LongLongBatchGroupBy.Int128Array.class).instanceSize();

    protected static final int NOT_EXISTS = -1;

    private GroupBy groupBy;

    @FieldMemoryCounter(value = false)
    private final OperatorMemoryAllocatorCtx memoryAllocator;

    protected final int expectedSize;
    protected final int chunkSize;

    @FieldMemoryCounter(value = false)
    protected final DataType[] groupKeyType;

    protected TypedBuffer groupKeyBuffer;

    protected int groupCount;

    protected final float loadFactor;

    @FieldMemoryCounter(value = false)
    protected ExecutionContext context;

    interface GroupBy extends MemoryCountable {

        /**
         * The key-chunk and input chunk will share the same blocks
         */
        void putChunkWithCheckDistinct(Chunk keyChunk, boolean[] isDistinct);

        /**
         * Get the precise fixed estimated size in bytes of this object.
         *
         * @return size in bytes
         */
        long fixedEstimatedSize();

        void close();
    }

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE
            + FastMemoryCounter.sizeOf(groupBy)
            + FastMemoryCounter.sizeOf(groupKeyBuffer);
    }

    public AggOpenHashSet(DataType[] groupKeyType,
                          int expectedSize, int chunkSize,
                          ExecutionContext context,
                          OperatorMemoryAllocatorCtx memoryAllocator) {
        this(groupKeyType, expectedSize, DEFAULT_LOAD_FACTOR, chunkSize,
            context,
            memoryAllocator);
    }

    public AggOpenHashSet(DataType[] groupKeyType,
                          int expectedSize, float loadFactor, int chunkSize,
                          ExecutionContext context, OperatorMemoryAllocatorCtx memoryAllocator) {
        Preconditions.checkArgument(loadFactor > 0 && loadFactor <= 1,
            "Load factor must be greater than 0 and smaller than or equal to 1");
        Preconditions.checkArgument(expectedSize >= 0, "The expected number of elements must be non-negative");

        this.loadFactor = loadFactor;

        this.groupKeyType = groupKeyType;
        this.groupKeyBuffer = TypedBuffer.create(groupKeyType, chunkSize, context);
        this.chunkSize = chunkSize;
        this.expectedSize = expectedSize;
        this.context = context;

        Preconditions.checkArgument(loadFactor > 0 && loadFactor <= 1,
            "Load factor must be greater than 0 and smaller than or equal to 1");
        Preconditions.checkArgument(expectedSize >= 0, "The expected number of elements must be non-negative");
        Preconditions.checkArgument(groupKeyType.length > 0, "at least on groupKey");

        this.memoryAllocator = memoryAllocator;

        boolean enableVecAccumulator =
            context.getParamManager().getBoolean(ConnectionParams.ENABLE_VEC_ACCUMULATOR);

        boolean groupKeyIntegerAndSlice = groupKeyType != null
            && groupKeyType.length == 2 && !context.isEnableOssCompatible()
            && ((groupKeyType[0] instanceof IntegerType && groupKeyType[1] instanceof IntegerType)
            || (groupKeyType[0] instanceof IntegerType && groupKeyType[1] instanceof SliceType)
            || (groupKeyType[0] instanceof SliceType && groupKeyType[1] instanceof IntegerType)
            || (groupKeyType[0] instanceof SliceType && groupKeyType[1] instanceof SliceType)
        );

        boolean groupKeyDoubleLong = groupKeyType != null
            && groupKeyType.length == 2 && !context.isEnableOssCompatible()
            && ((groupKeyType[0] instanceof IntegerType && groupKeyType[1] instanceof LongType)
            || (groupKeyType[0] instanceof LongType && groupKeyType[1] instanceof IntegerType)
            || (groupKeyType[0] instanceof LongType && groupKeyType[1] instanceof LongType));

        // group by long
        boolean singleGroupKeyLong = groupKeyType != null
            && groupKeyType.length == 1
            && groupKeyType[0] instanceof LongType;

        // group by int
        boolean singleGroupKeyInteger = groupKeyType != null
            && groupKeyType.length == 1
            && groupKeyType[0] instanceof IntegerType;
        ;

        if (enableVecAccumulator && groupKeyIntegerAndSlice) {
            this.groupBy = new SliceIntBatchGroupBy();
        } else if (enableVecAccumulator && groupKeyDoubleLong) {
            this.groupBy = new LongLongBatchGroupBy();
        } else if (enableVecAccumulator && singleGroupKeyLong) {
            this.groupBy = new LongBatchGroupBy();
        } else if (enableVecAccumulator && singleGroupKeyInteger) {
            this.groupBy = new IntBatchGroupBy();
        } else {
            this.groupBy = new DefaultGroupBy();
        }
        // for fixed memory cost of GroupBy objects.
        memoryAllocator.allocateReservedMemory(groupBy.fixedEstimatedSize());
    }

    private class IntBatchGroupBy implements GroupBy {
        protected int[] sourceArray = new int[chunkSize];

        protected int[] key;
        protected int mask;

        // The key=0 is stored in the last position in hash table.
        protected boolean containsZeroKey;

        // maintain a field: groupIdOfNull for null value.
        protected boolean hasNull;
        protected BitSet nullBitmap;
        protected boolean isMeetNull;

        protected int n;
        protected int maxFill;
        protected int size;
        protected final float f;

        public IntBatchGroupBy() {
            this.nullBitmap = new BitSet(chunkSize);
            this.containsZeroKey = false;
            this.isMeetNull = false;

            this.f = loadFactor;
            final int expected = expectedSize;
            if (!(f <= 0.0F) && !(f > 1.0F)) {
                if (expected < 0) {
                    throw new IllegalArgumentException("The expected number of elements must be non-negative");
                } else {
                    this.n = HashCommon.arraySize(expected, f);
                    this.mask = this.n - 1;
                    this.maxFill = HashCommon.maxFill(this.n, f);

                    // large memory allocation: hash-table for aggregation.
                    memoryAllocator.allocateReservedMemory(2 * SizeOf.sizeOfIntArray(n + 1));
                    this.key = new int[this.n + 1];
                }
            } else {
                throw new IllegalArgumentException("Load factor must be greater than 0 and smaller than or equal to 1");
            }
        }

        @Override
        public long getMemoryUsage() {
            return INT_BATCH_INSTANCE_SIZE
                + FastMemoryCounter.sizeOf(sourceArray)
                + FastMemoryCounter.sizeOf(key)
                + FastMemoryCounter.sizeOf(nullBitmap);
        }

        @Override
        public void putChunkWithCheckDistinct(Chunk keyChunk, boolean[] isDistinct) {
            Preconditions.checkArgument(keyChunk.getBlockCount() == 1);
            Preconditions.checkArgument(isDistinct != null);

            // clear null state.
            nullBitmap.clear();
            hasNull = false;

            final int positionCount = keyChunk.getPositionCount();

            // step 1. copy blocks into long arrays
            // step 2. long type-specific hash, and put group value when first hit.
            // step 3. accumulator with selection array.
            Block keyBlock = keyChunk.getBlock(0).cast(Block.class);
            keyBlock.copyToIntArray(0, positionCount, sourceArray, 0, null);

            if (keyBlock.mayHaveNull()) {
                // collect to null bitmap if have null.
                keyBlock.collectNulls(0, positionCount, nullBitmap, 0);
                hasNull = !nullBitmap.isEmpty();
            }

            for (int position = 0; position < positionCount; position++) {
                isDistinct[position] = checkDistinct(sourceArray, position);
            }
        }

        @Override
        public long fixedEstimatedSize() {
            return (chunkSize * Integer.BYTES)
                + SizeOf.sizeOfLongArray(nullBitmap.size() / 64)
                + Integer.BYTES * 5
                + Byte.BYTES * 3 + Float.BYTES;
        }

        private boolean checkDistinct(int[] sourceArray, final int position) {

            if (hasNull && nullBitmap.get(position)) {
                if (!isMeetNull) {
                    // put null value in the first time.
                    isMeetNull = true;
                    return true;
                }
                return false;
            }

            int k = sourceArray[position];
            int pos;
            if (k == 0) {
                if (this.containsZeroKey) {
                    return false;
                }
                this.containsZeroKey = true;
            } else {
                int[] key = this.key;
                int curr;
                if ((curr = key[pos = HashCommon.mix(k) & this.mask]) != 0) {
                    if (curr == k) {
                        return false;
                    }

                    while ((curr = key[pos = pos + 1 & this.mask]) != 0) {
                        if (curr == k) {
                            return false;
                        }
                    }
                }

                key[pos] = k;
            }

            if (this.size++ >= this.maxFill) {
                this.rehash(HashCommon.arraySize(this.size + 1, this.f));
            }

            return true;
        }

        protected void rehash(int newN) {
            int[] key = this.key;
            int mask = newN - 1;

            // large memory allocation: rehash of hash-table for aggregation.
            memoryAllocator.allocateReservedMemory(2 * SizeOf.sizeOfIntArray(newN + 1));

            int[] newKey = new int[newN + 1];
            int i = this.n;

            int pos;
            for (int j = this.realSize(); j-- != 0; ) {
                do {
                    --i;
                } while (key[i] == 0);

                if (newKey[pos = HashCommon.mix(key[i]) & mask] != 0) {
                    while (newKey[pos = pos + 1 & mask] != 0) {
                    }
                }

                newKey[pos] = key[i];
            }

            this.n = newN;
            this.mask = mask;
            this.maxFill = HashCommon.maxFill(this.n, this.f);
            this.key = newKey;
        }

        private int realSize() {
            return this.containsZeroKey ? this.size - 1 : this.size;
        }

        @Override
        public void close() {
            key = null;
            sourceArray = null;
        }
    }

    private class LongBatchGroupBy implements GroupBy {
        protected long[] sourceArray = new long[chunkSize];
        protected long[] key;
        protected int mask;
        protected boolean containsZeroKey;
        protected int n;
        protected int maxFill;
        protected int size;
        protected final float f;

        // maintain a field: groupIdOfNull for null value.
        protected boolean hasNull;
        protected BitSet nullBitmap;
        protected boolean isMeetNull;

        public LongBatchGroupBy() {
            this.nullBitmap = new BitSet(chunkSize);
            this.containsZeroKey = false;
            this.isMeetNull = false;

            final int expected = expectedSize;
            this.f = loadFactor;
            if (!(f <= 0.0F) && !(f > 1.0F)) {
                if (expected < 0) {
                    throw new IllegalArgumentException("The expected number of elements must be nonnegative");
                } else {
                    this.n = HashCommon.arraySize(expected, f);
                    this.mask = this.n - 1;
                    this.maxFill = HashCommon.maxFill(this.n, f);

                    // large memory allocation: hash-table for aggregation.
                    memoryAllocator.allocateReservedMemory(2 * SizeOf.sizeOfLongArray(n + 1));
                    this.key = new long[this.n + 1];
                }
            } else {
                throw new IllegalArgumentException("Load factor must be greater than 0 and smaller than or equal to 1");
            }
        }

        @Override
        public long getMemoryUsage() {
            return LONG_BATCH_INSTANCE_SIZE
                + FastMemoryCounter.sizeOf(sourceArray)
                + FastMemoryCounter.sizeOf(key)
                + FastMemoryCounter.sizeOf(nullBitmap);
        }

        @Override
        public void putChunkWithCheckDistinct(Chunk keyChunk, boolean[] isDistinct) {
            Preconditions.checkArgument(keyChunk.getBlockCount() == 1);
            Preconditions.checkArgument(isDistinct != null);

            // clear null state.
            nullBitmap.clear();
            hasNull = false;

            final int positionCount = keyChunk.getPositionCount();

            // step 1. copy blocks into long arrays
            // step 2. long type-specific hash, and put group value when first hit.
            // step 3. accumulator with selection array.
            Block keyBlock = keyChunk.getBlock(0).cast(Block.class);
            keyBlock.copyToLongArray(0, positionCount, sourceArray, 0);

            if (keyBlock.mayHaveNull()) {
                // collect to null bitmap if have null.
                keyBlock.collectNulls(0, positionCount, nullBitmap, 0);
                hasNull = !nullBitmap.isEmpty();
            }

            for (int position = 0; position < positionCount; position++) {
                isDistinct[position] = checkDistinct(sourceArray, position);
            }
        }

        private boolean checkDistinct(long[] sourceArray, final int position) {
            if (hasNull && nullBitmap.get(position)) {
                if (!isMeetNull) {
                    // put null value in the first time.
                    isMeetNull = true;
                    return true;
                }
                return false;
            }

            long k = sourceArray[position];
            int pos;
            if (k == 0L) {
                if (this.containsZeroKey) {
                    return false;
                }
                this.containsZeroKey = true;
            } else {
                long[] key = this.key;
                long curr;
                if ((curr = key[pos = (int) HashCommon.mix(k) & this.mask]) != 0L) {
                    if (curr == k) {
                        return false;
                    }

                    while ((curr = key[pos = pos + 1 & this.mask]) != 0L) {
                        if (curr == k) {
                            return false;
                        }
                    }
                }

                // not found, insert new key.
                key[pos] = k;
            }

            if (this.size++ >= this.maxFill) {
                this.rehash(HashCommon.arraySize(this.size + 1, this.f));
            }

            return true;
        }

        protected void rehash(int newN) {
            long[] key = this.key;
            int mask = newN - 1;

            // large memory allocation: rehash of hash-table for aggregation.
            memoryAllocator.allocateReservedMemory(2 * SizeOf.sizeOfLongArray(newN + 1));
            long[] newKey = new long[newN + 1];
            int i = this.n;

            int pos;
            for (int j = this.realSize(); j-- != 0; ) {
                do {
                    --i;
                } while (key[i] == 0L);

                if (newKey[pos = (int) HashCommon.mix(key[i]) & mask] != 0L) {
                    while (newKey[pos = pos + 1 & mask] != 0L) {
                    }
                }

                newKey[pos] = key[i];
            }

            this.n = newN;
            this.mask = mask;
            this.maxFill = HashCommon.maxFill(this.n, this.f);
            this.key = newKey;
        }

        private int realSize() {
            return this.containsZeroKey ? this.size - 1 : this.size;
        }

        @Override
        public void close() {
            key = null;
            sourceArray = null;
        }

        @Override
        public long fixedEstimatedSize() {
            return (chunkSize * Long.BYTES)
                + SizeOf.sizeOfLongArray(nullBitmap.size() / 64)
                + Integer.BYTES * 5
                + Byte.BYTES * 3
                + Float.BYTES;
        }
    }

    /**
     * Handle slice-slice, slice-int or int-slice type group by.
     * It can fall back to DefaultGroupBy if some blocks are not in dictionary.
     */
    private class SliceIntBatchGroupBy implements GroupBy {
        IntIntBatchGroupBy dictIntBatchGroupBy;
        DefaultGroupBy normalGroupBy;

        @Override
        public long getMemoryUsage() {
            return SLICE_INT_BATCH_INSTANCE_SIZE
                + FastMemoryCounter.sizeOf(dictIntBatchGroupBy)
                + FastMemoryCounter.sizeOf(normalGroupBy);
        }

        @Override
        public void putChunkWithCheckDistinct(Chunk keyChunk,
                                              boolean[] isDistinct) {
            Preconditions.checkArgument(keyChunk.getBlockCount() == 2);

            if (normalGroupBy != null) {
                // Already fall back to normal group by.
                normalGroupBy.putChunkWithCheckDistinct(keyChunk, isDistinct);
            } else if ((keyChunk.getBlock(0) instanceof SliceBlock
                && ((SliceBlock) keyChunk.getBlock(0)).getDictionary() == null)
                || (keyChunk.getBlock(1) instanceof SliceBlock
                && ((SliceBlock) keyChunk.getBlock(1)).getDictionary() == null)) {
                // if any block is a slice block but don't have dictionary.

                if (normalGroupBy == null) {
                    normalGroupBy = new DefaultGroupBy();
                }

                if (dictIntBatchGroupBy != null) {
                    // fall back IntIntBatchGroupBy to DefaultGroupBy
                    normalGroupBy.fillGroupKeyBuffer();
                    dictIntBatchGroupBy.close();
                    dictIntBatchGroupBy = null;
                }

                // Just put into DefaultGroupBy.
                normalGroupBy.putChunkWithCheckDistinct(keyChunk, isDistinct);
            } else {
                // Good Case: use dictionary for slice block group-by.
                if (dictIntBatchGroupBy == null) {
                    dictIntBatchGroupBy = new IntIntBatchGroupBy();
                }
                dictIntBatchGroupBy.putChunkWithCheckDistinct(keyChunk, isDistinct);
            }
        }

        @Override
        public long fixedEstimatedSize() {
            if (normalGroupBy != null) {
                return normalGroupBy.fixedEstimatedSize();
            } else if (dictIntBatchGroupBy != null) {
                return dictIntBatchGroupBy.fixedEstimatedSize();
            }
            return 0;
        }

        @Override
        public void close() {
            if (normalGroupBy != null) {
                normalGroupBy.close();
            }
            if (dictIntBatchGroupBy != null) {
                dictIntBatchGroupBy.close();
            }
        }
    }

    private class IntIntBatchGroupBy implements GroupBy {
        protected int[] intBlock1 = new int[chunkSize];
        protected int[] intBlock2 = new int[chunkSize];
        protected long[] serializedBlock = new long[chunkSize];
        protected DictionaryMappingImpl dictionaryMapping1 = new DictionaryMappingImpl();
        protected DictionaryMappingImpl dictionaryMapping2 = new DictionaryMappingImpl();

        protected long[] key;
        protected int mask;
        protected boolean containsZeroKey;
        protected int n;
        protected int maxFill;
        protected int size;
        protected final float f;

        // handle null value.
        // It's for pair of key: (null, xxx) and not initialized.
        MemoryCountableInt2BooleanOpenHashMap keyMap1;
        boolean hasNull1;
        BitSet nullBitmap1;

        // It's for pair of key: (xxx, null) and not initialized.
        MemoryCountableInt2BooleanOpenHashMap keyMap2;
        boolean hasNull2;
        BitSet nullBitmap2;

        // The groupId of key: (null, null).
        boolean isMeetNull;

        public IntIntBatchGroupBy() {
            keyMap1 = null;
            keyMap2 = null;
            hasNull1 = false;
            hasNull2 = false;
            nullBitmap1 = new BitSet(chunkSize);
            nullBitmap2 = new BitSet(chunkSize);
            isMeetNull = false;

            final int expected = expectedSize;
            this.f = loadFactor;
            if (!(f <= 0.0F) && !(f > 1.0F)) {
                if (expected < 0) {
                    throw new IllegalArgumentException("The expected number of elements must be nonnegative");
                } else {
                    this.n = HashCommon.arraySize(expected, f);
                    this.mask = this.n - 1;
                    this.maxFill = HashCommon.maxFill(this.n, f);

                    // large memory allocation: hash-table for aggregation.
                    memoryAllocator.allocateReservedMemory(
                        SizeOf.sizeOfIntArray(n + 1) + SizeOf.sizeOfLongArray(n + 1));
                    this.key = new long[this.n + 1];
                }
            } else {
                throw new IllegalArgumentException("Load factor must be greater than 0 and smaller than or equal to 1");
            }
        }

        @Override
        public long getMemoryUsage() {
            return INT_INT_BATCH_INSTANCE_SIZE
                + FastMemoryCounter.sizeOf(intBlock1)
                + FastMemoryCounter.sizeOf(intBlock2)
                + FastMemoryCounter.sizeOf(serializedBlock)
                + FastMemoryCounter.sizeOf(key)
                + FastMemoryCounter.sizeOf(nullBitmap1)
                + FastMemoryCounter.sizeOf(nullBitmap2)
                + FastMemoryCounter.sizeOf(dictionaryMapping1)
                + FastMemoryCounter.sizeOf(dictionaryMapping2)
                + FastMemoryCounter.sizeOf(keyMap1)
                + FastMemoryCounter.sizeOf(keyMap2);
        }

        @Override
        public void putChunkWithCheckDistinct(Chunk keyChunk,
                                              boolean[] isDistinct) {
            Preconditions.checkArgument(keyChunk.getBlockCount() == 2);

            // clear null state
            hasNull1 = false;
            hasNull2 = false;
            nullBitmap1.clear();
            nullBitmap2.clear();

            final int positionCount = keyChunk.getPositionCount();

            // step 1. copy blocks into arrays
            // step 2. serialize to long
            // step 3. long type-specific hash, and put group value when first hit.
            // step 4. accumulator with selection array.

            // step 1. copy blocks into arrays
            Block keyBlock1 = keyChunk.getBlock(0).cast(Block.class);
            Block keyBlock2 = keyChunk.getBlock(1).cast(Block.class);

            keyBlock1.copyToIntArray(0, positionCount, intBlock1, 0, dictionaryMapping1);
            keyBlock2.copyToIntArray(0, positionCount, intBlock2, 0, dictionaryMapping2);

            // collect null value for all key blocks.
            if (keyBlock1.mayHaveNull()) {
                keyBlock1.collectNulls(0, positionCount, nullBitmap1, 0);
                hasNull1 = !nullBitmap1.isEmpty();
            }
            if (keyBlock2.mayHaveNull()) {
                keyBlock2.collectNulls(0, positionCount, nullBitmap2, 0);
                hasNull2 = !nullBitmap2.isEmpty();
            }

            // DictMapping.merge(dict)
            // int[] remapping = DictMapping.get(hashCode)
            // int newDictId = remapping[dictId]

            // step 2. serialize to long
            // ((long) key1 << 32) | (key2 & serializedMask);
            for (int i = 0; i < positionCount; i++) {
                serializedBlock[i] = ((long) (intBlock1[i]) << 32) | ((intBlock2[i]) & SERIALIZED_MASK);
            }

            // step 3. long type-specific hash
            for (int position = 0; position < positionCount; position++) {
                isDistinct[position] = checkDistinct(serializedBlock, position);
            }
        }

        @Override
        public long fixedEstimatedSize() {
            // The groupId and value map is always growing during aggregation.
            return (chunkSize * Integer.BYTES) * 2
                + (chunkSize * Long.BYTES)
                + Integer.BYTES * 5
                + Byte.BYTES * 3
                + Float.BYTES
                + dictionaryMapping1.estimatedSize()
                + dictionaryMapping2.estimatedSize();
        }

        @Override
        public void close() {
            key = null;
            intBlock1 = null;
            intBlock2 = null;
            serializedBlock = null;
            dictionaryMapping1.close();
            dictionaryMapping2.close();
            dictionaryMapping1 = null;
            dictionaryMapping2 = null;

            if (keyMap1 != null) {
                keyMap1.clear();
                keyMap1 = null;
            }

            if (keyMap2 != null) {
                keyMap2.clear();
                keyMap2 = null;
            }
        }

        private boolean checkDistinct(long[] serializedBlock, final int position) {

            if (hasNull1 || hasNull2) {
                // handle null.
                if (hasNull1 && hasNull2 && nullBitmap1.get(position) && nullBitmap2.get(position)) {
                    // case1: both keys are null.
                    if (!isMeetNull) {
                        isMeetNull = true;
                        return isMeetNull;
                    }
                    return false;
                } else if (hasNull1 && nullBitmap1.get(position)) {
                    // case2: key of (null, xxx)
                    if (keyMap1 == null) {
                        // initialize keyMap1.
                        keyMap1 = new MemoryCountableInt2BooleanOpenHashMap();
                        keyMap1.defaultReturnValue(false);
                    }

                    // put the xxx into key map and allocate group id if needed.
                    int intVal = intBlock2[position];
                    if (!keyMap1.get(intVal)) {
                        keyMap1.put(intVal, true);
                    }
                    return true;
                } else if (hasNull2 && nullBitmap2.get(position)) {
                    // case3: key of (xxx, null)
                    if (keyMap2 == null) {
                        // initialize keyMap1.
                        keyMap2 = new MemoryCountableInt2BooleanOpenHashMap();
                        keyMap2.defaultReturnValue(false);
                    }

                    // put the xxx into key map and allocate group id if needed.
                    int intVal = intBlock1[position];
                    if (!keyMap2.get(intVal)) {
                        keyMap2.put(intVal, true);
                    }
                    return true;
                }

                // case 4: the key pair in this position is not (null, null), (null, xxx) or (xxx, null)
            }

            long k = serializedBlock[position];
            int pos;
            if (k == 0L) {
                if (this.containsZeroKey) {
                    return false;
                }
                this.containsZeroKey = true;
            } else {
                long[] key = this.key;
                long curr;
                if ((curr = key[pos = (int) HashCommon.mix(k) & this.mask]) != 0L) {
                    if (curr == k) {
                        return false;
                    }

                    while ((curr = key[pos = pos + 1 & this.mask]) != 0L) {
                        if (curr == k) {
                            return false;
                        }
                    }
                }

                // not found, insert new key.
                key[pos] = k;
            }

            if (this.size++ >= this.maxFill) {
                this.rehash(HashCommon.arraySize(this.size + 1, this.f));
            }

            return true;
        }

        protected void rehash(int newN) {
            long[] key = this.key;
            int mask = newN - 1;

            // large memory allocation: rehash of hash-table for aggregation.
            memoryAllocator.allocateReservedMemory(SizeOf.sizeOfIntArray(newN + 1) + SizeOf.sizeOfLongArray(newN + 1));
            long[] newKey = new long[newN + 1];
            int i = this.n;

            int pos;
            for (int j = this.realSize(); j-- != 0; ) {
                do {
                    --i;
                } while (key[i] == 0L);

                if (newKey[pos = (int) HashCommon.mix(key[i]) & mask] != 0L) {
                    while (newKey[pos = pos + 1 & mask] != 0L) {
                    }
                }

                newKey[pos] = key[i];
            }

            this.n = newN;
            this.mask = mask;
            this.maxFill = HashCommon.maxFill(this.n, this.f);
            this.key = newKey;
        }

        private int realSize() {
            return this.containsZeroKey ? this.size - 1 : this.size;
        }
    }

    private class LongLongBatchGroupBy implements GroupBy {

        private class Int128Array implements MemoryCountable {
            long[] low;
            long[] high;

            @Override
            public long getMemoryUsage() {
                return INT_128_ARRAY_INSTANCE_SIZE
                    + VMSupport.align((int) SizeOf.sizeOf(low))
                    + VMSupport.align((int) SizeOf.sizeOf(high));
            }

            Int128Array(int size) {
                low = new long[size];
                high = new long[size];
            }

            Int128Array(long[] low, long[] high) {
                this.low = low;
                this.high = high;
            }

            boolean isZero(int position) {
                if (position < 0 || position >= this.low.length) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "index out of bound: " + position);
                }
                return low[position] == 0 && high[position] == 0;
            }

            void setValue(int position, Int128Array right, int rightPosition) {
                this.low[position] = right.getLow(rightPosition);
                this.high[position] = right.getHigh(rightPosition);
            }

            long getLow(int position) {
                return low[position];
            }

            long getHigh(int position) {
                return high[position];
            }

            boolean slotEqual(int leftPosition, Int128Array right, int rightPosition) {
                return low[leftPosition] == right.getLow(rightPosition) &&
                    high[leftPosition] == right.getHigh(rightPosition);
            }

            int hash(int position, int mask) {
                long lowHash = HashCommon.mix(low[position]);
                long highHash = HashCommon.mix(high[position]);
                return (int) ((lowHash ^ highHash) & mask);
            }

            void close() {
                this.low = null;
                this.high = null;
            }
        }

        protected long[] longBlock1 = new long[chunkSize];
        protected long[] longBlock2 = new long[chunkSize];
        protected Int128Array serializedBlock;
        protected Int128Array key;
        protected int mask;
        protected boolean containsZeroKey;
        protected int n;
        protected int maxFill;
        protected int size;
        protected final float f;

        // handle null value.
        // It's for pair of key: (null, xxx) and not initialized.
        // Storing the mapping of (xxx) - (whether meet).
        MemoryCountableLong2BooleanOpenHashMap keyMap1;
        boolean hasNull1;
        BitSet nullBitmap1;

        // It's for pair of key: (xxx, null) and not initialized.
        // Storing the mapping of (xxx) - (whether meet).
        MemoryCountableLong2BooleanOpenHashMap keyMap2;
        boolean hasNull2;
        BitSet nullBitmap2;
        boolean isMeetNull;

        @Override
        public long getMemoryUsage() {
            return LONG_LONG_BATCH_INSTANCE_SIZE
                + FastMemoryCounter.sizeOf(longBlock1)
                + FastMemoryCounter.sizeOf(longBlock2)
                + (serializedBlock == null ? 0 : INT_128_ARRAY_INSTANCE_SIZE) // serializedBlock
                + FastMemoryCounter.sizeOf(key)
                + FastMemoryCounter.sizeOf(keyMap1)
                + FastMemoryCounter.sizeOf(keyMap2)
                + FastMemoryCounter.sizeOf(nullBitmap1)
                + FastMemoryCounter.sizeOf(nullBitmap2);
        }

        public LongLongBatchGroupBy() {
            keyMap1 = null;
            keyMap2 = null;
            hasNull1 = false;
            hasNull2 = false;
            nullBitmap1 = new BitSet(chunkSize);
            nullBitmap2 = new BitSet(chunkSize);
            isMeetNull = false;

            final int expected = expectedSize;
            this.f = loadFactor;
            if (!(f <= 0.0F) && !(f > 1.0F)) {
                if (expected < 0) {
                    throw new IllegalArgumentException("The expected number of elements must be nonnegative");
                } else {
                    this.n = HashCommon.arraySize(expected, f);
                    this.mask = this.n - 1;
                    this.maxFill = HashCommon.maxFill(this.n, f);

                    // large memory allocation: hash-table for aggregation.
                    memoryAllocator.allocateReservedMemory(
                        (SizeOf.sizeOfIntArray(n + 1) + SizeOf.sizeOfLongArray(n + 1)) * 2);
                    this.key = new Int128Array(this.n + 1);
                }
            } else {
                throw new IllegalArgumentException("Load factor must be greater than 0 and smaller than or equal to 1");
            }
        }

        @Override
        public void putChunkWithCheckDistinct(Chunk keyChunk,
                                              boolean[] isDistinct) {
            Preconditions.checkArgument(keyChunk.getBlockCount() == 2);

            // clear null state
            hasNull1 = false;
            hasNull2 = false;
            nullBitmap1.clear();
            nullBitmap2.clear();

            final int positionCount = keyChunk.getPositionCount();

            // step 1. copy blocks into arrays
            // step 2. serialize to long
            // step 3. long type-specific hash, and put group value when first hit.
            // step 4. accumulator with selection array.

            // step 1. copy blocks into arrays
            Block keyBlock1 = keyChunk.getBlock(0).cast(Block.class);
            Block keyBlock2 = keyChunk.getBlock(1).cast(Block.class);

            keyBlock1.copyToLongArray(0, positionCount, longBlock1, 0);
            keyBlock2.copyToLongArray(0, positionCount, longBlock2, 0);

            // collect null value for all key blocks.
            if (keyBlock1.mayHaveNull()) {
                keyBlock1.collectNulls(0, positionCount, nullBitmap1, 0);
                hasNull1 = !nullBitmap1.isEmpty();
            }
            if (keyBlock2.mayHaveNull()) {
                keyBlock2.collectNulls(0, positionCount, nullBitmap2, 0);
                hasNull2 = !nullBitmap2.isEmpty();
            }

            // DictMapping.merge(dict)
            // int[] remapping = DictMapping.get(hashCode)
            // int newDictId = remapping[dictId]

            // step 2. serialize to long
            // only maintain reference
            serializedBlock = new Int128Array(longBlock1, longBlock2);
            // step 3. long type-specific hash
            for (int position = 0; position < positionCount; position++) {
                isDistinct[position] = checkDistinct(serializedBlock, position);
            }
        }

        @Override
        public long fixedEstimatedSize() {
            return (chunkSize * Long.BYTES) * 2
                + Integer.BYTES * 5
                + Byte.BYTES * 3
                + Float.BYTES;
        }

        @Override
        public void close() {
            key = null;
            longBlock1 = null;
            longBlock2 = null;
            serializedBlock = null;

            if (keyMap1 != null) {
                keyMap1.clear();
                keyMap1 = null;
            }

            if (keyMap2 != null) {
                keyMap2.clear();
                keyMap2 = null;
            }
        }

        private boolean checkDistinct(Int128Array serializedBlock, final int position) {

            if (hasNull1 || hasNull2) {
                // handle null.
                if (hasNull1 && hasNull2 && nullBitmap1.get(position) && nullBitmap2.get(position)) {
                    // case1: both keys are null.
                    if (!isMeetNull) {
                        isMeetNull = true;
                        return true;
                    }
                    return false;
                } else if (hasNull1 && nullBitmap1.get(position)) {
                    // case2: key of (null, xxx)
                    if (keyMap1 == null) {
                        // initialize keyMap1.
                        keyMap1 = new MemoryCountableLong2BooleanOpenHashMap();
                        keyMap1.defaultReturnValue(false);
                    }

                    // put the xxx into key map and allocate group id if needed.
                    long longVal = longBlock2[position];
                    if (!keyMap1.get(longVal)) {
                        keyMap1.put(longVal, true);
                        return true;
                    }
                    return false;
                } else if (hasNull2 && nullBitmap2.get(position)) {
                    // case3: key of (xxx, null)
                    if (keyMap2 == null) {
                        // initialize keyMap1.
                        keyMap2 = new MemoryCountableLong2BooleanOpenHashMap();
                        keyMap2.defaultReturnValue(false);
                    }

                    // put the xxx into key map and allocate group id if needed.
                    long longValue = longBlock1[position];
                    if (!keyMap2.get(longValue)) {
                        keyMap2.put(longValue, true);
                        return true;
                    }
                    return false;
                }

                // case 4: the key pair in this position is not (null, null), (null, xxx) or (xxx, null)
            }

            int pos;
            if (serializedBlock.isZero(position)) {
                if (this.containsZeroKey) {
                    return false;
                }
                this.containsZeroKey = true;
            } else {
                pos = serializedBlock.hash(position, this.mask);
                if (!key.isZero(pos)) {
                    if (key.slotEqual(pos, serializedBlock, position)) {
                        return false;
                    }

                    while (!key.isZero(pos = pos + 1 & this.mask)) {
                        if (key.slotEqual(pos, serializedBlock, position)) {
                            return false;
                        }
                    }
                }

                // not found, insert new key.
                key.setValue(pos, serializedBlock, position);
            }

            if (this.size++ >= this.maxFill) {
                this.rehash(HashCommon.arraySize(this.size + 1, this.f));
            }

            return true;
        }

        protected void rehash(int newN) {
            int mask = newN - 1;

            // large memory allocation: rehash of hash-table for aggregation.
            memoryAllocator.allocateReservedMemory(SizeOf.sizeOfIntArray(newN + 1) + SizeOf.sizeOfLongArray(newN + 1));
            Int128Array newKey = new Int128Array(newN + 1);
            int i = this.n;

            int pos;
            for (int j = this.realSize(); j-- != 0; ) {
                do {
                    --i;
                } while (key.isZero(i));

                if (!newKey.isZero(pos = key.hash(i, mask))) {
                    while (!newKey.isZero(pos = pos + 1 & mask)) {
                    }
                }

                newKey.setValue(pos, key, i);
            }

            this.n = newN;
            this.mask = mask;
            this.maxFill = HashCommon.maxFill(this.n, this.f);
            this.key.close();
            this.key = newKey;
        }

        private int realSize() {
            return this.containsZeroKey ? this.size - 1 : this.size;
        }
    }

    private class DefaultGroupBy implements GroupBy {

        /**
         * The array of keys (buckets)
         */
        protected int[] keys;
        /**
         * The mask for wrapping a position counter
         */
        protected int mask;
        /**
         * The current table size.
         */
        protected int n;
        /**
         * Number of entries in the set (including the key zero, if present).
         */
        protected int size;
        /**
         * The acceptable load factor.
         */
        protected float f;
        /**
         * Threshold after which we rehash. It must be the table size times {@link #f}.
         */
        protected int maxFill;

        @Override
        public long getMemoryUsage() {
            return DEFAULT_INSTANCE_SIZE + FastMemoryCounter.sizeOf(keys);
        }

        public DefaultGroupBy() {
            this.f = loadFactor;
            this.n = HashCommon.arraySize(expectedSize, loadFactor);
            this.mask = n - 1;
            this.maxFill = HashCommon.maxFill(n, loadFactor);
            this.size = 0;

            // large memory allocation: hash-table for aggregation.
            memoryAllocator.allocateReservedMemory(SizeOf.sizeOfIntArray(n));
            int[] keys = new int[n];
            Arrays.fill(keys, NOT_EXISTS);
            this.keys = keys;
        }

        @Override
        public void putChunkWithCheckDistinct(Chunk keyChunk,
                                              boolean[] isDistinct) {
            for (int i = 0; i < keyChunk.getPositionCount(); i++) {
                int currentSize = getGroupCount();
                isDistinct[i] = innerPut(keyChunk, i, -1) == currentSize;
            }
        }

        @Override
        public long fixedEstimatedSize() {
            // The default implementation of GroupBy use dynamic memory allocation.
            return Integer.BYTES * 4 + Float.BYTES;
        }

        @Override
        public void close() {
            this.keys = null;
        }

        int innerPut(Chunk chunk, int position, int groupId) {
            return doInnerPutArray(chunk, position, groupId);
        }

        /**
         * Fill the elements from GroupKeyBuffer into hash table of this object,
         * but don't allocate new Group ID.
         * <p>
         * This is only for fall-back of other implementation of group-by.
         */
        public void fillGroupKeyBuffer() {
            List<Chunk> groupKeyChunks = groupKeyBuffer.buildChunks();

            // avoid rehash.
            final int currentSize = groupCount;
            if (currentSize > expectedSize) {
                this.n = HashCommon.arraySize(currentSize, loadFactor);
                this.mask = n - 1;
                this.maxFill = HashCommon.maxFill(n, loadFactor);
                this.size = 0;

                int[] keys = new int[n];
                Arrays.fill(keys, NOT_EXISTS);
                this.keys = keys;
            }

            // avoid to allocate group id.
            int groupId = 0;
            for (Chunk chunk : groupKeyChunks) {
                for (int i = 0; i < chunk.getPositionCount(); i++) {
                    innerPut(chunk, i, groupId++);
                }
            }
        }

        private int doInnerPutArray(Chunk chunk, int position, int groupId) {
            int h = HashCommon.mix(chunk.hashCode(position)) & mask;
            int k = keys[h];

            if (k != NOT_EXISTS) {
                if (groupKeyBuffer.equals(k, chunk, position)) {
                    return k;
                }
                // Open-address probing
                while ((k = keys[h = (h + 1) & mask]) != NOT_EXISTS) {
                    if (groupKeyBuffer.equals(k, chunk, position)) {
                        return k;
                    }
                }
            }

            // 去重，仅在保留第一次命中时的group value
            if (groupId == -1) {
                groupId = appendGroup(chunk, position);
            }

            // otherwise, insert this position
            keys[h] = groupId;

            if (size++ >= maxFill) {
                rehash();
            }
            return groupId;
        }

        protected void rehash() {
            this.n *= 2;
            this.mask = n - 1;
            this.maxFill = HashCommon.maxFill(n, this.f);
            this.size = 0;

            // large memory allocation: rehash of hash-table for aggregation.
            memoryAllocator.allocateReservedMemory(SizeOf.sizeOfIntArray(n));
            int[] keys = new int[n];
            Arrays.fill(keys, NOT_EXISTS);
            this.keys = keys;

            List<Chunk> groupChunks = groupKeyBuffer.buildChunks();
            int groupId = 0;
            for (Chunk chunk : groupChunks) {
                for (int i = 0; i < chunk.getPositionCount(); i++) {
                    innerPut(chunk, i, groupId++);
                }
            }
        }
    }

    // used for distinct to compatible with GroupOpenHashMap
    public boolean[] innerPutChunk(Chunk chunk, boolean[] isDistinct) {
        Preconditions.checkArgument(isDistinct != null);
        Preconditions.checkArgument(chunk.getPositionCount() == isDistinct.length);
        groupBy.putChunkWithCheckDistinct(chunk, isDistinct);
        return isDistinct;
    }

    protected int appendGroup(Chunk chunk, int position) {
        groupKeyBuffer.appendRow(chunk, position);
        return groupCount++;
    }

    @Override
    public long estimateSize() {
       return getMemoryUsage();
    }

    public int getGroupCount() {
        return groupCount;
    }

    @Override
    public void putChunk(Chunk keyChunk, Chunk inputChunk, MemoryCountableIntArrayList groupIdResult) {
        throw new UnsupportedOperationException();
    }

    @Override
    public AggResultIterator buildChunks() {
        throw new UnsupportedOperationException();
    }

    public void close() {
        if (groupBy != null) {
            groupBy.close();
            groupBy = null;
        }
    }
}
