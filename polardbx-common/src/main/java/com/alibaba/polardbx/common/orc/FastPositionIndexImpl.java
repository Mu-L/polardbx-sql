package com.alibaba.polardbx.common.orc;

import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import org.apache.orc.impl.PositionProvider;
import org.apache.orc.impl.RecordReaderImpl;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

public class FastPositionIndexImpl implements FastPositionIndex {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FastPositionIndexImpl.class).instanceSize();
    private int totalRowGroupCount;
    private byte[] positionListUnitSizeArray;
    // reference
    @FieldMemoryCounter(value = false)
    private final int[] accumulatedRowGroupCountPerStripe;
    private final int[] intPositionList;
    private final long[] positionList;

    public FastPositionIndexImpl(int totalRowGroupCount, byte[] positionListUnitSizeArray,
                                 int[] accumulatedRowGroupCountPerStripe,
                                 int[] intPositionList, long[] positionList) {
        this.totalRowGroupCount = totalRowGroupCount;
        this.positionListUnitSizeArray = positionListUnitSizeArray;
        this.accumulatedRowGroupCountPerStripe = accumulatedRowGroupCountPerStripe;
        this.intPositionList = intPositionList;
        this.positionList = positionList;
    }

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE
            + VMSupport.align((int) sizeOf(positionList))
            + VMSupport.align((int) sizeOf(intPositionList));
    }

    @Override
    public long getPosition(int stripeId, int rowGroupId, int indexInPosition) {
        int startIndex = getStartIndexInPositionArray(stripeId, rowGroupId);
        if (positionList != null) {
            return positionList[startIndex + indexInPosition];
        } else {
            return intPositionList[startIndex + indexInPosition];
        }
    }

    private int getStartIndexInPositionArray(int stripeId, int rowGroupId) {
        int startIndex = 0;
        for (int i = 0; i < stripeId; i++) {
            int rowGroupCountInStripe;
            if (i == accumulatedRowGroupCountPerStripe.length - 1) {
                rowGroupCountInStripe = totalRowGroupCount - accumulatedRowGroupCountPerStripe[i];
            } else {
                rowGroupCountInStripe = accumulatedRowGroupCountPerStripe[i + 1] - accumulatedRowGroupCountPerStripe[i];
            }

            startIndex += (positionListUnitSizeArray[i] & 0xFF) * rowGroupCountInStripe;
        }

        startIndex += rowGroupId * (positionListUnitSizeArray[stripeId] & 0xFF);
        return startIndex;
    }

    public int getTotalRowGroupCount() {
        return totalRowGroupCount;
    }

    @Override
    public int getPositionListUnitSize(int stripeIndex) {
        return positionListUnitSizeArray[stripeIndex] & 0xFF;
    }

    public int[] getAccumulatedRowGroupCountPerStripe() {
        return accumulatedRowGroupCountPerStripe;
    }

    @Override
    public PositionProvider getPositionProvider(int stripeIndex, int rowGroupId) {
        int startIndex = getStartIndexInPositionArray(stripeIndex, rowGroupId);

        if (positionList != null) {
            return new RecordReaderImpl.LongArrayPositionProviderImpl(positionList, startIndex);
        } else {
            return new RecordReaderImpl.IntArrayPositionProviderImpl(intPositionList, startIndex);
        }
    }
}
