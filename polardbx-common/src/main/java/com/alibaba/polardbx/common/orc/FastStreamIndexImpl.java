package com.alibaba.polardbx.common.orc;

import org.apache.orc.OrcProto;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.util.VMSupport;

import static com.alibaba.polardbx.common.utils.memory.SizeOf.sizeOf;

public class FastStreamIndexImpl implements FastStreamIndex {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(FastStreamIndexImpl.class).instanceSize();
    private final int streamListSize;
    private final int[] intStreamIndexArray;
    private final long[] streamIndexArray;

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE
            + VMSupport.align((int) sizeOf(streamIndexArray))
            + VMSupport.align((int) sizeOf(intStreamIndexArray));
    }

    public FastStreamIndexImpl(int streamListSize, int[] intStreamIndexArray,
                               long[] streamIndexArray) {
        this.streamListSize = streamListSize;
        this.intStreamIndexArray = intStreamIndexArray;
        this.streamIndexArray = streamIndexArray;
    }

    public int getStreamListSize() {
        return streamListSize;
    }

    public long getColumnAndKind(int streamIndex) {
        return streamIndexArray[streamIndex * 2];
    }

    public OrcProto.Stream.Kind getKind(int streamIndex) {
        if (streamIndexArray != null) {
            // serialize method:
            // column int32, kind int8
            // -> long (column, kind)
            // columnAndKind = ((columnId & 0xFFFFFFFFL) << 8) | (kind.getNumber() & 0xFF);
            int kindNumber = (byte) (streamIndexArray[streamIndex * 2] & 0xFF);
            if (kindNumber < 0) {
                return null;
            }
            return OrcProto.Stream.Kind.forNumber(kindNumber);
        } else {
            int kindNumber = intStreamIndexArray[streamIndex * 2] & 0xFF;
            if (kindNumber < 0) {
                return null;
            }
            return OrcProto.Stream.Kind.forNumber(kindNumber);
        }
    }

    public int getColumnId(int streamIndex) {
        if (streamIndexArray != null) {
            // serialize method:
            // column int32, kind int8
            // -> long (column, kind)
            // columnAndKind = ((columnId & 0xFFFFFFFFL) << 8) | (kind.getNumber() & 0xFF);
            return (int) (streamIndexArray[streamIndex * 2] >> 8);
        } else {
            int columnId = (intStreamIndexArray[streamIndex * 2] >>> 8) & 0xFF;
            return columnId;
        }
    }

    public long getLength(int streamIndex) {
        if (streamIndexArray != null) {
            return streamIndexArray[streamIndex * 2 + 1];
        } else {
            return intStreamIndexArray[streamIndex * 2 + 1];
        }
    }
}
