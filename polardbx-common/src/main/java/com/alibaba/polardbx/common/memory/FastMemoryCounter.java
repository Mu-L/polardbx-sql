package com.alibaba.polardbx.common.memory;

import io.airlift.slice.SizeOf;
import io.airlift.slice.SliceOutput;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.GraphLayout;
import org.openjdk.jol.util.VMSupport;
import org.roaringbitmap.RoaringBitmap;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.sql.Blob;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicIntegerArray;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicLongArray;

public interface FastMemoryCounter {

    static int align(int originalSize) {
        return VMSupport.align(originalSize);
    }

    // check if String object using byte[]
    boolean USE_BYTES_BASED_STRING = GraphLayout.parseInstance("SpilledTopNExec@666789756").totalSize() < 96;

    int OBJECT_INSTANCE_SIZE = ClassLayout.parseClass(Object.class).instanceSize();
    int STRING_INSTANCE_SIZE = ClassLayout.parseClass(String.class).instanceSize();
    int ARRAY_LIST_INSTANCE_SIZE = ClassLayout.parseClass(ArrayList.class).instanceSize();
    int BIT_SET_INSTANCE_SIZE = ClassLayout.parseClass(BitSet.class).instanceSize();
    int BIG_INTEGER_INSTANCE_SIZE = ClassLayout.parseClass(BigInteger.class).instanceSize();
    int BIG_DECIMAL_INSTANCE_SIZE = ClassLayout.parseClass(BigDecimal.class).instanceSize();

    int BYTE_INSTANCE_SIZE = ClassLayout.parseClass(Byte.class).instanceSize();
    int SHORT_INSTANCE_SIZE = ClassLayout.parseClass(Short.class).instanceSize();
    int INTEGER_INSTANCE_SIZE = ClassLayout.parseClass(Integer.class).instanceSize();
    int LONG_INSTANCE_SIZE = ClassLayout.parseClass(Long.class).instanceSize();
    int FLOAT_INSTANCE_SIZE = ClassLayout.parseClass(Float.class).instanceSize();
    int DOUBLE_INSTANCE_SIZE = ClassLayout.parseClass(Double.class).instanceSize();

    int ATOMIC_INTEGER_INSTANCE_SIZE = ClassLayout.parseClass(AtomicInteger.class).instanceSize();
    int ATOMIC_LONG_INSTANCE_SIZE = ClassLayout.parseClass(AtomicLong.class).instanceSize();
    int ATOMIC_LONG_ARRAY_INSTANCE_SIZE = ClassLayout.parseClass(AtomicLongArray.class).instanceSize();
    int ATOMIC_INTEGER_ARRAY_INSTANCE_SIZE = ClassLayout.parseClass(AtomicIntegerArray.class).instanceSize();
    int ATOMIC_BOOLEAN_INSTANCE_SIZE = ClassLayout.parseClass(AtomicBoolean.class).instanceSize();

    int HEAP_BYTE_BUFFER_INSTANCE_SIZE = (int) (GraphLayout.parseInstance(ByteBuffer.allocate(1)).totalSize()
        - FastMemoryCounter.align((int) SizeOf.sizeOfByteArray(1)));

    int DEFAULT_MAX_DEPTH = 128;

    public static void main(String[] args) {
        System.out.println();
    }

    static MemoryUsageReport parseInstance(Object root) {
        FastMemoryCounter fastMemoryCounter = new AnnotationBasedMemoryCounter();
        return fastMemoryCounter.getMemoryUsage(root);
    }

    static MemoryUsageReport parseInstance(Object root, int maxDepth, boolean useAnnotation,
                                           boolean generateFieldSizeMap,
                                           boolean generateTreeStruct) {
        FastMemoryCounter fastMemoryCounter =
            new AnnotationBasedMemoryCounter(maxDepth, useAnnotation, generateFieldSizeMap,
                generateTreeStruct, null);
        return fastMemoryCounter.getMemoryUsage(root);
    }

    static MemoryUsageReport parseInstance(Object root, int maxDepth, boolean useAnnotation,
                                           boolean generateFieldSizeMap,
                                           boolean generateTreeStruct, ConditionalMemoryType conditionalMemoryType) {
        FastMemoryCounter fastMemoryCounter =
            new AnnotationBasedMemoryCounter(maxDepth, useAnnotation, generateFieldSizeMap,
                generateTreeStruct, conditionalMemoryType);
        return fastMemoryCounter.getMemoryUsage(root);
    }

    MemoryUsageReport getMemoryUsage(Object root);

    static long sizeOfHeapByteBuffer(ByteBuffer buffer) {
        if (buffer == null) {
            return 0;
        }
        if (checkMarked(buffer)) {
            return 0;
        }
        return HEAP_BYTE_BUFFER_INSTANCE_SIZE + FastMemoryCounter.align(
            (int) SizeOf.sizeOfByteArray(buffer.capacity()));
    }

    static long sizeOf(MemoryCountable memoryCountable) {
        if (memoryCountable == null) {
            return 0;
        }

        if (checkMarked(memoryCountable)) {
            return 0;
        }

        return memoryCountable.getMemoryUsage();
    }

    static boolean checkMarked(Object object) {
        RoaringBitmap objectBitmap = MemoryTrackerManager.getCurrentRoaringBitmap();
        if (objectBitmap == null) {
            return false;
        }

        int hash = System.identityHashCode(object);
        return !objectBitmap.checkedAdd(hash);
    }

    static <T extends MemoryCountable> long sizeOf(T[] memoryCountableArray) {
        if (memoryCountableArray == null) {
            return 0;
        }

        if (checkMarked(memoryCountableArray)) {
            return 0;
        }

        long size = 0;
        size += FastMemoryCounter.align((int) SizeOf.sizeOf(memoryCountableArray));
        for (int i = 0; i < memoryCountableArray.length; i++) {
            size += sizeOf(memoryCountableArray[i]);
        }
        return size;
    }

    static long sizeOf(Long l) {
        if (l == null) {
            return 0;
        }
        return LONG_INSTANCE_SIZE;
    }

    static long sizeOfObjectArray(Object[] array) {
        if (array == null) {
            return 0;
        }
        if (checkMarked(array)) {
            return 0;
        }
        return FastMemoryCounter.align((int) SizeOf.sizeOf(array));
    }

    static long sizeOf(int[] array) {
        if (array == null) {
            return 0;
        }
        if (checkMarked(array)) {
            return 0;
        }
        return FastMemoryCounter.align((int) SizeOf.sizeOf(array));
    }

    static long sizeOf(long[] array) {
        if (array == null) {
            return 0;
        }
        if (checkMarked(array)) {
            return 0;
        }
        return FastMemoryCounter.align((int) SizeOf.sizeOf(array));
    }

    static long sizeOf(boolean[] array) {
        if (array == null) {
            return 0;
        }
        if (checkMarked(array)) {
            return 0;
        }
        return FastMemoryCounter.align((int) SizeOf.sizeOf(array));
    }

    static long sizeOf(byte[] array) {
        if (array == null) {
            return 0;
        }
        if (checkMarked(array)) {
            return 0;
        }
        return FastMemoryCounter.align((int) SizeOf.sizeOf(array));
    }

    static long sizeOf(boolean[][] array) {
        if (array == null) {
            return 0;
        }
        if (checkMarked(array)) {
            return 0;
        }
        long size = FastMemoryCounter.align((int) SizeOf.sizeOf(array));
        for (int i = 0; i < array.length; i++) {
            size += sizeOf(array[i]);
        }
        return size;
    }

    static long sizeOf(int[][] array) {
        if (array == null) {
            return 0;
        }
        if (checkMarked(array)) {
            return 0;
        }
        long size = FastMemoryCounter.align((int) SizeOf.sizeOf(array));
        for (int i = 0; i < array.length; i++) {
            size += sizeOf(array[i]);
        }
        return size;
    }

    /**
     * Get memory usage of string.
     */
    static long sizeOf(String str) {
        if (str == null) {
            return 0;
        }
        if (checkMarked(str)) {
            return 0;
        }

        if (USE_BYTES_BASED_STRING) {
            return STRING_INSTANCE_SIZE + FastMemoryCounter.align((int) SizeOf.sizeOfByteArray(str.length()));
        } else {
            return STRING_INSTANCE_SIZE + FastMemoryCounter.align((int) SizeOf.sizeOfCharArray(str.length()));
        }
    }

    static long sizeOf(SliceOutput sliceOutput) {
        if (sliceOutput == null) {
            return 0;
        }
        if (checkMarked(sliceOutput)) {
            return 0;
        }
        return sliceOutput.getMemoryUsage();
    }

    static long sizeOf(Blob blob, int instanceSize) {
        if (blob == null) {
            return 0;
        }
        if (checkMarked(blob)) {
            return 0;
        }
        try {
            return instanceSize + FastMemoryCounter.align((int) SizeOf.sizeOfByteArray((int) blob.length()));
        } catch (SQLException e) {
            return instanceSize;
        }
    }

    static long sizeOf(ArrayList list) {
        if (list == null) {
            return 0;
        }
        if (checkMarked(list)) {
            return 0;
        }
        return ARRAY_LIST_INSTANCE_SIZE;
    }

    static long sizeOf(BitSet bitSet) {
        if (bitSet == null) {
            return 0;
        }
        if (checkMarked(bitSet)) {
            return 0;
        }
        return BIT_SET_INSTANCE_SIZE
            + FastMemoryCounter.align((int) SizeOf.sizeOfLongArray(bitSet.size() / 64));
    }

    static long sizeOf(AtomicLongArray array) {
        if (array == null) {
            return 0;
        }
        if (checkMarked(array)) {
            return 0;
        }
        return ATOMIC_LONG_ARRAY_INSTANCE_SIZE + FastMemoryCounter.align((int) SizeOf.sizeOfLongArray(array.length()));
    }

    static long sizeOfAtomicLongArray(int size) {
        return ATOMIC_LONG_ARRAY_INSTANCE_SIZE + FastMemoryCounter.align((int) SizeOf.sizeOfLongArray(size));
    }

    static long sizeOf(AtomicIntegerArray array) {
        if (array == null) {
            return 0;
        }
        if (checkMarked(array)) {
            return 0;
        }
        return ATOMIC_INTEGER_ARRAY_INSTANCE_SIZE + FastMemoryCounter.align(
            (int) SizeOf.sizeOfIntArray(array.length()));
    }

    static long sizeOfNumber(Number number) {
        if (number == null) {
            return 0;
        }
        if (checkMarked(number)) {
            return 0;
        }

        if (number instanceof Integer) {
            return INTEGER_INSTANCE_SIZE;
        } else if (number instanceof Long) {
            return LONG_INSTANCE_SIZE;
        } else if (number instanceof Short) {
            return SHORT_INSTANCE_SIZE;
        } else if (number instanceof Byte) {
            return BYTE_INSTANCE_SIZE;
        } else if (number instanceof Float) {
            return FLOAT_INSTANCE_SIZE;
        } else if (number instanceof Double) {
            return DOUBLE_INSTANCE_SIZE;
        } else if (number instanceof BigInteger) {
            return sizeOfBigInteger((BigInteger) number);
        }
        return parseInstance(number).getTotalSize();
    }

    static long sizeOfBigInteger(BigInteger bigInteger) {
        if (bigInteger == null) {
            return 0;
        }
        if (checkMarked(bigInteger)) {
            return 0;
        }
        int magLength = (bigInteger.bitLength() + 31) / 32;
        return BIG_INTEGER_INSTANCE_SIZE + FastMemoryCounter.align((int) SizeOf.sizeOfIntArray(magLength));
    }

    static long sizeOf(AtomicInteger atomicInteger) {
        if (atomicInteger == null) {
            return 0;
        }
        if (checkMarked(atomicInteger)) {
            return 0;
        }
        return ATOMIC_INTEGER_INSTANCE_SIZE;
    }

    static long sizeOf(AtomicLong atomicLong) {
        if (atomicLong == null) {
            return 0;
        }
        if (checkMarked(atomicLong)) {
            return 0;
        }
        return ATOMIC_LONG_INSTANCE_SIZE;
    }

    static long sizeOf(AtomicBoolean atomicBoolean) {
        if (atomicBoolean == null) {
            return 0;
        }
        if (checkMarked(atomicBoolean)) {
            return 0;
        }
        return ATOMIC_BOOLEAN_INSTANCE_SIZE;
    }
}