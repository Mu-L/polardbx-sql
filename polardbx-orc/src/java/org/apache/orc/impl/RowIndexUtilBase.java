package org.apache.orc.impl;

import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;

public class RowIndexUtilBase {
    public static final int FLAG_NO_STATISTICS = -1;
    public static final int FLAG_INT_STATISTICS = 0;
    public static final int FLAG_DOUBLE_STATISTICS = 1;
    public static final int FLAG_STRING_STATISTICS = 2;
    public static final int FLAG_DECIMAL_STATISTICS = 3;
    public static final int FLAG_DATE_STATISTICS = 4;
    public static final int FLAG_TIMESTAMP_STATISTICS = 5;
    public static final int FLAG_BINARY_STATISTICS = 6;
    public static final int FLAG_COLLECTION_STATISTICS = 7;
    public static final int FLAG_BUCKET_STATISTICS = 8;


    public static final int POSITION_ADDRESSING_ALL_0 = 0;
    public static final int POSITION_ADDRESSING_ALL_1 = 1;
    public static final int POSITION_ADDRESSING_ALL_2 = 2;
    public static final int POSITION_ADDRESSING_ALL_3 = 3;
    public static final int POSITION_ADDRESSING_ALL_4 = 4;
    public static final int POSITION_ADDRESSING_ALL_5 = 5;
    public static final int POSITION_ADDRESSING_ALL_6 = 6;
    public static final int POSITION_ADDRESSING_ALL_7 = 7;
    public static final int POSITION_ADDRESSING_ALL_8 = 8;
    public static final int POSITION_ADDRESSING_ALL_9 = 9;
    public static final int POSITION_ADDRESSING_ALL_10 = 10;

    public static class DynamicByteBuffer {
        private ByteBuffer buffer;
        private static final int DEFAULT_CAPACITY = 1024;
        private static final double GROWTH_FACTOR = 1.5;

        public DynamicByteBuffer() {
            this(DEFAULT_CAPACITY);
        }

        public DynamicByteBuffer(int initialCapacity) {
            buffer = ByteBuffer.allocate(initialCapacity);
        }

        private void ensureCapacity(int additionalBytes) {
            if (buffer.remaining() < additionalBytes) {
                int newCapacity = (int) ((buffer.position() + additionalBytes) * GROWTH_FACTOR);
                ByteBuffer newBuffer = ByteBuffer.allocate(newCapacity);

                // 复制现有数据
                buffer.flip();
                newBuffer.put(buffer);
                buffer = newBuffer;
            }
        }

        public void appendByte(byte b) {
            ensureCapacity(1);
            buffer.put(b);
        }

        public void appendLong(long l) {
            ensureCapacity(8);
            buffer.putLong(l);
        }

        public void appendInt(int i) {
            ensureCapacity(4);
            buffer.putInt(i);
        }

        public void appendBoolean(boolean b) {
            ensureCapacity(1);
            buffer.put(b ? (byte) 1 : (byte) 0);
        }

        public void appendDouble(double d) {
            ensureCapacity(8);
            buffer.putDouble(d);
        }

        public void appendBytes(byte[] bytes) {
            ensureCapacity(bytes.length);
            buffer.put(bytes);
        }

        public byte[] toArray() {
            byte[] result = new byte[buffer.position()];
            buffer.flip();
            buffer.get(result);
            buffer.rewind();
            return result;
        }

        public boolean isEmpty() {
            return buffer.position() == 0;
        }

        public int size() {
            return buffer.position();
        }

        public ByteBuffer getBuffer() {
            return buffer.duplicate();
        }

        public ByteString getByteString() {
            return ByteString.copyFrom(toArray());
        }
    }
}
