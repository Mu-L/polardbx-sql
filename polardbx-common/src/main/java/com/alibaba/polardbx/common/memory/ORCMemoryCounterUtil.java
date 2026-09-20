package com.alibaba.polardbx.common.memory;

import io.airlift.slice.SizeOf;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import org.apache.orc.impl.BitFieldReader;
import org.apache.orc.impl.BufferChunk;
import org.apache.orc.impl.BufferChunkList;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.IntegerReader;
import org.apache.orc.impl.RunLengthByteReader;
import org.apache.orc.impl.RunLengthByteWriter;
import org.apache.orc.impl.RunLengthIntegerReader;
import org.apache.orc.impl.RunLengthIntegerReaderV2;
import org.apache.orc.impl.RunLengthIntegerWriter;
import org.apache.orc.impl.RunLengthIntegerWriterV2;
import org.apache.orc.impl.SerializationUtils;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.GraphLayout;
import org.openjdk.jol.util.VMSupport;
import org.roaringbitmap.RoaringBitmap;

import java.nio.ByteBuffer;

public class ORCMemoryCounterUtil {
    private static final int HEAP_BYTE_BUFFER_INSTANCE_SIZE =
        (int) (GraphLayout.parseInstance(ByteBuffer.allocate(1)).totalSize()
            - VMSupport.align((int) SizeOf.sizeOfByteArray(1)));

    private static final int BUFFER_CHUNK_LIST_INSTANCE_SIZE =
        ClassLayout.parseClass(BufferChunkList.class).instanceSize();
    private static final int BUFFER_CHUNK_INSTANCE_SIZE = ClassLayout.parseClass(BufferChunk.class).instanceSize();

    public static long sizeOfBufferChunkList(BufferChunkList bufferChunkList, RoaringBitmap objectBitmap) {
        if (bufferChunkList == null) {
            return 0L;
        }
        long memoryUsage = 0L;

        for (BufferChunk node = bufferChunkList.head; node != null; node = (BufferChunk) node.next) {

            ByteBuffer byteBuffer = node.getData();

            if (byteBuffer != null && objectBitmap.checkedAdd(System.identityHashCode(byteBuffer))) {
                byte[] data = byteBuffer.array();

                // calculate size of data without duplicated.
                memoryUsage += HEAP_BYTE_BUFFER_INSTANCE_SIZE;
                if (objectBitmap.checkedAdd(System.identityHashCode(data))) {
                    memoryUsage += VMSupport.align((int) SizeOf.sizeOf(data));
                }
            }

            // add instance size of buffer chunk.
            memoryUsage += BUFFER_CHUNK_INSTANCE_SIZE;
        }

        memoryUsage += BUFFER_CHUNK_LIST_INSTANCE_SIZE;

        return memoryUsage;
    }

    private static final int COMPRESSED_STREAM_INSTANCE_SIZE =
        ClassLayout.parseClass(InStream.CompressedStream.class).instanceSize();
    private static final int UNCOMPRESSED_STREAM_INSTANCE_SIZE =
        ClassLayout.parseClass(InStream.UncompressedStream.class).instanceSize();

    public static long sizeOfInStream(InStream inStream) {
        if (inStream == null) {
            return 0L;
        }
        if (inStream instanceof InStream.UncompressedStream) {
            return UNCOMPRESSED_STREAM_INSTANCE_SIZE;
        } else if (inStream instanceof InStream.CompressedStream) {
            long memoryUsage = COMPRESSED_STREAM_INSTANCE_SIZE;

            byte[] allocated = inStream.getAllocated();
            if (allocated != null) {
                memoryUsage += VMSupport.align((int) SizeOf.sizeOf(allocated));
            }

            return memoryUsage;

        }
        return ClassLayout.parseClass(inStream.getClass()).instanceSize();
    }

    private static final int RUN_LENGTH_BYTE_READER_INSTANCE_SIZE =
        ClassLayout.parseClass(RunLengthByteReader.class).instanceSize();

    public static long sizeOfRunLengthByteReader(RunLengthByteReader reader) {
        if (reader == null) {
            return 0L;
        }
        long memoryUsage = RUN_LENGTH_BYTE_READER_INSTANCE_SIZE;
        InStream inStream = reader.getInput();
        memoryUsage += sizeOfInStream(inStream);

        memoryUsage += VMSupport.align((int) SizeOf.sizeOfByteArray(RunLengthByteWriter.MAX_LITERAL_SIZE));
        return memoryUsage;
    }

    private static final int BIT_FIELD_READER_INSTANCE_SIZE =
        ClassLayout.parseClass(BitFieldReader.class).instanceSize();

    public static long sizeOfBitFieldReader(BitFieldReader bitFieldReader) {
        if (bitFieldReader == null) {
            return 0L;
        }

        long memoryUsage = BIT_FIELD_READER_INSTANCE_SIZE;
        memoryUsage += sizeOfRunLengthByteReader(bitFieldReader.getInput());
        return memoryUsage;
    }

    private static final int SERIALIZATION_INSTANCE_SIZE =
        ClassLayout.parseClass(SerializationUtils.class).instanceSize();

    public static long sizeOfSerializationUtils(SerializationUtils serializationUtils) {
        if (serializationUtils == null) {
            return 0L;
        }

        long memoryUsage = SERIALIZATION_INSTANCE_SIZE;
        memoryUsage += (2 * VMSupport.align((int) SizeOf.sizeOfByteArray(SerializationUtils.BUFFER_SIZE)));
        return memoryUsage;
    }

    private static final int RUN_LENGTH_INTEGER_READER_INSTANCE_SIZE =
        ClassLayout.parseClass(RunLengthIntegerReader.class).instanceSize();
    private static final int RUN_LENGTH_INTEGER_READER_V2_INSTANCE_SIZE = ClassLayout.parseClass(
        RunLengthIntegerReaderV2.class).instanceSize();

    public static long sizeOfIntegerReader(IntegerReader integerReader) {
        if (integerReader == null) {
            return 0L;
        }
        if (integerReader instanceof RunLengthIntegerReader) {
            long memoryUsage = RUN_LENGTH_INTEGER_READER_INSTANCE_SIZE;

            memoryUsage += sizeOfInStream(((RunLengthIntegerReader) integerReader).getInput());
            memoryUsage += sizeOfSerializationUtils(((RunLengthIntegerReader) integerReader).getSerializationUtils());
            memoryUsage += VMSupport.align((int) SizeOf.sizeOfLongArray(RunLengthIntegerWriter.MAX_LITERAL_SIZE));
            return memoryUsage;

        } else if (integerReader instanceof RunLengthIntegerReaderV2) {
            long memoryUsage = RUN_LENGTH_INTEGER_READER_V2_INSTANCE_SIZE;

            memoryUsage += sizeOfInStream(((RunLengthIntegerReaderV2) integerReader).getInput());
            memoryUsage += sizeOfSerializationUtils(((RunLengthIntegerReaderV2) integerReader).getSerializationUtils());
            memoryUsage += VMSupport.align((int) SizeOf.sizeOfLongArray(RunLengthIntegerWriterV2.MAX_SCOPE));
            return memoryUsage;
        }
        return ClassLayout.parseClass(integerReader.getClass()).instanceSize();
    }
}
