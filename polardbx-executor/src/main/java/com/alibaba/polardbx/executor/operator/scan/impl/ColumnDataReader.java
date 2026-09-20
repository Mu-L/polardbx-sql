package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.columnar.VersionStorageStatistics;
import com.alibaba.polardbx.common.memory.MemoryTrackerManager;
import com.alibaba.polardbx.common.memory.OperatorMemoryOwnerId;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCachingInputStream;
import io.airlift.slice.SizeOf;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.orc.DataReader;
import org.apache.orc.OrcProto;
import org.apache.orc.StripeInformation;
import org.apache.orc.customized.ORCProfile;
import org.apache.orc.impl.BufferChunk;
import org.apache.orc.impl.BufferChunkList;
import org.apache.orc.impl.DataReaderProperties;
import org.apache.orc.impl.HadoopShims;
import org.apache.orc.impl.InStream;
import org.apache.orc.impl.OrcCodecPool;
import org.apache.orc.impl.RecordReaderUtils;
import org.openjdk.jol.util.VMSupport;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.function.Supplier;

public class ColumnDataReader implements DataReader {
    private FSDataInputStream file;
    private RecordReaderUtils.ByteBufferAllocatorPool pool;
    private HadoopShims.ZeroCopyReaderShim zcr = null;
    private final Supplier<FileSystem> fileSystemSupplier;
    private final Path path;
    private final boolean useZeroCopy;
    private boolean isOpen = false;
    private final int maxDiskRangeChunkLimit;
    private final long maxMergeDistance;
    private Supplier<Boolean> isCanceled;

    private OperatorMemoryOwnerId operatorMemoryOwnerId;

    private long actualAllocatedBytes = 0L;

    public ColumnDataReader(DataReaderProperties properties) {
        this.fileSystemSupplier = properties.getFileSystemSupplier();
        this.path = properties.getPath();
        this.file = properties.getFile();
        this.useZeroCopy = properties.getZeroCopy();
        this.maxMergeDistance = properties.getMaxMergeDistance();
        this.maxDiskRangeChunkLimit = properties.getMaxDiskRangeChunkLimit();
        this.isCanceled = null;
    }

    public long getActualAllocatedBytes() {
        return actualAllocatedBytes;
    }

    public void setOperatorMemoryOwnerId(OperatorMemoryOwnerId operatorMemoryOwnerId) {
        this.operatorMemoryOwnerId = operatorMemoryOwnerId;
    }

    @Override
    public void open() throws IOException {
        if (file == null) {
            this.file = fileSystemSupplier.get().open(path);
        }
        // don't support zero copy here.
        zcr = null;
        isOpen = true;
    }

    @Override
    public void setController(Supplier<Boolean> isCanceled) {
        this.isCanceled = isCanceled;
    }

    @Override
    public OrcProto.StripeFooter readStripeFooter(StripeInformation stripe) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public BufferChunkList readFileData(BufferChunkList range,
                                        boolean doForceDirect) throws IOException {
        readDiskRanges(file, range, maxMergeDistance, isCanceled);
        return range;
    }

    @Override
    public BufferChunkList readFileData(BufferChunkList range, boolean doForceDirect,
                                        ORCProfile memoryCounter,
                                        ORCProfile ioBytesCounter,
                                        ORCProfile ioTimer) throws IOException {
        readDiskRanges(file, range, maxMergeDistance, isCanceled);
        return range;
    }

    private void readDiskRanges(FSDataInputStream file,
                                BufferChunkList list,
                                long maxMergeDistance,
                                Supplier<Boolean> isCanceled) throws IOException {
        long startMills = System.currentTimeMillis();
        try {
            BufferChunk current = list == null ? null : list.get();
            while ((isCanceled == null || !isCanceled.get()) && current != null) {
                while (current.hasData()) {
                    current = (BufferChunk) current.next;
                }
                BufferChunk last = findSingleRead(current, maxMergeDistance);

                // Get data of range from current node to last node.
                readRanges(file, current, last);

                current = (BufferChunk) last.next;
            }
        } finally {
            VersionStorageStatistics versionStorageStatistics = VersionStorageStatistics.getThreadLocalStatistics();
            if (versionStorageStatistics != null) {
                versionStorageStatistics.updateOrcStatistics(System.currentTimeMillis() - startMills);
            }
        }
    }

    private void readRanges(FSDataInputStream file,
                            BufferChunk first,
                            BufferChunk last) throws IOException {
        // assume that the chunks are sorted by offset
        long offset = first.getOffset();
        int readSize = (int) (computeEnd(first, last) - offset);

        // 1. Here. allocate byte[readSize]
        // 2. IO cache miss:
        //         2.1 OSSInputStream: new ReadBuffer(byteStart, byteEnd)
        //         2.2 FileMergeCacheManager.put: byte[] copy = data.getBytes();
        // 3. IO cache hit: read from file.

        MemoryTrackerManager.tryAllocate(operatorMemoryOwnerId,
            VMSupport.align((int) SizeOf.sizeOfByteArray(readSize)));
        byte[] buffer = new byte[readSize];
        actualAllocatedBytes += readSize;

        if (file instanceof FileMergeCachingInputStream) {
            ((FileMergeCachingInputStream) file).readFully(offset, buffer, 0, buffer.length, operatorMemoryOwnerId);
        } else {
            file.readFully(offset, buffer, 0, buffer.length);
        }

        // get the data into a ByteBuffer
        ByteBuffer bytes = ByteBuffer.wrap(buffer);

        // populate each BufferChunks with the data
        BufferChunk current = first;
        while (current != last.next) {
            ByteBuffer currentBytes = current == last ? bytes : bytes.duplicate();
            currentBytes.position((int) (current.getOffset() - offset));
            currentBytes.limit((int) (current.getEnd() - offset));
            current.setChunk(currentBytes);
            current = (BufferChunk) current.next;
        }
    }

    private long computeEnd(BufferChunk first, BufferChunk last) {
        long end = 0;
        for (BufferChunk ptr = first; ptr != last.next; ptr = (BufferChunk) ptr.next) {
            end = Math.max(ptr.getEnd(), end);
        }
        return end;
    }

    private BufferChunk findSingleRead(BufferChunk first, long maxMergeDistance) {
        BufferChunk last = first;
        long currentEnd = first.getEnd();

        while (last.next != null &&
            !last.next.hasData() &&
            last.next.getOffset() <= (currentEnd + maxMergeDistance) &&
            last.next.getEnd() - first.getOffset() <= maxDiskRangeChunkLimit) {
            last = (BufferChunk) last.next;
            currentEnd = Math.max(currentEnd, last.getEnd());
        }
        return last;
    }

    @Override
    public void close() throws IOException {
        if (pool != null) {
            pool.clear();
        }
        // close both zcr and file
        try (HadoopShims.ZeroCopyReaderShim myZcr = zcr) {
            if (file != null) {
                file.close();
                file = null;
            }
        }
    }

    @Override
    public boolean isTrackingDiskRanges() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void releaseBuffer(ByteBuffer buffer) {
        if (zcr != null) {
            zcr.releaseBuffer(buffer);
        }
    }

    @Override
    public DataReader clone() {
        throw new UnsupportedOperationException();
    }

    @Override
    public InStream.StreamOptions getCompressionOptions() {
        throw new UnsupportedOperationException();
    }
}

