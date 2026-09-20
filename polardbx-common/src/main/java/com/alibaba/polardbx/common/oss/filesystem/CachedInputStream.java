/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.common.oss.filesystem;

import com.alibaba.polardbx.cache.GeneralCache;
import com.alibaba.polardbx.cache.offheap.OffHeapArena;
import com.alibaba.polardbx.cache.offheap.bufferpool.ArenaClockBP;
import com.alibaba.polardbx.cache.statistics.CacheStatistics;
import com.alibaba.polardbx.cache.statistics.CacheStatisticsCollector;
import com.alibaba.polardbx.cache.unsafe.UnsafeBytes;
import com.alibaba.polardbx.common.cache.CacheLogger;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.google.common.util.concurrent.MoreExecutors;
import lombok.Getter;
import lombok.Setter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSExceptionMessages;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.EOFException;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Queue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

import static com.alibaba.polardbx.common.oss.filesystem.Constants.MULTIPART_DOWNLOAD_SIZE_DEFAULT;
import static com.alibaba.polardbx.common.oss.filesystem.Constants.MULTIPART_DOWNLOAD_SIZE_KEY;
import static com.alibaba.polardbx.common.oss.filesystem.Constants.OSS_FETCH_POLICY;

/**
 * A FSInputStream implementation that uses GeneralCache for caching.
 * This input stream supports both streaming reads and seekable/positioned reads
 * by leveraging the GeneralCache interface.
 * <p>
 * Supports multi-part read-ahead prefetching similar to {@link OSSInputStream},
 * with configurable fetch policies (FIXED, MERGE, REQUESTED).
 */
public class CachedInputStream extends FSInputStream {
    private static final Logger LOG = LoggerFactory.getLogger(CachedInputStream.class);

    private static final CacheStatisticsCollector COLLECTOR;

    static {
        COLLECTOR = new CacheStatisticsCollector("CN",
            new CacheLogger("CACHE_QUERY"),
            new CacheLogger("CACHE_STATISTICS"));
        COLLECTOR.start();
    }

    /**
     * Get the CN-side CacheStatisticsCollector for monitoring.
     */
    public static CacheStatisticsCollector getCollector() {
        return COLLECTOR;
    }

    private final GeneralCache cache;
    private final long fileId;
    @Getter
    private final String fileName;
    @Getter
    private final long fileSize;
    private final long getOptions;
    private final CacheStatistics statistics;
    private final FileSystem.Statistics fsStatistics;

    private boolean closed;
    private long position;

    // Read-ahead prefetch fields (similar to OSSInputStream)
    private final long downloadPartSize;
    private final int maxReadAheadPartNumber;
    private long partRemaining;
    private long latestPartSize;
    private byte[] buffer;
    private long expectNextPos;
    private long lastByteStart;

    private final ExecutorService readAheadExecutorService;
    private final Queue<CacheReadBuffer> readBufferQueue = new ArrayDeque<>();

    private final FetchPolicy fetchPolicy;

    /**
     * Constructs a CachedInputStream with read-ahead prefetching.
     *
     * @param conf the Hadoop configuration
     * @param readAheadExecutorService the executor for async prefetch tasks
     * @param maxReadAheadPartNumber max number of parts to prefetch
     * @param cache the GeneralCache instance
     * @param fileId the file ID (0 if invalid, will use IdNameProvider)
     * @param fileName the file name
     * @param fileSize the file size
     * @param getOptions the get options
     * @param statistics the cache statistics (nullable)
     * @param fsStatistics the file system statistics (nullable)
     */
    public CachedInputStream(Configuration conf,
                             ExecutorService readAheadExecutorService,
                             int maxReadAheadPartNumber,
                             GeneralCache cache,
                             long fileId,
                             String fileName,
                             long fileSize,
                             long getOptions,
                             CacheStatistics statistics,
                             FileSystem.Statistics fsStatistics) {
        this.readAheadExecutorService =
            MoreExecutors.listeningDecorator(readAheadExecutorService);
        this.maxReadAheadPartNumber = maxReadAheadPartNumber;
        this.cache = cache;
        this.fileId = 0 == fileId ? cache.getIdNameProvider().getId(fileName) : fileId;
        if (0 == this.fileId) {
            throw new IllegalArgumentException("Invalid file ID: " + fileId);
        }
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.getOptions = getOptions;
        this.statistics = statistics;
        this.fsStatistics = fsStatistics;
        this.position = 0;
        this.closed = false;

        this.downloadPartSize = conf.getLong(MULTIPART_DOWNLOAD_SIZE_KEY, MULTIPART_DOWNLOAD_SIZE_DEFAULT);
        this.expectNextPos = 0;
        this.lastByteStart = -1;
        this.fetchPolicy = FetchPolicy.valueOf(conf.get(OSS_FETCH_POLICY, FetchPolicy.REQUESTED.name()));
    }

    /**
     * Verify that the input stream is open.
     *
     * @throws IOException if the connection is closed
     */
    private void checkNotClosed() throws IOException {
        if (closed) {
            throw new IOException(FSExceptionMessages.STREAM_IS_CLOSED);
        }
    }

    /**
     * Fill read-ahead queue and fetch the next part from cache.
     *
     * @param pos position from start of a file
     * @param downloadPartSize size of each part to fetch
     * @throws IOException if failed to fetch
     */
    private synchronized void reopen(long pos, long downloadPartSize) throws IOException {
        long partSize;

        if (pos < 0) {
            throw new EOFException("Cannot seek at negative position:" + pos);
        } else if (pos > fileSize) {
            throw new EOFException("Cannot seek after EOF, fileSize:" +
                fileSize + " position:" + pos);
        } else if (pos + downloadPartSize > fileSize) {
            partSize = fileSize - pos;
        } else {
            partSize = downloadPartSize;
        }

        if (this.buffer != null) {
            if (LOG.isDebugEnabled()) {
                LOG.debug("Aborting old stream to open at pos " + pos);
            }
            this.buffer = null;
        }

        boolean isRandomIO = true;
        if (pos == this.expectNextPos && partSize == latestPartSize) {
            isRandomIO = false;
        } else {
            //new seek, remove cache buffers if its byteStart is not equal to pos
            while (!readBufferQueue.isEmpty()) {
                if (readBufferQueue.element().getByteStart() != pos) {
                    readBufferQueue.poll();
                } else {
                    break;
                }
            }
        }

        this.expectNextPos = pos + partSize;

        int currentSize = readBufferQueue.size();
        if (currentSize == 0) {
            //init lastByteStart to pos - partSize, used by for loop below
            lastByteStart = pos - partSize;
        } else {
            CacheReadBuffer[] readBuffers = readBufferQueue.toArray(
                new CacheReadBuffer[currentSize]);
            lastByteStart = readBuffers[currentSize - 1].getByteStart();
        }

        // current size: current read ahead part number
        int maxLen = this.maxReadAheadPartNumber - currentSize;
        for (int i = 0; i < maxLen && i < (currentSize + 1) * 2; i++) {
            if (lastByteStart + partSize * (i + 1) > fileSize) {
                break;
            }

            long byteStart = lastByteStart + partSize * (i + 1);
            long byteEnd = byteStart + partSize - 1;
            if (byteEnd >= fileSize) {
                byteEnd = fileSize - 1;
            }

            CacheReadBuffer readBuffer = new CacheReadBuffer(byteStart, byteEnd);
            if (readBuffer.getBuffer().length == 0) {
                //EOF
                readBuffer.setStatus(CacheReadBuffer.STATUS.SUCCESS);
            } else {
                this.readAheadExecutorService.execute(
                    new CacheReaderTask(readBuffer));
            }
            readBufferQueue.add(readBuffer);
            if (isRandomIO) {
                break;
            }
        }

        final CacheReadBuffer readBuffer = readBufferQueue.poll();
        assert readBuffer != null;
        readBuffer.lock();
        try {
            readBuffer.await(CacheReadBuffer.STATUS.INIT);
            if (readBuffer.getStatus() == CacheReadBuffer.STATUS.ERROR) {
                this.buffer = null;
            } else {
                this.buffer = readBuffer.getBuffer();
            }
        } catch (InterruptedException e) {
            LOG.warn("interrupted when wait a read buffer");
        } finally {
            readBuffer.unlock();
        }

        if (this.buffer == null) {
            throw new IOException("Null IO stream");
        }
        position = pos;
        partRemaining = partSize;
        latestPartSize = partSize;
    }

    /**
     * Reopen at given position with default part size.
     *
     * @param pos position from start of a file
     * @throws IOException if failed to reopen
     */
    private synchronized void reopen(long pos) throws IOException {
        reopen(pos, this.downloadPartSize);
    }

    @Override
    public synchronized int read() throws IOException {
        checkNotClosed();

        if (partRemaining <= 0 && position < fileSize) {
            reopen(position);
        }

        int byteRead = -1;
        if (partRemaining != 0) {
            byteRead = this.buffer[this.buffer.length - (int) partRemaining] & 0xFF;
        }
        if (byteRead >= 0) {
            position++;
            partRemaining--;
        }

        if (fsStatistics != null && byteRead >= 0) {
            fsStatistics.incrementBytesRead(1);
        }
        return byteRead;
    }

    private static int sizeFor(int cap) {
        int n = cap - 1;
        n |= n >>> 1;
        n |= n >>> 2;
        n |= n >>> 4;
        n |= n >>> 8;
        n |= n >>> 16;
        return n + 1;
    }

    @Override
    public synchronized int read(byte @NotNull [] buf, int off, int len) throws IOException {
        checkNotClosed();

        if (off < 0 || len < 0 || len > buf.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        if (position >= fileSize) {
            return -1;
        }

        int bytesRead = 0;
        // Not EOF, and read not done
        while (!Thread.currentThread().isInterrupted()
            && position < fileSize && bytesRead < len) {
            if (partRemaining == 0) {
                int delta = len - bytesRead;

                switch (fetchPolicy) {
                case FIXED:
                    reopen(position);
                    break;
                case MERGE:
                    if (delta > this.downloadPartSize) {
                        int toRead = sizeFor(delta);

                        while (toRead - delta > this.downloadPartSize) {
                            toRead = sizeFor(delta -= (int) this.downloadPartSize);
                        }
                        if (toRead < this.downloadPartSize) {
                            toRead = (int) this.downloadPartSize;
                        }

                        reopen(position, toRead);
                    } else {
                        reopen(position);
                    }
                    break;
                case REQUESTED:
                    reopen(position, delta);
                    break;
                }
            }

            int bytes = 0;
            for (int i = this.buffer.length - (int) partRemaining;
                 i < this.buffer.length; i++) {
                buf[off + bytesRead] = this.buffer[i];
                bytes++;
                bytesRead++;
                if (bytesRead >= len) {
                    break;
                }
            }

            if (bytes > 0) {
                position += bytes;
                partRemaining -= bytes;
            } else if (partRemaining != 0) {
                throw new IOException("Failed to read from stream. Remaining:" +
                    partRemaining);
            }
        }

        if (fsStatistics != null && bytesRead > 0) {
            fsStatistics.incrementBytesRead(bytesRead);
        }

        // Read nothing, but attempt to read something
        if (bytesRead == 0) {
            return -1;
        } else {
            return bytesRead;
        }
    }

    /**
     * Read data directly from cache without using buffer for positioned reads.
     * This is useful for random access patterns.
     *
     * @param position the position to read from
     * @param buf the buffer to read into
     * @param off the offset in the buffer
     * @param len the length to read
     * @return the number of bytes read, or -1 if at EOF
     * @throws IOException if failed to read
     */
    public int read(long position, byte[] buf, int off, int len) throws IOException {
        checkNotClosed();

        if (null == buf) {
            throw new NullPointerException();
        } else if (off < 0 || len < 0 || len > buf.length - off) {
            throw new IndexOutOfBoundsException();
        } else if (len == 0) {
            return 0;
        }

        if (position < 0) {
            throw new EOFException("Cannot read at negative position: " + position);
        } else if (position >= fileSize) {
            return -1;
        }

        final int realLen = (int) Math.min(fileSize - position, len);
        int currentOff = off;
        int actualRead = 0;

        try {
            // Calculate the page range to fetch.
            final int pageIdStart = (int) (position / OffHeapArena.BLOCK_SIZE);
            final int offsetInFirstPage = (int) (position % OffHeapArena.BLOCK_SIZE);
            final int totalPages =
                (offsetInFirstPage + realLen + OffHeapArena.BLOCK_SIZE - 1) / OffHeapArena.BLOCK_SIZE;

            // Cap pinned pages per cache.get so BP never holds more than
            // the configured amount of bytes at once. Read the dynamic value
            // once per call to keep loop iteration consistent.
            final int maxPinBytesPerGet = DynamicConfig.getInstance().getCacheMaxPinBytesPerGet();
            final int maxPagesPerGet = Math.max(1, maxPinBytesPerGet / OffHeapArena.BLOCK_SIZE);

            int remaining = realLen;
            int offsetInPage = offsetInFirstPage;
            int currentPageId = pageIdStart;
            int processedPages = 0;
            boolean incomplete = false;

            while (processedPages < totalPages && remaining > 0 && !incomplete) {
                final int chunkPages = Math.min(maxPagesPerGet, totalPages - processedPages);
                final long chunkTotalSize = Math.min((long) chunkPages * OffHeapArena.BLOCK_SIZE,
                    fileSize - (long) currentPageId * OffHeapArena.BLOCK_SIZE);

                try (final ArenaClockBP.ArrayBPRefer refers = cache.get(fileId, fileName, fileSize,
                    currentPageId, chunkPages, chunkTotalSize, statistics, getOptions)) {
                    for (int i = 0; i < chunkPages && remaining > 0; i++) {
                        final int loadedSize = refers.getLoadedSize(i);
                        if (loadedSize <= offsetInPage) {
                            LOG.error("Incomplete page data in positioned read: "
                                    + "loadedSize={}, offsetInPage={}, file={}, fileId={}, "
                                    + "pageIdx={}, currentPageId={}, chunkPages={}, totalPages={}, "
                                    + "position={}, realLen={}, remaining={}",
                                loadedSize, offsetInPage, fileName, fileId,
                                i, currentPageId, chunkPages, totalPages, position, realLen, remaining);
                            incomplete = true;
                            break;
                        }
                        final int copyLength = Math.min(remaining, loadedSize - offsetInPage);
                        UnsafeBytes.UNSAFE.copyMemory(null, refers.getAddress(i) + offsetInPage, buf,
                            UnsafeBytes.BYTE_ARRAY_BASE_OFFSET + currentOff, copyLength);
                        currentOff += copyLength;
                        remaining -= copyLength;
                        offsetInPage = 0; // subsequent pages start from offset 0
                    }
                }
                currentPageId += chunkPages;
                processedPages += chunkPages;
            }

            actualRead = realLen - remaining;
            if (actualRead != realLen) {
                LOG.error("Short read from cache: expected={}, actual={}, "
                        + "file={}, fileId={}, position={}, pageIdStart={}, currentPageId={}, "
                        + "processedPages={}, totalPages={}, maxPagesPerGet={}, incomplete={}",
                    realLen, actualRead, fileName, fileId, position, pageIdStart, currentPageId,
                    processedPages, totalPages, maxPagesPerGet, incomplete);
            }
        } catch (GeneralCache.NoDataAvailableException e) {
            LOG.error("No data available for file: {} at position: {}", fileName, position, e);
            throw new IOException(e.getMessage(), e);
        } catch (Exception e) {
            LOG.error("Unexpected error reading from cache: file={}, fileId={}, position={}, len={}",
                fileName, fileId, position, realLen, e);
            throw new IOException("Cache read error for file " + fileName + ": " + e.getMessage(), e);
        }

        if (actualRead <= 0) {
            throw new IOException(String.format(
                "Failed to read from cache: file=%s, fileId=%d, position=%d, requestedLen=%d",
                fileName, fileId, position, realLen));
        }

        if (fsStatistics != null) {
            fsStatistics.incrementBytesRead(actualRead);
        }

        return actualRead;
    }

    @Override
    public synchronized void seek(long pos) throws IOException {
        checkNotClosed();

        if (pos < 0) {
            throw new EOFException("Cannot seek at negative position: " + pos);
        } else if (pos > fileSize) {
            throw new EOFException("Cannot seek after EOF, fileSize: " + fileSize + " position: " + pos);
        }

        position = pos;
        partRemaining = 0;
    }

    @Override
    public synchronized long getPos() throws IOException {
        checkNotClosed();
        return position;
    }

    @Override
    public synchronized int available() throws IOException {
        checkNotClosed();

        final long remaining = fileSize - position;
        if (remaining > Integer.MAX_VALUE) {
            return Integer.MAX_VALUE;
        }
        return (int) remaining;
    }

    @Override
    public boolean seekToNewSource(long targetPos) throws IOException {
        checkNotClosed();
        return false;
    }

    @Override
    public synchronized void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        this.buffer = null;

        LOG.debug("Closed CachedInputStream for file: {}", fileName);

        // do statistics log
        if (statistics != null) {
            COLLECTOR.collect(
                "{\"invoke\":\"fs_request\",\"fileId\":" + fileId + ",\"fileName\":\"" + fileName + "\",\"fileSize\":"
                    + fileSize + '}', statistics);
        }
    }

    /**
     * Internal read buffer for cache prefetching, similar to {@link ReadBuffer}.
     * Manages a byte buffer with thread-safe status tracking for async cache reads.
     */
    static class CacheReadBuffer {
        enum STATUS {
            INIT, SUCCESS, ERROR
        }

        private final ReentrantLock lock = new ReentrantLock();
        private final Condition readyCondition = lock.newCondition();
        @Getter
        private final byte[] buffer;
        @Setter
        @Getter
        private STATUS status;
        @Getter
        private final long byteStart;
        @Getter
        private final long byteEnd;

        CacheReadBuffer(long byteStart, long byteEnd) {
            this.buffer = new byte[(int) (byteEnd - byteStart) + 1];
            this.status = STATUS.INIT;
            this.byteStart = byteStart;
            this.byteEnd = byteEnd;
        }

        public void lock() {
            lock.lock();
        }

        public void unlock() {
            lock.unlock();
        }

        public void await(STATUS waitStatus) throws InterruptedException {
            while (this.status == waitStatus) {
                readyCondition.await();
            }
        }

        public void signalAll() {
            readyCondition.signalAll();
        }

        @Override
        public String toString() {
            return String.format("{interval = [%s, %s], len = %s}", byteStart, byteEnd, byteEnd - byteStart);
        }
    }

    /**
     * Async task for reading cache data into a {@link CacheReadBuffer}.
     * Submitted to the read-ahead executor service for prefetching.
     * <p>
     * Since {@code cache.get()} blocks until data is available (returns
     * {@link ArenaClockBP.ArrayBPRefer} only after loading completes),
     * this task wraps the blocking call in an async execution context,
     * copies data from off-heap to the buffer, and signals completion.
     */
    private class CacheReaderTask implements Runnable {
        private final CacheReadBuffer readBuffer;

        CacheReaderTask(CacheReadBuffer readBuffer) {
            this.readBuffer = readBuffer;
        }

        @Override
        public void run() {
            readBuffer.lock();
            try {
                long pos = readBuffer.getByteStart();
                int toRead = (int) (readBuffer.getByteEnd() - readBuffer.getByteStart() + 1);

                int pageIdStart = (int) (pos / OffHeapArena.BLOCK_SIZE);
                int offsetInFirstPage = (int) (pos % OffHeapArena.BLOCK_SIZE);
                int totalPages = (offsetInFirstPage + toRead + OffHeapArena.BLOCK_SIZE - 1) / OffHeapArena.BLOCK_SIZE;

                // Cap pinned pages per cache.get so BP never holds more than
                // the configured amount of bytes at once. Read the dynamic
                // value once per task so the chunk size is stable.
                int maxPinBytesPerGet = DynamicConfig.getInstance().getCacheMaxPinBytesPerGet();
                int maxPagesPerGet = Math.max(1, maxPinBytesPerGet / OffHeapArena.BLOCK_SIZE);

                int bufferOffset = 0;
                int remaining = toRead;
                int currentOffset = offsetInFirstPage;
                int currentPageId = pageIdStart;
                int processedPages = 0;
                boolean incomplete = false;

                while (processedPages < totalPages && remaining > 0 && !incomplete) {
                    int chunkPages = Math.min(maxPagesPerGet, totalPages - processedPages);
                    long chunkTotalSize = Math.min((long) chunkPages * OffHeapArena.BLOCK_SIZE,
                        fileSize - (long) currentPageId * OffHeapArena.BLOCK_SIZE);

                    try (ArenaClockBP.ArrayBPRefer refers = cache.get(fileId, fileName, fileSize,
                        currentPageId, chunkPages, chunkTotalSize, statistics, getOptions)) {
                        for (int i = 0; i < chunkPages && remaining > 0; i++) {
                            int loadedSize = refers.getLoadedSize(i);
                            if (loadedSize <= currentOffset) {
                                LOG.error("Prefetch incomplete page data: "
                                        + "loadedSize={}, currentOffset={}, file={}, fileId={}, "
                                        + "pageIdx={}, currentPageId={}, chunkPages={}, totalPages={}, "
                                        + "pos={}, toRead={}, remaining={}",
                                    loadedSize, currentOffset, fileName, fileId,
                                    i, currentPageId, chunkPages, totalPages, pos, toRead, remaining);
                                incomplete = true;
                                break;
                            }
                            int copyLength = Math.min(remaining, loadedSize - currentOffset);
                            UnsafeBytes.UNSAFE.copyMemory(null, refers.getAddress(i) + currentOffset,
                                readBuffer.getBuffer(), UnsafeBytes.BYTE_ARRAY_BASE_OFFSET + bufferOffset, copyLength);
                            bufferOffset += copyLength;
                            remaining -= copyLength;
                            currentOffset = 0;
                        }
                    }
                    currentPageId += chunkPages;
                    processedPages += chunkPages;
                }

                if (incomplete || remaining > 0) {
                    LOG.error("Prefetch short read: expected={}, actual={}, "
                            + "file={}, fileId={}, pos={}, pageIdStart={}, currentPageId={}, "
                            + "processedPages={}, totalPages={}, maxPagesPerGet={}, incomplete={}",
                        toRead, toRead - remaining, fileName, fileId, pos, pageIdStart, currentPageId,
                        processedPages, totalPages, maxPagesPerGet, incomplete);
                    readBuffer.setStatus(CacheReadBuffer.STATUS.ERROR);
                } else {
                    readBuffer.setStatus(CacheReadBuffer.STATUS.SUCCESS);
                }

                readBuffer.signalAll();
            } catch (Exception e) {
                LOG.warn("Failed to prefetch cache data for file: {}", fileName, e);
                readBuffer.setStatus(CacheReadBuffer.STATUS.ERROR);
                readBuffer.signalAll();
            } finally {
                readBuffer.unlock();
            }
        }
    }
}
