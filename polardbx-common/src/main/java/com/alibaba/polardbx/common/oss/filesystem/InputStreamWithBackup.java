package com.alibaba.polardbx.common.oss.filesystem;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import org.jetbrains.annotations.NotNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;

public class InputStreamWithBackup extends InputStream {
    private static final Logger logger = LoggerFactory.getLogger(InputStreamWithBackup.class);

    private final AtomicBoolean fallbackOccurred = new AtomicBoolean(false);
    /**
     * Constructor from backup input stream with specified bytes used
     */
    final private Function<Integer, InputStream> backupStreamCtor;
    private InputStream currentStream;
    private volatile boolean backupStreamUsed = false;
    private final AtomicInteger bytesRead = new AtomicInteger(0);
    private final AtomicLong readTimeNs = new AtomicLong(0);
    private final String fileName;

    public InputStreamWithBackup(InputStream masterStream, Function<Integer, InputStream> backupStreamCtor,
                                 String fileName) {
        this.currentStream = masterStream;
        this.backupStreamCtor = backupStreamCtor;
        this.fileName = fileName;
    }

    @Override
    public int read() throws IOException {
        try {
            long startTime = System.nanoTime();
            int result = currentStream.read();
            if (!backupStreamUsed && result != -1) {
                bytesRead.incrementAndGet();
                readTimeNs.addAndGet(System.nanoTime() - startTime);
            }
            return result;
        } catch (Throwable e) {
            if (fallback(e)) {
                return currentStream.read();
            } else {
                throw e;
            }
        }
    }

    @Override
    public int read(byte @NotNull [] b, int off, int len) throws IOException {
        try {
            long startTime = System.nanoTime();
            int bytesReadDelta = currentStream.read(b, off, len);
            if (!backupStreamUsed && bytesReadDelta != -1) {
                bytesRead.addAndGet(bytesReadDelta);
                readTimeNs.addAndGet(System.nanoTime() - startTime);
            }
            return bytesReadDelta;
        } catch (Throwable e) {
            if (fallback(e)) {
                return currentStream.read(b, off, len);
            } else {
                throw e;
            }
        }
    }

    @Override
    public void close() throws IOException {
        if (currentStream != null) {
            currentStream.close();
        }
    }

    @Override
    public int available() throws IOException {
        try {
            return currentStream.available();
        } catch (Throwable e) {
            if (fallback(e)) {
                return currentStream.available();
            } else {
                throw e;
            }
        }
    }

    private boolean fallback(Throwable masterStreamException) {
        if (backupStreamUsed) {
            return false;
        }

        synchronized (this) {
            if (!backupStreamUsed) {
                if (currentStream != null) {
                    try {
                        currentStream.close();
                    } catch (Throwable t) {
                        // ignore
                    }
                }

                logger.warn(
                    String.format("CSV file %s fallback to backup stream when rpc already read %d bytes: ",
                        fileName, bytesRead.get()),
                    masterStreamException);

                currentStream = backupStreamCtor.apply(bytesRead.get());
                backupStreamUsed = true;
            }
        }
        return true;
    }

    /**
     * Check if a fallback occurred during reading.
     *
     * @return true if fallback occurred, false otherwise
     */
    public boolean isFallbackOccurred() {
        return fallbackOccurred.get();
    }

    /**
     * Method to be called when fallback occurs to set the flag
     */
    public void setFallbackOccurred() {
        fallbackOccurred.set(true);
    }

    public long getIOTimeFromColumnarMs() {
        return readTimeNs.get() / 1_000_000;
    }

    public int getBytesRead() {
        return bytesRead.get();
    }
}
