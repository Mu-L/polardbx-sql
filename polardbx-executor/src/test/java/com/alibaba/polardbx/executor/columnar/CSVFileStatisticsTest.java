package com.alibaba.polardbx.executor.columnar;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CSVFileStatisticsTest {

    private CSVFileStatistics statistics;

    @Before
    public void setUp() {
        statistics = CSVFileStatistics.getInstance();
        // Reset statistics before each test
        statistics.reset();
    }

    @After
    public void tearDown() {
        // Reset statistics after each test
        statistics.reset();
    }

    @Test
    public void testRecordReadOperation_Columnar() {
        long bytesRead = 1000L;
        boolean isColumnar = true;
        long readTimeMs = 50L;

        statistics.recordReadOperation(bytesRead, isColumnar, readTimeMs);

        assertEquals(bytesRead, statistics.getTotalBytesRead());
        assertEquals(1L, statistics.getTotalAccessCount());
        assertEquals(1L, statistics.getColumnarAccessCount());
        assertEquals(0L, statistics.getFileSystemAccessCount());
        assertEquals(readTimeMs, statistics.getTotalReadTimeMs());
        assertEquals(readTimeMs, statistics.getColumnarReadTimeMs());
        assertEquals(0L, statistics.getFileSystemReadTimeMs());
    }

    @Test
    public void testRecordReadOperation_FileSystem() {
        long bytesRead = 2000L;
        boolean isColumnar = false;
        long readTimeMs = 100L;

        statistics.recordReadOperation(bytesRead, isColumnar, readTimeMs);

        assertEquals(bytesRead, statistics.getTotalBytesRead());
        assertEquals(1L, statistics.getTotalAccessCount());
        assertEquals(0L, statistics.getColumnarAccessCount());
        assertEquals(1L, statistics.getFileSystemAccessCount());
        assertEquals(readTimeMs, statistics.getTotalReadTimeMs());
        assertEquals(0L, statistics.getColumnarReadTimeMs());
        assertEquals(readTimeMs, statistics.getFileSystemReadTimeMs());
    }

    @Test
    public void testRecordReadOperation_MultipleOperations() {
        // First operation - columnar
        long bytesRead1 = 1000L;
        boolean isColumnar1 = true;
        long readTimeMs1 = 50L;
        statistics.recordReadOperation(bytesRead1, isColumnar1, readTimeMs1);

        // Second operation - file system
        long bytesRead2 = 2000L;
        boolean isColumnar2 = false;
        long readTimeMs2 = 100L;
        statistics.recordReadOperation(bytesRead2, isColumnar2, readTimeMs2);

        // Third operation - columnar
        long bytesRead3 = 1500L;
        boolean isColumnar3 = true;
        long readTimeMs3 = 75L;
        statistics.recordReadOperation(bytesRead3, isColumnar3, readTimeMs3);

        // Check aggregated statistics
        assertEquals(bytesRead1 + bytesRead2 + bytesRead3, statistics.getTotalBytesRead());
        assertEquals(3L, statistics.getTotalAccessCount());
        assertEquals(2L, statistics.getColumnarAccessCount());
        assertEquals(1L, statistics.getFileSystemAccessCount());
        assertEquals(readTimeMs1 + readTimeMs2 + readTimeMs3, statistics.getTotalReadTimeMs());
        assertEquals(readTimeMs1 + readTimeMs3, statistics.getColumnarReadTimeMs());
        assertEquals(readTimeMs2, statistics.getFileSystemReadTimeMs());
    }

    @Test
    public void testReset() {
        // Record some operations
        statistics.recordReadOperation(1000L, true, 50L);
        statistics.recordReadOperation(2000L, false, 100L);

        // Verify statistics are not zero
        assertEquals(3000L, statistics.getTotalBytesRead());
        assertEquals(2L, statistics.getTotalAccessCount());

        // Reset statistics
        statistics.reset();

        // Verify statistics are reset to zero
        assertEquals(0L, statistics.getTotalBytesRead());
        assertEquals(0L, statistics.getTotalAccessCount());
        assertEquals(0L, statistics.getColumnarAccessCount());
        assertEquals(0L, statistics.getFileSystemAccessCount());
        assertEquals(0L, statistics.getTotalReadTimeMs());
        assertEquals(0L, statistics.getColumnarReadTimeMs());
        assertEquals(0L, statistics.getFileSystemReadTimeMs());
    }
}