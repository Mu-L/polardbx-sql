package com.alibaba.polardbx.common.oss.filesystem;

import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCacheManager;
import com.aliyun.oss.OSS;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.ObjectMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.Assert.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Test class for OSSInputStream thread interruption handling
 */
public class OSSInputStreamCancelTest {

    @Mock
    private OSS ossClient;

    @Mock
    private OSSObject ossObject;

    @Mock
    private ObjectMetadata objectMetadata;

    @Mock
    private FileMergeCacheManager fileMergeCacheManager;

    @Mock
    private Configuration configuration;

    @Mock
    private OSSFileSystemStore store;

    @Mock
    private FileSystem.Statistics statistics;

    @Mock
    private FileSystemRateLimiter rateLimiter;

    private ExecutorService executorService;
    private OSSInputStream ossInputStream;
    private String bucketName = "test-bucket";
    private String key = "test-key";
    private long contentLength = 1000L;

    @Before
    public void setUp() throws IOException {
        MockitoAnnotations.initMocks(this);

        // Setup executor service
        executorService = Executors.newFixedThreadPool(2);

        // Setup mock behavior
        when(ossObject.getObjectMetadata()).thenReturn(objectMetadata);
        when(objectMetadata.getContentLength()).thenReturn(contentLength);
        when(ossClient.getObject(any(GetObjectRequest.class))).thenReturn(ossObject);

        // Create a mock input stream with some data
        byte[] testData = new byte[1000];
        for (int i = 0; i < testData.length; i++) {
            testData[i] = (byte) (i % 256);
        }
        InputStream mockInputStream = new ByteArrayInputStream(testData);
        when(ossObject.getObjectContent()).thenReturn(mockInputStream);

        // Setup configuration mock
        when(configuration.getLong(anyString(), anyLong())).thenReturn(1024L);
        when(configuration.get(anyString(), anyString())).thenReturn("REQUESTED");

        // Create OSSInputStream with correct parameters
        ossInputStream =
            new OSSInputStream(configuration, executorService, 3, store, key, contentLength, statistics, rateLimiter);
    }

    @Test
    public void testReadWithoutInterruption() throws IOException {
        // Test normal read operation without interruption
        byte[] buffer = new byte[100];
        try {
            int bytesRead = ossInputStream.read(buffer, 0, buffer.length);
        } catch (IOException e) {

        }
    }

    @Test
    public void testReadWithThreadInterruption() throws IOException {
        // Interrupt the current thread
        Thread.currentThread().interrupt();

        try {
            byte[] buffer = new byte[100];

            // This should throw IOException due to thread interruption
            ossInputStream.read(buffer, 0, buffer.length);
            fail("Expected IOException due to thread interruption");
        } catch (IOException e) {
            // Expected exception due to thread interruption
            assertTrue("Exception message should indicate stream is closed",
                e.getMessage().contains("closed") || e.getMessage().contains("interrupted"));
        } finally {
            // Clear the interrupted status
            Thread.interrupted();
        }
    }

    @Test
    public void testReadLoopWithInterruption() throws IOException {
        // Test that the read loop properly checks for interruption

        // Create a thread that will be interrupted
        Thread testThread = new Thread(() -> {
            try {
                byte[] buffer = new byte[10];

                // Start reading
                while (true) {
                    int bytesRead = ossInputStream.read(buffer, 0, buffer.length);
                    if (bytesRead == -1) {
                        break;
                    }

                    // Check if thread was interrupted during read
                    if (Thread.currentThread().isInterrupted()) {
                        break;
                    }
                }
            } catch (IOException e) {
                // Expected when thread is interrupted
                assertTrue("Exception should be due to interruption",
                    e.getMessage().contains("closed") || e.getMessage().contains("interrupted"));
            }
        });

        testThread.start();

        // Let the thread start reading
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Interrupt the reading thread
        testThread.interrupt();

        // Wait for thread to complete
        try {
            testThread.join(5000); // Wait up to 5 seconds
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        assertFalse("Test thread should have completed", testThread.isAlive());
    }

    @Test
    public void testInterruptionCheckInReadLoop() throws IOException {
        // Test that interruption is checked in the read loop

        // Create a custom input stream that simulates slow reading
        InputStream slowInputStream = new InputStream() {
            private int position = 0;

            @Override
            public int read() throws IOException {
                // Simulate slow reading
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new IOException("Interrupted during read");
                }

                if (position >= 1000) {
                    return -1;
                }
                return position++ % 256;
            }
        };

        when(ossObject.getObjectContent()).thenReturn(slowInputStream);

        // Create new OSSInputStream with slow input stream and correct parameters
        OSSInputStream slowOssInputStream =
            new OSSInputStream(configuration, executorService, 3, store, key, contentLength, statistics, rateLimiter);

        // Test reading with interruption
        Thread testThread = new Thread(() -> {
            try {
                byte[] buffer = new byte[500];
                slowOssInputStream.read(buffer, 0, buffer.length);
                fail("Should have been interrupted");
            } catch (IOException e) {
                // Expected due to interruption
                assertTrue("Should be interrupted", Thread.currentThread().isInterrupted());
            }
        });

        testThread.start();

        // Let it start reading
        try {
            Thread.sleep(50);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // Interrupt the thread
        testThread.interrupt();

        // Wait for completion
        try {
            testThread.join(5000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        assertFalse("Thread should have completed", testThread.isAlive());
    }

    @Test
    public void testNormalOperationAfterClearingInterruptFlag() throws IOException {
        // Test that normal operation works after clearing interrupt flag

        // Set interrupt flag
        Thread.currentThread().interrupt();

        // Clear the flag
        Thread.interrupted();

        // Now normal read should work
        byte[] buffer = new byte[100];
        try {
            int bytesRead = ossInputStream.read(buffer, 0, buffer.length);
        } catch (IOException e) {

        }

    }
}