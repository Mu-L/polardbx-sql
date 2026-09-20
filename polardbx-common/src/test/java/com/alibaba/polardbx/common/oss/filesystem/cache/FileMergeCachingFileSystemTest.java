package com.alibaba.polardbx.common.oss.filesystem.cache;

import com.alibaba.polardbx.common.oss.filesystem.OSSFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.net.URI;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class FileMergeCachingFileSystemTest {

    @Mock
    private CacheManager cacheManager;

    @Mock
    private FileSystem dataTier;

    @Mock
    private OSSFileSystem ossFileSystem;

    private FileMergeCachingFileSystem cachingFileSystem;
    private AutoCloseable mocks;
    private URI uri;
    private Configuration configuration;

    @Before
    public void setUp() throws Exception {
        mocks = MockitoAnnotations.openMocks(this);
        uri = URI.create("test://bucket");
        configuration = new Configuration();

        // Create the FileMergeCachingFileSystem instance with mocked dependencies
        cachingFileSystem = new FileMergeCachingFileSystem(
            uri,
            configuration,
            cacheManager,
            dataTier,
            false,  // cacheValidationEnabled
            true    // enableCache
        );
    }

    @After
    public void tearDown() throws Exception {
        if (mocks != null) {
            mocks.close();
        }
    }

    @Test
    public void testOpenWithPathAndRangeForOSSFileSystem() throws IOException {
        // Setup
        Path testPath = new Path("/test/file.txt");
        long position = 100L;
        long length = 200L;

        // Mock the data tier to be an OSSFileSystem

        cachingFileSystem = new FileMergeCachingFileSystem(
            uri,
            configuration,
            cacheManager,
            ossFileSystem,
            false,  // cacheValidationEnabled
            true    // enableCache
        );
        when(ossFileSystem.uncheckedOpen(testPath, position + length)).thenReturn(mock(FSDataInputStream.class));

        // Mock cache manager behavior
        when(cacheManager.getMaxCacheQuota()).thenReturn(mock(CacheQuota.class));

        // Execute
        FSDataInputStream result = cachingFileSystem.open(testPath, position, length);

        // Verify
        assertNotNull("Result should not be null", result);
        assertTrue("Result should be an instance of FileMergeCachingInputStream",
            result instanceof FileMergeCachingInputStream);

        // Verify that uncheckedOpen was called on the OSSFileSystem with correct parameters
        verify(ossFileSystem).uncheckedOpen(eq(testPath), eq(position + length));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testOpenWithPathAndRangeForNonOSSFileSystem() throws IOException {
        // Setup
        Path testPath = new Path("/test/file.txt");
        long position = 100L;
        long length = 200L;

        // Mock the data tier to NOT be an OSSFileSystem (fallback scenario)
        when(dataTier.open(testPath)).thenReturn(mock(FSDataInputStream.class));

        // Mock cache manager behavior
        when(cacheManager.getMaxCacheQuota()).thenReturn(mock(CacheQuota.class));

        // Execute
        FSDataInputStream result = cachingFileSystem.open(testPath, position, length);
    }

    @Test(expected = IOException.class)
    public void testOpenWithPathAndRangeThrowsIOException() throws IOException {
        // Setup
        Path testPath = new Path("/test/file.txt");
        long position = 100L;
        long length = 200L;

        // Mock the data tier to be an OSSFileSystem
        cachingFileSystem = new FileMergeCachingFileSystem(
            uri,
            configuration,
            cacheManager,
            ossFileSystem,
            false,  // cacheValidationEnabled
            true    // enableCache
        );
        when(ossFileSystem.uncheckedOpen(testPath, position + length))
            .thenThrow(new IOException("Simulated IO exception"));

        // Execute - should throw IOException
        cachingFileSystem.open(testPath, position, length);
    }

    @Test
    public void testGetDataTier() {
        // Execute
        FileSystem result = cachingFileSystem.getDataTier();

        // Verify
        assertSame("Should return the same data tier", dataTier, result);
    }

    @Test
    public void testGetCacheManager() {
        // Execute
        CacheManager result = cachingFileSystem.getCacheManager();

        // Verify
        assertSame("Should return the same cache manager", cacheManager, result);
    }

    @Test
    public void testClose() throws IOException {
        // Execute
        cachingFileSystem.close();

        // Verify
        verify(cacheManager).close();
        verify(dataTier).close();
    }
}