package com.alibaba.polardbx.gms.engine;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.oss.filesystem.OSSFileSystem;
import com.alibaba.polardbx.common.oss.filesystem.cache.FileMergeCachingFileSystem;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FileSystemUtilsTest {

    private FileSystemGroup fileSystemGroup;
    private FileSystem masterFileSystem;

    @Before
    public void setUp() throws IOException {
        // Create mock file system
        masterFileSystem = mock(FileMergeCachingFileSystem.class);
        fileSystemGroup = mock(FileSystemGroup.class);

        when(fileSystemGroup.getMaster()).thenReturn(masterFileSystem);
    }

    @Test
    public void testReadFileWithOptimizedOSS() throws IOException {
        // Mock the FileSystemManager to return our mock file system group
        try (MockedStatic<FileSystemManager> mockedFileSystemManager = mockStatic(FileSystemManager.class)) {
            mockedFileSystemManager.when(() -> FileSystemManager.getFileSystemGroup(any(Engine.class)))
                .thenReturn(fileSystemGroup);

            // Create a mock OSSFileSystem
            OSSFileSystem ossFileSystem = mock(OSSFileSystem.class);

            // Mock the FileMergeCachingFileSystem to return our mock OSSFileSystem as dataTier
            when(((FileMergeCachingFileSystem) masterFileSystem).getDataTier()).thenReturn(ossFileSystem);

            // Mock the optimized open method
            FSDataInputStream mockInputStream = mock(FSDataInputStream.class);
            when(((FileMergeCachingFileSystem) masterFileSystem).open(
                any(Path.class), anyLong(), anyLong())).thenReturn(mockInputStream);
            when(masterFileSystem.getWorkingDirectory()).thenReturn(new Path("/"));

            // Mock the readFully method to populate the output array
            byte[] testData = "Hello, World!".getBytes();
            Mockito.doAnswer(invocation -> {
                byte[] output = invocation.getArgument(0);
                System.arraycopy(testData, 0, output, 0, Math.min(testData.length, output.length));
                return testData.length;
            }).when(mockInputStream).readFully(any(byte[].class));

            // Test the readFile method
            byte[] output = new byte[13];
            FileSystemUtils.readFile("test-file.txt", 0, 13, output, Engine.OSS, false);

            // Verify the output
            Assert.assertArrayEquals(testData, output);
        }
    }

    @Test
    public void testReadFileWithFallback() throws IOException {
        // Mock the FileSystemManager to return our mock file system group
        try (MockedStatic<FileSystemManager> mockedFileSystemManager = mockStatic(FileSystemManager.class)) {
            mockedFileSystemManager.when(() -> FileSystemManager.getFileSystemGroup(any(Engine.class)))
                .thenReturn(fileSystemGroup);

            // Create a mock non-OSS file system
            FileSystem nonOssFileSystem = mock(FileSystem.class);
            when(fileSystemGroup.getMaster()).thenReturn(nonOssFileSystem);

            // Mock the open method
            FSDataInputStream mockInputStream = mock(FSDataInputStream.class);
            when(nonOssFileSystem.open(any(Path.class))).thenReturn(mockInputStream);
            when(nonOssFileSystem.getWorkingDirectory()).thenReturn(new Path("/"));

            // Mock the readFully method to populate the output array
            byte[] testData = "Hello, World!".getBytes();
            Mockito.doAnswer(invocation -> {
                byte[] output = invocation.getArgument(1);
                System.arraycopy(testData, 0, output, 0, Math.min(testData.length, output.length));
                return testData.length;
            }).when(mockInputStream).readFully(anyLong(), any(byte[].class), anyInt(), anyInt());

            // Test the readFile method
            byte[] output = new byte[13];
            FileSystemUtils.readFile("test-file.txt", 0, 13, output, Engine.LOCAL_DISK, false);

            // Verify the output
            Assert.assertArrayEquals(testData, output);
        }
    }

    // ---------- cacheOverride branches (new) ----------

    /**
     * When fileSystem is FileMergeCachingFileSystem backed by OSSFileSystem and
     * cacheOverride=false, readFile must bypass the caching wrapper and read
     * directly via OSSFileSystem.uncheckedOpen(.., Boolean.FALSE).
     */
    @Test
    public void testReadFileCacheOverrideFalseBypassesFileMergeCache() throws IOException {
        try (MockedStatic<FileSystemManager> mockedFsm = mockStatic(FileSystemManager.class)) {
            mockedFsm.when(() -> FileSystemManager.getFileSystemGroup(any(Engine.class)))
                .thenReturn(fileSystemGroup);

            OSSFileSystem ossFs = mock(OSSFileSystem.class);
            when(((FileMergeCachingFileSystem) masterFileSystem).getDataTier()).thenReturn(ossFs);
            when(masterFileSystem.getWorkingDirectory()).thenReturn(new Path("/"));

            FSDataInputStream mockIn = mock(FSDataInputStream.class);
            when(ossFs.uncheckedOpen(any(Path.class), anyLong(), eq(Boolean.FALSE))).thenReturn(mockIn);

            byte[] testData = "HelloFalseCache".getBytes();
            Mockito.doAnswer(invocation -> {
                byte[] output = invocation.getArgument(1);
                int offset = invocation.getArgument(2);
                int length = invocation.getArgument(3);
                System.arraycopy(testData, 0, output, offset, Math.min(length, testData.length));
                return null;
            }).when(mockIn).readFully(anyLong(), any(byte[].class), anyInt(), anyInt());

            byte[] out = new byte[testData.length];
            FileSystemUtils.readFile("a.txt", 0, testData.length, out, Engine.OSS, false, Boolean.FALSE);

            Assert.assertArrayEquals(testData, out);
            // The FileMergeCachingFileSystem.open(Path, long, long) MUST NOT be called on bypass.
            verify((FileMergeCachingFileSystem) masterFileSystem, never())
                .open(any(Path.class), anyLong(), anyLong());
            // The direct OSS uncheckedOpen must be invoked with Boolean.FALSE.
            verify(ossFs).uncheckedOpen(any(Path.class), eq((long) testData.length), eq(Boolean.FALSE));
        }
    }

    /**
     * When cacheOverride is null on the FileMergeCachingFileSystem path, the
     * original optimized path (cachingFileSystem.open(path, offset, length)) is used.
     */
    @Test
    public void testReadFileCacheOverrideNullUsesOptimizedCachePath() throws IOException {
        try (MockedStatic<FileSystemManager> mockedFsm = mockStatic(FileSystemManager.class)) {
            mockedFsm.when(() -> FileSystemManager.getFileSystemGroup(any(Engine.class)))
                .thenReturn(fileSystemGroup);

            OSSFileSystem ossFs = mock(OSSFileSystem.class);
            when(((FileMergeCachingFileSystem) masterFileSystem).getDataTier()).thenReturn(ossFs);
            when(masterFileSystem.getWorkingDirectory()).thenReturn(new Path("/"));

            FSDataInputStream mockIn = mock(FSDataInputStream.class);
            when(((FileMergeCachingFileSystem) masterFileSystem).open(any(Path.class), anyLong(), anyLong()))
                .thenReturn(mockIn);

            byte[] testData = "NullOverride!!".getBytes();
            Mockito.doAnswer(invocation -> {
                byte[] output = invocation.getArgument(0);
                System.arraycopy(testData, 0, output, 0, Math.min(testData.length, output.length));
                return null;
            }).when(mockIn).readFully(any(byte[].class));

            byte[] out = new byte[testData.length];
            FileSystemUtils.readFile("a.txt", 0, testData.length, out, Engine.OSS, false, null);

            Assert.assertArrayEquals(testData, out);
            verify(ossFs, never()).uncheckedOpen(any(Path.class), anyLong(), eq(Boolean.FALSE));
            verify((FileMergeCachingFileSystem) masterFileSystem)
                .open(any(Path.class), eq(0L), eq((long) testData.length));
        }
    }

    /**
     * When cacheOverride=TRUE falls through on FileMergeCachingFileSystem path,
     * the code still uses the optimized cache path (no bypass).
     */
    @Test
    public void testReadFileCacheOverrideTrueUsesOptimizedCachePath() throws IOException {
        try (MockedStatic<FileSystemManager> mockedFsm = mockStatic(FileSystemManager.class)) {
            mockedFsm.when(() -> FileSystemManager.getFileSystemGroup(any(Engine.class)))
                .thenReturn(fileSystemGroup);

            OSSFileSystem ossFs = mock(OSSFileSystem.class);
            when(((FileMergeCachingFileSystem) masterFileSystem).getDataTier()).thenReturn(ossFs);
            when(masterFileSystem.getWorkingDirectory()).thenReturn(new Path("/"));

            FSDataInputStream mockIn = mock(FSDataInputStream.class);
            when(((FileMergeCachingFileSystem) masterFileSystem).open(any(Path.class), anyLong(), anyLong()))
                .thenReturn(mockIn);

            byte[] testData = "TrueOverride!!".getBytes();
            Mockito.doAnswer(invocation -> {
                byte[] output = invocation.getArgument(0);
                System.arraycopy(testData, 0, output, 0, Math.min(testData.length, output.length));
                return null;
            }).when(mockIn).readFully(any(byte[].class));

            byte[] out = new byte[testData.length];
            FileSystemUtils.readFile("a.txt", 5, testData.length, out, Engine.OSS, false, Boolean.TRUE);

            Assert.assertArrayEquals(testData, out);
            verify(ossFs, never()).uncheckedOpen(any(Path.class), anyLong(), eq(Boolean.FALSE));
            verify((FileMergeCachingFileSystem) masterFileSystem)
                .open(any(Path.class), eq(5L), eq((long) testData.length));
        }
    }

    /**
     * Bare OSSFileSystem path forwards the cacheOverride to uncheckedOpen.
     */
    @Test
    public void testReadFileOssFileSystemForwardsCacheOverride() throws IOException {
        try (MockedStatic<FileSystemManager> mockedFsm = mockStatic(FileSystemManager.class)) {
            mockedFsm.when(() -> FileSystemManager.getFileSystemGroup(any(Engine.class)))
                .thenReturn(fileSystemGroup);

            OSSFileSystem ossFs = mock(OSSFileSystem.class);
            when(fileSystemGroup.getMaster()).thenReturn(ossFs);
            when(ossFs.getWorkingDirectory()).thenReturn(new Path("/"));

            FSDataInputStream mockIn = mock(FSDataInputStream.class);
            when(ossFs.uncheckedOpen(any(Path.class), anyLong(), any())).thenReturn(mockIn);

            byte[] testData = "BareOssOverride".getBytes();
            Mockito.doAnswer(invocation -> {
                byte[] output = invocation.getArgument(1);
                int offset = invocation.getArgument(2);
                int length = invocation.getArgument(3);
                System.arraycopy(testData, 0, output, offset, Math.min(length, testData.length));
                return null;
            }).when(mockIn).readFully(anyLong(), any(byte[].class), anyInt(), anyInt());

            byte[] out = new byte[testData.length];
            FileSystemUtils.readFile("b.txt", 0, testData.length, out, Engine.OSS, false, Boolean.FALSE);

            Assert.assertArrayEquals(testData, out);
            verify(ossFs).uncheckedOpen(any(Path.class), eq((long) testData.length), eq(Boolean.FALSE));
        }
    }

    /**
     * Fallback branch: DynamicCacheFileSystem path with non-null cacheOverride
     * uses the 3-arg overload that accepts an explicit override.
     */
    @Test
    public void testReadFileDynamicCacheFileSystemOverrideBranch() throws IOException {
        try (MockedStatic<FileSystemManager> mockedFsm = mockStatic(FileSystemManager.class)) {
            mockedFsm.when(() -> FileSystemManager.getFileSystemGroup(any(Engine.class)))
                .thenReturn(fileSystemGroup);

            DynamicCacheFileSystem dynFs = mock(DynamicCacheFileSystem.class);
            when(fileSystemGroup.getMaster()).thenReturn(dynFs);
            when(dynFs.getWorkingDirectory()).thenReturn(new Path("/"));
            Configuration conf = new Configuration();
            conf.setInt("io.file.buffer.size", 8192);
            when(dynFs.getConf()).thenReturn(conf);

            FSDataInputStream mockIn = mock(FSDataInputStream.class);
            when(dynFs.open(any(Path.class), anyInt(), any(Boolean.class))).thenReturn(mockIn);

            byte[] testData = "DynamicFallback".getBytes();
            Mockito.doAnswer(invocation -> {
                byte[] output = invocation.getArgument(1);
                int offset = invocation.getArgument(2);
                int length = invocation.getArgument(3);
                System.arraycopy(testData, 0, output, offset, Math.min(length, testData.length));
                return null;
            }).when(mockIn).readFully(anyLong(), any(byte[].class), anyInt(), anyInt());

            byte[] out = new byte[testData.length];
            FileSystemUtils.readFile("c.txt", 0, testData.length, out, Engine.LOCAL_DISK, false, Boolean.FALSE);

            Assert.assertArrayEquals(testData, out);
            verify(dynFs).open(any(Path.class), eq(8192), eq(Boolean.FALSE));
        }
    }

    /**
     * The 6-arg overload must delegate to the 7-arg overload with cacheOverride=null.
     */
    @Test
    public void testReadFileLegacyOverloadPassesNullOverride() throws IOException {
        try (MockedStatic<FileSystemManager> mockedFsm = mockStatic(FileSystemManager.class)) {
            mockedFsm.when(() -> FileSystemManager.getFileSystemGroup(any(Engine.class)))
                .thenReturn(fileSystemGroup);

            OSSFileSystem ossFs = mock(OSSFileSystem.class);
            when(((FileMergeCachingFileSystem) masterFileSystem).getDataTier()).thenReturn(ossFs);
            when(masterFileSystem.getWorkingDirectory()).thenReturn(new Path("/"));

            FSDataInputStream mockIn = mock(FSDataInputStream.class);
            when(((FileMergeCachingFileSystem) masterFileSystem).open(any(Path.class), anyLong(), anyLong()))
                .thenReturn(mockIn);

            byte[] testData = "LegacyEntryPoint".getBytes();
            Mockito.doAnswer(invocation -> {
                byte[] output = invocation.getArgument(0);
                System.arraycopy(testData, 0, output, 0, Math.min(testData.length, output.length));
                return null;
            }).when(mockIn).readFully(any(byte[].class));

            byte[] out = new byte[testData.length];
            FileSystemUtils.readFile("d.txt", 0, testData.length, out, Engine.OSS, false);

            // No bypass should have happened under the legacy overload (cacheOverride=null).
            verify(ossFs, never()).uncheckedOpen(any(Path.class), anyLong(), eq(Boolean.FALSE));
            Assert.assertArrayEquals(testData, out);
        }
    }
}