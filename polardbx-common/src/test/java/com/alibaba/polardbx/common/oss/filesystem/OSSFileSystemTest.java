package com.alibaba.polardbx.common.oss.filesystem;

import com.alibaba.polardbx.common.orc.FileStatusManager;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.ObjectMetadata;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CreateFlag;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileAlreadyExistsException;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathIOException;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.util.BlockingThreadPoolExecutorService;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Comprehensive unit tests for OSSFileSystem covering all major code paths.
 */
public class OSSFileSystemTest {

    private OSSFileSystem fileSystem;
    private OSSFileSystemStore mockStore;
    private FileStatusManager mockFileStatusManager;
    private FileSystemRateLimiter mockRateLimiter;
    private BlockingThreadPoolExecutorService mockThreadPool;
    private BlockingThreadPoolExecutorService mockCopyThreadPool;
    private Configuration conf;
    private MockedStatic<OSSCacheAdapter> mockedCacheAdapter;
    private MockedStatic<DynamicConfig> mockedDynamicConfig;
    private DynamicConfig mockDynamicConfigInstance;

    private static final URI TEST_URI = URI.create("oss://test-bucket");
    private static final String TEST_USERNAME = "testuser";

    @Before
    public void setUp() throws Exception {
        mockFileStatusManager = mock(FileStatusManager.class);
        mockRateLimiter = mock(FileSystemRateLimiter.class);
        mockStore = mock(OSSFileSystemStore.class);
        mockThreadPool = mock(BlockingThreadPoolExecutorService.class);
        mockCopyThreadPool = mock(BlockingThreadPoolExecutorService.class);

        mockedDynamicConfig = Mockito.mockStatic(DynamicConfig.class);
        mockDynamicConfigInstance = mock(DynamicConfig.class);
        mockedDynamicConfig.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfigInstance);
        when(mockDynamicConfigInstance.getOssMaxReadAheadPartNumber()).thenReturn(4);
        when(mockDynamicConfigInstance.ossTransferPoolSize()).thenReturn(256);

        mockedCacheAdapter = Mockito.mockStatic(OSSCacheAdapter.class);
        mockedCacheAdapter.when(OSSCacheAdapter::getInstanceOrNull).thenReturn(null);

        fileSystem = new OSSFileSystem(mockFileStatusManager, mockRateLimiter);

        setField(fileSystem, "uri", TEST_URI);
        setField(fileSystem, "bucket", "test-bucket");
        setField(fileSystem, "username", TEST_USERNAME);
        setField(fileSystem, "workingDir", new Path("/user/testuser"));
        setField(fileSystem, "store", mockStore);
        setField(fileSystem, "maxKeys", 1000);
        setField(fileSystem, "maxConcurrentCopyTasksPerDir", 5);
        setField(fileSystem, "boundedThreadPool", mockThreadPool);
        setField(fileSystem, "boundedCopyThreadPool", mockCopyThreadPool);
        setField(fileSystem, "blockOutputActiveBlocks", 4);

        conf = new Configuration();
        conf.set(Constants.FS_OSS_BLOCK_SIZE_KEY, String.valueOf(64 * 1024 * 1024));
        fileSystem.setConf(conf);
    }

    @After
    public void tearDown() {
        if (mockedCacheAdapter != null) {
            mockedCacheAdapter.close();
        }
        if (mockedDynamicConfig != null) {
            mockedDynamicConfig.close();
        }
    }

    // ===================== Reflection / type checks =====================

    @Test
    public void testGetBoundedThreadPoolMethod() {
        try {
            java.lang.reflect.Method method = OSSFileSystem.class.getMethod("getBoundedThreadPool");
            Assert.assertNotNull("getBoundedThreadPool method should exist", method);
            Assert.assertEquals(BlockingThreadPoolExecutorService.class, method.getReturnType());
        } catch (NoSuchMethodException e) {
            Assert.fail("getBoundedThreadPool method should exist: " + e.getMessage());
        }
    }

    @Test
    public void testOssTransferPoolSizeUsage() {
        int poolSize = DynamicConfig.getInstance().ossTransferPoolSize();
        Assert.assertEquals(256, poolSize);
    }

    @Test
    public void testBoundedThreadPoolTypeChange() {
        try {
            java.lang.reflect.Field field = OSSFileSystem.class.getDeclaredField("boundedThreadPool");
            field.setAccessible(true);
            Assert.assertEquals(BlockingThreadPoolExecutorService.class, field.getType());
        } catch (NoSuchFieldException e) {
            Assert.fail("boundedThreadPool field should exist: " + e.getMessage());
        }
    }

    // ===================== trimSlash tests =====================

    @Test
    public void testTrimSlashNormal() {
        Assert.assertEquals("abc", OSSFileSystem.trimSlash("abc"));
    }

    @Test
    public void testTrimSlashLeadingSlashes() {
        Assert.assertEquals("abc", OSSFileSystem.trimSlash("/abc"));
        Assert.assertEquals("abc", OSSFileSystem.trimSlash("//abc"));
        Assert.assertEquals("abc", OSSFileSystem.trimSlash("///abc"));
    }

    @Test
    public void testTrimSlashTrailingSlashes() {
        Assert.assertEquals("abc", OSSFileSystem.trimSlash("abc/"));
        Assert.assertEquals("abc", OSSFileSystem.trimSlash("abc//"));
        Assert.assertEquals("abc", OSSFileSystem.trimSlash("abc///"));
    }

    @Test
    public void testTrimSlashBothSides() {
        Assert.assertEquals("abc", OSSFileSystem.trimSlash("/abc/"));
        Assert.assertEquals("dir/file", OSSFileSystem.trimSlash("/dir/file/"));
        Assert.assertEquals("a/b/c", OSSFileSystem.trimSlash("//a/b/c//"));
    }

    @Test
    public void testTrimSlashNull() {
        Assert.assertNull(OSSFileSystem.trimSlash(null));
    }

    @Test
    public void testTrimSlashEmpty() {
        Assert.assertEquals("", OSSFileSystem.trimSlash(""));
    }

    @Test
    public void testTrimSlashOnlySlashes() {
        Assert.assertEquals("", OSSFileSystem.trimSlash("/"));
        Assert.assertEquals("", OSSFileSystem.trimSlash("//"));
        Assert.assertEquals("", OSSFileSystem.trimSlash("///"));
    }

    @Test
    public void testTrimSlashMiddleSlashesPreserved() {
        Assert.assertEquals("a/b/c", OSSFileSystem.trimSlash("a/b/c"));
        Assert.assertEquals("instId/dir/file.orc", OSSFileSystem.trimSlash("/instId/dir/file.orc/"));
    }

    // ===================== getScheme / getUri / getDefaultPort / getCanonicalServiceName =====================

    @Test
    public void testGetScheme() {
        Assert.assertEquals("oss", fileSystem.getScheme());
    }

    @Test
    public void testGetUri() {
        Assert.assertEquals(TEST_URI, fileSystem.getUri());
    }

    @Test
    public void testGetDefaultPort() {
        Assert.assertEquals(Constants.OSS_DEFAULT_PORT, fileSystem.getDefaultPort());
    }

    @Test
    public void testGetCanonicalServiceName() {
        Assert.assertNull(fileSystem.getCanonicalServiceName());
    }

    // ===================== getRateLimiter =====================

    @Test
    public void testGetRateLimiter() {
        Assert.assertSame(mockRateLimiter, fileSystem.getRateLimiter());
    }

    // ===================== getWorkingDirectory / setWorkingDirectory =====================

    @Test
    public void testGetWorkingDirectory() {
        Assert.assertEquals(new Path("/user/testuser"), fileSystem.getWorkingDirectory());
    }

    @Test
    public void testSetWorkingDirectory() {
        Path newDir = new Path("/new/working/dir");
        fileSystem.setWorkingDirectory(newDir);
        Assert.assertEquals(newDir, fileSystem.getWorkingDirectory());
    }

    // ===================== pathToKey / keyToPath =====================

    @Test
    public void testPathToKeyAbsolute() {
        Assert.assertEquals("dir/file.orc", fileSystem.pathToKey(new Path("/dir/file.orc")));
    }

    @Test
    public void testPathToKeyRelative() {
        fileSystem.setWorkingDirectory(new Path("/working"));
        Assert.assertEquals("working/file.orc", fileSystem.pathToKey(new Path("file.orc")));
    }

    @Test
    public void testPathToKeyRoot() {
        Assert.assertEquals("", fileSystem.pathToKey(new Path("/")));
    }

    @Test
    public void testKeyToPath() {
        Assert.assertEquals(new Path("/dir/file.orc"), fileSystem.keyToPath("dir/file.orc"));
    }

    // ===================== getDefaultBlockSize / getStore / getBoundedThreadPool =====================

    @Test
    public void testGetDefaultBlockSize() {
        Assert.assertEquals(64 * 1024 * 1024, fileSystem.getDefaultBlockSize());
    }

    @Test
    public void testGetStore() {
        Assert.assertSame(mockStore, fileSystem.getStore());
    }

    @Test
    public void testGetBoundedThreadPool() {
        Assert.assertSame(mockThreadPool, fileSystem.getBoundedThreadPool());
    }

    // ===================== getFileStatus =====================

    @Test
    public void testGetFileStatusFromManager() throws IOException {
        Path path = new Path("/test/file.orc");
        FileStatus expected = new OSSFileStatus(100, false, 1, 64 * 1024 * 1024, 1000L, path, TEST_USERNAME);
        when(mockFileStatusManager.getFileStatus(path)).thenReturn(expected);
        Assert.assertSame(expected, fileSystem.getFileStatus(path));
    }

    @Test
    public void testGetFileStatusManagerReturnsNull() throws IOException {
        Path path = new Path("/test/file.orc");
        when(mockFileStatusManager.getFileStatus(path)).thenReturn(null);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(100);
        meta.setLastModified(new Date(1000L));
        when(mockStore.getObjectMetadata("test/file.orc")).thenReturn(meta);

        FileStatus result = fileSystem.getFileStatus(path);
        Assert.assertFalse(result.isDirectory());
        Assert.assertEquals(100, result.getLen());
    }

    @Test
    public void testGetFileStatusManagerThrowsException() throws IOException {
        Path path = new Path("/test/file.orc");
        when(mockFileStatusManager.getFileStatus(path)).thenThrow(new RuntimeException("error"));
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(200);
        meta.setLastModified(new Date(2000L));
        when(mockStore.getObjectMetadata("test/file.orc")).thenReturn(meta);

        Assert.assertEquals(200, fileSystem.getFileStatus(path).getLen());
    }

    // ===================== getFileStatusImpl =====================

    @Test
    public void testGetFileStatusImplRoot() throws IOException {
        FileStatus result = fileSystem.getFileStatusImpl(new Path("/"));
        Assert.assertTrue(result.isDirectory());
        Assert.assertEquals(0, result.getLen());
    }

    @Test
    public void testGetFileStatusImplFileFound() throws IOException {
        Path path = new Path("/dir/file.orc");
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(1024);
        meta.setLastModified(new Date(5000L));
        when(mockStore.getObjectMetadata("dir/file.orc")).thenReturn(meta);

        FileStatus result = fileSystem.getFileStatusImpl(path);
        Assert.assertFalse(result.isDirectory());
        Assert.assertEquals(1024, result.getLen());
    }

    @Test
    public void testGetFileStatusImplDirectoryBySlash() throws IOException {
        Path path = new Path("/dir/subdir");
        when(mockStore.getObjectMetadata("dir/subdir")).thenReturn(null);
        ObjectMetadata meta = new ObjectMetadata();
        meta.setContentLength(0);
        meta.setLastModified(new Date(3000L));
        when(mockStore.getObjectMetadata("dir/subdir/")).thenReturn(meta);

        Assert.assertTrue(fileSystem.getFileStatusImpl(path).isDirectory());
    }

    @Test
    public void testGetFileStatusImplDirectoryByListing() throws IOException {
        Path path = new Path("/dir/subdir");
        when(mockStore.getObjectMetadata("dir/subdir")).thenReturn(null);
        when(mockStore.getObjectMetadata("dir/subdir/")).thenReturn(null);

        ObjectListing listing = mock(ObjectListing.class);
        List<OSSObjectSummary> summaries = new ArrayList<>();
        OSSObjectSummary summary = new OSSObjectSummary();
        summary.setKey("dir/subdir/file1.orc");
        summaries.add(summary);
        when(listing.getObjectSummaries()).thenReturn(summaries);
        when(listing.getCommonPrefixes()).thenReturn(Collections.emptyList());
        when(mockStore.listObjects("dir/subdir/", 1, null, false)).thenReturn(listing);

        Assert.assertTrue(fileSystem.getFileStatusImpl(path).isDirectory());
    }

    @Test
    public void testGetFileStatusImplDirectoryByCommonPrefixes() throws IOException {
        Path path = new Path("/dir/subdir");
        when(mockStore.getObjectMetadata("dir/subdir")).thenReturn(null);
        when(mockStore.getObjectMetadata("dir/subdir/")).thenReturn(null);

        ObjectListing listing = mock(ObjectListing.class);
        when(listing.getObjectSummaries()).thenReturn(Collections.emptyList());
        List<String> prefixes = new ArrayList<>();
        prefixes.add("dir/subdir/child/");
        when(listing.getCommonPrefixes()).thenReturn(prefixes);
        when(mockStore.listObjects("dir/subdir/", 1, null, false)).thenReturn(listing);

        Assert.assertTrue(fileSystem.getFileStatusImpl(path).isDirectory());
    }

    @Test(expected = FileNotFoundException.class)
    public void testGetFileStatusImplNotFound() throws IOException {
        Path path = new Path("/dir/noexist");
        when(mockStore.getObjectMetadata("dir/noexist")).thenReturn(null);
        when(mockStore.getObjectMetadata("dir/noexist/")).thenReturn(null);

        ObjectListing listing = mock(ObjectListing.class);
        when(listing.getObjectSummaries()).thenReturn(Collections.emptyList());
        when(listing.getCommonPrefixes()).thenReturn(Collections.emptyList());
        when(listing.isTruncated()).thenReturn(false);
        when(mockStore.listObjects("dir/noexist/", 1, null, false)).thenReturn(listing);

        fileSystem.getFileStatusImpl(path);
    }

    @Test
    public void testGetFileStatusImplTruncatedThenFound() throws IOException {
        Path path = new Path("/dir/subdir");
        when(mockStore.getObjectMetadata("dir/subdir")).thenReturn(null);
        when(mockStore.getObjectMetadata("dir/subdir/")).thenReturn(null);

        ObjectListing listing1 = mock(ObjectListing.class);
        when(listing1.getObjectSummaries()).thenReturn(Collections.emptyList());
        when(listing1.getCommonPrefixes()).thenReturn(Collections.emptyList());
        when(listing1.isTruncated()).thenReturn(true);
        when(listing1.getNextMarker()).thenReturn("marker1");
        when(mockStore.listObjects("dir/subdir/", 1, null, false)).thenReturn(listing1);

        ObjectListing listing2 = mock(ObjectListing.class);
        List<OSSObjectSummary> summaries = new ArrayList<>();
        OSSObjectSummary summary = new OSSObjectSummary();
        summary.setKey("dir/subdir/file.orc");
        summaries.add(summary);
        when(listing2.getObjectSummaries()).thenReturn(summaries);
        when(listing2.getCommonPrefixes()).thenReturn(Collections.emptyList());
        when(mockStore.listObjects("dir/subdir/", 1000, "marker1", false)).thenReturn(listing2);

        Assert.assertTrue(fileSystem.getFileStatusImpl(path).isDirectory());
    }

    // ===================== delete =====================

    @Test
    public void testDeleteFileNotFound() throws IOException {
        Path path = new Path("/noexist");
        when(mockFileStatusManager.getFileStatus(path)).thenThrow(new RuntimeException());
        when(mockStore.getObjectMetadata("noexist")).thenReturn(null);
        when(mockStore.getObjectMetadata("noexist/")).thenReturn(null);
        ObjectListing listing = mock(ObjectListing.class);
        when(listing.getObjectSummaries()).thenReturn(Collections.emptyList());
        when(listing.getCommonPrefixes()).thenReturn(Collections.emptyList());
        when(listing.isTruncated()).thenReturn(false);
        when(mockStore.listObjects("noexist/", 1, null, false)).thenReturn(listing);

        Assert.assertFalse(fileSystem.delete(path, false));
    }

    @Test
    public void testDeleteFile() throws IOException {
        Path path = new Path("/dir/file.orc");
        FileStatus fileStatus = new OSSFileStatus(100, false, 1, 64 * 1024 * 1024, 1000L, path, TEST_USERNAME);
        when(mockFileStatusManager.getFileStatus(path)).thenReturn(fileStatus);
        Path parentPath = path.getParent();
        when(mockFileStatusManager.getFileStatus(parentPath))
            .thenReturn(new OSSFileStatus(0, true, 1, 0, 0, parentPath, TEST_USERNAME));

        Assert.assertTrue(fileSystem.delete(path, false));
        verify(mockStore).deleteObject("dir/file.orc");
    }

    @Test
    public void testDeleteDirectoryRecursive() throws IOException {
        Path path = new Path("/dir/subdir");
        when(mockFileStatusManager.getFileStatus(path))
            .thenReturn(new OSSFileStatus(0, true, 1, 0, 0, path, TEST_USERNAME));
        Path parentPath = path.getParent();
        when(mockFileStatusManager.getFileStatus(parentPath))
            .thenReturn(new OSSFileStatus(0, true, 1, 0, 0, parentPath, TEST_USERNAME));

        Assert.assertTrue(fileSystem.delete(path, true));
        verify(mockStore).deleteDirs("dir/subdir");
    }

    @Test(expected = IOException.class)
    public void testDeleteNonEmptyDirectoryNonRecursive() throws IOException {
        Path path = new Path("/dir/subdir");
        when(mockFileStatusManager.getFileStatus(path))
            .thenReturn(new OSSFileStatus(0, true, 1, 0, 0, path, TEST_USERNAME));

        ObjectListing listing = mock(ObjectListing.class);
        List<OSSObjectSummary> summaries = new ArrayList<>();
        OSSObjectSummary s = new OSSObjectSummary();
        s.setKey("dir/subdir/file.orc");
        s.setSize(100);
        s.setLastModified(new Date());
        summaries.add(s);
        when(listing.getObjectSummaries()).thenReturn(summaries);
        when(listing.getCommonPrefixes()).thenReturn(Collections.emptyList());
        when(listing.isTruncated()).thenReturn(false);
        when(mockStore.listObjects("dir/subdir", 1000, null, false)).thenReturn(listing);

        fileSystem.delete(path, false);
    }

    @Test
    public void testDeleteEmptyDirectoryNonRecursive() throws IOException {
        Path path = new Path("/dir/emptydir");
        when(mockFileStatusManager.getFileStatus(path))
            .thenReturn(new OSSFileStatus(0, true, 1, 0, 0, path, TEST_USERNAME));

        ObjectListing listing = mock(ObjectListing.class);
        when(listing.getObjectSummaries()).thenReturn(Collections.emptyList());
        when(listing.getCommonPrefixes()).thenReturn(Collections.emptyList());
        when(listing.isTruncated()).thenReturn(false);
        when(mockStore.listObjects("dir/emptydir", 1000, null, false)).thenReturn(listing);

        Path parentPath = path.getParent();
        when(mockFileStatusManager.getFileStatus(parentPath))
            .thenReturn(new OSSFileStatus(0, true, 1, 0, 0, parentPath, TEST_USERNAME));

        Assert.assertTrue(fileSystem.delete(path, false));
        verify(mockStore).deleteObject("dir/emptydir/");
    }

    @Test
    public void testDeleteRootEmptyDirectory() throws IOException {
        Path path = new Path("/");
        when(mockFileStatusManager.getFileStatus(path))
            .thenReturn(new OSSFileStatus(0, true, 1, 0, 0, path, TEST_USERNAME));
        ObjectListing listing = mock(ObjectListing.class);
        when(listing.getObjectSummaries()).thenReturn(Collections.emptyList());
        when(listing.getCommonPrefixes()).thenReturn(Collections.emptyList());
        when(listing.isTruncated()).thenReturn(false);
        when(mockStore.listObjects("", 1000, null, false)).thenReturn(listing);

        Assert.assertTrue(fileSystem.delete(path, false));
    }

    @Test
    public void testDeleteRootNonEmptyRecursive() throws IOException {
        Path path = new Path("/");
        when(mockFileStatusManager.getFileStatus(path))
            .thenReturn(new OSSFileStatus(0, true, 1, 0, 0, path, TEST_USERNAME));
        ObjectListing listing = mock(ObjectListing.class);
        List<OSSObjectSummary> summaries = new ArrayList<>();
        OSSObjectSummary s = new OSSObjectSummary();
        s.setKey("file.txt");
        s.setSize(10);
        s.setLastModified(new Date());
        summaries.add(s);
        when(listing.getObjectSummaries()).thenReturn(summaries);
        when(listing.getCommonPrefixes()).thenReturn(Collections.emptyList());
        when(listing.isTruncated()).thenReturn(false);
        when(mockStore.listObjects("", 1000, null, false)).thenReturn(listing);

        Assert.assertFalse(fileSystem.delete(path, true));
    }

    @Test(expected = PathIOException.class)
    public void testDeleteRootNonEmptyNonRecursive() throws IOException {
        Path path = new Path("/");
        when(mockFileStatusManager.getFileStatus(path))
            .thenReturn(new OSSFileStatus(0, true, 1, 0, 0, path, TEST_USERNAME));
        ObjectListing listing = mock(ObjectListing.class);
        List<OSSObjectSummary> summaries = new ArrayList<>();
        OSSObjectSummary s = new OSSObjectSummary();
        s.setKey("file.txt");
        s.setSize(10);
        s.setLastModified(new Date());
        summaries.add(s);
        when(listing.getObjectSummaries()).thenReturn(summaries);
        when(listing.getCommonPrefixes()).thenReturn(Collections.emptyList());
        when(listing.isTruncated()).thenReturn(false);
        when(mockStore.listObjects("", 1000, null, false)).thenReturn(listing);

        fileSystem.delete(path, false);
    }

    // ===================== create =====================

    @Test
    public void testCreateNewFile() throws IOException {
        Path path = new Path("/dir/newfile.orc");
        when(mockFileStatusManager.getFileStatus(path)).thenThrow(new RuntimeException());
        when(mockStore.getObjectMetadata("dir/newfile.orc")).thenReturn(null);
        when(mockStore.getObjectMetadata("dir/newfile.orc/")).thenReturn(null);
        ObjectListing listing = mock(ObjectListing.class);
        when(listing.getObjectSummaries()).thenReturn(Collections.emptyList());
        when(listing.getCommonPrefixes()).thenReturn(Collections.emptyList());
        when(listing.isTruncated()).thenReturn(false);
        when(mockStore.listObjects("dir/newfile.orc/", 1, null, false)).thenReturn(listing);

        FSDataOutputStream out = fileSystem.create(path, FsPermission.getFileDefault(), true, 4096, (short) 1,
            64 * 1024 * 1024, null);
        Assert.assertNotNull(out);
        out.close();
    }

    @Test(expected = FileAlreadyExistsException.class)
    public void testCreateExistingFileNoOverwrite() throws IOException {
        Path path = new Path("/dir/existing.orc");
        when(mockFileStatusManager.getFileStatus(path))
            .thenReturn(new OSSFileStatus(100, false, 1, 64 * 1024 * 1024, 1000L, path, TEST_USERNAME));
        fileSystem.create(path, FsPermission.getFileDefault(), false, 4096, (short) 1, 64 * 1024 * 1024, null);
    }

    @Test(expected = FileAlreadyExistsException.class)
    public void testCreatePathIsDirectory() throws IOException {
        Path path = new Path("/dir/subdir");
        when(mockFileStatusManager.getFileStatus(path))
            .thenReturn(new OSSFileStatus(0, true, 1, 0, 0, path, TEST_USERNAME));
        fileSystem.create(path, FsPermission.getFileDefault(), true, 4096, (short) 1, 64 * 1024 * 1024, null);
    }

    @Test
    public void testCreateOverwriteExistingFile() throws IOException {
        Path path = new Path("/dir/existing.orc");
        when(mockFileStatusManager.getFileStatus(path))
            .thenReturn(new OSSFileStatus(100, false, 1, 64 * 1024 * 1024, 1000L, path, TEST_USERNAME));
        FSDataOutputStream out = fileSystem.create(path, FsPermission.getFileDefault(), true, 4096, (short) 1,
            64 * 1024 * 1024, null);
        Assert.assertNotNull(out);
        out.close();
    }

    // ===================== createNonRecursive =====================

    @Test(expected = FileAlreadyExistsException.class)
    public void testCreateNonRecursiveParentNotDirectory() throws IOException {
        Path path = new Path("/dir/file/child.orc");
        Path parent = path.getParent();
        when(mockFileStatusManager.getFileStatus(parent))
            .thenReturn(new OSSFileStatus(100, false, 1, 64 * 1024 * 1024, 1000L, parent, TEST_USERNAME));
        fileSystem.createNonRecursive(path, FsPermission.getFileDefault(),
            EnumSet.of(CreateFlag.CREATE), 4096, (short) 1, 64 * 1024 * 1024, null);
    }

    // ===================== mkdirs =====================

    @Test
    public void testMkdirsAlreadyExists() throws IOException {
        Path path = new Path("/dir/existing");
        when(mockFileStatusManager.getFileStatus(path))
            .thenReturn(new OSSFileStatus(0, true, 1, 0, 0, path, TEST_USERNAME));
        Assert.assertTrue(fileSystem.mkdirs(path, FsPermission.getDirDefault()));
    }

    @Test(expected = FileAlreadyExistsException.class)
    public void testMkdirsPathIsFile() throws IOException {
        Path path = new Path("/dir/file.orc");
        when(mockFileStatusManager.getFileStatus(path))
            .thenReturn(new OSSFileStatus(100, false, 1, 64 * 1024 * 1024, 1000L, path, TEST_USERNAME));
        fileSystem.mkdirs(path, FsPermission.getDirDefault());
    }

    @Test
    public void testMkdirsNewDirectory() throws IOException {
        Path path = new Path("/dir/newdir");
        when(mockFileStatusManager.getFileStatus(path)).thenThrow(new RuntimeException());
        when(mockStore.getObjectMetadata("dir/newdir")).thenReturn(null);
        when(mockStore.getObjectMetadata("dir/newdir/")).thenReturn(null);
        ObjectListing listing = mock(ObjectListing.class);
        when(listing.getObjectSummaries()).thenReturn(Collections.emptyList());
        when(listing.getCommonPrefixes()).thenReturn(Collections.emptyList());
        when(listing.isTruncated()).thenReturn(false);
        when(mockStore.listObjects("dir/newdir/", 1, null, false)).thenReturn(listing);

        Path parent = path.getParent();
        when(mockFileStatusManager.getFileStatus(parent))
            .thenReturn(new OSSFileStatus(0, true, 1, 0, 0, parent, TEST_USERNAME));

        Assert.assertTrue(fileSystem.mkdirs(path, FsPermission.getDirDefault()));
        verify(mockStore).storeEmptyFile("dir/newdir/");
    }

    // ===================== listStatus =====================

    @Test
    public void testListStatusDirectory() throws IOException {
        Path path = new Path("/dir");
        when(mockFileStatusManager.getFileStatus(path))
            .thenReturn(new OSSFileStatus(0, true, 1, 0, 0, path, TEST_USERNAME));

        ObjectListing listing = mock(ObjectListing.class);
        List<OSSObjectSummary> summaries = new ArrayList<>();
        OSSObjectSummary s = new OSSObjectSummary();
        s.setKey("dir/file1.orc");
        s.setSize(200);
        s.setLastModified(new Date(1000L));
        summaries.add(s);
        when(listing.getObjectSummaries()).thenReturn(summaries);
        when(listing.getCommonPrefixes()).thenReturn(Collections.emptyList());
        when(listing.isTruncated()).thenReturn(false);
        when(mockStore.listObjects("dir", 1000, null, false)).thenReturn(listing);

        FileStatus[] results = fileSystem.listStatus(path);
        Assert.assertEquals(1, results.length);
        Assert.assertFalse(results[0].isDirectory());
    }

    @Test
    public void testListStatusDirectoryIgnoresSelf() throws IOException {
        Path path = new Path("/dir");
        when(mockFileStatusManager.getFileStatus(path))
            .thenReturn(new OSSFileStatus(0, true, 1, 0, 0, path, TEST_USERNAME));

        ObjectListing listing = mock(ObjectListing.class);
        List<OSSObjectSummary> summaries = new ArrayList<>();
        OSSObjectSummary selfSummary = new OSSObjectSummary();
        selfSummary.setKey("dir/");
        selfSummary.setSize(0);
        selfSummary.setLastModified(new Date());
        summaries.add(selfSummary);
        OSSObjectSummary fileSummary = new OSSObjectSummary();
        fileSummary.setKey("dir/file.orc");
        fileSummary.setSize(500);
        fileSummary.setLastModified(new Date(2000L));
        summaries.add(fileSummary);
        when(listing.getObjectSummaries()).thenReturn(summaries);
        when(listing.getCommonPrefixes()).thenReturn(Collections.emptyList());
        when(listing.isTruncated()).thenReturn(false);
        when(mockStore.listObjects("dir", 1000, null, false)).thenReturn(listing);

        Assert.assertEquals(1, fileSystem.listStatus(path).length);
    }

    @Test
    public void testListStatusFile() throws IOException {
        Path path = new Path("/dir/file.orc");
        FileStatus fileStatus = new OSSFileStatus(100, false, 1, 64 * 1024 * 1024, 1000L, path, TEST_USERNAME);
        when(mockFileStatusManager.getFileStatus(path)).thenReturn(fileStatus);

        FileStatus[] results = fileSystem.listStatus(path);
        Assert.assertEquals(1, results.length);
        Assert.assertSame(fileStatus, results[0]);
    }

    @Test
    public void testListStatusWithCommonPrefixes() throws IOException {
        Path path = new Path("/dir");
        FileStatus dirStatus = new OSSFileStatus(0, true, 1, 0, 0, path, TEST_USERNAME);
        when(mockFileStatusManager.getFileStatus(path)).thenReturn(dirStatus);

        ObjectListing listing = mock(ObjectListing.class);
        when(listing.getObjectSummaries()).thenReturn(Collections.emptyList());
        List<String> prefixes = new ArrayList<>();
        prefixes.add("dir/subdir/");
        when(listing.getCommonPrefixes()).thenReturn(prefixes);
        when(listing.isTruncated()).thenReturn(false);
        when(mockStore.listObjects("dir", 1000, null, false)).thenReturn(listing);

        Path subdirPath = new Path("oss://test-bucket/dir/subdir/");
        FileStatus subdirStatus = new OSSFileStatus(0, true, 1, 0, 0, subdirPath, TEST_USERNAME);
        when(mockFileStatusManager.getFileStatus(any(Path.class))).thenReturn(dirStatus)
            .thenReturn(subdirStatus);

        Assert.assertEquals(1, fileSystem.listStatus(path).length);
    }

    @Test
    public void testListStatusTruncated() throws IOException {
        Path path = new Path("/dir");
        when(mockFileStatusManager.getFileStatus(path))
            .thenReturn(new OSSFileStatus(0, true, 1, 0, 0, path, TEST_USERNAME));

        ObjectListing listing1 = mock(ObjectListing.class);
        List<OSSObjectSummary> summaries1 = new ArrayList<>();
        OSSObjectSummary s1 = new OSSObjectSummary();
        s1.setKey("dir/file1.orc");
        s1.setSize(100);
        s1.setLastModified(new Date());
        summaries1.add(s1);
        when(listing1.getObjectSummaries()).thenReturn(summaries1);
        when(listing1.getCommonPrefixes()).thenReturn(Collections.emptyList());
        when(listing1.isTruncated()).thenReturn(true);
        when(listing1.getNextMarker()).thenReturn("marker1");
        when(mockStore.listObjects("dir", 1000, null, false)).thenReturn(listing1);

        ObjectListing listing2 = mock(ObjectListing.class);
        List<OSSObjectSummary> summaries2 = new ArrayList<>();
        OSSObjectSummary s2 = new OSSObjectSummary();
        s2.setKey("dir/file2.orc");
        s2.setSize(200);
        s2.setLastModified(new Date());
        summaries2.add(s2);
        when(listing2.getObjectSummaries()).thenReturn(summaries2);
        when(listing2.getCommonPrefixes()).thenReturn(Collections.emptyList());
        when(listing2.isTruncated()).thenReturn(false);
        when(mockStore.listObjects("dir", 1000, "marker1", false)).thenReturn(listing2);

        Assert.assertEquals(2, fileSystem.listStatus(path).length);
    }

    // ===================== rename =====================

    @Test
    public void testRenameRoot() throws IOException {
        Assert.assertFalse(fileSystem.rename(new Path("/"), new Path("/dst")));
    }

    @Test
    public void testRenameDstIsSubdirectoryOfSrc() throws IOException {
        Assert.assertFalse(fileSystem.rename(new Path("/parent"), new Path("/parent/child")));
    }

    @Test
    public void testRenameFileSuccess() throws IOException {
        Path srcPath = new Path("/dir/src.orc");
        Path dstPath = new Path("/dir/dst.orc");
        FileStatus srcStatus = new OSSFileStatus(100, false, 1, 64 * 1024 * 1024, 1000L, srcPath, TEST_USERNAME);
        when(mockFileStatusManager.getFileStatus(srcPath)).thenReturn(srcStatus);

        when(mockFileStatusManager.getFileStatus(dstPath)).thenThrow(new RuntimeException());
        when(mockStore.getObjectMetadata("dir/dst.orc")).thenReturn(null);
        when(mockStore.getObjectMetadata("dir/dst.orc/")).thenReturn(null);
        ObjectListing dstListing = mock(ObjectListing.class);
        when(dstListing.getObjectSummaries()).thenReturn(Collections.emptyList());
        when(dstListing.getCommonPrefixes()).thenReturn(Collections.emptyList());
        when(dstListing.isTruncated()).thenReturn(false);
        when(mockStore.listObjects("dir/dst.orc/", 1, null, false)).thenReturn(dstListing);

        Path dstParent = dstPath.getParent();
        when(mockFileStatusManager.getFileStatus(dstParent))
            .thenReturn(new OSSFileStatus(0, true, 1, 0, 0, dstParent, TEST_USERNAME));
        when(mockStore.copyFile("dir/src.orc", 100, "dir/dst.orc")).thenReturn(true);

        Assert.assertTrue(fileSystem.rename(srcPath, dstPath));
    }

    @Test(expected = IOException.class)
    public void testRenameDstParentIsFile() throws IOException {
        Path srcPath = new Path("/dir/src.orc");
        Path dstPath = new Path("/file/dst.orc");
        when(mockFileStatusManager.getFileStatus(srcPath))
            .thenReturn(new OSSFileStatus(100, false, 1, 64 * 1024 * 1024, 1000L, srcPath, TEST_USERNAME));

        when(mockFileStatusManager.getFileStatus(dstPath)).thenThrow(new RuntimeException());
        when(mockStore.getObjectMetadata("file/dst.orc")).thenReturn(null);
        when(mockStore.getObjectMetadata("file/dst.orc/")).thenReturn(null);
        ObjectListing dstListing = mock(ObjectListing.class);
        when(dstListing.getObjectSummaries()).thenReturn(Collections.emptyList());
        when(dstListing.getCommonPrefixes()).thenReturn(Collections.emptyList());
        when(dstListing.isTruncated()).thenReturn(false);
        when(mockStore.listObjects("file/dst.orc/", 1, null, false)).thenReturn(dstListing);

        Path dstParent = dstPath.getParent();
        when(mockFileStatusManager.getFileStatus(dstParent))
            .thenReturn(new OSSFileStatus(50, false, 1, 64 * 1024 * 1024, 1000L, dstParent, TEST_USERNAME));
        fileSystem.rename(srcPath, dstPath);
    }

    @Test(expected = FileAlreadyExistsException.class)
    public void testRenameDstExistsAsFile() throws IOException {
        Path srcPath = new Path("/dir/src.orc");
        Path dstPath = new Path("/dir/dst.orc");
        when(mockFileStatusManager.getFileStatus(srcPath))
            .thenReturn(new OSSFileStatus(100, false, 1, 64 * 1024 * 1024, 1000L, srcPath, TEST_USERNAME));
        when(mockFileStatusManager.getFileStatus(dstPath))
            .thenReturn(new OSSFileStatus(200, false, 1, 64 * 1024 * 1024, 2000L, dstPath, TEST_USERNAME));
        fileSystem.rename(srcPath, dstPath);
    }

    @Test
    public void testRenameSrcSameAsDstFile() throws IOException {
        Path path = new Path("/dir/file.orc");
        when(mockFileStatusManager.getFileStatus(path))
            .thenReturn(new OSSFileStatus(100, false, 1, 64 * 1024 * 1024, 1000L, path, TEST_USERNAME));
        Assert.assertTrue(fileSystem.rename(path, path));
    }

    @Test
    public void testRenameSrcSameAsDstDirectory() throws IOException {
        Path path = new Path("/dir/subdir");
        when(mockFileStatusManager.getFileStatus(path))
            .thenReturn(new OSSFileStatus(0, true, 1, 0, 0, path, TEST_USERNAME));
        Assert.assertFalse(fileSystem.rename(path, path));
    }

    // ===================== open =====================

    @Test(expected = FileNotFoundException.class)
    public void testOpenDirectory() throws IOException {
        Path path = new Path("/dir");
        when(mockFileStatusManager.getFileStatus(path))
            .thenReturn(new OSSFileStatus(0, true, 1, 0, 0, path, TEST_USERNAME));
        fileSystem.open(path, 4096);
    }

    @Test
    public void testOpenFileNoCacheAvailable() throws IOException {
        Path path = new Path("/dir/file.orc");
        when(mockFileStatusManager.getFileStatus(path))
            .thenReturn(new OSSFileStatus(1024, false, 1, 64 * 1024 * 1024, 1000L, path, TEST_USERNAME));
        org.apache.hadoop.fs.FSDataInputStream in = fileSystem.open(path, 4096);
        Assert.assertNotNull(in);
        in.close();
    }

    @Test(expected = IllegalStateException.class)
    public void testOpenFileCacheEnabledButKeyNotInPrefix() throws IOException {
        Path path = new Path("/dir/file.orc");
        when(mockFileStatusManager.getFileStatus(path))
            .thenReturn(new OSSFileStatus(1024, false, 1, 64 * 1024 * 1024, 1000L, path, TEST_USERNAME));

        OSSCacheAdapter mockAdapter = mock(OSSCacheAdapter.class);
        when(mockAdapter.isEnabled()).thenReturn(true);
        when(mockAdapter.tryOpenCachedStream(anyString(), Mockito.anyLong(), any(), any(), anyInt(), any()))
            .thenReturn(null);
        mockedCacheAdapter.when(OSSCacheAdapter::getInstanceOrNull).thenReturn(mockAdapter);
        mockedCacheAdapter.when(OSSCacheAdapter::isBypassDetectionEnabled).thenReturn(true);
        fileSystem.open(path, 4096);
    }

    // ===================== uncheckedOpen =====================

    @Test(expected = IllegalArgumentException.class)
    public void testUncheckedOpenNegativeContentLength() throws IOException {
        fileSystem.uncheckedOpen(new Path("/dir/file.orc"), -1);
    }

    @Test
    public void testUncheckedOpenNoCacheAvailable() throws IOException {
        org.apache.hadoop.fs.FSDataInputStream in = fileSystem.uncheckedOpen(new Path("/dir/file.orc"), 1024);
        Assert.assertNotNull(in);
        in.close();
    }

    @Test(expected = IllegalStateException.class)
    public void testUncheckedOpenCacheEnabledButKeyNotInPrefix() throws IOException {
        OSSCacheAdapter mockAdapter = mock(OSSCacheAdapter.class);
        when(mockAdapter.isEnabled()).thenReturn(true);
        when(mockAdapter.tryOpenCachedStream(anyString(), Mockito.anyLong(), any(), any(), anyInt(), any()))
            .thenReturn(null);
        mockedCacheAdapter.when(OSSCacheAdapter::getInstanceOrNull).thenReturn(mockAdapter);
        mockedCacheAdapter.when(OSSCacheAdapter::isBypassDetectionEnabled).thenReturn(true);
        fileSystem.uncheckedOpen(new Path("/dir/file.orc"), 1024);
    }

    @Test
    public void testOpenFileCacheEnabledBypassDetectionDisabledFallsThrough() throws IOException {
        // When bypassDetection is disabled (e.g. Columnar mode), a direct OSS read
        // must silently fall through to OSSInputStream instead of throwing.
        Path path = new Path("/dir/file.orc");
        when(mockFileStatusManager.getFileStatus(path))
            .thenReturn(new OSSFileStatus(1024, false, 1, 64 * 1024 * 1024, 1000L, path, TEST_USERNAME));

        OSSCacheAdapter mockAdapter = mock(OSSCacheAdapter.class);
        when(mockAdapter.isEnabled()).thenReturn(true);
        when(mockAdapter.tryOpenCachedStream(anyString(), Mockito.anyLong(), any(), any(), anyInt(), any()))
            .thenReturn(null);
        mockedCacheAdapter.when(OSSCacheAdapter::getInstanceOrNull).thenReturn(mockAdapter);
        mockedCacheAdapter.when(OSSCacheAdapter::isBypassDetectionEnabled).thenReturn(false);

        org.apache.hadoop.fs.FSDataInputStream in = fileSystem.open(path, 4096);
        Assert.assertNotNull(in);
        in.close();
    }

    @Test
    public void testUncheckedOpenCacheEnabledBypassDetectionDisabledFallsThrough() throws IOException {
        // Silent fall-through branch for uncheckedOpen when bypass detection is disabled.
        OSSCacheAdapter mockAdapter = mock(OSSCacheAdapter.class);
        when(mockAdapter.isEnabled()).thenReturn(true);
        when(mockAdapter.tryOpenCachedStream(anyString(), Mockito.anyLong(), any(), any(), anyInt(), any()))
            .thenReturn(null);
        mockedCacheAdapter.when(OSSCacheAdapter::getInstanceOrNull).thenReturn(mockAdapter);
        mockedCacheAdapter.when(OSSCacheAdapter::isBypassDetectionEnabled).thenReturn(false);

        org.apache.hadoop.fs.FSDataInputStream in = fileSystem.uncheckedOpen(new Path("/dir/file.orc"), 1024);
        Assert.assertNotNull(in);
        in.close();
    }

    // ===================== open/uncheckedOpen with cacheOverride =====================

    /**
     * cacheOverride=null must behave identically to the 2-arg open(), so when
     * DynamicConfig is on and prefix matches, the cached stream is returned.
     */
    @Test
    public void testOpenWithNullOverrideFallsBackToDynamicConfig() throws IOException {
        Path path = new Path("/dir/file.orc");
        when(mockFileStatusManager.getFileStatus(path))
            .thenReturn(new OSSFileStatus(1024, false, 1, 64 * 1024 * 1024, 1000L, path, TEST_USERNAME));

        OSSCacheAdapter mockAdapter = mock(OSSCacheAdapter.class);
        when(mockAdapter.isEnabled()).thenReturn(true);
        org.apache.hadoop.fs.FSDataInputStream cached = mock(org.apache.hadoop.fs.FSDataInputStream.class);
        when(mockAdapter.tryOpenCachedStream(anyString(), Mockito.anyLong(), any(), any(), anyInt(), any()))
            .thenReturn(cached);
        mockedCacheAdapter.when(OSSCacheAdapter::getInstanceOrNull).thenReturn(mockAdapter);

        org.apache.hadoop.fs.FSDataInputStream in = fileSystem.open(path, 4096, null);
        Assert.assertSame(cached, in);
    }

    /**
     * cacheOverride=TRUE must activate the cache branch via adapter.getCache().getRemoteStorageService()
     * regardless of DynamicConfig state.
     */
    @Test
    public void testOpenWithTrueOverrideUsesCacheWhenRemoteStorageAvailable() throws IOException {
        Path path = new Path("/dir/file.orc");
        when(mockFileStatusManager.getFileStatus(path))
            .thenReturn(new OSSFileStatus(1024, false, 1, 64 * 1024 * 1024, 1000L, path, TEST_USERNAME));

        OSSCacheAdapter mockAdapter = mock(OSSCacheAdapter.class);
        // isEnabled() is NOT consulted in the cacheOverride=true branch.
        when(mockAdapter.isEnabled()).thenReturn(false);
        com.alibaba.polardbx.cache.GeneralCache dummyCache =
            (com.alibaba.polardbx.cache.GeneralCache) java.lang.reflect.Proxy.newProxyInstance(
                com.alibaba.polardbx.cache.GeneralCache.class.getClassLoader(),
                new Class<?>[] {com.alibaba.polardbx.cache.GeneralCache.class},
                (proxy, method, args) -> {
                    if ("getRemoteStorageService".equals(method.getName())) {
                        return mock(com.alibaba.polardbx.cache.external.RemoteStorageService.class);
                    }
                    return null;
                });
        when(mockAdapter.getCache()).thenReturn(dummyCache);
        org.apache.hadoop.fs.FSDataInputStream cached = mock(org.apache.hadoop.fs.FSDataInputStream.class);
        when(mockAdapter.tryOpenCachedStream(anyString(), Mockito.anyLong(), any(), any(), anyInt(), any()))
            .thenReturn(cached);
        mockedCacheAdapter.when(OSSCacheAdapter::getInstanceOrNull).thenReturn(mockAdapter);

        org.apache.hadoop.fs.FSDataInputStream in = fileSystem.open(path, 4096, Boolean.TRUE);
        Assert.assertSame(cached, in);
    }

    /**
     * cacheOverride=FALSE must skip the cache entirely and NOT throw even when
     * DynamicConfig says cache is on with bypass detection enabled.
     */
    @Test
    public void testOpenWithFalseOverrideBypassesGuardEvenWhenCacheOn() throws IOException {
        Path path = new Path("/dir/file.orc");
        when(mockFileStatusManager.getFileStatus(path))
            .thenReturn(new OSSFileStatus(1024, false, 1, 64 * 1024 * 1024, 1000L, path, TEST_USERNAME));

        OSSCacheAdapter mockAdapter = mock(OSSCacheAdapter.class);
        when(mockAdapter.isEnabled()).thenReturn(true);
        mockedCacheAdapter.when(OSSCacheAdapter::getInstanceOrNull).thenReturn(mockAdapter);
        mockedCacheAdapter.when(OSSCacheAdapter::isBypassDetectionEnabled).thenReturn(true);

        // Should construct a raw OSSInputStream with allowBypass=true, no exception.
        org.apache.hadoop.fs.FSDataInputStream in = fileSystem.open(path, 4096, Boolean.FALSE);
        Assert.assertNotNull(in);
        in.close();
    }

    /**
     * cacheOverride=TRUE with remoteStorageService == null must fall through to the
     * "key not in any prefix" path. With bypass detection enabled this should throw.
     */
    @Test(expected = IllegalStateException.class)
    public void testOpenWithTrueOverrideNoRemoteStorageFallsThroughAndThrows() throws IOException {
        Path path = new Path("/dir/file.orc");
        when(mockFileStatusManager.getFileStatus(path))
            .thenReturn(new OSSFileStatus(1024, false, 1, 64 * 1024 * 1024, 1000L, path, TEST_USERNAME));

        OSSCacheAdapter mockAdapter = mock(OSSCacheAdapter.class);
        com.alibaba.polardbx.cache.GeneralCache dummyCache =
            (com.alibaba.polardbx.cache.GeneralCache) java.lang.reflect.Proxy.newProxyInstance(
                com.alibaba.polardbx.cache.GeneralCache.class.getClassLoader(),
                new Class<?>[] {com.alibaba.polardbx.cache.GeneralCache.class},
                (proxy, method, args) -> null);
        when(mockAdapter.getCache()).thenReturn(dummyCache);
        mockedCacheAdapter.when(OSSCacheAdapter::getInstanceOrNull).thenReturn(mockAdapter);

        // cacheActive branch becomes false because rss is null -> fall through to
        // "cache not available" direct OSS read; no override signal, no bypass -> OK.
        // But we set cacheOverride=TRUE, allowBypass = (override!=null && !override) = false.
        // Adapter.isEnabled() controls the downstream OSSInputStream guard.
        when(mockAdapter.isEnabled()).thenReturn(true);
        mockedCacheAdapter.when(OSSCacheAdapter::isBypassDetectionEnabled).thenReturn(true);

        fileSystem.open(path, 4096, Boolean.TRUE);
    }

    @Test
    public void testUncheckedOpenWithFalseOverrideBypassesGuard() throws IOException {
        OSSCacheAdapter mockAdapter = mock(OSSCacheAdapter.class);
        when(mockAdapter.isEnabled()).thenReturn(true);
        mockedCacheAdapter.when(OSSCacheAdapter::getInstanceOrNull).thenReturn(mockAdapter);
        mockedCacheAdapter.when(OSSCacheAdapter::isBypassDetectionEnabled).thenReturn(true);

        org.apache.hadoop.fs.FSDataInputStream in =
            fileSystem.uncheckedOpen(new Path("/dir/file.orc"), 1024, Boolean.FALSE);
        Assert.assertNotNull(in);
        in.close();
    }

    @Test
    public void testUncheckedOpenWithTrueOverrideUsesCache() throws IOException {
        OSSCacheAdapter mockAdapter = mock(OSSCacheAdapter.class);
        when(mockAdapter.isEnabled()).thenReturn(false);
        com.alibaba.polardbx.cache.GeneralCache dummyCache =
            (com.alibaba.polardbx.cache.GeneralCache) java.lang.reflect.Proxy.newProxyInstance(
                com.alibaba.polardbx.cache.GeneralCache.class.getClassLoader(),
                new Class<?>[] {com.alibaba.polardbx.cache.GeneralCache.class},
                (proxy, method, args) -> {
                    if ("getRemoteStorageService".equals(method.getName())) {
                        return mock(com.alibaba.polardbx.cache.external.RemoteStorageService.class);
                    }
                    return null;
                });
        when(mockAdapter.getCache()).thenReturn(dummyCache);
        org.apache.hadoop.fs.FSDataInputStream cached = mock(org.apache.hadoop.fs.FSDataInputStream.class);
        when(mockAdapter.tryOpenCachedStream(anyString(), Mockito.anyLong(), any(), any(), anyInt(), any()))
            .thenReturn(cached);
        mockedCacheAdapter.when(OSSCacheAdapter::getInstanceOrNull).thenReturn(mockAdapter);

        org.apache.hadoop.fs.FSDataInputStream in =
            fileSystem.uncheckedOpen(new Path("/dir/file.orc"), 1024, Boolean.TRUE);
        Assert.assertSame(cached, in);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testUncheckedOpenWithOverrideNegativeContentLength() throws IOException {
        fileSystem.uncheckedOpen(new Path("/dir/file.orc"), -1, Boolean.FALSE);
    }

    // ===================== close =====================

    @Test
    public void testClose() throws IOException {
        doNothing().when(mockStore).close();
        try {
            fileSystem.close();
        } catch (IOException e) {
            // super.close() might throw because FS is not properly initialized
        }
        verify(mockStore).close();
        verify(mockThreadPool).shutdown();
        verify(mockCopyThreadPool).shutdown();
    }

    // ===================== Helper methods =====================

    private void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = findField(target.getClass(), fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private Field findField(Class<?> clazz, String fieldName) throws NoSuchFieldException {
        Class<?> current = clazz;
        while (current != null) {
            try {
                return current.getDeclaredField(fieldName);
            } catch (NoSuchFieldException e) {
                current = current.getSuperclass();
            }
        }
        throw new NoSuchFieldException(fieldName);
    }
}
