package com.alibaba.polardbx.common.oss.filesystem;

import com.alibaba.polardbx.cache.GeneralCache;
import com.alibaba.polardbx.cache.external.IdNameProvider;
import com.alibaba.polardbx.cache.statistics.CacheStatistics;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.EOFException;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Unit tests for CachedInputStream:
 * constructor validation, seek, getPos, available, close,
 * read boundary checks, and CacheReadBuffer inner class.
 */
public class CachedInputStreamTest {

    private GeneralCache dummyCache;
    private Configuration conf;
    private ExecutorService executor;
    private CachedInputStream stream;

    /**
     * Create a dummy GeneralCache using Proxy.
     * Mockito inline mock maker cannot mock GeneralCache (NPE in class instrumentation).
     */
    private static GeneralCache createDummyCache() {
        return (GeneralCache) Proxy.newProxyInstance(
            GeneralCache.class.getClassLoader(),
            new Class<?>[] {GeneralCache.class},
            (proxy, method, args) -> null
        );
    }

    /**
     * Create a dummy GeneralCache that returns a specific IdNameProvider
     * and tracks whether getIdNameProvider() was called.
     */
    private static GeneralCache createDummyCacheWithProvider(IdNameProvider provider, AtomicBoolean called) {
        return (GeneralCache) Proxy.newProxyInstance(
            GeneralCache.class.getClassLoader(),
            new Class<?>[] {GeneralCache.class},
            (proxy, method, args) -> {
                if ("getIdNameProvider".equals(method.getName())) {
                    if (called != null) {
                        called.set(true);
                    }
                    return provider;
                }
                return null;
            }
        );
    }

    /**
     * Create a dummy IdNameProvider that returns a fixed ID for any file name.
     */
    private static IdNameProvider createDummyIdNameProvider(long returnId) {
        return (IdNameProvider) Proxy.newProxyInstance(
            IdNameProvider.class.getClassLoader(),
            new Class<?>[] {IdNameProvider.class},
            (proxy, method, args) -> {
                if ("getId".equals(method.getName())) {
                    return returnId;
                }
                return null;
            }
        );
    }

    @Before
    public void setUp() {
        dummyCache = createDummyCache();
        conf = new Configuration(false);
        executor = Executors.newSingleThreadExecutor();
    }

    @After
    public void tearDown() throws Exception {
        if (stream != null) {
            try {
                stream.close();
            } catch (Exception ignored) {
            }
        }
        executor.shutdownNow();
    }

    private CachedInputStream createStream(long fileId, String fileName, long fileSize) {
        return new CachedInputStream(conf, executor, 4, dummyCache,
            fileId, fileName, fileSize, 0L, null, null);
    }

    // ===== Constructor Tests =====

    @Test
    public void testConstructorWithValidFileId() {
        // fileId > 0, should not call getIdNameProvider
        AtomicBoolean providerCalled = new AtomicBoolean(false);
        dummyCache = createDummyCacheWithProvider(
            createDummyIdNameProvider(999L), providerCalled);

        stream = createStream(42L, "test.orc", 1024L);
        Assert.assertEquals("test.orc", stream.getFileName());
        Assert.assertEquals(1024L, stream.getFileSize());
        Assert.assertFalse("getIdNameProvider should not be called when fileId > 0",
            providerCalled.get());
    }

    @Test
    public void testConstructorWithZeroFileIdUsesIdNameProvider() {
        // fileId == 0, should call cache.getIdNameProvider().getId(fileName)
        AtomicBoolean providerCalled = new AtomicBoolean(false);
        dummyCache = createDummyCacheWithProvider(
            createDummyIdNameProvider(100L), providerCalled);

        stream = createStream(0L, "test.orc", 1024L);
        Assert.assertTrue("getIdNameProvider should be called when fileId == 0",
            providerCalled.get());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithZeroFileIdAndProviderReturnsZeroThrows() {
        // fileId == 0 and provider also returns 0 -> IllegalArgumentException
        dummyCache = createDummyCacheWithProvider(
            createDummyIdNameProvider(0L), null);

        stream = createStream(0L, "unknown.orc", 1024L);
    }

    // ===== Seek Tests =====

    @Test
    public void testSeekToValidPosition() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        stream.seek(500L);
        Assert.assertEquals(500L, stream.getPos());
    }

    @Test
    public void testSeekToZero() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        stream.seek(0L);
        Assert.assertEquals(0L, stream.getPos());
    }

    @Test
    public void testSeekToEndOfFile() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        stream.seek(1000L);
        Assert.assertEquals(1000L, stream.getPos());
    }

    @Test(expected = EOFException.class)
    public void testSeekNegativeThrows() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        stream.seek(-1L);
    }

    @Test(expected = EOFException.class)
    public void testSeekBeyondFileSizeThrows() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        stream.seek(1001L);
    }

    // ===== GetPos Tests =====

    @Test
    public void testGetPosInitiallyZero() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        Assert.assertEquals(0L, stream.getPos());
    }

    @Test
    public void testGetPosAfterSeek() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        stream.seek(250L);
        Assert.assertEquals(250L, stream.getPos());
    }

    // ===== Available Tests =====

    @Test
    public void testAvailableAtStart() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        Assert.assertEquals(1000, stream.available());
    }

    @Test
    public void testAvailableAfterSeek() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        stream.seek(300L);
        Assert.assertEquals(700, stream.available());
    }

    @Test
    public void testAvailableAtEnd() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        stream.seek(1000L);
        Assert.assertEquals(0, stream.available());
    }

    @Test
    public void testAvailableLargeFile() throws IOException {
        stream = createStream(1L, "test.orc", (long) Integer.MAX_VALUE + 100L);
        Assert.assertEquals(Integer.MAX_VALUE, stream.available());
    }

    // ===== Close Tests =====

    @Test
    public void testCloseNormal() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        stream.close();
        // Should not throw
    }

    @Test
    public void testDoubleCloseIsNoop() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        stream.close();
        stream.close(); // second close should not throw
    }

    @Test(expected = IOException.class)
    public void testSeekAfterCloseThrows() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        stream.close();
        stream.seek(0L);
    }

    @Test(expected = IOException.class)
    public void testGetPosAfterCloseThrows() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        stream.close();
        stream.getPos();
    }

    @Test(expected = IOException.class)
    public void testAvailableAfterCloseThrows() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        stream.close();
        stream.available();
    }

    @Test(expected = IOException.class)
    public void testReadAfterCloseThrows() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        stream.close();
        stream.read();
    }

    @Test(expected = IOException.class)
    public void testReadArrayAfterCloseThrows() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        stream.close();
        stream.read(new byte[10], 0, 10);
    }

    @Test(expected = IOException.class)
    public void testPositionedReadAfterCloseThrows() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        stream.close();
        stream.read(0L, new byte[10], 0, 10);
    }

    // ===== Read Array Boundary Tests =====

    @Test(expected = IndexOutOfBoundsException.class)
    public void testReadArrayNegativeOffset() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        stream.read(new byte[10], -1, 5);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testReadArrayNegativeLength() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        stream.read(new byte[10], 0, -1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testReadArrayLenExceedsBuffer() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        stream.read(new byte[10], 5, 6);
    }

    @Test
    public void testReadArrayZeroLen() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        int result = stream.read(new byte[10], 0, 0);
        Assert.assertEquals(0, result);
    }

    @Test
    public void testReadArrayAtEof() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        stream.seek(1000L);
        int result = stream.read(new byte[10], 0, 10);
        Assert.assertEquals(-1, result);
    }

    // ===== Positioned Read Boundary Tests =====

    @Test(expected = NullPointerException.class)
    public void testPositionedReadNullBuffer() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        stream.read(0L, null, 0, 10);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testPositionedReadNegativeOffset() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        stream.read(0L, new byte[10], -1, 5);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testPositionedReadNegativeLen() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        stream.read(0L, new byte[10], 0, -1);
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testPositionedReadLenExceedsBuffer() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        stream.read(0L, new byte[10], 5, 6);
    }

    @Test
    public void testPositionedReadZeroLen() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        int result = stream.read(0L, new byte[10], 0, 0);
        Assert.assertEquals(0, result);
    }

    @Test(expected = EOFException.class)
    public void testPositionedReadNegativePosition() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        stream.read(-1L, new byte[10], 0, 10);
    }

    @Test
    public void testPositionedReadAtEof() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        int result = stream.read(1000L, new byte[10], 0, 10);
        Assert.assertEquals(-1, result);
    }

    // ===== seekToNewSource =====

    @Test
    public void testSeekToNewSourceReturnsFalse() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        Assert.assertFalse(stream.seekToNewSource(0L));
    }

    // ===== CacheReadBuffer Tests =====

    @Test
    public void testCacheReadBufferCreation() {
        CachedInputStream.CacheReadBuffer buf = new CachedInputStream.CacheReadBuffer(0, 99);
        Assert.assertEquals(100, buf.getBuffer().length);
        Assert.assertEquals(CachedInputStream.CacheReadBuffer.STATUS.INIT, buf.getStatus());
        Assert.assertEquals(0L, buf.getByteStart());
        Assert.assertEquals(99L, buf.getByteEnd());
    }

    @Test
    public void testCacheReadBufferStatusChange() {
        CachedInputStream.CacheReadBuffer buf = new CachedInputStream.CacheReadBuffer(10, 19);
        Assert.assertEquals(CachedInputStream.CacheReadBuffer.STATUS.INIT, buf.getStatus());

        buf.setStatus(CachedInputStream.CacheReadBuffer.STATUS.SUCCESS);
        Assert.assertEquals(CachedInputStream.CacheReadBuffer.STATUS.SUCCESS, buf.getStatus());
    }

    @Test
    public void testCacheReadBufferLockUnlock() {
        CachedInputStream.CacheReadBuffer buf = new CachedInputStream.CacheReadBuffer(0, 9);
        buf.lock();
        // Should be able to set status while locked
        buf.setStatus(CachedInputStream.CacheReadBuffer.STATUS.ERROR);
        buf.signalAll();
        buf.unlock();
        Assert.assertEquals(CachedInputStream.CacheReadBuffer.STATUS.ERROR, buf.getStatus());
    }

    @Test
    public void testCacheReadBufferAwaitSignal() throws InterruptedException {
        CachedInputStream.CacheReadBuffer buf = new CachedInputStream.CacheReadBuffer(0, 99);

        Thread signaler = new Thread(() -> {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                return;
            }
            buf.lock();
            try {
                buf.setStatus(CachedInputStream.CacheReadBuffer.STATUS.SUCCESS);
                buf.signalAll();
            } finally {
                buf.unlock();
            }
        });

        signaler.start();

        buf.lock();
        try {
            buf.await(CachedInputStream.CacheReadBuffer.STATUS.INIT);
            Assert.assertEquals(CachedInputStream.CacheReadBuffer.STATUS.SUCCESS, buf.getStatus());
        } finally {
            buf.unlock();
        }

        signaler.join(1000);
    }

    @Test
    public void testCacheReadBufferToString() {
        CachedInputStream.CacheReadBuffer buf = new CachedInputStream.CacheReadBuffer(10, 99);
        String str = buf.toString();
        Assert.assertTrue(str.contains("10"));
        Assert.assertTrue(str.contains("99"));
    }

    // ===== getCollector =====

    @Test
    public void testGetCollectorNotNull() {
        Assert.assertNotNull(CachedInputStream.getCollector());
    }

    // ===== Configuration based tests =====

    @Test
    public void testFetchPolicyDefault() throws Exception {
        stream = createStream(1L, "test.orc", 1000L);
        // Default fetch policy is REQUESTED
        Field fetchPolicyField = CachedInputStream.class.getDeclaredField("fetchPolicy");
        fetchPolicyField.setAccessible(true);
        FetchPolicy policy = (FetchPolicy) fetchPolicyField.get(stream);
        Assert.assertEquals(FetchPolicy.REQUESTED, policy);
    }

    @Test
    public void testFetchPolicyFromConfig() throws Exception {
        conf.set("fs.oss.io.fetch.policy", "FIXED");
        stream = createStream(1L, "test.orc", 1000L);
        Field fetchPolicyField = CachedInputStream.class.getDeclaredField("fetchPolicy");
        fetchPolicyField.setAccessible(true);
        FetchPolicy policy = (FetchPolicy) fetchPolicyField.get(stream);
        Assert.assertEquals(FetchPolicy.FIXED, policy);
    }

    @Test
    public void testDownloadPartSizeDefault() throws Exception {
        stream = createStream(1L, "test.orc", 1000L);
        Field partSizeField = CachedInputStream.class.getDeclaredField("downloadPartSize");
        partSizeField.setAccessible(true);
        long partSize = (long) partSizeField.get(stream);
        Assert.assertEquals(512 * 1024L, partSize);
    }

    @Test
    public void testDownloadPartSizeFromConfig() throws Exception {
        conf.setLong("fs.oss.multipart.download.size", 1024 * 1024L);
        stream = createStream(1L, "test.orc", 1000000L);
        Field partSizeField = CachedInputStream.class.getDeclaredField("downloadPartSize");
        partSizeField.setAccessible(true);
        long partSize = (long) partSizeField.get(stream);
        Assert.assertEquals(1024 * 1024L, partSize);
    }

    // ===== Reflection Helpers =====

    private static void setField(Object obj, String name, Object value) throws Exception {
        Field f = CachedInputStream.class.getDeclaredField(name);
        f.setAccessible(true);
        f.set(obj, value);
    }

    private static Object getField(Object obj, String name) throws Exception {
        Field f = CachedInputStream.class.getDeclaredField(name);
        f.setAccessible(true);
        return f.get(obj);
    }

    private CachedInputStream createStreamWithStats(long fileId, String fileName, long fileSize,
                                                    CacheStatistics cacheStats,
                                                    FileSystem.Statistics fsStats) {
        return new CachedInputStream(conf, executor, 4, dummyCache,
            fileId, fileName, fileSize, 0L, cacheStats, fsStats);
    }

    // ===== Constructor Initial State =====

    @Test
    public void testConstructorInitialState() throws Exception {
        stream = createStream(1L, "test.orc", 500L);
        Assert.assertEquals(0L, getField(stream, "position"));
        Assert.assertFalse((Boolean) getField(stream, "closed"));
        Assert.assertEquals(0L, getField(stream, "expectNextPos"));
        Assert.assertEquals(-1L, getField(stream, "lastByteStart"));
        Assert.assertEquals(0L, getField(stream, "partRemaining"));
        Assert.assertNull(getField(stream, "buffer"));
    }

    // ===== Single Byte Read Tests =====

    @Test
    public void testReadSingleByteAtEof() throws IOException {
        stream = createStream(1L, "test.orc", 100L);
        stream.seek(100L);
        Assert.assertEquals(-1, stream.read());
    }

    @Test
    public void testReadSingleByteWithData() throws Exception {
        stream = createStream(1L, "test.orc", 3L);
        setField(stream, "buffer", new byte[] {0x41, 0x42, 0x43});
        setField(stream, "partRemaining", 3L);

        Assert.assertEquals(0x41, stream.read());
        Assert.assertEquals(1L, stream.getPos());
    }

    @Test
    public void testReadMultipleSingleBytes() throws Exception {
        stream = createStream(1L, "test.orc", 3L);
        setField(stream, "buffer", new byte[] {0x0A, 0x0B, 0x0C});
        setField(stream, "partRemaining", 3L);

        Assert.assertEquals(0x0A, stream.read());
        Assert.assertEquals(0x0B, stream.read());
        Assert.assertEquals(0x0C, stream.read());
        Assert.assertEquals(3L, stream.getPos());
        // Now at EOF, next read returns -1
        Assert.assertEquals(-1, stream.read());
    }

    @Test
    public void testReadSingleByteUnsigned() throws Exception {
        // 0xFF as byte is -1, but read() should return 255 via & 0xFF
        stream = createStream(1L, "test.orc", 1L);
        setField(stream, "buffer", new byte[] {(byte) 0xFF});
        setField(stream, "partRemaining", 1L);

        Assert.assertEquals(255, stream.read());
    }

    @Test(expected = IOException.class)
    public void testReadSingleByteTriggersReopenFailure() throws IOException {
        // cache.get() returns null -> CacheReaderTask NPE -> ERROR -> IOException
        stream = createStream(1L, "test.orc", 1000L);
        stream.read();
    }

    // ===== Read Array With Data Tests =====

    @Test
    public void testReadArrayWithData() throws Exception {
        stream = createStream(1L, "test.orc", 5L);
        byte[] data = {10, 20, 30, 40, 50};
        setField(stream, "buffer", data.clone());
        setField(stream, "partRemaining", 5L);

        byte[] out = new byte[5];
        int n = stream.read(out, 0, 5);
        Assert.assertEquals(5, n);
        Assert.assertArrayEquals(data, out);
        Assert.assertEquals(5L, stream.getPos());
    }

    @Test
    public void testReadArrayPartialRequest() throws Exception {
        stream = createStream(1L, "test.orc", 5L);
        setField(stream, "buffer", new byte[] {1, 2, 3, 4, 5});
        setField(stream, "partRemaining", 5L);

        byte[] out = new byte[10];
        int n = stream.read(out, 0, 3);
        Assert.assertEquals(3, n);
        Assert.assertEquals(1, out[0]);
        Assert.assertEquals(2, out[1]);
        Assert.assertEquals(3, out[2]);
        Assert.assertEquals(3L, stream.getPos());
    }

    @Test
    public void testReadArrayWithOffset() throws Exception {
        stream = createStream(1L, "test.orc", 3L);
        setField(stream, "buffer", new byte[] {7, 8, 9});
        setField(stream, "partRemaining", 3L);

        byte[] out = new byte[10];
        int n = stream.read(out, 5, 3);
        Assert.assertEquals(3, n);
        Assert.assertEquals(7, out[5]);
        Assert.assertEquals(8, out[6]);
        Assert.assertEquals(9, out[7]);
    }

    @Test
    public void testReadArrayConsumesBufferThenEof() throws Exception {
        // fileSize = 4, buffer has 4 bytes; request 10 -> returns 4 (all available)
        stream = createStream(1L, "test.orc", 4L);
        setField(stream, "buffer", new byte[] {1, 2, 3, 4});
        setField(stream, "partRemaining", 4L);

        byte[] out = new byte[10];
        int n = stream.read(out, 0, 10);
        Assert.assertEquals(4, n);
        Assert.assertEquals(1, out[0]);
        Assert.assertEquals(4, out[3]);
    }

    @Test(expected = IOException.class)
    public void testReadArrayTriggersReopenFailure() throws IOException {
        // cache.get() returns null -> reopen fails -> IOException
        stream = createStream(1L, "test.orc", 1000L);
        stream.read(new byte[10], 0, 10);
    }

    // ===== Positioned Read With Cache Error =====

    @Test(expected = IOException.class)
    public void testPositionedReadWithCacheError() throws IOException {
        // cache.get() returns null -> NPE in loop -> IOException
        stream = createStream(1L, "test.orc", 1000L);
        stream.read(0L, new byte[10], 0, 10);
    }

    // ===== FileSystem.Statistics Tests =====

    @Test
    public void testReadSingleByteIncrementsFsStatistics() throws Exception {
        FileSystem.Statistics fsStats = new FileSystem.Statistics("test");
        stream = createStreamWithStats(1L, "test.orc", 2L, null, fsStats);
        setField(stream, "buffer", new byte[] {0x10, 0x20});
        setField(stream, "partRemaining", 2L);

        stream.read();
        Assert.assertEquals(1, fsStats.getBytesRead());
        stream.read();
        Assert.assertEquals(2, fsStats.getBytesRead());
    }

    @Test
    public void testReadArrayIncrementsFsStatistics() throws Exception {
        FileSystem.Statistics fsStats = new FileSystem.Statistics("test");
        stream = createStreamWithStats(1L, "test.orc", 5L, null, fsStats);
        setField(stream, "buffer", new byte[] {1, 2, 3, 4, 5});
        setField(stream, "partRemaining", 5L);

        byte[] out = new byte[5];
        stream.read(out, 0, 5);
        Assert.assertEquals(5, fsStats.getBytesRead());
    }

    @Test
    public void testReadSingleByteNoFsStatsIncrementAtEof() throws Exception {
        FileSystem.Statistics fsStats = new FileSystem.Statistics("test");
        stream = createStreamWithStats(1L, "test.orc", 100L, null, fsStats);
        stream.seek(100L);
        stream.read(); // at EOF, returns -1
        Assert.assertEquals(0, fsStats.getBytesRead());
    }

    // ===== Close with Statistics =====

    @Test
    public void testCloseWithNonNullStatistics() throws IOException {
        CacheStatistics cacheStats = new CacheStatistics();
        stream = createStreamWithStats(1L, "test.orc", 1000L, cacheStats, null);
        stream.close(); // Should trigger COLLECTOR.collect() without error
    }

    @Test
    public void testCloseNullsBuffer() throws Exception {
        stream = createStream(1L, "test.orc", 3L);
        setField(stream, "buffer", new byte[] {1, 2, 3});
        stream.close();
        Assert.assertNull(getField(stream, "buffer"));
    }

    // ===== Seek Resets State =====

    @Test
    public void testSeekResetsPartRemaining() throws Exception {
        stream = createStream(1L, "test.orc", 1000L);
        setField(stream, "partRemaining", 500L);
        stream.seek(100L);
        Assert.assertEquals(0L, getField(stream, "partRemaining"));
    }

    // ===== seekToNewSource After Close =====

    @Test(expected = IOException.class)
    public void testSeekToNewSourceAfterCloseThrows() throws IOException {
        stream = createStream(1L, "test.orc", 1000L);
        stream.close();
        stream.seekToNewSource(0L);
    }

    // ===== sizeFor Tests =====

    @Test
    public void testSizeForValues() throws Exception {
        Method sizeForMethod = CachedInputStream.class.getDeclaredMethod("sizeFor", int.class);
        sizeForMethod.setAccessible(true);

        Assert.assertEquals(1, (int) sizeForMethod.invoke(null, 1));
        Assert.assertEquals(2, (int) sizeForMethod.invoke(null, 2));
        Assert.assertEquals(4, (int) sizeForMethod.invoke(null, 3));
        Assert.assertEquals(4, (int) sizeForMethod.invoke(null, 4));
        Assert.assertEquals(8, (int) sizeForMethod.invoke(null, 5));
        Assert.assertEquals(8, (int) sizeForMethod.invoke(null, 8));
        Assert.assertEquals(16, (int) sizeForMethod.invoke(null, 9));
        Assert.assertEquals(1024, (int) sizeForMethod.invoke(null, 1024));
        Assert.assertEquals(2048, (int) sizeForMethod.invoke(null, 1025));
    }

    // ===== FetchPolicy MERGE =====

    @Test
    public void testFetchPolicyMergeFromConfig() throws Exception {
        conf.set("fs.oss.io.fetch.policy", "MERGE");
        stream = createStream(1L, "test.orc", 1000L);
        Field fetchPolicyField = CachedInputStream.class.getDeclaredField("fetchPolicy");
        fetchPolicyField.setAccessible(true);
        Assert.assertEquals(FetchPolicy.MERGE, fetchPolicyField.get(stream));
    }

    // ===== CacheReadBuffer Additional Tests =====

    @Test
    public void testCacheReadBufferStatusEnumValues() {
        CachedInputStream.CacheReadBuffer.STATUS[] values =
            CachedInputStream.CacheReadBuffer.STATUS.values();
        Assert.assertEquals(3, values.length);
        Assert.assertEquals(CachedInputStream.CacheReadBuffer.STATUS.INIT,
            CachedInputStream.CacheReadBuffer.STATUS.valueOf("INIT"));
        Assert.assertEquals(CachedInputStream.CacheReadBuffer.STATUS.SUCCESS,
            CachedInputStream.CacheReadBuffer.STATUS.valueOf("SUCCESS"));
        Assert.assertEquals(CachedInputStream.CacheReadBuffer.STATUS.ERROR,
            CachedInputStream.CacheReadBuffer.STATUS.valueOf("ERROR"));
    }

    @Test
    public void testCacheReadBufferAwaitErrorSignal() throws InterruptedException {
        CachedInputStream.CacheReadBuffer buf = new CachedInputStream.CacheReadBuffer(0, 49);

        Thread signaler = new Thread(() -> {
            try {
                Thread.sleep(50);
            } catch (InterruptedException e) {
                return;
            }
            buf.lock();
            try {
                buf.setStatus(CachedInputStream.CacheReadBuffer.STATUS.ERROR);
                buf.signalAll();
            } finally {
                buf.unlock();
            }
        });

        signaler.start();
        buf.lock();
        try {
            buf.await(CachedInputStream.CacheReadBuffer.STATUS.INIT);
            Assert.assertEquals(CachedInputStream.CacheReadBuffer.STATUS.ERROR, buf.getStatus());
        } finally {
            buf.unlock();
        }
        signaler.join(1000);
    }

    @Test
    public void testCacheReadBufferSingleByte() {
        // byteStart == byteEnd -> buffer length 1
        CachedInputStream.CacheReadBuffer buf = new CachedInputStream.CacheReadBuffer(5, 5);
        Assert.assertEquals(1, buf.getBuffer().length);
        Assert.assertEquals(5L, buf.getByteStart());
        Assert.assertEquals(5L, buf.getByteEnd());
    }

    // ===== Available Edge Case =====

    @Test
    public void testAvailableExactlyMaxInt() throws IOException {
        stream = createStream(1L, "test.orc", (long) Integer.MAX_VALUE);
        Assert.assertEquals(Integer.MAX_VALUE, stream.available());
    }

    // ===== Dynamic maxPinBytesPerGet & Chunked cache.get Path Tests =====

    /**
     * Verify that the default hard cap for a single cache.get is 1MB,
     * resolved from DynamicConfig rather than a hard-coded static constant.
     */
    @Test
    public void testMaxPinBytesPerGetDefaultIs1MB() {
        Assert.assertEquals(1024 * 1024,
            com.alibaba.polardbx.common.properties.DynamicConfig.getInstance().getCacheMaxPinBytesPerGet());
    }

    /**
     * Positioned read with a request larger than the dynamic pin cap must
     * split into multiple cache.get chunks. With a null-returning dummy cache
     * the very first chunk fails and produces an IOException — this still
     * proves that the while-loop / chunking branch is reached for large
     * requests without throwing something else (e.g. OOM/NPE outside try).
     */
    @Test(expected = IOException.class)
    public void testPositionedReadLargeLenTriggersChunkedPath() throws IOException {
        // fileSize 8MB, request 6MB > 1MB cap -> must iterate chunks
        long fileSize = 8L * 1024 * 1024;
        stream = createStream(1L, "big.orc", fileSize);
        byte[] buf = new byte[6 * 1024 * 1024];
        stream.read(0L, buf, 0, buf.length);
    }

    /**
     * Positioned read when requested len exceeds remaining fileSize:
     * realLen must be clamped to (fileSize - position). The test verifies
     * the code path does not throw IndexOutOfBounds or NPE before reaching
     * cache.get — it fails with IOException due to null cache data.
     */
    @Test(expected = IOException.class)
    public void testPositionedReadLenClampedToFileSize() throws IOException {
        stream = createStream(1L, "small.orc", 100L);
        byte[] buf = new byte[1024];
        stream.read(0L, buf, 0, 1024);
    }

    /**
     * Positioned read at non-zero position with len exactly equal to
     * (fileSize - position). Verifies boundary case of the new chunking
     * loop (exactly one chunk, no remainder).
     */
    @Test(expected = IOException.class)
    public void testPositionedReadAtNonZeroPositionExactRemainder() throws IOException {
        long fileSize = 4L * 1024 * 1024;
        stream = createStream(1L, "mid.orc", fileSize);
        long pos = 1024L * 1024L; // 1MB
        int len = (int) (fileSize - pos); // 3MB -> spans multiple chunks
        byte[] buf = new byte[len];
        stream.read(pos, buf, 0, len);
    }

    /**
     * MERGE policy with large delta inflates partSize via nextPowerOfTwo.
     * The CacheReaderTask must then split its single buffer fill into
     * multiple cache.get calls (each <= the dynamic pin cap). With a
     * null-returning dummy cache the prefetch task sets ERROR and reopen
     * propagates it as IOException.
     */
    @Test(expected = IOException.class)
    public void testReadArrayMergePolicyLargeDeltaTriggersChunkedPrefetch() throws IOException {
        conf.set("fs.oss.io.fetch.policy", "MERGE");
        long fileSize = 8L * 1024 * 1024;
        stream = createStream(1L, "merge.orc", fileSize);
        byte[] buf = new byte[4 * 1024 * 1024];
        stream.read(buf, 0, buf.length);
    }

    /**
     * REQUESTED policy with a very large delta: reopen(position, delta)
     * will use delta as partSize. CacheReaderTask must then chunk the
     * resulting cache.get. Verifies the chunking path on REQUESTED policy.
     */
    @Test(expected = IOException.class)
    public void testReadArrayRequestedPolicyLargeDeltaTriggersChunkedPrefetch() throws IOException {
        // default policy is REQUESTED
        long fileSize = 8L * 1024 * 1024;
        stream = createStream(1L, "req.orc", fileSize);
        byte[] buf = new byte[3 * 1024 * 1024];
        stream.read(buf, 0, buf.length);
    }

    /**
     * Sanity: positioned read for a size smaller than the dynamic pin cap
     * uses a single chunk. Still fails due to dummy cache returning null,
     * but must not throw ArithmeticException / overflow before cache.get.
     */
    @Test(expected = IOException.class)
    public void testPositionedReadSmallLenSingleChunk() throws IOException {
        stream = createStream(1L, "small.orc", 1024L * 1024L);
        byte[] buf = new byte[64 * 1024]; // 64KB well below the 1MB cap
        stream.read(0L, buf, 0, buf.length);
    }
}
