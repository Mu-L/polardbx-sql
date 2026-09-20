package com.alibaba.polardbx.common.oss.filesystem;

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.GetObjectRequest;
import com.aliyun.oss.model.OSSObject;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.util.function.UnaryOperator;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Unit tests for PrefixRoutingRemoteStorageService: prefix resolution, rate limiting, updateRateLimit.
 */
public class PrefixRoutingRemoteStorageServiceTest {

    private OSS mockOss;
    private OSSObject mockOssObject;
    private PrefixRoutingRemoteStorageService service;
    private UnaryOperator<String> pathMapper;

    @Before
    public void setUp() {
        mockOss = mock(OSS.class);
        mockOssObject = mock(OSSObject.class);
        when(mockOss.getObject(any(GetObjectRequest.class))).thenReturn(mockOssObject);
        when(mockOssObject.getObjectContent()).thenReturn(new ByteArrayInputStream(new byte[100]));

        pathMapper = cacheFileName -> "instId/" + cacheFileName;

        // Use a very high rate limit so tests don't block
        service = new PrefixRoutingRemoteStorageService(
            mockOss, "my-bucket", "AES256", Long.MAX_VALUE / 2, pathMapper);
    }

    @Test
    public void testReadResolvesPrefix() throws Exception {
        InputStream result = service.read("abc.orc", 0, 100);
        Assert.assertNotNull(result);

        ArgumentCaptor<GetObjectRequest> captor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(mockOss).getObject(captor.capture());

        GetObjectRequest captured = captor.getValue();
        Assert.assertEquals("my-bucket", captured.getBucketName());
        Assert.assertEquals("instId/abc.orc", captured.getKey());
    }

    @Test
    public void testReadSetsRangeCorrectly() throws Exception {
        service.read("data.csv", 1024, 512);

        ArgumentCaptor<GetObjectRequest> captor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(mockOss).getObject(captor.capture());

        GetObjectRequest captured = captor.getValue();
        long[] range = captured.getRange();
        Assert.assertEquals(1024L, range[0]);
        Assert.assertEquals(1535L, range[1]); // offset + length - 1
    }

    @Test
    public void testDynamicPrefixResolver() throws Exception {
        // Use a stateful mapper that changes prefix
        final String[] prefix = {"dir1"};
        UnaryOperator<String> dynamicMapper =
            cacheFileName -> prefix[0] + "/" + cacheFileName;

        PrefixRoutingRemoteStorageService svc = new PrefixRoutingRemoteStorageService(
            mockOss, "bucket", "", Long.MAX_VALUE / 2, dynamicMapper);

        svc.read("file.orc", 0, 10);
        ArgumentCaptor<GetObjectRequest> captor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(mockOss).getObject(captor.capture());
        Assert.assertEquals("dir1/file.orc", captor.getValue().getKey());

        // Change prefix dynamically
        prefix[0] = "dir2";
        Mockito.reset(mockOss);
        when(mockOss.getObject(any(GetObjectRequest.class))).thenReturn(mockOssObject);

        svc.read("file.orc", 0, 10);
        captor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(mockOss).getObject(captor.capture());
        Assert.assertEquals("dir2/file.orc", captor.getValue().getKey());
    }

    @Test
    public void testUpdateRateLimit() throws Exception {
        // Just verify updateRateLimit doesn't throw and subsequent read still works
        service.updateRateLimit(1024 * 1024); // 1MB/s
        InputStream result = service.read("test.orc", 0, 50);
        Assert.assertNotNull(result);
        verify(mockOss).getObject(any(GetObjectRequest.class));
    }

    @Test
    public void testReadMultipleFiles() throws Exception {
        service.read("a.orc", 0, 100);
        service.read("b.csv", 200, 300);

        ArgumentCaptor<GetObjectRequest> captor = ArgumentCaptor.forClass(GetObjectRequest.class);
        verify(mockOss, times(2)).getObject(captor.capture());

        Assert.assertEquals("instId/a.orc", captor.getAllValues().get(0).getKey());
        Assert.assertEquals("instId/b.csv", captor.getAllValues().get(1).getKey());
    }

    @Test
    public void testConstructorWithNullEncAlg() throws Exception {
        // encAlg can be null/empty
        PrefixRoutingRemoteStorageService svc = new PrefixRoutingRemoteStorageService(
            mockOss, "bucket", null, Long.MAX_VALUE / 2, pathMapper);
        InputStream result = svc.read("test.orc", 0, 10);
        Assert.assertNotNull(result);
    }
}
