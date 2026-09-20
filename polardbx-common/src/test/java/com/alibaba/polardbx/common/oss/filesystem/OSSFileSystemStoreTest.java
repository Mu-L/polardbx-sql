/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.common.oss.filesystem;

import com.aliyun.oss.ClientConfiguration;
import com.aliyun.oss.OSSClient;
import org.apache.hadoop.conf.Configuration;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.net.URI;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class OSSFileSystemStoreTest {

    private MockedStatic<OSSCacheAdapter> mockedCacheAdapter;

    @Before
    public void setUp() {
        mockedCacheAdapter = Mockito.mockStatic(OSSCacheAdapter.class);
    }

    @After
    public void tearDown() {
        if (mockedCacheAdapter != null) {
            mockedCacheAdapter.close();
        }
    }

    @Test
    public void test() throws Exception {
        mockedCacheAdapter.when(OSSCacheAdapter::getInstanceOrNull).thenReturn(null);
        final OSSFileSystemStore ossFileSystemStore = new OSSFileSystemStore();
        Configuration conf = new Configuration();
        conf.set("fs.oss.endpoint", "endpoint");
        conf.set("fs.oss.accessKeyId", "xx");
        conf.set("fs.oss.accessKeySecret", "xx");
        conf.set("fs.oss.request.timeout", "10000");
        ossFileSystemStore.initialize(new URI("oss://test"), conf, null, null);
    }

    @Test
    public void testPrivateCloudEnabled() throws Exception {
        final OSSFileSystemStore ossFileSystemStore = new OSSFileSystemStore();
        Configuration conf = new Configuration();
        conf.set("fs.oss.endpoint", "endpoint");
        conf.set("fs.oss.accessKeyId", "xx");
        conf.set("fs.oss.accessKeySecret", "xx");
        conf.setBoolean("fs.oss.private.cloud", true);
        ossFileSystemStore.initialize(new URI("oss://test"), conf, null, null);

        OSSClient ossClient = ossFileSystemStore.getOssClient();
        Assert.assertNotNull(ossClient);
        ClientConfiguration clientConfig = ossClient.getClientConfiguration();
        Assert.assertFalse("CNAME support should be disabled for private cloud",
            clientConfig.isSupportCname());
    }

    @Test
    public void testPrivateCloudDisabledByDefault() throws Exception {
        final OSSFileSystemStore ossFileSystemStore = new OSSFileSystemStore();
        Configuration conf = new Configuration();
        conf.set("fs.oss.endpoint", "endpoint");
        conf.set("fs.oss.accessKeyId", "xx");
        conf.set("fs.oss.accessKeySecret", "xx");
        ossFileSystemStore.initialize(new URI("oss://test"), conf, null, null);

        OSSClient ossClient = ossFileSystemStore.getOssClient();
        Assert.assertNotNull(ossClient);
        ClientConfiguration clientConfig = ossClient.getClientConfiguration();
        Assert.assertTrue("CNAME support should be enabled by default",
            clientConfig.isSupportCname());
    }

    @Test
    public void testPrivateCloudExplicitlyDisabled() throws Exception {
        final OSSFileSystemStore ossFileSystemStore = new OSSFileSystemStore();
        Configuration conf = new Configuration();
        conf.set("fs.oss.endpoint", "endpoint");
        conf.set("fs.oss.accessKeyId", "xx");
        conf.set("fs.oss.accessKeySecret", "xx");
        conf.setBoolean("fs.oss.private.cloud", false);
        ossFileSystemStore.initialize(new URI("oss://test"), conf, null, null);

        OSSClient ossClient = ossFileSystemStore.getOssClient();
        Assert.assertNotNull(ossClient);
        ClientConfiguration clientConfig = ossClient.getClientConfiguration();
        Assert.assertTrue("CNAME support should be enabled when private cloud is false",
            clientConfig.isSupportCname());
    }

    // ===================== retrieve() bypass-detection guard =====================

    @Test(expected = IllegalStateException.class)
    public void testRetrieveThrowsWhenCacheEnabledAndBypassDetectionOn() {
        OSSCacheAdapter adapter = mock(OSSCacheAdapter.class);
        when(adapter.isEnabled()).thenReturn(true);
        mockedCacheAdapter.when(OSSCacheAdapter::getInstanceOrNull).thenReturn(adapter);
        mockedCacheAdapter.when(OSSCacheAdapter::isBypassDetectionEnabled).thenReturn(true);

        // Uninitialized store: if the guard did NOT fire, the subsequent OSS read
        // would NPE on the null ossClient and be swallowed into a `null` return.
        // The guard must fire first and raise IllegalStateException.
        new OSSFileSystemStore().retrieve("abc.orc", 0, 100);
    }

    @Test
    public void testRetrieveBypassesGuardWhenDetectionDisabled() {
        OSSCacheAdapter adapter = mock(OSSCacheAdapter.class);
        when(adapter.isEnabled()).thenReturn(true);
        mockedCacheAdapter.when(OSSCacheAdapter::getInstanceOrNull).thenReturn(adapter);
        mockedCacheAdapter.when(OSSCacheAdapter::isBypassDetectionEnabled).thenReturn(false);

        // With detection disabled, the method proceeds to the real OSS call branch.
        // The uninitialized ossClient makes the actual call fail; the important point
        // is that the exception is NOT IllegalStateException from the bypass guard.
        try {
            new OSSFileSystemStore().retrieve("abc.orc", 0, 100);
            // retrieve() may also return null if its catch handler converts the failure.
        } catch (IllegalStateException e) {
            Assert.fail("Guard must not fire when bypassDetection is disabled: " + e);
        } catch (RuntimeException expected) {
            // NPE (or similar) caused by uninitialized OSS client is acceptable:
            // it proves the guard was skipped and the real call path was entered.
        }
    }

    @Test
    public void testRetrieveBypassesGuardWhenCacheDisabled() {
        OSSCacheAdapter adapter = mock(OSSCacheAdapter.class);
        when(adapter.isEnabled()).thenReturn(false);
        mockedCacheAdapter.when(OSSCacheAdapter::getInstanceOrNull).thenReturn(adapter);
        mockedCacheAdapter.when(OSSCacheAdapter::isBypassDetectionEnabled).thenReturn(true);

        // Cache not enabled: guard does not fire even when detection flag is on.
        try {
            new OSSFileSystemStore().retrieve("abc.orc", 0, 100);
        } catch (IllegalStateException e) {
            Assert.fail("Guard must not fire when cache is disabled: " + e);
        } catch (RuntimeException expected) {
            // Expected: uninitialized OSS client.
        }
    }

    @Test
    public void testRetrieveBypassesGuardWhenAdapterAbsent() {
        mockedCacheAdapter.when(OSSCacheAdapter::getInstanceOrNull).thenReturn(null);
        try {
            new OSSFileSystemStore().retrieve("abc.orc", 0, 100);
        } catch (IllegalStateException e) {
            Assert.fail("Guard must not fire when adapter is absent: " + e);
        } catch (RuntimeException expected) {
            // Expected: uninitialized OSS client.
        }
    }

    // ===================== retrieve(..., allowBypass) overload =====================

    /**
     * 4-arg retrieve with allowBypass=true must skip the guard even when cache is
     * enabled and bypass detection is on (per-statement HINT turning cache off).
     */
    @Test
    public void testRetrieveAllowBypassSkipsGuardWhenCacheEnabled() {
        OSSCacheAdapter adapter = mock(OSSCacheAdapter.class);
        when(adapter.isEnabled()).thenReturn(true);
        mockedCacheAdapter.when(OSSCacheAdapter::getInstanceOrNull).thenReturn(adapter);
        mockedCacheAdapter.when(OSSCacheAdapter::isBypassDetectionEnabled).thenReturn(true);

        try {
            new OSSFileSystemStore().retrieve("abc.orc", 0, 100, true);
        } catch (IllegalStateException e) {
            Assert.fail("Guard must not fire when allowBypass=true: " + e);
        } catch (RuntimeException expected) {
            // Expected: uninitialized OSS client.
        }
    }

    /**
     * 4-arg retrieve with allowBypass=false must preserve the old behavior and
     * fire the guard when cache is enabled and detection is on.
     */
    @Test(expected = IllegalStateException.class)
    public void testRetrieveAllowBypassFalseStillFiresGuard() {
        OSSCacheAdapter adapter = mock(OSSCacheAdapter.class);
        when(adapter.isEnabled()).thenReturn(true);
        mockedCacheAdapter.when(OSSCacheAdapter::getInstanceOrNull).thenReturn(adapter);
        mockedCacheAdapter.when(OSSCacheAdapter::isBypassDetectionEnabled).thenReturn(true);

        new OSSFileSystemStore().retrieve("abc.orc", 0, 100, false);
    }

    /**
     * 3-arg retrieve must delegate to the 4-arg overload with allowBypass=false.
     * Verified indirectly: the guard still fires with the same setup as the 4-arg
     * allowBypass=false test.
     */
    @Test(expected = IllegalStateException.class)
    public void testThreeArgRetrieveDelegatesWithAllowBypassFalse() {
        OSSCacheAdapter adapter = mock(OSSCacheAdapter.class);
        when(adapter.isEnabled()).thenReturn(true);
        mockedCacheAdapter.when(OSSCacheAdapter::getInstanceOrNull).thenReturn(adapter);
        mockedCacheAdapter.when(OSSCacheAdapter::isBypassDetectionEnabled).thenReturn(true);

        new OSSFileSystemStore().retrieve("abc.orc", 0, 100);
    }
}
