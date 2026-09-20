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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Unit tests for the cache-bypass guard in the {@link OSSInputStream} constructor.
 */
public class OSSInputStreamTest {

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

    private OSSInputStream newStream() throws IOException {
        Configuration conf = new Configuration();
        OSSFileSystemStore store = mock(OSSFileSystemStore.class);
        FileSystem.Statistics stats = new FileSystem.Statistics("oss");
        FileSystemRateLimiter rl = mock(FileSystemRateLimiter.class);
        return new OSSInputStream(conf, Executors.newSingleThreadExecutor(), 1,
            store, "abc.orc", 0L, stats, rl);
    }

    private OSSInputStream newStreamWithBypass(boolean allowBypass) throws IOException {
        Configuration conf = new Configuration();
        OSSFileSystemStore store = mock(OSSFileSystemStore.class);
        FileSystem.Statistics stats = new FileSystem.Statistics("oss");
        FileSystemRateLimiter rl = mock(FileSystemRateLimiter.class);
        return new OSSInputStream(conf, Executors.newSingleThreadExecutor(), 1,
            store, "abc.orc", 0L, stats, rl, allowBypass);
    }

    @Test
    public void testNoGuardWhenAdapterAbsent() throws IOException {
        mockedCacheAdapter.when(OSSCacheAdapter::getInstanceOrNull).thenReturn(null);
        // No adapter -> no guard, construction succeeds.
        OSSInputStream stream = newStream();
        Assert.assertNotNull(stream);
        stream.close();
    }

    @Test(expected = IllegalStateException.class)
    public void testGuardThrowsWhenCacheEnabledAndBypassDetectionOn() throws IOException {
        OSSCacheAdapter adapter = mock(OSSCacheAdapter.class);
        when(adapter.isEnabled()).thenReturn(true);
        mockedCacheAdapter.when(OSSCacheAdapter::getInstanceOrNull).thenReturn(adapter);
        mockedCacheAdapter.when(OSSCacheAdapter::isBypassDetectionEnabled).thenReturn(true);

        newStream();
    }

    @Test
    public void testGuardSkippedWhenBypassDetectionDisabled() throws IOException {
        // Cache enabled, but detection disabled (Columnar mode): no throw.
        OSSCacheAdapter adapter = mock(OSSCacheAdapter.class);
        when(adapter.isEnabled()).thenReturn(true);
        mockedCacheAdapter.when(OSSCacheAdapter::getInstanceOrNull).thenReturn(adapter);
        mockedCacheAdapter.when(OSSCacheAdapter::isBypassDetectionEnabled).thenReturn(false);

        OSSInputStream stream = newStream();
        Assert.assertNotNull(stream);
        stream.close();
    }

    @Test
    public void testGuardSkippedWhenCacheDisabled() throws IOException {
        // Adapter present but cache disabled -> no throw regardless of detection flag.
        OSSCacheAdapter adapter = mock(OSSCacheAdapter.class);
        when(adapter.isEnabled()).thenReturn(false);
        mockedCacheAdapter.when(OSSCacheAdapter::getInstanceOrNull).thenReturn(adapter);
        mockedCacheAdapter.when(OSSCacheAdapter::isBypassDetectionEnabled).thenReturn(true);

        OSSInputStream stream = newStream();
        Assert.assertNotNull(stream);
        stream.close();
    }

    // ===================== allowBypass overload =====================

    /**
     * allowBypass=true must skip the guard even when cache is enabled and bypass
     * detection is on. This is the per-statement HINT=OFF code path.
     */
    @Test
    public void testAllowBypassTrueSkipsGuardEvenWhenCacheEnabled() throws IOException {
        OSSCacheAdapter adapter = mock(OSSCacheAdapter.class);
        when(adapter.isEnabled()).thenReturn(true);
        mockedCacheAdapter.when(OSSCacheAdapter::getInstanceOrNull).thenReturn(adapter);
        mockedCacheAdapter.when(OSSCacheAdapter::isBypassDetectionEnabled).thenReturn(true);

        OSSInputStream stream = newStreamWithBypass(true);
        Assert.assertNotNull(stream);
        stream.close();
    }

    /**
     * allowBypass=false must preserve the old guard behavior and throw when
     * cache is enabled with bypass detection on.
     */
    @Test(expected = IllegalStateException.class)
    public void testAllowBypassFalseStillFiresGuard() throws IOException {
        OSSCacheAdapter adapter = mock(OSSCacheAdapter.class);
        when(adapter.isEnabled()).thenReturn(true);
        mockedCacheAdapter.when(OSSCacheAdapter::getInstanceOrNull).thenReturn(adapter);
        mockedCacheAdapter.when(OSSCacheAdapter::isBypassDetectionEnabled).thenReturn(true);

        newStreamWithBypass(false);
    }

    /**
     * The legacy 8-arg constructor must delegate to the 9-arg one with
     * allowBypass=false, keeping backward compatibility.
     */
    @Test(expected = IllegalStateException.class)
    public void testLegacyConstructorDelegatesWithAllowBypassFalse() throws IOException {
        OSSCacheAdapter adapter = mock(OSSCacheAdapter.class);
        when(adapter.isEnabled()).thenReturn(true);
        mockedCacheAdapter.when(OSSCacheAdapter::getInstanceOrNull).thenReturn(adapter);
        mockedCacheAdapter.when(OSSCacheAdapter::isBypassDetectionEnabled).thenReturn(true);

        // Should throw because legacy constructor sets allowBypass=false.
        newStream();
    }
}
