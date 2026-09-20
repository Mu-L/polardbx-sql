/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.gms.metadb.cache;

import com.alibaba.polardbx.cache.GeneralCache;
import com.alibaba.polardbx.cache.external.impl.rpc.meta.PeerInfo;
import com.alibaba.polardbx.cache.external.impl.rpc.meta.PeerRole;
import com.alibaba.polardbx.cache.external.impl.rpc.meta.RpcMetaService;
import com.alibaba.polardbx.cache.statistics.CacheStatisticsCollector;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.lang.reflect.Proxy;
import java.sql.Connection;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class GmsRpcMetaServiceFactoryTest {

    private final CacheStatisticsCollector mockCollector = mock(CacheStatisticsCollector.class);

    /**
     * Create a dummy GeneralCache instance using Proxy.
     * Mockito inline mock maker cannot mock the GeneralCache interface (NPE in class instrumentation),
     * but tests only need identity checks, so a Proxy-based stub is sufficient.
     */
    private static GeneralCache createDummyCache() {
        return (GeneralCache) Proxy.newProxyInstance(
            GeneralCache.class.getClassLoader(),
            new Class<?>[] {GeneralCache.class},
            (proxy, method, args) -> null
        );
    }

    @Test
    public void testGetRpcMetaService() throws Exception {
        try (final MockedStatic<MetaDbDataSource> metaDbDataSourceMockedStatic = mockStatic(MetaDbDataSource.class)) {
            MetaDbDataSource mockInstance = mock(MetaDbDataSource.class);
            Connection mockConn = mock(Connection.class);

            metaDbDataSourceMockedStatic.when(MetaDbDataSource::getInstance).thenReturn(mockInstance);
            when(mockInstance.getConnection()).thenReturn(mockConn);

            GmsRpcMetaServiceFactory factory = new GmsRpcMetaServiceFactory(mockCollector);
            RpcMetaService service = factory.getRpcMetaService();
            Assert.assertNotNull(service);
            Assert.assertTrue(service instanceof GmsRpcMetaService);
        }
    }

    @Test
    public void testGetGeneralCacheDefault() {
        GmsRpcMetaServiceFactory factory = new GmsRpcMetaServiceFactory(mockCollector);
        Assert.assertNull(factory.getGeneralCache());
    }

    @Test
    public void testGetGeneralCacheWithValue() {
        GeneralCache dummyCache = createDummyCache();
        GmsRpcMetaServiceFactory factory = new GmsRpcMetaServiceFactory(mockCollector, dummyCache);
        Assert.assertNotNull(factory.getGeneralCache());
        Assert.assertSame(dummyCache, factory.getGeneralCache());
    }

    @Test
    public void testSetGeneralCache() {
        GmsRpcMetaServiceFactory factory = new GmsRpcMetaServiceFactory(mockCollector);
        Assert.assertNull(factory.getGeneralCache());

        GeneralCache dummyCache = createDummyCache();
        factory.setGeneralCache(dummyCache);
        Assert.assertSame(dummyCache, factory.getGeneralCache());

        factory.setGeneralCache(null);
        Assert.assertNull(factory.getGeneralCache());
    }

    @Test
    public void testGetRpcMetaServiceReturnsCorrectType() throws Exception {
        try (final MockedStatic<MetaDbDataSource> metaDbDataSourceMockedStatic = mockStatic(MetaDbDataSource.class)) {
            MetaDbDataSource mockInstance = mock(MetaDbDataSource.class);
            Connection mockConn = mock(Connection.class);

            metaDbDataSourceMockedStatic.when(MetaDbDataSource::getInstance).thenReturn(mockInstance);
            when(mockInstance.getConnection()).thenReturn(mockConn);

            GmsRpcMetaServiceFactory factory = new GmsRpcMetaServiceFactory(mockCollector);
            RpcMetaService service = factory.getRpcMetaService();

            // Verify the service is the expected implementation
            Assert.assertEquals(GmsRpcMetaService.class, service.getClass());
        }
    }

    @Test
    public void testMultipleCallsWithDifferentCaches() {
        GmsRpcMetaServiceFactory factory = new GmsRpcMetaServiceFactory(mockCollector);

        GeneralCache cache1 = createDummyCache();
        GeneralCache cache2 = createDummyCache();

        factory.setGeneralCache(cache1);
        Assert.assertSame(cache1, factory.getGeneralCache());

        factory.setGeneralCache(cache2);
        Assert.assertSame(cache2, factory.getGeneralCache());
        Assert.assertNotSame(cache1, factory.getGeneralCache());
    }

    @Test
    public void testGetCacheStatisticsCollector() {
        GmsRpcMetaServiceFactory factory = new GmsRpcMetaServiceFactory(mockCollector);
        Assert.assertSame(mockCollector, factory.getCacheStatisticsCollector());
    }

    @Test
    public void testInitMyselfAndGetMyself() {
        GmsRpcMetaServiceFactory factory = new GmsRpcMetaServiceFactory(mockCollector);
        factory.initMyself(8080, "testTag", PeerRole.OTHERS);

        PeerInfo myself = factory.getMyself();
        Assert.assertNotNull(myself);
        Assert.assertTrue(myself.name.startsWith("testTag@"));
        Assert.assertTrue(myself.name.contains(":8080"));
        Assert.assertEquals(PeerRole.OTHERS, myself.role);
        Assert.assertFalse(myself.leader);
    }

    @Test(expected = IllegalStateException.class)
    public void testGetMyselfBeforeInit() {
        GmsRpcMetaServiceFactory factory = new GmsRpcMetaServiceFactory(mockCollector);
        factory.getMyself();
    }

    @Test
    public void testUpdateRole() {
        GmsRpcMetaServiceFactory factory = new GmsRpcMetaServiceFactory(mockCollector);
        factory.initMyself(8080, "testTag", PeerRole.OTHERS);

        factory.updateRole(PeerRole.CACHE_WRITER);
        PeerInfo updated = factory.getMyself();
        Assert.assertEquals(PeerRole.CACHE_WRITER, updated.role);

        // name, address, nodeHash, leader should remain unchanged
        Assert.assertTrue(updated.name.startsWith("testTag@"));
        Assert.assertFalse(updated.leader);
    }

    @Test(expected = IllegalStateException.class)
    public void testUpdateRoleBeforeInit() {
        GmsRpcMetaServiceFactory factory = new GmsRpcMetaServiceFactory(mockCollector);
        factory.updateRole(PeerRole.CACHE_WRITER);
    }

    @Test
    public void testInitMyselfWithDifferentRoles() {
        GmsRpcMetaServiceFactory factory = new GmsRpcMetaServiceFactory(mockCollector);

        factory.initMyself(9090, "writer", PeerRole.CACHE_WRITER);
        Assert.assertEquals(PeerRole.CACHE_WRITER, factory.getMyself().role);

        factory.initMyself(9090, "reader", PeerRole.CACHE_READER);
        Assert.assertEquals(PeerRole.CACHE_READER, factory.getMyself().role);
        Assert.assertTrue(factory.getMyself().name.startsWith("reader@"));
    }
}
