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

import com.alibaba.polardbx.cache.external.impl.rpc.meta.PeerRole;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.SQLIntegrityConstraintViolationException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mockStatic;

public class CachePeerAccessorTest {

    private static final String PEER_NAME = "node-1";
    private static final String HOST = "192.168.1.1:8080";
    private static final long NODE_HASH = 123456789L;
    private static final long NOW_UTC = System.currentTimeMillis();
    private static final long LEASE_MS = 30000L;

    @Test
    public void testRefreshUpdateExisting() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            AtomicInteger updateCount = new AtomicInteger(1);

            // update returns 1 (existing peer found), no insert needed
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenAnswer(invocationOnMock -> updateCount.get());

            CachePeerAccessor accessor = new CachePeerAccessor();
            // refresh() now returns void, just verify it doesn't throw
            accessor.refresh(PEER_NAME, HOST, NODE_HASH, PeerRole.OTHERS.name(), NOW_UTC, LEASE_MS, null);

            // Verify update was called
            metaDbUtilMockedStatic.verify(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any()), Mockito.atLeastOnce());
        }
    }

    @Test
    public void testRefreshInsertNew() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            // update returns 0, then insert is called
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenReturn(0);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insert(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenReturn(1);

            CachePeerAccessor accessor = new CachePeerAccessor();
            // refresh() now returns void, just verify it doesn't throw
            accessor.refresh(PEER_NAME, HOST, NODE_HASH, PeerRole.OTHERS.name(), NOW_UTC, LEASE_MS, null);

            // Verify insert was called
            metaDbUtilMockedStatic.verify(() -> MetaDbUtil.insert(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any()), Mockito.times(1));
        }
    }

    @Test
    public void testRefreshInsertDuplicate() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            // update returns 0, insert throws duplicate key (race condition)
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenReturn(0);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.insert(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenThrow(new SQLIntegrityConstraintViolationException("Duplicate entry"));

            CachePeerAccessor accessor = new CachePeerAccessor();
            // Should not throw, duplicate is gracefully handled
            accessor.refresh(PEER_NAME, HOST, NODE_HASH, PeerRole.OTHERS.name(), NOW_UTC, LEASE_MS, null);
            // Test passes if no exception is thrown
        }
    }

    @Test
    public void testElectSuccess() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            AtomicInteger updateCount = new AtomicInteger(0);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenAnswer(invocationOnMock -> {
                int current = updateCount.getAndIncrement();
                if (current == 0) {
                    return 0; // demote expired leaders (no expired leaders)
                }
                return 1; // promote self successfully
            });

            // No existing leader
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(CachePeerRecord.class), Mockito.any())).thenReturn(Collections.emptyList());

            CachePeerAccessor accessor = new CachePeerAccessor();
            boolean result = accessor.elect(PEER_NAME, NOW_UTC, LEASE_MS);
            Assert.assertTrue(result);
        }
    }

    @Test
    public void testElectFailAnotherLeaderExists() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            // Demote expired leaders
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenReturn(0);

            // Another leader exists
            List<CachePeerRecord> leaders = new ArrayList<>();
            CachePeerRecord leader = new CachePeerRecord();
            leader.peerName = "other-node";
            leader.leader = true;
            leaders.add(leader);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(CachePeerRecord.class), Mockito.any())).thenReturn(leaders);

            CachePeerAccessor accessor = new CachePeerAccessor();
            boolean result = accessor.elect(PEER_NAME, NOW_UTC, LEASE_MS);
            Assert.assertFalse(result);
        }
    }

    @Test
    public void testElectRenewLeaderSuccess() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            AtomicInteger updateCount = new AtomicInteger(0);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenAnswer(invocationOnMock -> {
                int current = updateCount.getAndIncrement();
                if (current == 0) {
                    return 0; // demote expired leaders (none)
                }
                return 1; // renew leader lease successfully
            });

            // I am the current leader
            List<CachePeerRecord> leaders = new ArrayList<>();
            CachePeerRecord leader = new CachePeerRecord();
            leader.peerName = PEER_NAME;
            leader.leader = true;
            leaders.add(leader);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(CachePeerRecord.class), Mockito.any())).thenReturn(leaders);

            CachePeerAccessor accessor = new CachePeerAccessor();
            boolean result = accessor.elect(PEER_NAME, NOW_UTC, LEASE_MS);
            Assert.assertTrue(result);
        }
    }

    @Test
    public void testElectRenewLeaderFail() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            AtomicInteger updateCount = new AtomicInteger(0);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenAnswer(invocationOnMock -> {
                int current = updateCount.getAndIncrement();
                if (current == 0) {
                    return 0; // demote expired leaders (none)
                }
                return 0; // renew leader lease failed (lease expired)
            });

            // I am the current leader
            List<CachePeerRecord> leaders = new ArrayList<>();
            CachePeerRecord leader = new CachePeerRecord();
            leader.peerName = PEER_NAME;
            leader.leader = true;
            leaders.add(leader);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(CachePeerRecord.class), Mockito.any())).thenReturn(leaders);

            CachePeerAccessor accessor = new CachePeerAccessor();
            boolean result = accessor.elect(PEER_NAME, NOW_UTC, LEASE_MS);
            Assert.assertFalse(result);
        }
    }

    @Test
    public void testGetLeader() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<CachePeerRecord> recordList = new ArrayList<>();
            CachePeerRecord record = new CachePeerRecord();
            record.peerName = "leader-node";
            record.leader = true;
            record.role = "CACHE_WRITER";
            recordList.add(record);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(CachePeerRecord.class), Mockito.any())).thenReturn(recordList);

            CachePeerAccessor accessor = new CachePeerAccessor();
            CachePeerRecord result = accessor.getLeader(NOW_UTC);
            Assert.assertNotNull(result);
            Assert.assertEquals("leader-node", result.peerName);
            Assert.assertTrue(result.leader);
            Assert.assertEquals("CACHE_WRITER", result.role);
        }
    }

    @Test
    public void testGetLeaderNotFound() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(CachePeerRecord.class), Mockito.any())).thenReturn(Collections.emptyList());

            CachePeerAccessor accessor = new CachePeerAccessor();
            CachePeerRecord result = accessor.getLeader(NOW_UTC);
            Assert.assertNull(result);
        }
    }

    @Test
    public void testGetActivePeers() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<CachePeerRecord> recordList = new ArrayList<>();
            recordList.add(new CachePeerRecord());
            recordList.add(new CachePeerRecord());
            recordList.add(new CachePeerRecord());

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(CachePeerRecord.class), Mockito.any())).thenReturn(recordList);

            CachePeerAccessor accessor = new CachePeerAccessor();
            List<CachePeerRecord> result = accessor.getActivePeers(NOW_UTC);
            Assert.assertEquals(3, result.size());
        }
    }

    @Test
    public void testGetAllPeers() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<CachePeerRecord> recordList = new ArrayList<>();
            recordList.add(new CachePeerRecord());

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(),
                Mockito.eq(CachePeerRecord.class), Mockito.any())).thenReturn(recordList);

            CachePeerAccessor accessor = new CachePeerAccessor();
            List<CachePeerRecord> result = accessor.getAllPeers();
            Assert.assertEquals(1, result.size());
        }
    }

    @Test
    public void testDeletePeer() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            AtomicInteger deleteCount = new AtomicInteger(1);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenAnswer(invocationOnMock -> deleteCount.get());

            CachePeerAccessor accessor = new CachePeerAccessor();

            deleteCount.set(1);
            boolean result = accessor.deletePeer(PEER_NAME);
            Assert.assertTrue(result);

            deleteCount.set(0);
            result = accessor.deletePeer("nonexistent");
            Assert.assertFalse(result);
        }
    }

    @Test
    public void testCleanup() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenReturn(5);

            CachePeerAccessor accessor = new CachePeerAccessor();
            int result = accessor.cleanup(NOW_UTC);
            Assert.assertEquals(5, result);
        }
    }

    @Test
    public void testElectError() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(Mockito.anyString(), Mockito.anyMap(),
                Mockito.any())).thenThrow(new RuntimeException("mock error"));

            CachePeerAccessor accessor = new CachePeerAccessor();
            try {
                accessor.elect(PEER_NAME, NOW_UTC, LEASE_MS);
                Assert.fail();
            } catch (Exception ignored) {
            }
        }
    }

    @Test
    public void testGetAllPeersError() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(),
                Mockito.eq(CachePeerRecord.class), Mockito.any())).thenThrow(new RuntimeException("mock error"));

            CachePeerAccessor accessor = new CachePeerAccessor();
            try {
                accessor.getAllPeers();
                Assert.fail();
            } catch (Exception ignored) {
            }
        }
    }
}
