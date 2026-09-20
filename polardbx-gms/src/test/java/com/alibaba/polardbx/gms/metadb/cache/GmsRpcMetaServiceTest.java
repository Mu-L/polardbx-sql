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

import com.alibaba.polardbx.cache.external.impl.rpc.meta.PeerInfo;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.gms.util.PasswdUtil;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class GmsRpcMetaServiceTest {

    @Test
    public void testGetPasswordFound() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class);
            final MockedStatic<PasswdUtil> passwdUtilMockedStatic = mockStatic(PasswdUtil.class)) {

            List<CacheUserRecord> recordList = new ArrayList<>();
            CacheUserRecord record = new CacheUserRecord();
            record.userName = "testuser";
            record.password = "JnB+e+4WS9qat9i+hrj+AA==";
            recordList.add(record);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(CacheUserRecord.class), Mockito.any())).thenReturn(recordList);

            passwdUtilMockedStatic.when(() -> PasswdUtil.decrypt("JnB+e+4WS9qat9i+hrj+AA=="))
                .thenReturn("test_1234");

            Connection mockConn = mock(Connection.class);
            GmsRpcMetaService service = new GmsRpcMetaService(mockConn);

            byte[] password = service.getPassword("testuser".getBytes(StandardCharsets.UTF_8));
            Assert.assertNotNull(password);
            Assert.assertEquals("test_1234", new String(password, StandardCharsets.UTF_8));
        }
    }

    @Test
    public void testGetPasswordNotFound() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(CacheUserRecord.class), Mockito.any())).thenReturn(Collections.emptyList());

            Connection mockConn = mock(Connection.class);
            GmsRpcMetaService service = new GmsRpcMetaService(mockConn);

            byte[] password = service.getPassword("unknown".getBytes(StandardCharsets.UTF_8));
            Assert.assertNull(password);
        }
    }

    @Test
    public void testGetUserWithAdmin() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<CacheUserRecord> recordList = new ArrayList<>();

            CacheUserRecord normalUser = new CacheUserRecord();
            normalUser.userName = "normal";
            normalUser.adminPriv = 0;
            recordList.add(normalUser);

            CacheUserRecord adminUser = new CacheUserRecord();
            adminUser.userName = "admin";
            adminUser.adminPriv = 1;
            recordList.add(adminUser);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(),
                Mockito.eq(CacheUserRecord.class), Mockito.any())).thenReturn(recordList);

            Connection mockConn = mock(Connection.class);
            GmsRpcMetaService service = new GmsRpcMetaService(mockConn);

            byte[] user = service.getUser();
            Assert.assertEquals("admin", new String(user, StandardCharsets.UTF_8));
        }
    }

    @Test
    public void testGetUserNoAdmin() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<CacheUserRecord> recordList = new ArrayList<>();

            CacheUserRecord normalUser = new CacheUserRecord();
            normalUser.userName = "user1";
            normalUser.adminPriv = 0;
            recordList.add(normalUser);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(),
                Mockito.eq(CacheUserRecord.class), Mockito.any())).thenReturn(recordList);

            Connection mockConn = mock(Connection.class);
            GmsRpcMetaService service = new GmsRpcMetaService(mockConn);

            byte[] user = service.getUser();
            Assert.assertEquals("user1", new String(user, StandardCharsets.UTF_8));
        }
    }

    @Test
    public void testGetUserEmpty() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(),
                Mockito.eq(CacheUserRecord.class), Mockito.any())).thenReturn(Collections.emptyList());

            Connection mockConn = mock(Connection.class);
            GmsRpcMetaService service = new GmsRpcMetaService(mockConn);

            byte[] user = service.getUser();
            Assert.assertNotNull(user);
            Assert.assertEquals(0, user.length);
        }
    }

    @Test
    public void testGetPeers() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<CachePeerRecord> peerRecords = new ArrayList<>();

            CachePeerRecord peer1 = new CachePeerRecord();
            peer1.peerName = "node-1";
            peer1.host = "192.168.1.1:8080";
            peer1.nodeHash = 111L;
            peerRecords.add(peer1);

            CachePeerRecord peer2 = new CachePeerRecord();
            peer2.peerName = "node-2";
            peer2.host = "192.168.1.2:8080";
            peer2.nodeHash = 222L;
            peerRecords.add(peer2);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(CachePeerRecord.class), Mockito.any())).thenReturn(peerRecords);

            Connection mockConn = mock(Connection.class);
            GmsRpcMetaService service = new GmsRpcMetaService(mockConn);

            Collection<PeerInfo> peers = service.getPeers();
            Assert.assertEquals(2, peers.size());
        }
    }

    @Test
    public void testGetPeersWithInvalidHost() {
        try (final MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            List<CachePeerRecord> peerRecords = new ArrayList<>();

            CachePeerRecord validPeer = new CachePeerRecord();
            validPeer.peerName = "node-1";
            validPeer.host = "192.168.1.1:8080";
            validPeer.nodeHash = 111L;
            peerRecords.add(validPeer);

            // Invalid host - no port
            CachePeerRecord invalidPeer = new CachePeerRecord();
            invalidPeer.peerName = "node-2";
            invalidPeer.host = "192.168.1.2";
            invalidPeer.nodeHash = 222L;
            peerRecords.add(invalidPeer);

            // null host
            CachePeerRecord nullHostPeer = new CachePeerRecord();
            nullHostPeer.peerName = "node-3";
            nullHostPeer.host = null;
            nullHostPeer.nodeHash = 333L;
            peerRecords.add(nullHostPeer);

            // empty host
            CachePeerRecord emptyHostPeer = new CachePeerRecord();
            emptyHostPeer.peerName = "node-4";
            emptyHostPeer.host = "";
            emptyHostPeer.nodeHash = 444L;
            peerRecords.add(emptyHostPeer);

            // host ending with colon
            CachePeerRecord colonPeer = new CachePeerRecord();
            colonPeer.peerName = "node-5";
            colonPeer.host = "192.168.1.5:";
            colonPeer.nodeHash = 555L;
            peerRecords.add(colonPeer);

            // non-numeric port
            CachePeerRecord badPortPeer = new CachePeerRecord();
            badPortPeer.peerName = "node-6";
            badPortPeer.host = "192.168.1.6:abc";
            badPortPeer.nodeHash = 666L;
            peerRecords.add(badPortPeer);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(Mockito.anyString(), Mockito.anyMap(),
                Mockito.eq(CachePeerRecord.class), Mockito.any())).thenReturn(peerRecords);

            Connection mockConn = mock(Connection.class);
            GmsRpcMetaService service = new GmsRpcMetaService(mockConn);

            Collection<PeerInfo> peers = service.getPeers();
            // Only the valid peer should be returned
            Assert.assertEquals(1, peers.size());
        }
    }

    @Test
    public void testBeginRepeatableRead() throws Exception {
        Connection mockConn = mock(Connection.class);
        GmsRpcMetaService service = new GmsRpcMetaService(mockConn);

        service.beginRepeatableRead();
        verify(mockConn).setTransactionIsolation(Connection.TRANSACTION_REPEATABLE_READ);
        verify(mockConn).setAutoCommit(false);
    }

    @Test
    public void testBeginReadCommitted() throws Exception {
        Connection mockConn = mock(Connection.class);
        GmsRpcMetaService service = new GmsRpcMetaService(mockConn);

        service.beginReadCommitted();
        verify(mockConn).setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
        verify(mockConn).setAutoCommit(false);
    }

    @Test
    public void testBeginReadUncommitted() throws Exception {
        Connection mockConn = mock(Connection.class);
        GmsRpcMetaService service = new GmsRpcMetaService(mockConn);

        service.beginReadUncommitted();
        verify(mockConn).setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
        verify(mockConn).setAutoCommit(false);
    }

    @Test
    public void testCommit() throws Exception {
        Connection mockConn = mock(Connection.class);
        GmsRpcMetaService service = new GmsRpcMetaService(mockConn);

        service.commit();
        verify(mockConn).commit();
    }

    @Test
    public void testRollback() throws Exception {
        Connection mockConn = mock(Connection.class);
        GmsRpcMetaService service = new GmsRpcMetaService(mockConn);

        service.rollback();
        verify(mockConn).rollback();
    }

    @Test
    public void testClose() throws Exception {
        Connection mockConn = mock(Connection.class);
        when(mockConn.isClosed()).thenReturn(false);

        GmsRpcMetaService service = new GmsRpcMetaService(mockConn);
        service.close();
        verify(mockConn).close();
    }

    @Test
    public void testCloseAlreadyClosed() throws Exception {
        Connection mockConn = mock(Connection.class);
        when(mockConn.isClosed()).thenReturn(true);

        GmsRpcMetaService service = new GmsRpcMetaService(mockConn);
        service.close();
        // close() should not be called on the connection since it is already closed
        verify(mockConn, Mockito.never()).close();
    }

    @Test
    public void testGetAccessors() {
        Connection mockConn = mock(Connection.class);
        GmsRpcMetaService service = new GmsRpcMetaService(mockConn);

        Assert.assertNotNull(service.getUserAccessor());
        Assert.assertNotNull(service.getPeerAccessor());
    }
}
