/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package com.alibaba.polardbx.net;

import com.alibaba.polardbx.Capabilities;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.net.buffer.BufferQueue;
import com.alibaba.polardbx.net.compress.RawPacketByteBufferOutputProxy;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

@RunWith(MockitoJUnitRunner.class)
public class FrontendConnectionTest {

    @InjectMocks
    private FrontendConnection target = mock(FrontendConnection.class, CALLS_REAL_METHODS);

    @Before
    public void setUp() throws Exception {
        Field activeProxyField = FrontendConnection.class.getDeclaredField("activePacketOutputProxy");
        activeProxyField.setAccessible(true);
        activeProxyField.set(target, new AtomicReference<RawPacketByteBufferOutputProxy>());
        target.setWriteQueue(new BufferQueue(1));
    }

    @Test
    public void testConnectorJPartialResultCompatibility() {
        assertClientSafety("MySQL Connector Java", "5.0.49", false);
        assertClientSafety("MySQL Connector Java", "5.1.33", false);
        assertClientSafety("MySQL Connector Java", "5.1.34", false);
        assertClientSafety("MySQL Connector/J", "5.1.34-bin", false);
        assertClientSafety("MySQL Connector/J", "5.1.34.12", false);
        assertClientSafety("MySQL Connector Java", "5.1.35", true);
        assertClientSafety("MySQL Connector/J", "5.1.35-bin", true);
        assertClientSafety("mysql connector/j", "5.1.36", true);
        assertClientSafety("MySQL Connector Java", "5.1.40.12", true);
        assertClientSafety("MySQL Connector/J", "5.2.0", true);
        assertClientSafety("MySQL Connector/J", "8.0.33", true);
    }

    @Test
    public void testUnknownOrInvalidClientIsAllowedByBlacklist() {
        assertClientSafety(null, "8.0.33", true);
        assertClientSafety("libmysql", "8.0.33", true);
        assertClientSafety("MySQL Connector/J", null, true);
        assertClientSafety("MySQL Connector/J", " ", true);
        assertClientSafety("MySQL Connector/J", "5.1", true);
        assertClientSafety("MySQL Connector/J", "5.1.35.invalid", true);
        assertClientSafety("MySQL Connector/J", "9223372036854775808.1.35", true);
    }

    @Test
    public void testConnectAttributesCapabilityFollowsSwitch() {
        DynamicConfig.getInstance().loadValue(null,
            ConnectionProperties.ENABLE_ERR_PACKET_AFTER_PARTIAL_RESULT, "false");
        try {
            Assert.assertEquals(0, target.getServerCapabilities() & Capabilities.CLIENT_CONNECT_ATTRS);

            DynamicConfig.getInstance().loadValue(null,
                ConnectionProperties.ENABLE_ERR_PACKET_AFTER_PARTIAL_RESULT, "true");

            Assert.assertTrue((target.getServerCapabilities() & Capabilities.CLIENT_CONNECT_ATTRS) != 0);
        } finally {
            DynamicConfig.getInstance().loadValue(null,
                ConnectionProperties.ENABLE_ERR_PACKET_AFTER_PARTIAL_RESULT, "false");
        }
    }

    @Test
    public void testPacketOutputStateReset() {
        target.markPacketOutputDirty();
        Assert.assertEquals(FrontendConnection.PacketOutputState.DIRTY, target.getPacketOutputState());

        target.markPacketOutputClean();
        Assert.assertEquals(FrontendConnection.PacketOutputState.CLEAN, target.getPacketOutputState());

        target.resetPacketOutputState();
        Assert.assertEquals(FrontendConnection.PacketOutputState.NONE, target.getPacketOutputState());
    }

    @Test
    public void testActivePacketOutputProxyLifecycle() {
        RawPacketByteBufferOutputProxy firstProxy = mock(RawPacketByteBufferOutputProxy.class);
        RawPacketByteBufferOutputProxy secondProxy = mock(RawPacketByteBufferOutputProxy.class);

        target.registerActivePacketOutputProxy(firstProxy);
        target.registerActivePacketOutputProxy(firstProxy);
        try {
            target.registerActivePacketOutputProxy(secondProxy);
            Assert.fail("A second active proxy must be rejected");
        } catch (TddlRuntimeException expected) {
            Assert.assertTrue(expected.getMessage().contains("Another packet output proxy"));
        }

        target.flushActivePacketOutputProxy();
        target.flushActivePacketOutputProxy();
        verify(firstProxy).flushPendingBuffer();

        target.registerActivePacketOutputProxy(firstProxy);
        target.clearActivePacketOutputProxy(secondProxy);
        target.flushActivePacketOutputProxy();
        verify(firstProxy, times(2)).flushPendingBuffer();

        target.registerActivePacketOutputProxy(firstProxy);
        target.clearActivePacketOutputProxy(firstProxy);
        target.flushActivePacketOutputProxy();
        verify(firstProxy, times(2)).flushPendingBuffer();
    }

    @Test
    public void testCleanupClosesActivePacketOutputProxy() {
        RawPacketByteBufferOutputProxy proxy = mock(RawPacketByteBufferOutputProxy.class);
        target.registerActivePacketOutputProxy(proxy);

        target.cleanup();

        verify(proxy).close();
    }

    private void assertClientSafety(String clientName, String clientVersion, boolean expected) {
        target.setClientName(clientName);
        target.setClientVersion(clientVersion);

        Assert.assertEquals(clientName, target.getClientName());
        Assert.assertEquals(clientVersion, target.getClientVersion());
        Assert.assertEquals(expected, target.isClientSafeForErrAfterPartialResult());
    }
}
