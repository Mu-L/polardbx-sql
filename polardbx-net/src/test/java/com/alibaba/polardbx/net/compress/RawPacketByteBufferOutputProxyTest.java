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
package com.alibaba.polardbx.net.compress;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class RawPacketByteBufferOutputProxyTest {

    @Mock
    private FrontendConnection mockConnection;

    @Mock
    private ByteBufferHolder mockBuffer;

    @InjectMocks
    private RawPacketByteBufferOutputProxy target;

    @Test
    public void testNestedPacketStateAndOutermostFlush() {
        when(mockBuffer.position()).thenReturn(1);
        when(mockConnection.writeToBuffer(any(byte[].class), any(ByteBufferHolder.class))).thenReturn(mockBuffer);

        target.packetBegin();
        target.packetBegin();

        // packetBegin only registers the proxy; output stays aligned until bytes are written.
        verify(mockConnection).registerActivePacketOutputProxy(target);
        verify(mockConnection, never()).markPacketOutputDirty();

        target.write(new byte[] {0x01});
        target.write(new byte[] {0x02});

        // Only the first written byte flips the state to DIRTY.
        verify(mockConnection, times(1)).markPacketOutputDirty();

        target.packetEnd();

        verify(mockConnection).markPacketOutputClean();
        verify(mockConnection, never()).write(any(ByteBufferHolder.class));
        verify(mockConnection, never()).clearActivePacketOutputProxy(target);

        target.packetEnd();

        verify(mockConnection).write(mockBuffer);
        verify(mockConnection).clearActivePacketOutputProxy(target);
        // The outermost end wrote no extra bytes, so no additional CLEAN transition happens.
        verify(mockConnection, times(1)).markPacketOutputClean();
    }

    @Test
    public void testEmptyPendingBufferIsRecycledOnce() {
        when(mockBuffer.position()).thenReturn(0);

        target.flushPendingBuffer();
        target.flushPendingBuffer();

        verify(mockConnection).recycle(mockBuffer);
        verify(mockConnection, never()).write(any(ByteBufferHolder.class));
    }

    @Test
    public void testActiveProxyIsClearedWhenFlushFails() {
        when(mockBuffer.position()).thenReturn(1);
        when(mockConnection.writeToBuffer(any(byte[].class), any(ByteBufferHolder.class))).thenReturn(mockBuffer);
        doThrow(new RuntimeException("write failed")).when(mockConnection).write(mockBuffer);

        target.packetBegin();
        target.write(new byte[] {0x01});
        try {
            target.packetEnd();
            Assert.fail("Expected pending buffer flush to fail");
        } catch (RuntimeException expected) {
            Assert.assertEquals("write failed", expected.getMessage());
        }

        verify(mockConnection).clearActivePacketOutputProxy(target);
        verify(mockConnection, never()).markPacketOutputClean();
    }

    @Test
    public void testPutMarksDirtyOnlyOnFirstByte() {
        when(mockConnection.checkWriteBuffer(any(ByteBufferHolder.class), anyInt())).thenReturn(mockBuffer);
        when(mockBuffer.put(anyByte())).thenReturn(mockBuffer);

        target.packetBegin();
        target.put((byte) 0x01);
        target.put((byte) 0x02);

        verify(mockConnection, times(1)).markPacketOutputDirty();
    }

    @Test
    public void testZeroLengthWritesDoNotMarkDirty() {
        when(mockConnection.writeToBuffer(any(byte[].class), any(ByteBufferHolder.class))).thenReturn(mockBuffer);
        when(mockConnection.writeToBuffer(any(byte[].class), anyInt(), anyInt(), any(ByteBufferHolder.class)))
            .thenReturn(mockBuffer);

        target.packetBegin();
        target.write(new byte[0]);
        target.write(new byte[] {0x01}, 0, 0);

        verify(mockConnection, never()).markPacketOutputDirty();
    }

    @Test
    public void testEmptyPacketScopeDoesNotChangeOutputState() {
        when(mockBuffer.position()).thenReturn(0);

        target.packetBegin();
        target.packetEnd();

        verify(mockConnection).registerActivePacketOutputProxy(target);
        verify(mockConnection).recycle(mockBuffer);
        verify(mockConnection).clearActivePacketOutputProxy(target);
        verify(mockConnection, never()).markPacketOutputDirty();
        verify(mockConnection, never()).markPacketOutputClean();
    }

    @Test
    public void testNonEmptySliceMarksDirtyOnlyOnce() {
        when(mockConnection.writeToBuffer(any(byte[].class), anyInt(), anyInt(), any(ByteBufferHolder.class)))
            .thenReturn(mockBuffer);

        target.packetBegin();
        target.write(new byte[] {0x01}, 0, 1);
        target.write(new byte[] {0x02}, 0, 1);

        verify(mockConnection, times(1)).markPacketOutputDirty();
    }

    @Test
    public void testDirtyIsMarkedAgainForEveryPacket() {
        when(mockConnection.writeToBuffer(any(byte[].class), any(ByteBufferHolder.class))).thenReturn(mockBuffer);

        target.packetBegin();
        target.packetBegin();
        target.write(new byte[] {0x01});
        target.packetEnd();

        verify(mockConnection, times(1)).markPacketOutputDirty();
        verify(mockConnection, times(1)).markPacketOutputClean();

        // The next packet of the same output scope must flip the state back to DIRTY.
        target.packetBegin();
        target.write(new byte[] {0x02});

        verify(mockConnection, times(2)).markPacketOutputDirty();
    }

    @Test
    public void testCheckWriteCapacityDoesNotMarkDirty() {
        when(mockConnection.checkWriteBuffer(any(ByteBufferHolder.class), anyInt())).thenReturn(mockBuffer);

        target.packetBegin();
        target.checkWriteCapacity(16);

        // Reserving capacity writes no byte, so the output stays aligned.
        verify(mockConnection, never()).markPacketOutputDirty();
    }

    @Test
    public void testPacketEndWithoutBeginFails() {
        try {
            target.packetEnd();
            Assert.fail("Expected unbalanced packetEnd to fail");
        } catch (TddlRuntimeException expected) {
            Assert.assertTrue(expected.getMessage().contains("packetEnd nested: -1"));
        }

        verify(mockConnection, never()).markPacketOutputClean();
        verify(mockConnection, never()).clearActivePacketOutputProxy(target);
    }

}
