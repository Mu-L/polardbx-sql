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

package com.alibaba.polardbx.net.compress;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;

/**
 * Created by simiao on 15-4-17.
 */
public class RawPacketByteBufferOutputProxy extends PacketByteBufferOutputProxy {

    /**
     * Tracks nested output scopes. The proxy flushes and unregisters when the outermost scope ends.
     */
    private int nestedPacketCount = 0;

    private boolean dirtyMarked;

    public RawPacketByteBufferOutputProxy(FrontendConnection c) {
        super(c);
    }

    public RawPacketByteBufferOutputProxy(FrontendConnection c, ByteBufferHolder buffer) {
        super(c, buffer);
    }

    private void markDirtyOnFirstWrite() {
        if (!dirtyMarked) {
            dirtyMarked = true;
            c.markPacketOutputDirty();
        }
    }

    /**
     * Marks every completed packet as aligned and flushes when the outermost output scope ends.
     */
    @Override
    public void packetEnd() {
        int nested = --nestedPacketCount;
        if (nested < 0) {
            throw new TddlRuntimeException(ErrorCode.ERR_PACKET_COMPOSE, "packetEnd nested: " + nested);
        }

        if (nested == 0) {
            try {
                flushPendingBuffer();
            } finally {
                c.clearActivePacketOutputProxy(this);
            }
        }

        if (dirtyMarked) {
            c.markPacketOutputClean();
            dirtyMarked = false;
        }
    }

    /**
     * Submit complete packets currently retained by this proxy.
     */
    public void flushPendingBuffer() {
        ByteBufferHolder buffer = currentBuffer;
        if (buffer == null) {
            return;
        }

        currentBuffer = null;

        if (buffer.position() == 0) {
            c.recycle(buffer);
            return;
        }

        c.write(buffer);
    }

    /**
     * 对于非压缩情况，可以在分配前直接输出.记住需要更新currentBuffer
     */
    @Override
    public void checkWriteCapacity(int capacity) {
        // 没用过，直接返回，不用新建

        currentBuffer = c.checkWriteBuffer(currentBuffer, capacity);
    }

    @Override
    public void write(byte[] src) {
        if (src.length > 0) {
            markDirtyOnFirstWrite();
        }
        currentBuffer = c.writeToBuffer(src, currentBuffer);
    }

    @Override
    public void write(byte[] src, int off, int len) {
        if (len > 0) {
            markDirtyOnFirstWrite();
        }
        currentBuffer = c.writeToBuffer(src, off, len, currentBuffer);
    }

    @Override
    public void packetBegin() {
        if (nestedPacketCount == 0) {
            // Keep the proxy reachable if result-set construction fails.
            c.registerActivePacketOutputProxy(this);
        }
        ++nestedPacketCount;
    }

    @Override
    public ByteBufferHolder put(byte b) {
        ByteBufferHolder buffer = super.put(b);
        markDirtyOnFirstWrite();
        return buffer;
    }
}
