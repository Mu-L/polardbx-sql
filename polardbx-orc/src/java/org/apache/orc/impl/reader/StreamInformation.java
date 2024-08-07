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

package org.apache.orc.impl.reader;

import org.apache.orc.DataReader;
import org.apache.orc.OrcProto;
import org.apache.orc.impl.BufferChunk;

import java.nio.ByteBuffer;

public class StreamInformation {
    public final OrcProto.Stream.Kind kind;
    public final int column;
    public final long offset;
    public final long length;
    public BufferChunk firstChunk;

    public StreamInformation(OrcProto.Stream.Kind kind,
                             int column, long offset, long length) {
        this.kind = kind;
        this.column = column;
        this.offset = offset;
        this.length = length;
    }

    public void releaseBuffers(DataReader reader) {
        long end = offset + length;
        BufferChunk ptr = firstChunk;
        while (ptr != null && ptr.getOffset() < end) {
            ByteBuffer buffer = ptr.getData();
            if (buffer != null) {
                reader.releaseBuffer(buffer);
                ptr.setChunk(null);
            }
            ptr = (BufferChunk) ptr.next;
        }
        firstChunk = null;
    }

    public long releaseBuffers() {
        long releasedBytes = 0L;
        long end = offset + length;
        BufferChunk ptr = firstChunk;
        while (ptr != null && ptr.getOffset() < end) {
            ByteBuffer buffer = ptr.getData();
            if (buffer != null) {
                // record the capacity of buffer because the total array will be GC.
                releasedBytes += buffer.capacity();
                ptr.setChunk(null);
            }
            ptr = (BufferChunk) ptr.next;
        }
        firstChunk = null;
        return releasedBytes;
    }
}