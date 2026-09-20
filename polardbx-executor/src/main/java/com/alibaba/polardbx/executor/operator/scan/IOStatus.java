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

package com.alibaba.polardbx.executor.operator.scan;

import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.scan.impl.IOStatusImpl;
import com.alibaba.polardbx.executor.operator.scan.impl.RingBufferIOStatus;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

public interface IOStatus<BATCH> extends MemoryCountable {

    static IOStatus<Chunk> createUnBounded(String workId) {
        return new IOStatusImpl(workId);
    }

    static IOStatus<Chunk> createBounded(String workId, int boundSize) {
        return new RingBufferIOStatus(workId, boundSize);
    }

    /**
     * The unique identifier of the scan work.
     */
    String workId();

    ScanState state();

    ListenableFuture<?> isBlocked();

    ListenableFuture<?> waitForEmpty();

    boolean addResult(BATCH batch);

    // void addResults(List<BATCH> batches);

    void addResult(Integer rowGroupId, Runnable evictable, List<BATCH> batches);

    BATCH popResult();

    void addException(Throwable t);

    void throwIfFailed();

    void finish();

    void close();

    long rowCount();
}
