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

package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.executor.ExecutorHelper;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.mpp.operator.LocalBufferExec;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.profiler.memory.MemoryStatAttribute;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.memory.MemoryType;
import com.alibaba.polardbx.optimizer.memory.QueryMemoryPoolHolder;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import com.alibaba.polardbx.statistics.RuntimeStatistics;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.ListenableFuture;
import org.apache.calcite.rel.core.RecursiveCTE;

import java.util.List;

import static com.alibaba.polardbx.common.exception.code.ErrorCode.ERR_EXECUTOR;
import static com.alibaba.polardbx.common.properties.ConnectionParams.MAX_RECURSIVE_COUNT;
import static com.alibaba.polardbx.common.properties.ConnectionParams.MAX_RECURSIVE_CTE_MEM_BYTES;

/**
 * Recursive Executor
 *
 * @author fangwu
 */
public class RecursiveCTEExec extends AbstractExecutor {
    private static final String CTE_PRE = "CTE_PRE";
    protected RecursiveCTE cte;
    protected final Executor anchorExec;

    private final long fetchSize;

    // Internal States
    private boolean isAnchorExecFinished;
    private boolean isFinished = false;
    private long currentRowIndex = 0L;

    private ListenableFuture<?> blocked;
    private LocalBufferExec recursiveExecutor;
    private ExecutionContext recursiveContext;

    private int recursiveCount;
    private long memBytes;

    private List<Chunk> chunks = Lists.newArrayList();

    private final String dataKey;

    public RecursiveCTEExec(String cteName,
                            RecursiveCTE cte,
                            Executor anchorExec,
                            long fetchSize,
                            ExecutionContext context) {
        super(context);
        this.fetchSize = fetchSize;
        this.cte = cte;
        this.anchorExec = anchorExec;
        this.blocked = NOT_BLOCKED;
        this.dataKey = buildCTEKey(cteName);
    }

    @Override
    void doOpen() {
        createBlockBuilders();
        anchorExec.open();
    }

    @Override
    Chunk doNextChunk() {
        if (isFinished) {
            return null;
        }
        // loop iteration check
        if (recursiveCount >= InstConfUtil.getInt(MAX_RECURSIVE_COUNT)) {
            isFinished = true;
            throw new TddlRuntimeException(ERR_EXECUTOR,
                " Recursive query aborted after " + recursiveCount + " iterations. "
                    + "Try increasing @@cte_max_recursion_depth to a larger value.");
        }

        if (!isAnchorExecFinished) {
            // process anchor exec until it finished
            blocked = anchorExec.produceIsBlocked();
            Chunk c;
            if (anchorExec.produceIsFinished()) {
                c = anchorExec.nextChunk();
                if (c == null) {
                    isAnchorExecFinished = true;
                    return null;
                }
            } else {
                c = anchorExec.nextChunk();
                if (c == null) {
                    return null;
                }
            }

            // Handle the chunk before returning
            handleChunk(c);
            return c;
        }

        // recursive part
        if (recursiveExecutor == null) {
            buildNewRecursiveExec();
            return null;
        }

        Chunk r;
        if (recursiveExecutor.produceIsFinished()) {
            r = recursiveExecutor.nextChunk();
            if (r == null) {
                if (chunks.isEmpty()) {
                    // Mark finished if last iteration executor produced none data chunk and is finished
                    isFinished = true;
                } else {
                    // Rebuild recursive executor if last iteration executor produced any data chunk and is finished
                    buildNewRecursiveExec();
                }
                return null;
            }
        } else {
            r = recursiveExecutor.nextChunk();
            if (r == null) {
                return null;
            }
        }

        // Handle the chunk before returning
        handleChunk(r);
        return r;
    }

    private void handleChunk(Chunk r) {
        // mem check, chunk should not be null here
        memCheck(r);

        chunks.add(r);

        // fetch size check
        currentRowIndex += r.getPositionCount();
        if (currentRowIndex >= fetchSize) {
            isFinished = true;
        }
    }

    /**
     * Performs memory check by accumulating the element used bytes of the chunk.
     * Throws an exception if the accumulated memory exceeds the maximum configured limit.
     */
    private void memCheck(Chunk c) {
        // mem check
        memBytes += c.getElementUsedBytes();
        if (memBytes > InstConfUtil.getLong(MAX_RECURSIVE_CTE_MEM_BYTES)) {
            throw GeneralUtil.nestedException(" recursive cte mem bytes exceed" + memBytes);
        }
    }

    // Builds a new recursive executor for the CTE iteration.
    private void buildNewRecursiveExec() {
        String queryId = context.getTraceId() + "_" + Thread.currentThread().getName() + "_" + recursiveCount;

        // Clear all memory pools if the recursive context exists.
        if (recursiveContext != null) {
            recursiveContext.clearAllMemoryPool();
        }

        // Close the previous recursive executor if it exists and is finished.
        if (recursiveExecutor != null && recursiveExecutor.produceIsFinished()) {
            recursiveExecutor.close();
        }

        // Prepare a new execution context for the CTE iteration.
        recursiveContext = prepareContext(context, queryId);

        // Set parallelism level to 1.
        recursiveContext.getExtraCmds().put(ConnectionProperties.PARALLELISM, 1);

        // Store cache references.
        recursiveContext.getCacheRefs().put(dataKey.hashCode(), chunks);

        // Execute local execution with the prepared context.
        recursiveExecutor = ExecutorHelper.executeLocalExec(queryId, cte.getRight(), recursiveContext);

        // Check if the recursive executor is blocked.
        blocked = recursiveExecutor.produceIsBlocked();

        recursiveExecutor.open();

        // Reset chunks list and memory usage counter.
        chunks = Lists.newArrayList();
        memBytes = 0L;

        // Increment the recursive count.
        recursiveCount++;
    }

    @Override
    void doClose() {
        anchorExec.close();
        context.getCacheRefs().remove(dataKey.hashCode());
        if (recursiveExecutor != null) {
            recursiveExecutor.close();
        }
        if (recursiveContext != null) {
            recursiveContext.clearAllMemoryPool();
        }
    }

    @Override
    public List<DataType> getDataTypes() {
        return anchorExec.getDataTypes();
    }

    @Override
    public List<Executor> getInputs() {
        return ImmutableList.of(anchorExec);
    }

    @Override
    public boolean produceIsFinished() {
        return isFinished;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return blocked;
    }

    public static String buildCTEKey(String cteName) {
        return CTE_PRE + "_" + cteName;
    }

    // Prepares a new execution context for a cte iteration exec based on the original execution context.
    public static ExecutionContext prepareContext(ExecutionContext executionContext, String cteId) {
        // Creates a copy option for the execution context.
        ExecutionContext.CopyOption copyOption = new ExecutionContext.CopyOption()
            .setMemoryPoolHolder(new QueryMemoryPoolHolder())
            .setParameters(executionContext.cloneParamsOrNull());

        // Copies the original execution context with the specified options.
        ExecutionContext cteContext = executionContext.copy(copyOption);

        String cteMemoryPoolName = MemoryStatAttribute.CTE + "_" + cteId;

        // Retrieves the SQL memory pool from the original execution context.
        MemoryPool sqlMemoryPool = executionContext.getMemoryPool();

        MemoryPool cteMemoryPool = sqlMemoryPool.getOrCreatePool(cteMemoryPoolName,
            sqlMemoryPool.getMaxLimit(), MemoryType.CTE);

        // Sets the cte memory pool for the cte context.
        cteContext.setMemoryPool(cteMemoryPool);

        RuntimeStatistics newRuntimeStat = RuntimeStatHelper.buildRuntimeStat(cteContext);

        cteContext.setRuntimeStatistics(newRuntimeStat);

        return cteContext;
    }
}
