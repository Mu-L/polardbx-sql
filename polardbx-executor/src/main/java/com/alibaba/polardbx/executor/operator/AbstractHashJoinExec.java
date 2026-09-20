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

import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.memory.OperatorMemoryOwnerId;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.util.BatchBlockWriter;
import com.alibaba.polardbx.executor.operator.util.ConcurrentRawHashTable;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.InputRefExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.ScalarFunctionExpression;
import com.alibaba.polardbx.optimizer.core.join.EquiJoinKey;
import com.google.common.base.Preconditions;
import org.apache.calcite.rel.core.JoinRelType;

import java.util.List;

/**
 * Hash Join Executor
 *
 */
public abstract class AbstractHashJoinExec extends AbstractSharedHashJoinExec implements ConsumerExecutor {

    /**
     * A placeholder to mark there is no more element in this position link
     */
    public static final int LIST_END = ConcurrentRawHashTable.NOT_EXISTS;

    @FieldMemoryCounter(value = false)
    protected OperatorMemoryOwnerId consumerMemoryOwnerId = null;

    public AbstractHashJoinExec(Executor outerInput,
                                Executor innerInput,
                                JoinRelType joinType,
                                boolean maxOneRow,
                                List<EquiJoinKey> joinKeys,
                                IExpression otherCondition,
                                List<IExpression> antiJoinOperands,
                                ExecutionContext context,
                                Synchronizer synchronizer,
                                int operatorId) {
        super(outerInput, innerInput, joinType, maxOneRow, joinKeys, otherCondition, antiJoinOperands, null,
            context, synchronizer, operatorId);
    }

    @Override
    public void setConsumerOperatorMemoryOwnerId(OperatorMemoryOwnerId operatorMemoryOwnerId) {
        this.consumerMemoryOwnerId = operatorMemoryOwnerId;
    }

    @Override
    public OperatorMemoryOwnerId getConsumerMemoryOwnerId() {
        return consumerMemoryOwnerId;
    }

    @Override
    protected void createBlockBuilders() {
        if (!useVecJoin || !enableVecBuildJoinRow) {
            super.createBlockBuilders();
            return;
        }
        // Create all block builders by default
        final List<DataType> columns = getDataTypes();
        blockBuilders = new BlockBuilder[columns.size()];
        for (int i = 0; i < columns.size(); i++) {
            blockBuilders[i] = BatchBlockWriter.create(columns.get(i), context, chunkLimit);
        }
    }

    @Override
    int matchInit(Chunk keyChunk, int[] hashCodes, int position) {
        int hashCode = hashCodes[position];
        if (shared.bloomFilter != null && !shared.bloomFilter.mightContainInt(hashCode)) {
            return LIST_END;
        }

        int matchedPosition = shared.hashTable.get(hashCode);
        while (matchedPosition != LIST_END) {
            if (shared.builderKeyChunks.equals(matchedPosition, keyChunk, position)) {
                break;
            }
            matchedPosition = shared.positionLinks[matchedPosition];
        }
        return matchedPosition;
    }

    @Override
    int matchNext(int current, Chunk keyChunk, int position) {
        int matchedPosition = shared.positionLinks[current];
        while (matchedPosition != LIST_END) {
            if (shared.builderKeyChunks.equals(matchedPosition, keyChunk, position)) {
                break;
            }
            matchedPosition = shared.positionLinks[matchedPosition];
        }
        return matchedPosition;
    }

    @Override
    boolean matchValid(int current) {
        return current != LIST_END;
    }

    /**
     * get the condition index of BuildChunk for TypedHashTable,
     * should check isScalarInputRefCondition before calling this method
     */
    protected int getBuildChunkConditionIndex() {
        List<IExpression> args = ((ScalarFunctionExpression) condition).getArgs();
        Preconditions.checkArgument(args.size() == 2, "Join condition arg count should be 2");

        // get build chunk condition index for TypedHashTable
        int idx1 = ((InputRefExpression) args.get(0)).getInputRefIndex();
        int idx2 = ((InputRefExpression) args.get(1)).getInputRefIndex();

        if (buildOuterInput) {
            // since this is outer build, build chunk is on the left side and has a smaller index
            return Math.min(idx1, idx2);
        } else {
            int buildIndex = Math.max(idx1, idx2);
            // since this is inner build, build chunk is on the right side
            return buildIndex - outerInput.getDataTypes().size();
        }
    }

    /**
     * get the condition index of ProbeChunk for TypedHashTable,
     * should check isScalarInputRefCondition before calling this method
     */
    protected int getProbeChunkConditionIndex() {
        List<IExpression> args = ((ScalarFunctionExpression) condition).getArgs();
        Preconditions.checkArgument(args.size() == 2, "Join condition arg count should be 2");

        // get build chunk condition index for TypedHashTable
        int idx1 = ((InputRefExpression) args.get(0)).getInputRefIndex();
        int idx2 = ((InputRefExpression) args.get(1)).getInputRefIndex();

        if (buildOuterInput) {
            // since this is outer build, the probe index is on the right side
            int probeIndex = Math.max(idx1, idx2);
            // since this is outer build (reverse), the probe index is on the right side
            return probeIndex - outerInput.getDataTypes().size();
        } else {
            // since this is inner build, the probe index is on the left side
            return Math.min(idx1, idx2);
        }
    }
}