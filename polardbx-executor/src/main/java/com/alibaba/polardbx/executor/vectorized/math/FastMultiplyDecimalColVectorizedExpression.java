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

package com.alibaba.polardbx.executor.vectorized.math;

import com.alibaba.polardbx.common.datatype.DecimalConverter;
import com.alibaba.polardbx.common.datatype.DecimalStructure;
import com.alibaba.polardbx.common.datatype.FastDecimalUtils;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.utils.MathUtils;
import com.alibaba.polardbx.executor.chunk.DecimalBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpressionUtils;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import io.airlift.slice.Slice;

import java.util.Arrays;

import static com.alibaba.polardbx.common.datatype.DecimalTypeBase.DECIMAL_MEMORY_SIZE;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;
import static com.alibaba.polardbx.executor.vectorized.metadata.ExpressionPriority.SPECIAL;

@ExpressionSignatures(
    names = {"*", "multiply"},
    argumentTypes = {"Decimal", "Decimal"},
    argumentKinds = {Variable, Variable},
    priority = SPECIAL)
public class FastMultiplyDecimalColVectorizedExpression extends AbstractVectorizedExpression {

    // for fast decimal multiply
    long[] sum0s;
    long[] sum9s;
    long[] sum18s;
    long[] carry0s;
    long[] carry9s;
    long[] carry18s;
    int[] nonNullSelection;
    boolean enableDecimal128;

    public FastMultiplyDecimalColVectorizedExpression(int outputIndex, VectorizedExpression[] children) {
        super(DataTypes.DecimalType, outputIndex, children);
        this.enableDecimal128 = DynamicConfig.getInstance().enableDecimal128();
    }

    @Override
    public void eval(EvaluationContext ctx) {
        super.evalChildren(ctx);
        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        DecimalBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType).cast(DecimalBlock.class);
        DecimalBlock leftInputVectorSlot =
            chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType()).cast(DecimalBlock.class);
        DecimalBlock rightInputVectorSlot =
            chunk.slotIn(children[1].getOutputIndex(), children[1].getOutputDataType()).cast(DecimalBlock.class);
        VectorizedExpressionUtils
            .mergeNulls(chunk, outputIndex, children[0].getOutputIndex(), children[1].getOutputIndex());
        if (leftInputVectorSlot.isDecimal64() && rightInputVectorSlot.isDecimal64()
            && checkResultScaleDecimal64(leftInputVectorSlot.getScale(), rightInputVectorSlot.getScale(),
            outputVectorSlot.getScale())) {
            boolean success = doDecimal64Multiply(batchSize, isSelectionInUse, sel,
                leftInputVectorSlot, rightInputVectorSlot, outputVectorSlot);
            if (success) {
                return;
            }
        }

        Slice output = outputVectorSlot.getMemorySegments();

        boolean[] isNulls = outputVectorSlot.nulls();

        leftInputVectorSlot.collectDecimalInfo();
        rightInputVectorSlot.collectDecimalInfo();

        // normal multiply
        normalMul(chunk, batchSize, isSelectionInUse, sel, outputVectorSlot,
            leftInputVectorSlot, rightInputVectorSlot, output);
    }

    private boolean checkResultScaleDecimal64(int leftScale, int rightScale, int actualResultScale) {
        int resultScale = leftScale + rightScale;
        if (resultScale != actualResultScale) {
            return false;
        }
        if (resultScale == 0) {
            return true;
        }
        return DecimalConverter.isDecimal64(resultScale) || DecimalConverter.isDecimal128(resultScale);
    }

    private boolean doDecimal64Multiply(int batchSize, boolean isSelectionInUse, int[] sel,
                                        DecimalBlock leftInputVectorSlot, DecimalBlock rightInputVectorSlot,
                                        DecimalBlock outputVectorSlot) {
        long[] decimal64Output = outputVectorSlot.allocateDecimal64();
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];

                long x = leftInputVectorSlot.getLong(j);
                long y = rightInputVectorSlot.getLong(j);
                long result = x * y;
                if (MathUtils.longMultiplyOverflow(x, y, result)) {
                    if (enableDecimal128) {
                        return doDecimal64MulTo128(batchSize, isSelectionInUse, sel,
                            leftInputVectorSlot, rightInputVectorSlot, outputVectorSlot);
                    } else {
                        outputVectorSlot.deallocateDecimal64();
                        return false;
                    }

                }

                decimal64Output[j] = result;
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                long x = leftInputVectorSlot.getLong(i);
                long y = rightInputVectorSlot.getLong(i);
                long result = x * y;
                if (MathUtils.longMultiplyOverflow(x, y, result)) {
                    if (enableDecimal128) {
                        return doDecimal64MulTo128(batchSize, isSelectionInUse, sel,
                            leftInputVectorSlot, rightInputVectorSlot, outputVectorSlot);
                    } else {
                        outputVectorSlot.deallocateDecimal64();
                        return false;
                    }

                }

                decimal64Output[i] = result;
            }
        }
        return true;
    }

    /**
     * decimal64 * decimal64 will not overflow decimal128
     */
    private boolean doDecimal64MulTo128(int batchSize, boolean isSelectionInUse, int[] sel,
                                        DecimalBlock leftInputVectorSlot, DecimalBlock rightInputVectorSlot,
                                        DecimalBlock outputVectorSlot) {
        outputVectorSlot.allocateDecimal128();
        long[] outputDecimal128Low = outputVectorSlot.getDecimal128LowValues();
        long[] outputDecimal128High = outputVectorSlot.getDecimal128HighValues();

        if (isSelectionInUse) {
            mul64To128(batchSize, sel,
                leftInputVectorSlot, rightInputVectorSlot, outputDecimal128Low, outputDecimal128High);
        } else {
            mul64To128(batchSize, leftInputVectorSlot, rightInputVectorSlot, outputDecimal128Low, outputDecimal128High);
        }
        return true;
    }

    /**
     * no overflow
     */
    private void mul64To128(int batchSize, int[] sel, DecimalBlock leftInputVectorSlot,
                            DecimalBlock rightInputVectorSlot,
                            long[] outputDecimal128Low, long[] outputDecimal128High) {
        for (int i = 0; i < batchSize; i++) {
            int j = sel[i];

            long decimal64 = leftInputVectorSlot.getLong(j);
            long multiplier = rightInputVectorSlot.getLong(j);

            if (decimal64 == 0 || multiplier == 0) {
                outputDecimal128Low[j] = 0;
                outputDecimal128High[j] = 0;
                continue;
            }
            if (decimal64 == 1) {
                outputDecimal128Low[j] = multiplier;
                outputDecimal128High[j] = multiplier >= 0 ? 0 : -1;
                continue;
            }
            if (decimal64 == -1 && multiplier != 0x8000000000000000L) {
                long negMultiplier = -multiplier;
                outputDecimal128Low[j] = negMultiplier;
                outputDecimal128High[j] = negMultiplier >= 0 ? 0 : -1;
                continue;
            }
            if (multiplier == 1) {
                outputDecimal128Low[j] = decimal64;
                outputDecimal128High[j] = decimal64 >= 0 ? 0 : -1;
                continue;
            }
            if (multiplier == -1 && decimal64 != 0x8000000000000000L) {
                outputDecimal128Low[j] = -decimal64;
                outputDecimal128High[j] = -decimal64 >= 0 ? 0 : -1;
                continue;
            }
            boolean positive;
            long multiplierAbs = multiplier;
            long decimal64Abs = Math.abs(decimal64);
            if (multiplier < 0) {
                multiplierAbs = -multiplierAbs;
                positive = decimal64 < 0;
            } else {
                positive = decimal64 >= 0;
            }
            long res;
            int x1 = (int) decimal64Abs;
            int x2 = (int) (decimal64Abs >>> 32);
            int y1 = (int) multiplierAbs;
            int y2 = (int) (multiplierAbs >>> 32);

            res = (y1 & 0xFFFFFFFFL) * (x1 & 0xFFFFFFFFL);
            int z1 = (int) res;

            res = (y1 & 0xFFFFFFFFL) * (x2 & 0xFFFFFFFFL)
                + (y2 & 0xFFFFFFFFL) * (x1 & 0xFFFFFFFFL) + (res >>> 32);
            int z2 = (int) res;

            res = (y2 & 0xFFFFFFFFL) * (x2 & 0xFFFFFFFFL) + (res >>> 32);
            int z3 = (int) res;

            res = (res >>> 32);
            int z4 = (int) res;
            if (positive) {
                outputDecimal128Low[j] = (z1 & 0xFFFFFFFFL) | (((long) z2) << 32);
                outputDecimal128High[j] = (z3 & 0xFFFFFFFFL) | (((long) z4) << 32);
            } else {
                outputDecimal128Low[j] = ~((z1 & 0xFFFFFFFFL) | (((long) z2) << 32)) + 1;
                outputDecimal128High[j] = ~((z3 & 0xFFFFFFFFL) | (((long) z4) << 32));
                if (outputDecimal128Low[j] == 0) {
                    outputDecimal128High[j] += 1;
                }
            }
        }
    }

    /**
     * no overflow
     */
    private void mul64To128(int batchSize, DecimalBlock leftInputVectorSlot, DecimalBlock rightInputVectorSlot,
                            long[] outputDecimal128Low, long[] outputDecimal128High) {
        for (int i = 0; i < batchSize; i++) {
            long decimal64 = leftInputVectorSlot.getLong(i);
            long multiplier = rightInputVectorSlot.getLong(i);

            if (decimal64 == 0 || multiplier == 0) {
                outputDecimal128Low[i] = 0;
                outputDecimal128High[i] = 0;
                continue;
            }
            if (decimal64 == 1) {
                outputDecimal128Low[i] = multiplier;
                outputDecimal128High[i] = multiplier >= 0 ? 0 : -1;
                continue;
            }
            if (decimal64 == -1 && multiplier != 0x8000000000000000L) {
                long negMultiplier = -multiplier;
                outputDecimal128Low[i] = negMultiplier;
                outputDecimal128High[i] = negMultiplier >= 0 ? 0 : -1;
                continue;
            }
            if (multiplier == 1) {
                outputDecimal128Low[i] = decimal64;
                outputDecimal128High[i] = decimal64 >= 0 ? 0 : -1;
                continue;
            }
            if (multiplier == -1 && decimal64 != 0x8000000000000000L) {
                outputDecimal128Low[i] = -decimal64;
                outputDecimal128High[i] = -decimal64 >= 0 ? 0 : -1;
                continue;
            }
            boolean positive;
            long multiplierAbs = multiplier;
            long decimal64Abs = Math.abs(decimal64);
            if (multiplier < 0) {
                multiplierAbs = -multiplierAbs;
                positive = decimal64 < 0;
            } else {
                positive = decimal64 >= 0;
            }
            long res;
            int x1 = (int) decimal64Abs;
            int x2 = (int) (decimal64Abs >>> 32);
            int y1 = (int) multiplierAbs;
            int y2 = (int) (multiplierAbs >>> 32);

            res = (y1 & 0xFFFFFFFFL) * (x1 & 0xFFFFFFFFL);
            int z1 = (int) res;

            res = (y1 & 0xFFFFFFFFL) * (x2 & 0xFFFFFFFFL)
                + (y2 & 0xFFFFFFFFL) * (x1 & 0xFFFFFFFFL) + (res >>> 32);
            int z2 = (int) res;

            res = (y2 & 0xFFFFFFFFL) * (x2 & 0xFFFFFFFFL) + (res >>> 32);
            int z3 = (int) res;

            res = (res >>> 32);
            int z4 = (int) res;
            if (positive) {
                outputDecimal128Low[i] = (z1 & 0xFFFFFFFFL) | (((long) z2) << 32);
                outputDecimal128High[i] = (z3 & 0xFFFFFFFFL) | (((long) z4) << 32);
            } else {
                outputDecimal128Low[i] = ~((z1 & 0xFFFFFFFFL) | (((long) z2) << 32)) + 1;
                outputDecimal128High[i] = ~((z3 & 0xFFFFFFFFL) | (((long) z4) << 32));
                if (outputDecimal128Low[i] == 0) {
                    outputDecimal128High[i] += 1;
                }
            }
        }
    }

    private void normalMul(MutableChunk chunk, int batchSize, boolean isSelectionInUse, int[] sel,
                           DecimalBlock outputVectorSlot, DecimalBlock leftInputVectorSlot,
                           DecimalBlock rightInputVectorSlot, Slice output) {
        DecimalStructure leftDec;
        DecimalStructure rightDec;

        boolean isLeftUnsigned = children[0].getOutputDataType().isUnsigned();
        boolean isRightUnsigned = children[1].getOutputDataType().isUnsigned();

        Slice leftOutput = leftInputVectorSlot.allocCachedSlice();
        Slice rightOutput = rightInputVectorSlot.allocCachedSlice();

        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                int fromIndex = j * DECIMAL_MEMORY_SIZE;

                // wrap memory in specified position
                Slice decimalMemorySegment = output.slice(fromIndex, DECIMAL_MEMORY_SIZE);
                DecimalStructure toValue = new DecimalStructure(decimalMemorySegment);

                // do reset

                // fetch left decimal value
                leftDec = new DecimalStructure(leftInputVectorSlot.getRegion(j, leftOutput));

                // fetch right decimal value
                rightDec = new DecimalStructure(rightInputVectorSlot.getRegion(j, rightOutput));

                // do operator
                FastDecimalUtils.mul(leftDec, rightDec, toValue);
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                int fromIndex = i * DECIMAL_MEMORY_SIZE;

                // wrap memory in specified position
                Slice decimalMemorySegment = output.slice(fromIndex, DECIMAL_MEMORY_SIZE);
                DecimalStructure toValue = new DecimalStructure(decimalMemorySegment);

                // do reset

                // fetch left decimal value
                leftDec = new DecimalStructure(leftInputVectorSlot.getRegion(i, leftOutput));

                // fetch right decimal value
                rightDec = new DecimalStructure(rightInputVectorSlot.getRegion(i, rightOutput));

                // do operator
                FastDecimalUtils.mul(leftDec, rightDec, toValue);
            }
        }
        outputVectorSlot.setFullState();
    }

    private void initForFastMethod(int batchSize) {
        sum0s = new long[batchSize];
        sum9s = new long[batchSize];
        sum18s = new long[batchSize];
        carry0s = new long[batchSize];
        carry9s = new long[batchSize];
        carry18s = new long[batchSize];
        nonNullSelection = new int[batchSize];
    }
}
