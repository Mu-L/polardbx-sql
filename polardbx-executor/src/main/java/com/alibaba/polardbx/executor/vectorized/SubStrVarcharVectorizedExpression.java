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

package com.alibaba.polardbx.executor.vectorized;

import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.ReferenceBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Const;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

@ExpressionSignatures(
    names = {"SUBSTRING", "SUBSTR"},
    argumentTypes = {"Varchar", "Long", "Long"},
    argumentKinds = {Variable, Const, Const}
)
public class SubStrVarcharVectorizedExpression extends AbstractVectorizedExpression {
    private boolean shouldReturnNull;
    private boolean shouldReturnEmpty;
    private boolean useNegativeStart;
    private int startPos;
    private int subStrLen;

    private boolean isLatin1;

    public SubStrVarcharVectorizedExpression(DataType<?> outputDataType,
                                             int outputIndex, VectorizedExpression[] children) {
        super(outputDataType, outputIndex, children);

        DataType inputDataType = children[0].getOutputDataType();
        this.isLatin1 = inputDataType instanceof SliceType && inputDataType.isLatin1Encoding();

        Object operand1Value = ((LiteralVectorizedExpression) children[1]).getConvertedValue();
        Object operand2Value = ((LiteralVectorizedExpression) children[2]).getConvertedValue();
        if (operand1Value == null || operand2Value == null) {
            startPos = 0;
            subStrLen = 0;
            shouldReturnNull = true;
            shouldReturnEmpty = false;
        } else {
            startPos = DataTypes.LongType.convertFrom(operand1Value).intValue();

            // Assumes that the maximum length of a String is < INT_MAX32
            subStrLen = DataTypes.LongType.convertFrom(operand2Value).intValue();

            // Negative or zero length, will return empty string.
            if (subStrLen <= 0) {
                shouldReturnNull = false;
                shouldReturnEmpty = true;
            }

            // handle start position
            // In MySQL: start= ((start < 0) ? res->numchars() + start : start - 1);
            if (startPos < 0) {
                useNegativeStart = true;
            } else if (startPos == 0) {
                shouldReturnEmpty = true;
            } else {
                useNegativeStart = false;
                startPos = startPos - 1;
            }
        }

    }

    /**
     * Get the number of UTF-8 characters in a slice.
     */
    private int getUtf8CharCount(Slice slice) {
        int charCount = 0;
        int byteLength = slice.length();
        for (int i = 0; i < byteLength; ) {
            byte b = slice.getByte(i);
            int charBytes = getUtf8CharBytes(b);
            i += charBytes;
            charCount++;
        }
        return charCount;
    }

    /**
     * Get the byte length of a UTF-8 character by its first byte.
     */
    private int getUtf8CharBytes(byte firstByte) {
        int b = firstByte & 0xFF;
        if ((b & 0x80) == 0) {
            return 1; // 0xxxxxxx
        } else if ((b & 0xE0) == 0xC0) {
            return 2; // 110xxxxx
        } else if ((b & 0xF0) == 0xE0) {
            return 3; // 1110xxxx
        } else if ((b & 0xF8) == 0xF0) {
            return 4; // 11110xxx
        }
        return 1; // Invalid UTF-8, treat as 1 byte
    }

    /**
     * Find the byte offset of the character at the given character position.
     * Returns -1 if charPos is out of bounds.
     */
    private int findUtf8CharByteOffset(Slice slice, int charPos) {
        int byteOffset = 0;
        int charCount = 0;
        int byteLength = slice.length();

        while (byteOffset < byteLength && charCount < charPos) {
            byte b = slice.getByte(byteOffset);
            int charBytes = getUtf8CharBytes(b);
            byteOffset += charBytes;
            charCount++;
        }

        if (charCount == charPos) {
            return byteOffset;
        }
        return -1; // Out of bounds
    }

    /**
     * Extract substring from slice based on character positions.
     */
    private Slice substringByCharPosition(Slice slice, int charStart, int charLen) {
        if (isLatin1) {
            // For Latin1, byte position equals character position
            int start = useNegativeStart ? slice.length() + startPos : startPos;
            int len = Math.min(slice.length() - start, subStrLen);
            if (start < 0 || start >= slice.length()) {
                return Slices.EMPTY_SLICE;
            }
            return slice.slice(start, len);
        } else {
            // For UTF-8, need to find byte offsets
            int totalChars = getUtf8CharCount(slice);
            int actualCharStart = useNegativeStart ? totalChars + startPos : startPos;

            if (actualCharStart < 0 || actualCharStart >= totalChars) {
                return Slices.EMPTY_SLICE;
            }

            int byteStart = findUtf8CharByteOffset(slice, actualCharStart);
            if (byteStart < 0) {
                return Slices.EMPTY_SLICE;
            }

            int actualCharLen = Math.min(totalChars - actualCharStart, subStrLen);
            int byteEnd = findUtf8CharByteOffset(slice, actualCharStart + actualCharLen);
            if (byteEnd < 0) {
                byteEnd = slice.length();
            }

            return slice.slice(byteStart, byteEnd - byteStart);
        }
    }

    @Override
    public void eval(EvaluationContext ctx) {
        children[0].eval(ctx);
        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] selection = chunk.selection();

        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
        RandomAccessBlock leftInputVectorSlot =
            chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());

        if (shouldReturnNull) {
            boolean[] outputNulls = outputVectorSlot.nulls();
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = selection[i];
                    outputNulls[j] = true;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    outputNulls[i] = true;
                }
            }
            return;
        }

        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex());

        Object[] objectArray = ((ReferenceBlock) outputVectorSlot).objectArray();
        if (shouldReturnEmpty) {
            // Directly returning the empty slice.
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = selection[i];
                    objectArray[j] = Slices.EMPTY_SLICE;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    objectArray[i] = Slices.EMPTY_SLICE;
                }
            }
            return;
        }

        if (leftInputVectorSlot instanceof SliceBlock) {
            SliceBlock sliceBlock = (SliceBlock) leftInputVectorSlot;
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = selection[i];
                    Slice slice = sliceBlock.getRegion(j);
                    objectArray[j] = substringByCharPosition(slice, startPos, subStrLen);
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    Slice slice = sliceBlock.getRegion(i);
                    objectArray[i] = substringByCharPosition(slice, startPos, subStrLen);
                }
            }
        } else if (leftInputVectorSlot instanceof ReferenceBlock) {
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = selection[i];
                    Slice slice = ((Slice) leftInputVectorSlot.elementAt(j));
                    if (slice == null) {
                        objectArray[j] = Slices.EMPTY_SLICE;
                    } else {
                        objectArray[j] = substringByCharPosition(slice, startPos, subStrLen);
                    }
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    Slice slice = ((Slice) leftInputVectorSlot.elementAt(i));
                    if (slice == null) {
                        objectArray[i] = Slices.EMPTY_SLICE;
                    } else {
                        objectArray[i] = substringByCharPosition(slice, startPos, subStrLen);
                    }
                }
            }
        }
    }
}
