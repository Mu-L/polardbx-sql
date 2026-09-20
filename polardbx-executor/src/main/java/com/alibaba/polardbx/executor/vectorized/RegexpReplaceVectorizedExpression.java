package com.alibaba.polardbx.executor.vectorized;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.ReferenceBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import io.airlift.jcodings.specific.NonStrictUTF8Encoding;
import io.airlift.joni.Matcher;
import io.airlift.joni.Option;
import io.airlift.joni.Regex;
import io.airlift.joni.Region;
import io.airlift.joni.Syntax;
import io.airlift.joni.exception.ValueException;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Const;
import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;
import static io.airlift.joni.Option.DEFAULT;
import static jdk.nashorn.internal.runtime.regexp.joni.Option.DONT_CAPTURE_GROUP;

@ExpressionSignatures(names = {"REGEXP_REPLACE"}, argumentTypes = {"Varchar", "Char", "Char"},
    argumentKinds = {Variable, Const, Const})
public class RegexpReplaceVectorizedExpression extends AbstractVectorizedExpression {
    private final boolean patternIsNull;
    private final Regex pattern;
    private final boolean replacementIsNull;
    private final Slice replacement;

    public RegexpReplaceVectorizedExpression(int outputIndex,
                                             VectorizedExpression[] children) {
        super(DataTypes.VarcharType, outputIndex, children);

        SliceType sliceType = (SliceType) children[0].getOutputDataType();
        Object operand1Value = ((LiteralVectorizedExpression) children[1]).getConvertedValue();
        Object operand2Value = ((LiteralVectorizedExpression) children[2]).getConvertedValue();
        if (operand1Value == null) {
            patternIsNull = true;
            pattern = null;
        } else {
            patternIsNull = false;
            pattern = joniRegexp(sliceType.convertFrom(operand1Value));
        }
        if (operand2Value == null) {
            replacementIsNull = true;
            replacement = null;
        } else {
            replacementIsNull = false;
            replacement = sliceType.convertFrom(operand2Value);
        }
    }

    @Override
    public void eval(EvaluationContext ctx) {
        children[0].eval(ctx);

        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);
        Object[] output = ((ReferenceBlock) outputVectorSlot).objectArray();
        RandomAccessBlock leftInputVectorSlot =
            chunk.slotIn(children[0].getOutputIndex(), children[0].getOutputDataType());

        if (replacementIsNull || patternIsNull) {
            outputVectorSlot.setHasNull(true);
            boolean[] outputNulls = outputVectorSlot.nulls();
            Arrays.fill(outputNulls, true);
            return;
        } else {
            boolean isInputHashNull = leftInputVectorSlot.hasNull();
            outputVectorSlot.setHasNull(isInputHashNull);
        }

        VectorizedExpressionUtils.mergeNulls(chunk, outputIndex, children[0].getOutputIndex());
        doBatchRegexpReplace(leftInputVectorSlot, isSelectionInUse, sel, batchSize, output, ctx.getExecutionContext());
    }

    private void doBatchRegexpReplace(RandomAccessBlock leftInputVectorSlot, boolean isSelectionInUse, int[] sel,
                                      int batchSize, Object[] output, ExecutionContext ec) {
        Slice cachedSlice = new Slice();
        SliceBlock sliceBlock = leftInputVectorSlot.cast(SliceBlock.class);
        if (isSelectionInUse) {
            for (int i = 0; i < batchSize; i++) {
                int j = sel[i];
                Slice slice = sliceBlock.getRegion(j, cachedSlice);
                output[j] = handleRegexpReplace(slice);
            }
        } else {
            for (int i = 0; i < batchSize; i++) {
                Slice slice = sliceBlock.getRegion(i, cachedSlice);
                output[i] = handleRegexpReplace(slice);
            }
        }
    }

    private Slice handleRegexpReplace(Slice source) {
        if (source == null) {
            return null;
        }
        Matcher matcher = pattern.matcher(source.getBytes());
        SliceOutput sliceOutput = new DynamicSliceOutput(source.length() + replacement.length() * 5);

        int lastEnd = 0;
        // nextStart is the same as lastEnd, unless the last match was zero-width. In such case, nextStart is lastEnd + 1.
        int nextStart =
            0;
        while (true) {
            int offset = getMatchingOffset(matcher, nextStart, source.length());
            if (offset == -1) {
                break;
            }
            if (matcher.getEnd() == matcher.getBegin()) {
                nextStart = matcher.getEnd() + 1;
            } else {
                nextStart = matcher.getEnd();
            }
            Slice sliceBetweenReplacements = source.slice(lastEnd, matcher.getBegin() - lastEnd);
            lastEnd = matcher.getEnd();
            sliceOutput.appendBytes(sliceBetweenReplacements);
            appendReplacement(sliceOutput, source, pattern, matcher.getEagerRegion(), replacement);
        }
        sliceOutput.appendBytes(source.slice(lastEnd, source.length() - lastEnd));

        return sliceOutput.slice();
    }

    private static void appendReplacement(SliceOutput result, Slice source, Regex pattern, Region region,
                                          Slice replacement) {
        // Handle the following items:
        // 1. ${name};
        // 2. $0, $1, $123 (group 123, if exists; or group 12, if exists; or group 1);
        // 3. \\, \$, \t (literal 't').
        // 4. Anything that doesn't starts with \ or $ is considered regular bytes

        int idx = 0;

        while (idx < replacement.length()) {
            byte nextByte = replacement.getByte(idx);
            if (nextByte == '$') {
                idx++;
                if (idx == replacement.length()) { // not using checkArgument because `.toStringUtf8` is expensive
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "Illegal replacement sequence: " + replacement.toStringUtf8());
                }
                nextByte = replacement.getByte(idx);
                int backref;
                if (nextByte == '{') { // case 1 in the above comment
                    idx++;
                    int startCursor = idx;
                    while (idx < replacement.length()) {
                        nextByte = replacement.getByte(idx);
                        if (nextByte == '}') {
                            break;
                        }
                        idx++;
                    }
                    byte[] groupName = replacement.getBytes(startCursor, idx - startCursor);
                    try {
                        backref = pattern.nameToBackrefNumber(groupName, 0, groupName.length, region);
                    } catch (ValueException e) {
                        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                            "Illegal replacement sequence: unknown group { " + new String(groupName,
                                StandardCharsets.UTF_8) + " }");
                    }
                    idx++;
                } else { // case 2 in the above comment
                    backref = nextByte - '0';
                    if (backref < 0 || backref > 9) { // not using checkArgument because `.toStringUtf8` is expensive
                        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                            "Illegal replacement sequence: " + replacement.toStringUtf8());
                    }
                    if (region.numRegs <= backref) {
                        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                            "Illegal replacement sequence: unknown group " + backref);
                    }
                    idx++;
                    while (idx
                        < replacement.length()) { // Adaptive group number: find largest group num that is not greater than actual number of groups
                        int nextDigit = replacement.getByte(idx) - '0';
                        if (nextDigit < 0 || nextDigit > 9) {
                            break;
                        }
                        int newBackref = (backref * 10) + nextDigit;
                        if (region.numRegs <= newBackref) {
                            break;
                        }
                        backref = newBackref;
                        idx++;
                    }
                }
                int beg = region.beg[backref];
                int end = region.end[backref];
                if (beg != -1 && end != -1) { // the specific group doesn't exist in the current match, skip
                    result.appendBytes(source.slice(beg, end - beg));
                }
            } else { // case 3 and 4 in the above comment
                if (nextByte == '\\') {
                    idx++;
                    if (idx == replacement.length()) { // not using checkArgument because `.toStringUtf8` is expensive
                        throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                            "Illegal replacement sequence: " + replacement.toStringUtf8());
                    }
                    nextByte = replacement.getByte(idx);
                }
                result.appendByte(nextByte);
                idx++;
            }
        }
    }

    private static int getMatchingOffset(Matcher matcher, int at, int range) {
        return getMatchingOffset(matcher, at, range, true);
    }

    private static int getMatchingOffset(Matcher matcher, int at, int range, boolean noGroups) {
        try {
            return matcher.searchInterruptible(at, range, noGroups ? DONT_CAPTURE_GROUP : DEFAULT);
        } catch (InterruptedException interruptedException) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "Regexp matching interrupted");
        }
    }

    public static Regex joniRegexp(Slice pattern) {
        Regex regex;
        try {
            // When normal UTF8 encoding instead of non-strict UTF8) is used, joni can infinite loop when invalid UTF8 slice is supplied to it.
            regex = new Regex(pattern.getBytes(), 0, pattern.length(), Option.DEFAULT, NonStrictUTF8Encoding.INSTANCE,
                Syntax.Java);
        } catch (Exception e) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, e);
        }
        return regex;
    }
}
