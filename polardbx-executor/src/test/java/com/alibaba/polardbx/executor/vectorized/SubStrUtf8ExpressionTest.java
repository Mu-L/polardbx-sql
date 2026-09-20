package com.alibaba.polardbx.executor.vectorized;

import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.ReferenceBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlock;
import com.alibaba.polardbx.executor.chunk.SliceBlockBuilder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.SliceType;
import io.airlift.slice.Slice;
import org.junit.Assert;
import org.junit.Test;

/**
 * Test SubStrVarcharVectorizedExpression with UTF-8 strings including:
 * - Pure ASCII/English
 * - Chinese characters
 * - Emoji characters
 * - Mixed content
 */
public class SubStrUtf8ExpressionTest extends BaseVectorizedExpressionTest {

    private final DataType outputDataType = DataTypes.StringType;

    @Test
    public void testSubStrPureEnglish() {
        String input = "Hello World";
        // SUBSTR(input, 1, 5) = "Hello"
        testSubStr(input, 1, 5, "Hello");
        // SUBSTR(input, 7, 5) = "World"
        testSubStr(input, 7, 5, "World");
        // SUBSTR(input, 1, 100) = "Hello World"
        testSubStr(input, 1, 100, "Hello World");
    }

    @Test
    public void testSubStrPureChinese() {
        String input = "你好世界测试";
        // SUBSTR(input, 1, 2) = "你好"
        testSubStr(input, 1, 2, "你好");
        // SUBSTR(input, 3, 2) = "世界"
        testSubStr(input, 3, 2, "世界");
        // SUBSTR(input, 5, 2) = "测试"
        testSubStr(input, 5, 2, "测试");
        // SUBSTR(input, 1, 100) = "你好世界测试"
        testSubStr(input, 1, 100, "你好世界测试");
    }

    @Test
    public void testSubStrPureEmoji() {
        String input = "😀😁😂🤣😃";
        // SUBSTR(input, 1, 2) = "😀😁"
        testSubStr(input, 1, 2, "😀😁");
        // SUBSTR(input, 3, 2) = "😂🤣"
        testSubStr(input, 3, 2, "😂🤣");
        // SUBSTR(input, 5, 1) = "😃"
        testSubStr(input, 5, 1, "😃");
        // SUBSTR(input, 1, 100) = "😀😁😂🤣😃"
        testSubStr(input, 1, 100, "😀😁😂🤣😃");
    }

    @Test
    public void testSubStrMixedContent() {
        String input = "Hello你好😀World世界😁";
        // SUBSTR(input, 1, 5) = "Hello"
        testSubStr(input, 1, 5, "Hello");
        // SUBSTR(input, 6, 2) = "你好"
        testSubStr(input, 6, 2, "你好");
        // SUBSTR(input, 8, 1) = "😀"
        testSubStr(input, 8, 1, "😀");
        // SUBSTR(input, 9, 5) = "World"
        testSubStr(input, 9, 5, "World");
        // SUBSTR(input, 14, 2) = "世界"
        testSubStr(input, 14, 2, "世界");
        // SUBSTR(input, 16, 1) = "😁"
        testSubStr(input, 16, 1, "😁");
    }

    @Test
    public void testSubStrNegativeStart() {
        String input = "你好世界😀";
        // SUBSTR(input, -2, 2) = "界😀"
        testSubStr(input, -2, 2, "界😀");
        // SUBSTR(input, -3, 2) = "世界"
        testSubStr(input, -3, 2, "世界");
        // SUBSTR(input, -5, 3) = "你好世"
        testSubStr(input, -5, 3, "你好世");
    }

    @Test
    public void testSubStrNegativeStartEnglish() {
        String input = "Hello";
        // SUBSTR(input, -2, 2) = "lo"
        testSubStr(input, -2, 2, "lo");
        // SUBSTR(input, -5, 3) = "Hel"
        testSubStr(input, -5, 3, "Hel");
    }

    @Test
    public void testSubStrZeroStart() {
        String input = "你好😀";
        // SUBSTR(input, 0, 5) = ""
        testSubStr(input, 0, 5, "");
    }

    @Test
    public void testSubStrNegativeLength() {
        String input = "你好😀";
        // SUBSTR(input, 1, -5) = ""
        testSubStr(input, 1, -5, "");
    }

    @Test
    public void testSubStrOutOfBounds() {
        String input = "你好😀";
        // SUBSTR(input, 10, 5) = ""
        testSubStr(input, 10, 5, "");
        // SUBSTR(input, -10, 2) = ""
        testSubStr(input, -10, 2, "");
    }

    @Test
    public void testSubStrEdgeCases() {
        // Empty string
        testSubStr("", 1, 5, "");

        // Single character
        testSubStr("A", 1, 1, "A");
        testSubStr("你", 1, 1, "你");
        testSubStr("😀", 1, 1, "😀");

        // Start at last character
        testSubStr("你好😀", 3, 1, "😀");
        testSubStr("你好😀", 3, 10, "😀");
    }

    @Test
    public void testSubStrBatchProcessing() {
        String[] inputs = {
            "Hello",
            "你好",
            "😀😁",
            "Hello你好😀",
            "World世界🎉"
        };

        String[] expected = {
            "ello",
            "好",
            "😁",
            "ello",
            "orld"
        };

        testSubStrBatch(inputs, 2, 4, expected);
    }

    private void testSubStr(String input, int start, int length, String expected) {
        SliceBlockBuilder inputBlockBuilder = new SliceBlockBuilder(
            new SliceType(), 1, new ExecutionContext(), false);
        inputBlockBuilder.writeString(input);

        SubStrVarcharVectorizedExpression expr = createExpression(start, length);
        SliceBlock inputBlock = (SliceBlock) inputBlockBuilder.build();
        ReferenceBlock outputBlock = new ReferenceBlock(DataTypes.StringType, 1);

        MutableChunk chunk = new MutableChunk(inputBlock, outputBlock);
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        expr.eval(evaluationContext);

        Slice result = (Slice) outputBlock.getObject(0);
        String actual = result == null ? null : result.toStringUtf8();

        Assert.assertEquals("SUBSTR('" + input + "', " + start + ", " + length + ")",
            expected, actual);
    }

    private void testSubStrBatch(String[] inputs, int start, int length, String[] expected) {
        Assert.assertEquals(inputs.length, expected.length);

        int count = inputs.length;
        SliceBlockBuilder inputBlockBuilder = new SliceBlockBuilder(
            new SliceType(), count, new ExecutionContext(), false);

        for (String input : inputs) {
            inputBlockBuilder.writeString(input);
        }

        SubStrVarcharVectorizedExpression expr = createExpression(start, length);
        SliceBlock inputBlock = (SliceBlock) inputBlockBuilder.build();
        ReferenceBlock outputBlock = new ReferenceBlock(DataTypes.StringType, count);

        MutableChunk chunk = new MutableChunk(inputBlock, outputBlock);
        EvaluationContext evaluationContext = new EvaluationContext(chunk, executionContext);
        expr.eval(evaluationContext);

        for (int i = 0; i < count; i++) {
            Slice result = (Slice) outputBlock.getObject(i);
            String actual = result == null ? null : result.toStringUtf8();
            Assert.assertEquals("SUBSTR('" + inputs[i] + "', " + start + ", " + length + ")",
                expected[i], actual);
        }
    }

    private SubStrVarcharVectorizedExpression createExpression(int start, int length) {
        VectorizedExpression[] children = new VectorizedExpression[3];
        children[1] = new LiteralVectorizedExpression(DataTypes.LongType, start, 0);
        children[2] = new LiteralVectorizedExpression(DataTypes.LongType, length, 0);
        children[0] = new VectorizedExpression() {
            @Override
            public void eval(EvaluationContext ctx) {
            }

            @Override
            public VectorizedExpression[] getChildren() {
                return new VectorizedExpression[0];
            }

            @Override
            public DataType<?> getOutputDataType() {
                return DataTypes.StringType;
            }

            @Override
            public int getOutputIndex() {
                return 0;
            }
        };
        return new SubStrVarcharVectorizedExpression(outputDataType, 1, children);
    }
}
