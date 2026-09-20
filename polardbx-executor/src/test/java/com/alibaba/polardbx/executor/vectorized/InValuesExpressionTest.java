package com.alibaba.polardbx.executor.vectorized;

import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import io.airlift.slice.Slices;
import org.apache.commons.lang.RandomStringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

public class InValuesExpressionTest {

    @Test
    public void testDiffType() {
        InValuesVectorizedExpression.InValueSet longValueSet =
            new InValuesVectorizedExpression.InValueSet(DataTypes.LongType, 10);

        longValueSet.add(1L);
        longValueSet.add(new Long(2));
        try {
            longValueSet.add(3);
            Assert.fail("Expect failed with different type");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("cannot be cast"));
        }
        Assert.assertTrue(longValueSet.contains(1L));
        Assert.assertTrue(longValueSet.contains(new Integer(1)));
        Assert.assertTrue(longValueSet.contains("1"));
        Assert.assertTrue(longValueSet.contains(2));
        Assert.assertTrue(longValueSet.contains(new Long(2)));
        Assert.assertTrue(longValueSet.contains(2L));
        Assert.assertFalse(longValueSet.contains(new Integer(3)));
        Assert.assertFalse(longValueSet.contains(new Long(3)));

        InValuesVectorizedExpression.InValueSet intValueSet =
            new InValuesVectorizedExpression.InValueSet(DataTypes.IntegerType, 10);
        intValueSet.add(-1);
        intValueSet.add(new Integer(-2));
        try {
            intValueSet.add(3L);
            Assert.fail("Expect failed with different type");
        } catch (Exception e) {
            Assert.assertTrue(e.getMessage().contains("cannot be cast"));
        }
        Assert.assertTrue(intValueSet.contains(-1));
        Assert.assertTrue(intValueSet.contains(new Integer(-1)));
        Assert.assertTrue(intValueSet.contains("-1"));
        Assert.assertTrue(intValueSet.contains(-2));
        Assert.assertTrue(intValueSet.contains(new Long(-2)));
        Assert.assertTrue(intValueSet.contains(-2L));
        Assert.assertFalse(intValueSet.contains(new Integer(3)));
        Assert.assertFalse(intValueSet.contains(new Long(3)));
        Assert.assertFalse(intValueSet.contains(Integer.MAX_VALUE + 1L));

        // check memory
        MemoryCountable.checkDeviation(intValueSet, .05d, true);
        MemoryCountable.checkDeviation(longValueSet, .05d, true);
    }

    @Test
    public void testSliceTest() {
        final int count = 3000;
        final int len = 10;
        InValuesVectorizedExpression.InValueSet sliceValueSet =
            new InValuesVectorizedExpression.InValueSet(DataTypes.CharType, count);
        List<String> result = new ArrayList<>(100);
        for (int i = 0; i < count; i++) {
            String val = RandomStringUtils.randomAlphabetic(len);
            sliceValueSet.add(Slices.utf8Slice(val));
            if (i < 100) {
                result.add(val);
            }
        }
        for (String s : result) {
            Assert.assertTrue(sliceValueSet.contains(s));
        }
        for (int i = 0; i < 10; i++) {
            Assert.assertFalse(sliceValueSet.contains(RandomStringUtils.randomAlphabetic(len - 1)));
        }
        MemoryCountable.checkDeviation(sliceValueSet, .05d, true);
    }
}
