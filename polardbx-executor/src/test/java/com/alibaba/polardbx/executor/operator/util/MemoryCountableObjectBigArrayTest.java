package com.alibaba.polardbx.executor.operator.util;

import com.alibaba.polardbx.common.memory.FastMemoryCounter;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import org.junit.Assert;
import org.junit.Test;
import org.openjdk.jol.info.ClassLayout;
import org.openjdk.jol.info.GraphLayout;

public class MemoryCountableObjectBigArrayTest {

    @Test
    public void test() {
        MemoryCountableObjectBigArray bigArray = new MemoryCountableObjectBigArray();

        MemoryCountable.checkDeviation(bigArray, 0d, false);

        bigArray.set(0, new MemoryCountableString("a"));
        bigArray.set(3, new MemoryCountableString("abc"));
        bigArray.set(5, new MemoryCountableString("x"));
        bigArray.set(8, new MemoryCountableString("F"));

        MemoryCountable.checkDeviation(bigArray, 0d, false);

        bigArray.set(5, new MemoryCountableString("parseInstance"));
        MemoryCountable.checkDeviation(bigArray, 0d, false);

        bigArray.set(5, new MemoryCountableString("MemoryCountableObjectBigArray"));
        bigArray.set(8, new MemoryCountableString("checkDeviation"));
        MemoryCountable.checkDeviation(bigArray, 0d, false);

        bigArray.ensureCapacity(MemoryCountableObjectBigArray.SEGMENT_SIZE * 2);
        bigArray.set(1025, new MemoryCountableString("G"));
        bigArray.set(2047, new MemoryCountableString("Z"));

        MemoryCountable.checkDeviation(bigArray, 0d, false);

        bigArray.segment(16);
        bigArray.offset(1025);
        bigArray.sizeOf();
        Assert.assertEquals(GraphLayout.parseInstance(bigArray).totalSize(), bigArray.getMemoryUsage());

    }

    private static class MemoryCountableString implements MemoryCountable {
        private static final int INSTANCE_SIZE = ClassLayout.parseClass(MemoryCountableString.class).instanceSize();
        private final String value;

        public MemoryCountableString(String value) {
            this.value = value;
        }

        @Override
        public long getMemoryUsage() {
            return INSTANCE_SIZE + FastMemoryCounter.sizeOf(value);
        }
    }

}