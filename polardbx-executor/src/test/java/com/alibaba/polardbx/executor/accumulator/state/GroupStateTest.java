package com.alibaba.polardbx.executor.accumulator.state;

import com.alibaba.polardbx.common.OrderInvariantHasher;
import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.datatype.DecimalBox;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import org.junit.Test;

import java.util.Random;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class GroupStateTest {

    private final Random random = new Random();

    @Test
    public void testDecimalBoxGroupState() {
        DecimalBoxGroupState state = new DecimalBoxGroupState(1024, 8);

        // add decimal box.
        int groupId = 0;
        for (; groupId < 100; groupId++) {
            state.appendNull();
            state.set(groupId, new DecimalBox(8));
        }
        MemoryCountable.checkDeviation(state, 0.01d, true);

        // add decimal 64
        for (; groupId < 200; groupId++) {
            state.appendNull();
            state.set(groupId, groupId * 99999);
        }
        MemoryCountable.checkDeviation(state, 0.01d, true);

        // add decimal 128
        for (; groupId < 300; groupId++) {
            state.appendNull();
            state.set(groupId, groupId * 99, groupId * 999);
        }
        MemoryCountable.checkDeviation(state, 0.01d, true);

        // add normal decimal
        for (; groupId < 400; groupId++) {
            state.appendNull();
            state.set(groupId, Decimal.fromString("12438941285712957492142134.43295342"));
        }
        MemoryCountable.checkDeviation(state, 0.01d, true);
    }

    @Test
    public void testLongGroupState() {
        final int COUNT = 1000;
        long[] values = new long[COUNT];
        for (int i = 0; i < values.length; i++) {
            values[i] = random.nextLong();
        }

        LongGroupState state = new LongGroupState(COUNT / 10);
        for (int i = 0; i < values.length; i++) {
            state.append(values[i]);
        }
        MemoryCountable.checkDeviation(state, 0.01d, true);

        for (int i = 0; i < values.length / 2; i += 2) {
            long l = random.nextLong();
            state.set(i, l);
            values[i] = l;
        }
        MemoryCountable.checkDeviation(state, 0.01d, true);

        for (int i = 0; i < values.length; i++) {
            assertEquals(values[i], state.get(i));
        }
        MemoryCountable.checkDeviation(state, 0.01d, true);
    }

    @Test
    public void testNullableCheckSumGroupState() {
        NullableCheckSumGroupState state = new NullableCheckSumGroupState(1024, OrderInvariantHasher.class);

        int groupId = 0;
        for (; groupId < 1024; groupId++) {
            state.appendNull();
            state.set(groupId, new OrderInvariantHasher());
        }
        MemoryCountable.checkDeviation(state, 0.01d, true);
    }

    @Test
    public void testNullableDecimalGroupState() {
        final Decimal[] values = new Decimal[] {
            null,
            Decimal.fromString("3.14"),
            Decimal.ZERO,
            Decimal.fromString("99999999999999999999999999999999999999999999999999999999999999999"),
            null
        };

        NullableDecimalGroupState state = new NullableDecimalGroupState(100);
        for (int i = 0; i < values.length; i++) {
            final Decimal value = values[i];
            state.appendNull();
            if (value != null) {
                state.set(i, value);
            }
        }
        MemoryCountable.checkDeviation(state, 0.01d, true);

        values[2] = Decimal.fromLong(1);
        state.set(2, values[2]);
        values[3] = Decimal.fromString("7712.025316455696202531645569620253");
        state.set(3, values[3]);

        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                assertFalse(state.isNull(i));
                assertEquals(values[i], state.get(i));
            } else {
                assertTrue(state.isNull(i));
            }
        }
        MemoryCountable.checkDeviation(state, 0.01d, true);
    }

    @Test
    public void testNullableDecimalLongGroupState() {
        NullableDecimalLongGroupState state = new NullableDecimalLongGroupState(
            1024
        );

        int groupId = 0;
        for (; groupId < 1024; groupId++) {
            state.appendNull();
            state.set(groupId, Decimal.fromString("1234" + groupId + ".5435938"), groupId);
        }
        MemoryCountable.checkDeviation(state, 0.01d, true);
    }

    @Test
    public void testNullableDoubleGroupState() {
        final int COUNT = 1000;
        Double[] values = new Double[COUNT];
        for (int i = 0; i < values.length; i++) {
            if (i % 2 == 0) {
                values[i] = null;
            } else {
                values[i] = random.nextDouble();
            }
        }

        NullableDoubleGroupState state = new NullableDoubleGroupState(100);
        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                state.append(values[i]);
            } else {
                state.appendNull();
            }
        }
        MemoryCountable.checkDeviation(state, 0.01d, true);

        for (int i = 0; i < values.length / 2; i += 2) {
            double d = random.nextDouble();
            state.set(i, d);
            values[i] = d;
        }
        MemoryCountable.checkDeviation(state, 0.01d, true);

        for (int i = 0; i < values.length; i++) {
            if (values[i] != null) {
                assertFalse(state.isNull(i));
                assertEquals(values[i], state.get(i), 1e-10);
            } else {
                assertTrue(state.isNull(i));
            }
        }
        MemoryCountable.checkDeviation(state, 0.01d, true);
    }

    @Test
    public void testNullableDoubleLongGroupState() {
        NullableDoubleLongGroupState state = new NullableDoubleLongGroupState(
            1024
        );

        int groupId = 0;
        for (; groupId < 1024; groupId++) {
            state.appendNull();
            state.set(groupId, Double.valueOf(groupId + ".534"), groupId);
        }
        MemoryCountable.checkDeviation(state, 0.01d, true);
    }

    @Test
    public void testNullableHyperLogLogGroupState() {
        NullableHyperLogLogGroupState state = new NullableHyperLogLogGroupState(
          1024
        );
        int groupId = 0;
        for (; groupId < 1024; groupId++) {
            state.append();
            state.set(groupId, (groupId + "_").getBytes());
        }
        MemoryCountable.checkDeviation(state, 0.01d, true);
    }

    @Test
    public void testNullableLongGroupState() {
        NullableLongGroupState state = new NullableLongGroupState(1024);
        int groupId = 0;
        for (; groupId < 1024; groupId++) {
            state.appendNull();
            state.append(groupId * 1000);
        }
        MemoryCountable.checkDeviation(state, 0.01d, true);
    }
}
