package com.alibaba.polardbx.common.collection;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Test;

public class MemoryCountableObjectArrayListTest {
    @Test
    public void test() {
        MemoryCountableObjectArrayList list = new MemoryCountableObjectArrayList();

        for (int i = 0; i < 128; i++) {
            list.add(Decimal.fromLong(i));
        }
        MemoryCountable.checkDeviation(list, 0d, true);

        for (int i = 0; i < 128; i++) {
            list.set(i, Decimal.fromLong(i * 1000));
        }
        MemoryCountable.checkDeviation(list, 0d, true);

        for (int i = 0; i < 10; i++) {
            if (i % 2 == 0) {
                list.remove(i);
            }
        }
        MemoryCountable.checkDeviation(list, 0d, true);

        list.addAll(ImmutableList.of(
            Decimal.fromLong(111), Decimal.fromLong(222), Decimal.fromLong(333), Decimal.fromLong(444)
        ));

        MemoryCountable.checkDeviation(list, 0d, true);

        list.addAll(3,
            ImmutableList.of(
                Decimal.fromLong(111), Decimal.fromLong(222), Decimal.fromLong(333), Decimal.fromLong(444)
            ));

        MemoryCountable.checkDeviation(list, 0d, true);

        list.clear();
        MemoryCountable.checkDeviation(list, 0d, true);
    }

    @Test
    public void testIndexOf() {
        MemoryCountableObjectArrayList<Slice> sliceList = new MemoryCountableObjectArrayList<>();

        sliceList.add(Slices.utf8Slice("R"));
        sliceList.add(Slices.utf8Slice("F"));
        sliceList.add(Slices.utf8Slice("O"));
        sliceList.add(Slices.utf8Slice("R"));
        sliceList.add(Slices.utf8Slice("F"));
        sliceList.add(Slices.utf8Slice("O"));

        Assert.assertEquals(0, sliceList.indexOf(Slices.utf8Slice("R")));
        Assert.assertEquals(1, sliceList.indexOf(Slices.utf8Slice("F")));
        Assert.assertEquals(2, sliceList.indexOf(Slices.utf8Slice("O")));

        Assert.assertEquals(3, sliceList.lastIndexOf(Slices.utf8Slice("R")));
        Assert.assertEquals(4, sliceList.lastIndexOf(Slices.utf8Slice("F")));
        Assert.assertEquals(5, sliceList.lastIndexOf(Slices.utf8Slice("O")));
    }
}