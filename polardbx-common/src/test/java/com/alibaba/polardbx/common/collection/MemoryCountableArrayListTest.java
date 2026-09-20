package com.alibaba.polardbx.common.collection;

import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Assert;
import org.junit.Test;

public class MemoryCountableArrayListTest {
    @Test
    public void testIndexOf() {
        MemoryCountableArrayList<Slice> sliceList = new MemoryCountableArrayList<>();

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