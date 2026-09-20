package com.alibaba.polardbx.executor.operator.scan.impl;

import org.junit.Assert;
import org.junit.Test;
import org.roaringbitmap.RoaringBitmap;

public class ReadablePositionTest {
    @Test
    public void testFirstReadablePosition() {
        int position = AbstractScanWork.firstReadablePosition(
            new int[] {5000, 500},
            RoaringBitmap.bitmapOf(5000, 5001, 5002, 5003, 5100, 5200)
        );

        Assert.assertEquals(4, position);

        position = AbstractScanWork.firstReadablePosition(
            new int[] {6000, 5},
            RoaringBitmap.bitmapOf(6000, 6001, 6002, 6003, 6004, 6005)
        );

        Assert.assertEquals(-1, position);
    }

    @Test
    public void testLastReadablePosition() {
        int position = AbstractScanWork.lastReadablePosition(
            new int[] {5000, 500},
            RoaringBitmap.bitmapOf(5495, 5496, 5497, 5498, 5499, 5500)
        );

        Assert.assertEquals(494, position);

        position = AbstractScanWork.lastReadablePosition(
            new int[] {6000, 5},
            RoaringBitmap.bitmapOf(6000, 6001, 6002, 6003, 6004, 6005)
        );

        Assert.assertEquals(-1, position);
    }
}
