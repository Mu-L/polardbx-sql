package com.alibaba.polardbx.optimizer.config.mqtest;

import com.google.common.collect.ImmutableMap;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.Test;

import java.util.Map;

import static com.alibaba.polardbx.optimizer.config.meta.DrdsRelMdColumnGroupSize.greedyMinSetCover;

public class GreedyMinSetCoverTest {

    @Test
    public void testTopNnewBuild() {
        Map<ImmutableBitSet, Integer> subsetMap;
        subsetMap = ImmutableMap.of(
            ImmutableBitSet.of(1, 2, 3), 20,
            ImmutableBitSet.of(4, 5, 6), 10,
            ImmutableBitSet.of(1, 4), 3,
            ImmutableBitSet.of(2, 5), 4,
            ImmutableBitSet.of(3, 6), 6
        );
        assert greedyMinSetCover(subsetMap).getValue() == 72;

        subsetMap = ImmutableMap.of(
            ImmutableBitSet.of(1, 2, 3), 20,
            ImmutableBitSet.of(4, 5, 6), 10,
            ImmutableBitSet.of(1, 4), 20,
            ImmutableBitSet.of(2, 5), 20,
            ImmutableBitSet.of(3, 6), 20
        );
        assert greedyMinSetCover(subsetMap).getValue() == 200;

        subsetMap = ImmutableMap.of(
            ImmutableBitSet.of(1, 2, 3), Integer.MAX_VALUE,
            ImmutableBitSet.of(4, 5, 6), Integer.MAX_VALUE,
            ImmutableBitSet.of(1, 4), Integer.MAX_VALUE,
            ImmutableBitSet.of(2, 5), Integer.MAX_VALUE,
            ImmutableBitSet.of(3, 6), Integer.MAX_VALUE
        );
        assert greedyMinSetCover(subsetMap).getValue() == Integer.MAX_VALUE;

        subsetMap = ImmutableMap.of(
            ImmutableBitSet.of(1, 2, 3), 20,
            ImmutableBitSet.of(4, 5), 1000,
            ImmutableBitSet.of(1, 4), 1,
            ImmutableBitSet.of(2, 5), 20,
            ImmutableBitSet.of(3), 20
        );
        assert greedyMinSetCover(subsetMap).getValue() == 400;
    }

}
