package com.alibaba.polardbx.executor.operator.scan.impl;

import com.alibaba.polardbx.common.memory.MemoryCountable;
import com.alibaba.polardbx.executor.operator.scan.BlockDictionary;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.*;

public class DictionaryMappingImplTest {
    // build dictionary
    static final int SIZE = 8;
    static final Slice[] DICT = new Slice[SIZE];

    static {
        DICT[0] = Slices.utf8Slice("White");
        DICT[1] = Slices.utf8Slice("Yellow");
        DICT[2] = Slices.utf8Slice("Orange");
        DICT[3] = Slices.utf8Slice("Red");
        DICT[4] = Slices.utf8Slice("Purple");
        DICT[5] = Slices.utf8Slice("Blue");
        DICT[6] = Slices.utf8Slice("Gray");
        DICT[7] = Slices.utf8Slice("Green");
    }

    @Test
    public void test() {
        DictionaryMappingImpl dictionaryMapping = new DictionaryMappingImpl();

        BlockDictionary dictionary = new LocalBlockDictionary(DICT);
        dictionaryMapping.merge(dictionary);
        MemoryCountable.checkDeviation(dictionaryMapping, 0d, true);

        BlockDictionary dictionary1 = new LocalBlockDictionary(Arrays.copyOf(DICT, 2));
        dictionaryMapping.merge(dictionary1);
        MemoryCountable.checkDeviation(dictionaryMapping, 0d, true);

        BlockDictionary dictionary2 = new LocalBlockDictionary(Arrays.copyOf(DICT, 3));
        dictionaryMapping.merge(dictionary2);
        MemoryCountable.checkDeviation(dictionaryMapping, 0d, true);
    }
}