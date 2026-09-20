package com.alibaba.polardbx.executor.chunk;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.optimizer.core.datatype.DecimalType;
import org.junit.Assert;
import org.junit.Test;

public class DecimalBlockCopySelectedTest {
    @Test
    public void testDecimal64() {

        DecimalType type1 = new DecimalType(10, 2);
        DecimalType type2 = new DecimalType(10, 3);
        DecimalType type3 = new DecimalType(10, 4);

        DecimalBlockBuilder builder1 = new DecimalBlockBuilder(5, type1);
        builder1.writeLong(12345L);  // 123.45
        builder1.writeLong(67890L);  // 678.90
        DecimalBlock block1 = (DecimalBlock) builder1.build();

        RandomAccessBlock targetBlock = BlockUtils.createBlock(type3, 4);
        block1.copySelected(true, new int[] {0, 1}, 2, targetBlock);

        DecimalBlockBuilder builder2 = new DecimalBlockBuilder(5, type2);
        builder2.writeLong(12345L);  // 12.345
        builder2.writeLong(67890L);  // 67.890
        builder2.writeLong(12345L);  // 12.345
        builder2.writeLong(67890L);  // 67.890
        DecimalBlock block2 = (DecimalBlock) builder2.build();

        block2.copySelected(true, new int[] {2, 3}, 2, targetBlock);

        Assert.assertEquals(Decimal.fromString("123.45"), targetBlock.elementAt(0)); // 123.45
        Assert.assertEquals(Decimal.fromString("678.90"), targetBlock.elementAt(1)); // 678.90
        Assert.assertEquals(Decimal.fromString("12.345"), targetBlock.elementAt(2)); // 12.345
        Assert.assertEquals(Decimal.fromString("67.890"), targetBlock.elementAt(3)); // 67.890

    }
}
