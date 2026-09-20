package com.alibaba.polardbx.executor.chunk;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import org.junit.Test;

public class ConvertersTest {

    @Test
    public void testLongToDecimalWithNull() {
        LongBlockBuilder builder = new LongBlockBuilder(3);
        builder.appendNull();
        builder.writeLong(10);
        builder.appendNull();
        LongBlock block = (LongBlock) builder.build();
        BlockConverter blockConverter = Converters.createBlockConverter(DataTypes.LongType, DataTypes.DecimalType, new ExecutionContext());
        DecimalBlock decimalBlock = (DecimalBlock) blockConverter.apply(block);
        Assert.assertTrue(decimalBlock.isNull(0)
                                    && decimalBlock.getLong(1) == 10
                                    && decimalBlock.isNull(2));
    }
    @Test
    public void testShortToDecimalWithNull() {
        ShortBlockBuilder shortBuilder = new ShortBlockBuilder(3);
        shortBuilder.appendNull();
        shortBuilder.writeShort((short) 10);
        shortBuilder.appendNull();
        ShortBlock shortBlock = (ShortBlock) shortBuilder.build();
        BlockConverter blockConverter = Converters.createBlockConverter(DataTypes.LongType, DataTypes.DecimalType, new ExecutionContext());
        DecimalBlock decimalBlock = (DecimalBlock) blockConverter.apply(shortBlock);
        Assert.assertTrue(decimalBlock.isNull(0)
                          && decimalBlock.getLong(1) == 10
                          && decimalBlock.isNull(2));
    }
}
