package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.datatype.Decimal;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.DecimalBlockBuilder;
import com.alibaba.polardbx.executor.chunk.IntegerBlock;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.AvgV2;
import org.junit.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class PreHashAggExecTest extends BaseExecTest {
    private static int DEFAULT_AGG_HASH_TABLE_SIZE = 1024;

    AbstractHashAggExec createHashAggExec(MockExec inputExec, int[] groups, List<Aggregator> aggregators,
                                          List<DataType> outputColumn) {
        Map connectionMap = new HashMap();
        connectionMap.put(ConnectionParams.ENABLE_TRANSPARENT_PARTIAL_AGG.getName(), true);
        connectionMap.put(ConnectionParams.PRE_AGG_STREAM_BATCH_THRESHOLD.getName(), 1);
        connectionMap.put(ConnectionParams.TRANSPARENT_PRE_AGG_JUDGE_RATE.getName(), 0.1);
        context.setParamManager(new ParamManager(connectionMap));
        return new PreHashAggExec(inputExec.getDataTypes(), groups, aggregators, outputColumn,
            DEFAULT_AGG_HASH_TABLE_SIZE,
            context);
    }

    @Test
    public void testHashAggSimpleAvg() {
        MockExec inputExec = MockExec.builder(DataTypes.IntegerType, DataTypes.IntegerType)
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(3, 4, 9, 7)))
            .withChunk(new Chunk(
                IntegerBlock.of(0, 1, 2, 3),
                IntegerBlock.of(5, 3, 8, 1)))
            .build();
        /** groups */
        int[] groups = {0};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new AvgV2(1, false, context.getMemoryPool().getMemoryAllocatorCtx(), -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.IntegerType);
        outputColumn.add(DataTypes.DecimalType);

        AbstractHashAggExec exec = createHashAggExec(inputExec, groups, aggregators, outputColumn);

        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        DecimalBlockBuilder decimalBlockBuilder = new DecimalBlockBuilder(256);
        BigDecimal[] bigDecimals = {
            new BigDecimal("3"), new BigDecimal("4"), new BigDecimal("9"), new BigDecimal("7"),
            new BigDecimal("5"), new BigDecimal("3"), new BigDecimal("8"), new BigDecimal("1")};
        Arrays.stream(bigDecimals).map(Decimal::fromBigDecimal).forEach(decimalBlockBuilder::writeDecimal);

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            IntegerBlock.of(0, 1, 2, 3, 0, 1, 2, 3),
            decimalBlockBuilder.build()
        )), false);

    }

}
