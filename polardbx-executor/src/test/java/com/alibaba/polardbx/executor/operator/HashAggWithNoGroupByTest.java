package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.time.RandomTimeGenerator;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.chunk.DateBlock;
import com.alibaba.polardbx.executor.chunk.DateBlockBuilder;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.expression.calc.Aggregator;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.MaxV2;
import com.alibaba.polardbx.optimizer.core.expression.calc.aggfunctions.MinV2;
import org.junit.Before;
import org.junit.Test;

import java.sql.Date;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class HashAggWithNoGroupByTest extends BaseExecTest {
    private static int DEFAULT_AGG_HASH_TABLE_SIZE = 1024;
    private int aggHashTableSize = 2;
    private boolean compatible = false;

    @Before
    public void setParam() {
        Map connectionMap = new HashMap();
        connectionMap.put(ConnectionParams.CHUNK_SIZE.getName(), 1000);

        // open vectorization implementation of agg.
        connectionMap.put(ConnectionParams.ENABLE_VEC_ACCUMULATOR.getName(), true);
        connectionMap.put(ConnectionParams.ENABLE_OSS_COMPATIBLE.getName(), compatible);
        context.setParamManager(new ParamManager(connectionMap));
    }

    //min(date), max(date) with no group by
    @Test
    public void TestCountDistinctIntWithNoGroupBy() {
        int n = 1000;
        DateBlock chunk0Block0 = mockDateBlock(n);
        DateBlock chunk0Block1 = mockDateBlock(n);
        DateBlock chunk1Block0 = mockDateBlock(n);
        DateBlock chunk1Block1 = mockDateBlock(n);

        MockExec inputExec = MockExec.builder(DataTypes.DateType, DataTypes.DateType)
            .withChunk(new Chunk(
                chunk0Block0,
                chunk0Block1))
            .withChunk(new Chunk(
                chunk1Block0,
                chunk1Block1))
            .build();
        /** groups */
        int[] groups = {};
        /** aggregators */
        List<Aggregator> aggregators = new ArrayList<>();
        aggregators.add(new MinV2(0, -1));
        aggregators.add(new MaxV2(1, -1));
        /** outputColumnMeta */
        List<DataType> outputColumn = new ArrayList<>();
        outputColumn.add(DataTypes.DateType);
        outputColumn.add(DataTypes.DateType);

        HashAggExec exec =
            new HashAggExec(inputExec.getDataTypes(), groups, aggregators, outputColumn, DEFAULT_AGG_HASH_TABLE_SIZE,
                context);
        SingleExecTest test = new SingleExecTest.Builder(exec, inputExec.getChunks()).build();
        test.exec();

        assertExecResultByRow(test.result(), Collections.singletonList(new Chunk(
            wrapDate(findMinMaxDate(true, chunk0Block0, chunk1Block0)),
            wrapDate(findMinMaxDate(false, chunk0Block1, chunk1Block1))
        )), true);
    }

    private DateBlock mockDateBlock(int n) {
        List<Object> list =
            RandomTimeGenerator.generateValidDatetimeString(5);
        DateBlockBuilder dateBlockBuilder = new DateBlockBuilder(n, DataTypes.DateType, context);
        for (Object object : list) {
            dateBlockBuilder.writeString((String) object);
        }
        return (DateBlock) dateBlockBuilder.build();
    }

    private DateBlock wrapDate(Date... dates) {
        DateBlockBuilder dateBlockBuilder = new DateBlockBuilder(dates.length, DataTypes.DateType, context);
        for (Date date : dates) {
            dateBlockBuilder.writeDate(date);
        }
        return (DateBlock) dateBlockBuilder.build();
    }

    private Date findMinMaxDate(boolean isMin, DateBlock... dateBlocks) {
        Date result = dateBlocks[0].getDate(0);
        if (isMin) {
            for (DateBlock dateBlock : dateBlocks) {
                for (int pos = 0; pos < dateBlock.getPositionCount(); pos++) {
                    if (DataTypes.DateType.compare(dateBlock.getDate(pos), result) < 0) {
                        result = dateBlock.getDate(pos);
                    }
                }
            }
        } else {
            for (DateBlock dateBlock : dateBlocks) {
                for (int pos = 0; pos < dateBlock.getPositionCount(); pos++) {
                    if (DataTypes.DateType.compare(dateBlock.getDate(pos), result) > 0) {
                        result = dateBlock.getDate(pos);
                    }
                }
            }
        }
        return result;
    }
}
