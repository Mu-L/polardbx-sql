package com.alibaba.polardbx.executor.accumulator;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.memory.FieldMemoryCounter;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.chunk.Block;
import com.alibaba.polardbx.executor.chunk.BlockBuilder;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.datatype.DateType;
import org.apache.calcite.sql.SqlKind;
import org.openjdk.jol.info.ClassLayout;

public class DateMaxMinNoGroupByAccumulator extends AbstractAccumulator {
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DateMaxMinNoGroupByAccumulator.class).instanceSize();
    private static final DataType[] INPUT_TYPES = new DataType[] {DataTypes.DateType};

    private long beforeValue;
    private boolean isVisited = false;

    @FieldMemoryCounter(value = false)
    private final SqlKind sqlKind;

    DateMaxMinNoGroupByAccumulator(SqlKind sqlKind) {
        this.sqlKind = sqlKind;
    }

    @Override
    public void appendInitValue() {
        beforeValue = 0;
    }

    // chunk indicate the converted agg chunk, inputChunk is the original chunk
    @Override
    public void accumulate(Chunk aggChunk, Chunk inputChunk) {
        Block block = aggChunk.getBlock(0);
        int position = block.getPositionCount();
        if (!isVisited) {
            beforeValue = block.getPackedLong(0);
            isVisited = true;
        }
        switch (sqlKind) {
        case MIN:
            for (int i = 0; i < position; i++) {
                long newValue = block.getPackedLong(i);
                if (newValue < beforeValue) {
                    beforeValue = newValue;
                }
            }
            break;
        case MAX:
            for (int i = 0; i < position; i++) {
                long newValue = block.getPackedLong(i);
                if (newValue > beforeValue) {
                    beforeValue = newValue;
                }
            }
            break;
        default:
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, "can not reach here");
        }
    }

    @Override
    public DataType[] getInputTypes() {
        return INPUT_TYPES;
    }

    @Override
    public void writeResultTo(int groupId, BlockBuilder bb) {
        bb.writeDatetimeRawLong(beforeValue);
    }

    @Override
    public long estimateSize() {
        return Long.BYTES;
    }

    @Override
    public long getMemoryUsage() {
        return INSTANCE_SIZE;
    }
}
