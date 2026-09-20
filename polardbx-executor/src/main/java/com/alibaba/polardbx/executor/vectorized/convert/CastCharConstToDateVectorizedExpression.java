package com.alibaba.polardbx.executor.vectorized.convert;

import com.alibaba.polardbx.common.utils.time.MySQLTimeTypeUtil;
import com.alibaba.polardbx.common.utils.time.core.MysqlDateTime;
import com.alibaba.polardbx.common.utils.time.core.OriginalDate;
import com.alibaba.polardbx.common.utils.time.core.TimeStorage;
import com.alibaba.polardbx.executor.chunk.DateBlock;
import com.alibaba.polardbx.executor.chunk.MutableChunk;
import com.alibaba.polardbx.executor.chunk.RandomAccessBlock;
import com.alibaba.polardbx.executor.chunk.ReferenceBlock;
import com.alibaba.polardbx.executor.vectorized.AbstractVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.EvaluationContext;
import com.alibaba.polardbx.executor.vectorized.LiteralVectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;

import java.sql.Date;
import java.util.Optional;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Const;

@SuppressWarnings("unused")
//
//@ExpressionSignatures(names = {"CastToDate", "ConvertToDate"}, argumentTypes = {"Char"},
//    argumentKinds = {Const})
public class CastCharConstToDateVectorizedExpression extends AbstractVectorizedExpression {

    private boolean constIsNull;
    private long packedLong;
    private OriginalDate originalDate;

    public CastCharConstToDateVectorizedExpression(DataType<?> outputDataType, int outputIndex,
                                                   VectorizedExpression[] children) {
        super(outputDataType, outputIndex, children);
        this.constIsNull = true;
        Object constValue = ((LiteralVectorizedExpression) children[0]).getConvertedValue();
        if (constValue != null) {
            Date date = null;
            try {
                date = (Date) outputDataType.convertFrom(constValue);
                // pack to long value
                MysqlDateTime t = Optional.ofNullable(date)
                    .map(MySQLTimeTypeUtil::toMysqlDate)
                    .orElse(null);
                if (t != null) {
                    this.packedLong = TimeStorage.writeDate(t);
                    this.originalDate = new OriginalDate(t);
                    this.constIsNull = false;
                }
            } catch (Exception e) {
                // ignore
            }
        }
    }

    @Override
    public void eval(EvaluationContext ctx) {
        MutableChunk chunk = ctx.getPreAllocatedChunk();
        int batchSize = chunk.batchSize();
        boolean isSelectionInUse = chunk.isSelectionInUse();
        int[] sel = chunk.selection();

        RandomAccessBlock outputVectorSlot = chunk.slotIn(outputIndex, outputDataType);

        if (constIsNull) {
            boolean[] outputNulls = outputVectorSlot.nulls();
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    outputNulls[j] = true;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    outputNulls[i] = true;
                }
            }
            return;
        }

        if (outputVectorSlot instanceof DateBlock) {
            long[] output = outputVectorSlot.cast(DateBlock.class).getPacked();

            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    output[j] = packedLong;
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    output[i] = packedLong;
                }
            }
        } else if (outputVectorSlot instanceof ReferenceBlock) {
            if (isSelectionInUse) {
                for (int i = 0; i < batchSize; i++) {
                    int j = sel[i];
                    outputVectorSlot.setElementAt(j, originalDate);
                }
            } else {
                for (int i = 0; i < batchSize; i++) {
                    outputVectorSlot.setElementAt(i, originalDate);
                }
            }
        }

    }
}
