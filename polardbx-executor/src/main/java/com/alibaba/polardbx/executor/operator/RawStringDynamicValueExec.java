package com.alibaba.polardbx.executor.operator;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.PruneRawString;
import com.alibaba.polardbx.common.jdbc.RawString;
import com.alibaba.polardbx.executor.chunk.Chunk;
import com.alibaba.polardbx.executor.operator.util.DataTypeUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.expression.calc.DynamicParamExpression;
import com.alibaba.polardbx.optimizer.core.expression.calc.IExpression;
import com.alibaba.polardbx.optimizer.core.row.ArrayRow;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.ArrayList;
import java.util.List;

/**
 * Streams rows from RawString array parameters for a single-tuple DynamicValues. Two shapes:
 * 1. zip: one RawString per column, rows are zipped by position (single-column IN / folded VALUES);
 * 2. row list (rowListMode): one RawString whose elements are row lists expanded across columns
 * (multi-column IN rewrite).
 * Each column is emitted through a template expression (e.g. CAST) built by the factory.
 */
public class RawStringDynamicValueExec extends AbstractExecutor {

    private final List<IExpression> templateExpressions;
    private final List<RawString> rawStrings;
    private final List<DataType> outputColumnMeta;
    // true when a single RawString holds row lists instead of one array per column
    private boolean rowListMode;
    private List<List<?>> columnValues;
    private List<?> rowValues;
    private ArrayRow currentRow;
    private int rowIndex;
    private int rowCount;

    public RawStringDynamicValueExec(List<IExpression> templateExpressions,
                                     List<RawString> rawStrings,
                                     List<DataType> outputColumnMeta,
                                     ExecutionContext context) {
        super(context);
        this.templateExpressions = templateExpressions;
        this.rawStrings = rawStrings;
        this.outputColumnMeta = outputColumnMeta;
    }

    @Override
    void doOpen() {
        validateShape();
        createBlockBuilders();
        currentRow = new ArrayRow(new Object[outputColumnMeta.size()]);
        rowIndex = 0;
    }

    private void validateShape() {
        if (rawStrings.isEmpty() || templateExpressions.size() != outputColumnMeta.size()) {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Invalid RawString template shape: rawStrings=" + rawStrings.size()
                    + ", templateExpressions=" + templateExpressions.size()
                    + ", outputColumns=" + outputColumnMeta.size());
        }
        for (RawString rawString : rawStrings) {
            if (rawString instanceof PruneRawString) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "PruneRawString is not supported for RawString DynamicValues");
            }
        }

        if (rawStrings.size() == outputColumnMeta.size()) {
            columnValues = new ArrayList<>(rawStrings.size());
            rowCount = -1;
            for (RawString rawString : rawStrings) {
                List<?> values = rawString.getObjList();
                if (rowCount < 0) {
                    rowCount = values.size();
                } else if (values.size() != rowCount) {
                    throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                        "RawString column size mismatch: expected " + rowCount + ", actual " + values.size());
                }
                columnValues.add(values);
            }
        } else if (rawStrings.size() == 1) {
            rowListMode = true;
            rowValues = rawStrings.get(0).getObjList();
            rowCount = rowValues.size();
        } else {
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                "Invalid RawString column count: expected " + outputColumnMeta.size()
                    + " (one per column) or 1 (row list), actual " + rawStrings.size());
        }
    }

    @Override
    Chunk doNextChunk() {
        if (rowIndex >= rowCount) {
            return null;
        }

        while (rowIndex < rowCount) {
            writeRow(rowIndex++);
            if (currentPosition() >= chunkLimit) {
                return buildChunkAndReset();
            }
        }
        return buildChunkAndReset();
    }

    private void writeRow(int currentRowIndex) {
        if (rowListMode) {
            Object rowObject = rowValues.get(currentRowIndex);
            if (!(rowObject instanceof List) || ((List<?>) rowObject).size() != outputColumnMeta.size()) {
                throw new TddlRuntimeException(ErrorCode.ERR_EXECUTOR,
                    "RawString row shape mismatch: expected " + outputColumnMeta.size() + " columns");
            }
            List<?> row = (List<?>) rowObject;
            for (int columnIndex = 0; columnIndex < outputColumnMeta.size(); columnIndex++) {
                currentRow.setObject(columnIndex, DynamicParamExpression.convertParameterType(row.get(columnIndex)));
            }
        } else {
            for (int columnIndex = 0; columnIndex < columnValues.size(); columnIndex++) {
                Object value = columnValues.get(columnIndex).get(currentRowIndex);
                currentRow.setObject(columnIndex, DynamicParamExpression.convertParameterType(value));
            }
        }
        for (int columnIndex = 0; columnIndex < outputColumnMeta.size(); columnIndex++) {
            Object value = templateExpressions.get(columnIndex).eval(currentRow, context);
            blockBuilders[columnIndex].writeObject(DataTypeUtils.convert(outputColumnMeta.get(columnIndex), value));
        }
    }

    @Override
    void doClose() {
        currentRow = null;
        columnValues = null;
        rowValues = null;
    }

    @Override
    public List<DataType> getDataTypes() {
        return outputColumnMeta;
    }

    @Override
    public List<Executor> getInputs() {
        return ImmutableList.of();
    }

    @Override
    public boolean produceIsFinished() {
        return opened && rowIndex >= rowCount;
    }

    @Override
    public ListenableFuture<?> produceIsBlocked() {
        return ProducerExecutor.NOT_BLOCKED;
    }
}
