package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

@SuppressWarnings("unused")
@ExpressionSignatures(names = {"IS NULL"}, argumentTypes = {"Varchar"}, argumentKinds = {Variable})
public class IsNullVarcharColVectorizedExpression extends AbstractIsNullColExpression {
    public IsNullVarcharColVectorizedExpression(int outputIndex, VectorizedExpression[] children) {
        super(outputIndex, children);
    }
}