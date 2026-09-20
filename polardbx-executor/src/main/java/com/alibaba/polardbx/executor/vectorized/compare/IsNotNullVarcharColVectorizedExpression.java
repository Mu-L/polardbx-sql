package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

@SuppressWarnings("unused")
@ExpressionSignatures(names = {"IS NOT NULL"}, argumentTypes = {"Varchar"}, argumentKinds = {Variable})
public class IsNotNullVarcharColVectorizedExpression extends AbstractIsNotNullColExpression {

    public IsNotNullVarcharColVectorizedExpression(int outputIndex, VectorizedExpression[] children) {
        super(outputIndex, children);
    }
}