package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

@SuppressWarnings("unused")
@ExpressionSignatures(names = {"IS NULL"}, argumentTypes = {"Datetime"}, argumentKinds = {Variable})
public class IsNullDatetimeColVectorizedExpression extends AbstractIsNullColExpression {
    public IsNullDatetimeColVectorizedExpression(int outputIndex,
                                                 VectorizedExpression[] children) {
        super(outputIndex, children);
    }
}
