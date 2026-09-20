package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

@SuppressWarnings("unused")
@ExpressionSignatures(names = {"IS NULL"}, argumentTypes = {"Date"}, argumentKinds = {Variable})
public class IsNullDateColVectorizedExpression extends AbstractIsNullColExpression {
    public IsNullDateColVectorizedExpression(int outputIndex,
                                             VectorizedExpression[] children) {
        super(outputIndex, children);
    }
}
