package com.alibaba.polardbx.executor.vectorized.compare;

import com.alibaba.polardbx.executor.vectorized.VectorizedExpression;
import com.alibaba.polardbx.executor.vectorized.metadata.ExpressionSignatures;

import static com.alibaba.polardbx.executor.vectorized.metadata.ArgumentKind.Variable;

@SuppressWarnings("unused")

@ExpressionSignatures(names = {"IS NOT NULL"}, argumentTypes = {"Char"}, argumentKinds = {Variable})
public class IsNotNullCharColVectorizedExpression extends AbstractIsNotNullColExpression {

    public IsNotNullCharColVectorizedExpression(int outputIndex, VectorizedExpression[] children) {
        super(outputIndex, children);
    }
}