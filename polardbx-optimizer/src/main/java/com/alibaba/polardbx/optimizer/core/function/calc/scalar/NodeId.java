package com.alibaba.polardbx.optimizer.core.function.calc.scalar;

import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;

import java.util.List;

/**
 * @author junqi
 */
public class NodeId extends AbstractScalarFunction {
    public NodeId(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        return TddlNode.getNodeId();
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"NODE_ID"};
    }
}
