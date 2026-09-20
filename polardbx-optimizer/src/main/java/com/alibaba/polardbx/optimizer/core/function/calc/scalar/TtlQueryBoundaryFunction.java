package com.alibaba.polardbx.optimizer.core.function.calc.scalar;

import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataType;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypeUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.function.calc.AbstractScalarFunction;
import com.alibaba.polardbx.optimizer.ttl.query.TtlQueryUtil;

import java.util.List;

/**
 * 在执行器之前就需要被替换成常量，避免mpp执行器的影响
 */
public class TtlQueryBoundaryFunction extends AbstractScalarFunction {
    public TtlQueryBoundaryFunction() {
    }

    public TtlQueryBoundaryFunction(List<DataType> operandTypes, DataType resultType) {
        super(operandTypes, resultType);
    }

    @Override
    public Object compute(Object[] args, ExecutionContext ec) {
        if (args.length != 2) {
            throw new IllegalArgumentException("TTL_QUERY_BOUNDARY() requires 2 arguments");
        }
        String schemaName = DataTypeUtil.convert(getOperandType(0), DataTypes.StringType, args[0]).toLowerCase();
        String tableName = DataTypeUtil.convert(getOperandType(1), DataTypes.StringType, args[1]).toLowerCase();

        return TtlQueryUtil.getTtlQueryBoundary(ec, schemaName, tableName);
    }

    @Override
    public String[] getFunctionNames() {
        return new String[] {"TTL_QUERY_BOUNDARY"};
    }

}
