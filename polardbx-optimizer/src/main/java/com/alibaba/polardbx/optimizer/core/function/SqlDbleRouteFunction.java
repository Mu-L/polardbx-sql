package com.alibaba.polardbx.optimizer.core.function;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.optimizer.partition.pruning.PartitionPrunerUtils;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.sql.type.InferTypes.FIRST_KNOWN;
import static org.apache.calcite.sql.type.InferTypes.VARCHAR_2000;

public class SqlDbleRouteFunction extends SqlFunction {

    private List<RelDataType> dbleInputDatatypes = new ArrayList<>();
    private List<RelDataType> dbleOutputDatatypes = new ArrayList<>();
    public SqlDbleRouteFunction(String funcName) {
        super(
            funcName,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.BIGINT,
            InferTypes.VARCHAR_2000,
            OperandTypes.VARIADIC,
            SqlFunctionCategory.STRING);

        try {
            RelDataTypeFactory typeFactory = PartitionPrunerUtils.getTypeFactory();
            inferReturnType(typeFactory, dbleOutputDatatypes);
            RelDataType dbleRouteInputDataType = typeFactory.createSqlType(SqlTypeName.VARCHAR, 2000);
            dbleInputDatatypes.add(dbleRouteInputDataType);
        } catch (Throwable ex) {
            throw new TddlRuntimeException(ErrorCode.ERR_INVALID_DDL_PARAMS,
                String.format("Failed create dbleRoute sqlFunction, err is %s", ex.getMessage()), ex);
        }
    }

    @Override
    public List<RelDataType> getParamTypes() {
        return dbleInputDatatypes;
    }

    @Override
    public boolean canPushDown() {
        return false;
    }
}
