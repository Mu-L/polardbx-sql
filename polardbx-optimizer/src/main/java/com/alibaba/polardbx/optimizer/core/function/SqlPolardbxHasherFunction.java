package com.alibaba.polardbx.optimizer.core.function;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

import static org.apache.calcite.sql.type.InferTypes.FIRST_KNOWN;

public class SqlPolardbxHasherFunction extends SqlFunction {
    public SqlPolardbxHasherFunction(String funcName) {
        super(
            funcName,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.BIGINT,
            FIRST_KNOWN,
            OperandTypes.VARIADIC,
            SqlFunctionCategory.NUMERIC);
    }
}
