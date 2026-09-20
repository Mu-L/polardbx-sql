/*
 * Copyright 2019 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.optimizer.core.function;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperandCountRange;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;

import java.util.List;

/**
 * VEC_DISTANCE_INNER_PRODUCT operator and DN SQL unparse support.
 */
public class MySQLVecDistanceInnerProduct extends SqlFunction {

    public static final MySQLVecDistanceInnerProduct INSTANCE = new MySQLVecDistanceInnerProduct();

    public MySQLVecDistanceInnerProduct() {
        super("VEC_DISTANCE_INNER_PRODUCT",
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.DOUBLE,
            InferTypes.FIRST_KNOWN,
            OperandTypes.ANY,
            SqlFunctionCategory.NUMERIC);
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        writer.keyword("VEC_DISTANCE_INNER_PRODUCT");
        SqlWriter.Frame frame = writer.startList("(", ")");
        List<SqlNode> operands = call.getOperandList();
        if (operands.size() >= 2) {
            operands.get(0).unparse(writer, 0, 0);
            writer.sep(",");
            operands.get(1).unparse(writer, 0, 0);
        }
        writer.endList(frame);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(2);
    }
}
