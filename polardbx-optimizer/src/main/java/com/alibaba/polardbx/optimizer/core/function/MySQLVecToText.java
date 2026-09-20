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
 * VEC_TOTEXT function to convert a VECTOR binary format to a JSON text representation.
 * This function is designed to be pushed down to DN for execution.
 * Syntax: VEC_TOTEXT(vector_column)
 * Example: VEC_TOTEXT(embedding)
 */
public class MySQLVecToText extends SqlFunction {

    public static final MySQLVecToText INSTANCE = new MySQLVecToText("VEC_TOTEXT");
    public static final MySQLVecToText FROM_VECTOR = new MySQLVecToText("FROM_VECTOR");
    public static final MySQLVecToText VECTOR_TO_STRING = new MySQLVecToText("VECTOR_TO_STRING");

    public MySQLVecToText(String name) {
        super(name,
            SqlKind.OTHER_FUNCTION,
            ReturnTypes.VARCHAR_2000,
            InferTypes.FIRST_KNOWN,
            OperandTypes.ANY,
            SqlFunctionCategory.STRING);
    }

    @Override
    public void unparse(SqlWriter writer, SqlCall call, int leftPrec, int rightPrec) {
        writer.keyword(getName());
        SqlWriter.Frame frame = writer.startList("(", ")");
        List<SqlNode> operands = call.getOperandList();
        if (!operands.isEmpty()) {
            operands.get(0).unparse(writer, 0, 0);
        }
        writer.endList(frame);
    }

    @Override
    public SqlOperandCountRange getOperandCountRange() {
        return SqlOperandCountRanges.of(1);
    }
}
