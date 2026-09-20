/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.sql;

import com.google.common.collect.ImmutableList;
import lombok.Getter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;

@Getter
public class SqlCreateIndexInDatabase extends SqlDdl {
    private static final SqlSpecialOperator OPERATOR = new SqlCreateIndexInDatabase.SqlCreateIndexInDatabaseOperator();

    final SqlIdentifier dbName;
    final boolean isColumnar;
    final boolean isIn;

    public SqlCreateIndexInDatabase(SqlParserPos pos, SqlIdentifier dbName, boolean isColumnar, boolean isIn) {
        super(OPERATOR, pos);
        this.dbName = dbName;
        this.isColumnar = isColumnar;
        this.isIn = isIn;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE");

        if (isColumnar) {
            writer.keyword("CLUSTERED COLUMNAR INDEX ");
        }
        if (isIn) {
            writer.keyword("IN ");
        } else {
            writer.keyword("FROM ");
        }

        dbName.unparse(writer, leftPrec, rightPrec);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    public static class SqlCreateIndexInDatabaseOperator extends SqlSpecialOperator {

        public SqlCreateIndexInDatabaseOperator() {
            super("CREATE INDEX IN DATABASE", SqlKind.CREATE_INDEX_IN_DATABASE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);

            return typeFactory.createStructType(
                ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("CREATE_INDEX_IH_DATABASE_RESULT",
                    0,
                    columnType)));
        }
    }
}
