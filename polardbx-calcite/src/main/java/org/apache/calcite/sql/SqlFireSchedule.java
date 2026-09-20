/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
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
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;

/**
 * An operator that trigger one schedule job executed immediately
 */
public class SqlFireSchedule extends SqlDal {
    private static final SqlSpecialOperator OPERATOR = new SqlFireSchedule.SqlFireScheduleOperator();

    final long scheduleId;
    protected boolean byScheduleName = false;
    protected boolean byTableName = false;
    protected SqlNode targetExpr;

    public SqlFireSchedule(SqlParserPos pos, long scheduleId) {
        super(pos);
        this.scheduleId = scheduleId;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("FIRE SCHEDULE");
        if (isByScheduleName()) {
            writer.keyword("BY NAME");
            targetExpr.unparse(writer, leftPrec, rightPrec);
        } else if (isByTableName()) {
            writer.keyword("BY TABLE");
            targetExpr.unparse(writer, leftPrec, rightPrec);
        } else {
            writer.keyword(String.valueOf(scheduleId));
        }
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.FIRE_SCHEDULE;
    }

    public long getScheduleId() {
        return this.scheduleId;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    public boolean isByScheduleName() {
        return byScheduleName;
    }

    public void setByScheduleName(boolean byScheduleName) {
        this.byScheduleName = byScheduleName;
    }

    public boolean isByTableName() {
        return byTableName;
    }

    public void setByTableName(boolean byTableName) {
        this.byTableName = byTableName;
    }

    public SqlNode getTargetExpr() {
        return targetExpr;
    }

    public void setTargetExpr(SqlNode targetExpr) {
        this.targetExpr = targetExpr;
    }

    public static class SqlFireScheduleOperator extends SqlSpecialOperator {
        public SqlFireScheduleOperator() {
            super("FIRE_SCHEDULE", SqlKind.FIRE_SCHEDULE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final RelDataType columnType = typeFactory.createSqlType(SqlTypeName.CHAR);
            return typeFactory.createStructType(
                ImmutableList.of((RelDataTypeField) new RelDataTypeFieldImpl("FIRE_SCHEDULE",
                    0,
                    columnType)));
        }
    }
}