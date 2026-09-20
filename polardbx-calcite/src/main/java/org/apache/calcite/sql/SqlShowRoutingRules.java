package org.apache.calcite.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.LinkedList;
import java.util.List;

public class SqlShowRoutingRules extends SqlShow {
    private SqlSpecialOperator operator = new SqlShowRoutingRulesOperator();

    public SqlShowRoutingRules(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers, List<SqlNode> operands,
                               SqlNode like, SqlNode where, SqlNode orderBy, SqlNode limit) {
        super(pos, specialIdentifiers, operands, like, where, orderBy, limit);
    }

    @Override
    public SqlOperator getOperator() {
        return operator;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_ROUTING_RULES;
    }

    @Override
    protected boolean showWhere() {
        return false;
    }

    public static class SqlShowRoutingRulesOperator extends SqlSpecialOperator {

        public SqlShowRoutingRulesOperator() {
            super("SHOW_ROUTING_RULES", SqlKind.SHOW_ROUTING_RULES);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("ID", 0, typeFactory.createSqlType(SqlTypeName.BIGINT)));
            columns.add(new RelDataTypeFieldImpl("RULE_NAME", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("USER_NAME", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("TEMPLATE_ID", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("KEYWORDS", 4, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("ROUTING_TYPE", 5, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("CREATE_TIME", 6, typeFactory.createSqlType(SqlTypeName.TIMESTAMP)));
            columns.add(new RelDataTypeFieldImpl("HIT_COUNT", 7, typeFactory.createSqlType(SqlTypeName.BIGINT)));
            return typeFactory.createStructType(columns);
        }
    }
}
