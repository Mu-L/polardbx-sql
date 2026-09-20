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

public class SqlShowConnectors extends SqlShow {
    private final SqlSpecialOperator operator = new SqlShowConnectorsOperator();
    private boolean full;

    public SqlShowConnectors(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers, List<SqlNode> operands,
                             SqlNode like, SqlNode where, SqlNode orderBy, SqlNode limit) {
        this(pos, specialIdentifiers, operands, like, where, orderBy, limit, false);
    }

    public SqlShowConnectors(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers, List<SqlNode> operands,
                             SqlNode like, SqlNode where, SqlNode orderBy, SqlNode limit, boolean full) {
        super(pos, specialIdentifiers, operands, like, where, orderBy, limit);
        this.full = full;
    }

    public boolean isFull() {
        return full;
    }

    @Override
    public SqlOperator getOperator() {
        return operator;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_CONNECTORS;
    }

    @Override
    protected boolean showWhere() {
        return false;
    }

    public static class SqlShowConnectorsOperator extends SqlSpecialOperator {

        public SqlShowConnectorsOperator() {
            super("SHOW_CONNECTORS", SqlKind.SHOW_CONNECTORS);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            boolean full = (call instanceof SqlShowConnectors) && ((SqlShowConnectors) call).isFull();
            columns.add(new RelDataTypeFieldImpl("CONNECTOR_NAME", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            if (full) {
                columns.add(new RelDataTypeFieldImpl("SECRET_TYPE", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl("REQUIRED_KEYS", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl("OPTIONAL_KEYS", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl("SENSITIVE_KEYS", 4, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl("ALLOW_UNKNOWN_KEYS", 5, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            }
            return typeFactory.createStructType(columns);
        }
    }
}
