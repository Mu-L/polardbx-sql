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

/**
 * SQL AST for: SHOW EXPAND STATUS [FOR table_name]
 */
public class SqlShowExpandStatus extends SqlShow {

    private static final SqlSpecialOperator OPERATOR = new SqlShowExpandStatusOperator();

    /** Optional table name for: SHOW EXPAND STATUS FOR <table_name> */
    private final SqlNode tableName;

    public SqlShowExpandStatus(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers, SqlNode tableName) {
        super(pos, specialIdentifiers);
        this.tableName = tableName;
    }

    public SqlNode getTableName() {
        return tableName;
    }

    @Override
    protected boolean showWhere() {
        return false;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_EXPAND_STATUS;
    }

    public static class SqlShowExpandStatusOperator extends SqlSpecialOperator {

        public SqlShowExpandStatusOperator() {
            super("SHOW_EXPAND_STATUS", SqlKind.SHOW_EXPAND_STATUS);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("SCHEMA_NAME", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("TABLE_NAME", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("STATUS", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("INITIAL_PARTS", 3, typeFactory.createSqlType(SqlTypeName.BIGINT)));
            columns.add(new RelDataTypeFieldImpl("TARGET_PARTS", 4, typeFactory.createSqlType(SqlTypeName.BIGINT)));
            columns.add(new RelDataTypeFieldImpl("COMPLETED", 5, typeFactory.createSqlType(SqlTypeName.BIGINT)));
            columns.add(new RelDataTypeFieldImpl("PENDING", 6, typeFactory.createSqlType(SqlTypeName.BIGINT)));
            columns.add(new RelDataTypeFieldImpl("DDL_JOB_ID", 7, typeFactory.createSqlType(SqlTypeName.BIGINT)));
            columns.add(new RelDataTypeFieldImpl("GMT_CREATED", 8, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("GMT_MODIFIED", 9, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            return typeFactory.createStructType(columns);
        }
    }
}
