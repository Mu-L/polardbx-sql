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

public class SqlShowExternalCatalogs extends SqlShow {
    private SqlSpecialOperator operator = new SqlShowExternalCatalogsOperator();

    public SqlShowExternalCatalogs(SqlParserPos pos, List<SqlSpecialIdentifier> specialIdentifiers,
                                   List<SqlNode> operands, SqlNode like, SqlNode where, SqlNode orderBy,
                                   SqlNode limit) {
        super(pos, specialIdentifiers, operands, like, where, orderBy, limit);
    }

    @Override
    public SqlOperator getOperator() {
        return operator;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_EXTERNAL_CATALOGS;
    }

    @Override
    protected boolean showWhere() {
        return false;
    }

    public static class SqlShowExternalCatalogsOperator extends SqlSpecialOperator {

        public SqlShowExternalCatalogsOperator() {
            super("SHOW_EXTERNAL_CATALOGS", SqlKind.SHOW_EXTERNAL_CATALOGS);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("CATALOG_NAME", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("CONNECTOR", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("SECRET", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("COMMENT", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            return typeFactory.createStructType(columns);
        }
    }
}
