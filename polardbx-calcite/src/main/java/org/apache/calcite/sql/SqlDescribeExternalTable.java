package org.apache.calcite.sql;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.LinkedList;
import java.util.List;

public class SqlDescribeExternalTable extends SqlShow {
    private static final SqlSpecialOperator OPERATOR = new SqlDescribeExternalTableOperator();
    private final String catalogName;
    private final String externalDbName;
    private final String tableName;

    public SqlDescribeExternalTable(SqlParserPos pos, String catalogName, String externalDbName, String tableName) {
        super(pos,
            ImmutableList.of(SqlSpecialIdentifier.TABLES),
            ImmutableList.of(),
            null, null, null, null);
        this.catalogName = catalogName;
        this.externalDbName = externalDbName;
        this.tableName = tableName;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getExternalDbName() {
        return externalDbName;
    }

    public String getExternalTableName() {
        return tableName;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.DESCRIBE_EXTERNAL_TABLE;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DESC");
        new SqlIdentifier(ImmutableList.of(catalogName, externalDbName, tableName), getParserPosition())
            .unparse(writer, leftPrec, rightPrec);
    }

    @Override
    protected boolean showWhere() {
        return false;
    }

    public static class SqlDescribeExternalTableOperator extends SqlSpecialOperator {

        public SqlDescribeExternalTableOperator() {
            super("DESCRIBE_EXTERNAL_TABLE", SqlKind.DESCRIBE_EXTERNAL_TABLE);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("Field", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Type", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Null", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Key", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Default", 4, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("Extra", 5, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            return typeFactory.createStructType(columns);
        }
    }
}
