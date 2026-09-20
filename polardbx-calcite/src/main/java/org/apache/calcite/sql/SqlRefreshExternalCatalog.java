package org.apache.calcite.sql;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;

public class SqlRefreshExternalCatalog extends SqlDal {

    private static final SqlOperator OPERATOR = new SqlRefreshExternalCatalogOperator();

    private final String catalogName;
    private final String dbName;
    private final String tableName;

    public SqlRefreshExternalCatalog(SqlParserPos pos, String catalogName,
                                     String dbName, String tableName) {
        super(pos);
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.tableName = tableName;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getExternalDbName() {
        return dbName;
    }

    public String getExternalTableName() {
        return tableName;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        if (dbName != null || tableName != null) {
            writer.keyword("REFRESH EXTERNAL TABLE");
            writer.literal(catalogName + "." + dbName + "." + tableName);
        } else {
            writer.keyword("REFRESH EXTERNAL CATALOG");
            writer.literal(catalogName);
        }
    }

    public static class SqlRefreshExternalCatalogOperator extends SqlSpecialOperator {
        public SqlRefreshExternalCatalogOperator() {
            super("REFRESH_EXTERNAL_CATALOG", SqlKind.REFRESH_EXTERNAL_CATALOG);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            return typeFactory.createStructType(
                ImmutableList.of(new RelDataTypeFieldImpl("RESULT", 0,
                    typeFactory.createSqlType(SqlTypeName.CHAR))));
        }
    }
}
