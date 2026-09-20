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
import java.util.Map;

public class SqlAlterExternalCatalog extends SqlDdl {

    private static final SqlOperator OPERATOR =
        new SqlAlterExternalCatalogOperator();

    private final String catalogName;
    private final String secret;
    private final Map<String, String> properties;
    private final String comment;

    public SqlAlterExternalCatalog(SqlParserPos pos, String catalogName, String secret,
                                   Map<String, String> properties, String comment) {
        super(OPERATOR, pos);
        this.catalogName = catalogName;
        this.secret = secret;
        this.properties = properties;
        this.comment = comment;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getSecret() {
        return secret;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.ALTER_EXTERNAL_CATALOG;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("ALTER EXTERNAL CATALOG");
        writer.literal(catalogName);
    }

    public static class SqlAlterExternalCatalogOperator extends SqlSpecialOperator {
        public SqlAlterExternalCatalogOperator() {
            super("ALTER EXTERNAL CATALOG", SqlKind.ALTER_EXTERNAL_CATALOG);
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
