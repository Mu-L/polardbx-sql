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

public class SqlCreateExternalCatalog extends SqlDdl {

    private static final SqlOperator OPERATOR =
        new SqlCreateExternalCatalogOperator();

    private final String catalogName;
    private final boolean ifNotExists;
    private final String connector;
    private final Map<String, String> properties;
    private final String secretName;
    private final String comment;

    public SqlCreateExternalCatalog(SqlParserPos pos, String catalogName, boolean ifNotExists,
                                    String connector, Map<String, String> properties,
                                    String secretName, String comment) {
        super(OPERATOR, pos);
        this.catalogName = catalogName;
        this.ifNotExists = ifNotExists;
        this.connector = connector;
        this.properties = properties;
        this.secretName = secretName;
        this.comment = comment;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public boolean isIfNotExists() {
        return ifNotExists;
    }

    public String getConnector() {
        return connector;
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    public String getSecretName() {
        return secretName;
    }

    public String getComment() {
        return comment;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.CREATE_EXTERNAL_CATALOG;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CREATE EXTERNAL CATALOG");
        writer.literal(catalogName);
    }

    public static class SqlCreateExternalCatalogOperator extends SqlSpecialOperator {
        public SqlCreateExternalCatalogOperator() {
            super("CREATE EXTERNAL CATALOG", SqlKind.CREATE_EXTERNAL_CATALOG);
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
