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

public class SqlDropExternalCatalog extends SqlDdl {

    private static final SqlOperator OPERATOR =
        new SqlDropExternalCatalogOperator();

    private final String catalogName;
    private final boolean ifExists;

    public SqlDropExternalCatalog(SqlParserPos pos, String catalogName, boolean ifExists) {
        super(OPERATOR, pos);
        this.catalogName = catalogName;
        this.ifExists = ifExists;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public boolean isIfExists() {
        return ifExists;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.DROP_EXTERNAL_CATALOG;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("DROP EXTERNAL CATALOG");
        writer.literal(catalogName);
    }

    public static class SqlDropExternalCatalogOperator extends SqlSpecialOperator {
        public SqlDropExternalCatalogOperator() {
            super("DROP EXTERNAL CATALOG", SqlKind.DROP_EXTERNAL_CATALOG);
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
