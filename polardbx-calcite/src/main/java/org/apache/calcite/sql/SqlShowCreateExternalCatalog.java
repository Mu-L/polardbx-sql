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

public class SqlShowCreateExternalCatalog extends SqlShow {
    private static final SqlSpecialOperator OPERATOR = new SqlShowCreateExternalCatalogOperator();
    private final String catalogName;

    public SqlShowCreateExternalCatalog(SqlParserPos pos, String catalogName) {
        super(pos,
            ImmutableList.of(SqlSpecialIdentifier.CREATE, SqlSpecialIdentifier.EXTERNAL_CATALOGS),
            ImmutableList.of(),
            null, null, null, null);
        this.catalogName = catalogName;
    }

    public String getCatalogName() {
        return catalogName;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_CREATE_EXTERNAL_CATALOG;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("SHOW CREATE EXTERNAL CATALOG");
        new SqlIdentifier(catalogName, getParserPosition()).unparse(writer, leftPrec, rightPrec);
    }

    @Override
    protected boolean showWhere() {
        return false;
    }

    public static class SqlShowCreateExternalCatalogOperator extends SqlSpecialOperator {

        public SqlShowCreateExternalCatalogOperator() {
            super("SHOW_CREATE_EXTERNAL_CATALOG", SqlKind.SHOW_CREATE_EXTERNAL_CATALOG);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("CATALOG_NAME", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("CREATE_STATEMENT", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            return typeFactory.createStructType(columns);
        }
    }
}
