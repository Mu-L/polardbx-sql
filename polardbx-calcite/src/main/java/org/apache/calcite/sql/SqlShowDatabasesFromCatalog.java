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

public class SqlShowDatabasesFromCatalog extends SqlShow {
    private static final SqlShowDatabasesFromCatalogOperator OPERATOR = new SqlShowDatabasesFromCatalogOperator();

    private final String catalogName;

    public SqlShowDatabasesFromCatalog(SqlParserPos pos, String catalogName, SqlNode like, SqlNode where) {
        super(pos,
            ImmutableList.of(SqlSpecialIdentifier.DATABASES, SqlSpecialIdentifier.FROM),
            ImmutableList.of(),
            like, where, null, null);
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
        return SqlKind.SHOW_DATABASES_FROM_CATALOG;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.keyword("SHOW DATABASES FROM");
        new SqlIdentifier(catalogName, getParserPosition()).unparse(writer, leftPrec, rightPrec);
        unparseSearchCondition(writer, leftPrec, rightPrec);
        writer.endList(frame);
    }

    @Override
    protected boolean showWhere() {
        return false;
    }

    public static class SqlShowDatabasesFromCatalogOperator extends SqlSpecialOperator {

        public SqlShowDatabasesFromCatalogOperator() {
            super("SHOW_DATABASES_FROM_CATALOG", SqlKind.SHOW_DATABASES_FROM_CATALOG);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final String catalogName = ((SqlShowDatabasesFromCatalog) call).getCatalogName();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("Database_in_" + catalogName,
                0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            return typeFactory.createStructType(columns);
        }
    }
}
