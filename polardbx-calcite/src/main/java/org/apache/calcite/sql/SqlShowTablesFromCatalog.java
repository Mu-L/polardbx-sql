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

public class SqlShowTablesFromCatalog extends SqlShow {
    /**
     * Column shape follows the local SHOW FULL TABLES so that a client cannot tell the
     * two apart. Auto_partition and Table_group describe PolarDB-X partitioning and are
     * reported with the same placeholders the local handler uses for foreign rows.
     */
    public static final String TABLE_TYPE_COLUMN = "Table_type";
    public static final String AUTO_PARTITION_COLUMN = "Auto_partition";
    public static final String TABLE_GROUP_COLUMN = "Table_group";

    private static final SqlShowTablesFromCatalogOperator OPERATOR = new SqlShowTablesFromCatalogOperator();

    private final String catalogName;
    private final String dbName;
    private final boolean isFull;

    public SqlShowTablesFromCatalog(SqlParserPos pos, String catalogName, String dbName,
                                    SqlNode like, SqlNode where, boolean isFull) {
        super(pos,
            isFull
                ? ImmutableList.of(SqlSpecialIdentifier.FULL, SqlSpecialIdentifier.TABLES,
                SqlSpecialIdentifier.FROM)
                : ImmutableList.of(SqlSpecialIdentifier.TABLES, SqlSpecialIdentifier.FROM),
            ImmutableList.of(),
            like, where, null, null);
        this.catalogName = catalogName;
        this.dbName = dbName;
        this.isFull = isFull;
    }

    public String getCatalogName() {
        return catalogName;
    }

    public String getExternalDbName() {
        return dbName;
    }

    public boolean isFull() {
        return isFull;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public SqlKind getShowKind() {
        return SqlKind.SHOW_TABLES_FROM_CATALOG;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame frame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.keyword(isFull ? "SHOW FULL TABLES FROM" : "SHOW TABLES FROM");
        new SqlIdentifier(ImmutableList.of(catalogName, dbName), getParserPosition())
            .unparse(writer, leftPrec, rightPrec);
        unparseSearchCondition(writer, leftPrec, rightPrec);
        writer.endList(frame);
    }

    @Override
    protected boolean showWhere() {
        return false;
    }

    public static String tablesColumnName(String catalogName, String dbName) {
        return "Tables_in_" + catalogName + "." + dbName;
    }

    public static class SqlShowTablesFromCatalogOperator extends SqlSpecialOperator {

        public SqlShowTablesFromCatalogOperator() {
            super("SHOW_TABLES_FROM_CATALOG", SqlKind.SHOW_TABLES_FROM_CATALOG);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            final SqlShowTablesFromCatalog show = (SqlShowTablesFromCatalog) call;
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl(tablesColumnName(show.getCatalogName(), show.getExternalDbName()),
                0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            if (show.isFull()) {
                columns.add(new RelDataTypeFieldImpl(TABLE_TYPE_COLUMN,
                    1, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl(AUTO_PARTITION_COLUMN,
                    2, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
                columns.add(new RelDataTypeFieldImpl(TABLE_GROUP_COLUMN,
                    3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            }
            return typeFactory.createStructType(columns);
        }
    }
}
