package org.apache.calcite.sql;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;

/**
 * @author pangzhaoxing
 */
public class SqlCollectStatistic extends SqlDal{

    private static final SqlOperator OPERATOR = new SqlCollectStatisticOperator();

    private List<SqlIdentifier> schemas;

    protected SqlCollectStatistic(SqlParserPos pos) {
        super(pos);
    }

    public SqlCollectStatistic(SqlParserPos pos, List<SqlIdentifier> schemas) {
        super(pos);
        this.schemas = schemas;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.COLLECT_STATISTIC;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("COLLECT STATISTIC");
        if (schemas != null && schemas.size() != 0){
            schemas.get(0).unparse(writer, leftPrec, rightPrec);
            for (int i = 1; i < schemas.size(); i++){
                writer.keyword(",");
                schemas.get(i).unparse(writer, leftPrec, rightPrec);
            }
        }
    }

    public List<SqlIdentifier> getSchemas() {
        return schemas;
    }

    public static class SqlCollectStatisticOperator extends SqlSpecialOperator {

        public SqlCollectStatisticOperator() {
            super("COLLECT_STATISTIC", SqlKind.COLLECT_STATISTIC);
        }

        @Override
        public RelDataType deriveType(final SqlValidator validator, final SqlValidatorScope scope, final SqlCall call) {

            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            return typeFactory.createStructType(ImmutableList.of(
                    new RelDataTypeFieldImpl("schema", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                    new RelDataTypeFieldImpl("table", 1, typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                    new RelDataTypeFieldImpl("enable_collect_hll", 2, typeFactory.createSqlType(SqlTypeName.VARCHAR)),
                    new RelDataTypeFieldImpl("collect_statistic", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR))
            ));
        }
    }
}
