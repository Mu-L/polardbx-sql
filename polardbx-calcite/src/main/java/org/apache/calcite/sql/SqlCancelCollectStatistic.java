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

/**
 * @author pangzhaoxing
 */
public class SqlCancelCollectStatistic extends SqlDal{

    private static final SqlSpecialOperator OPERATOR = new SqlCancelCollectStatisticOperator();

    private List<SqlLiteral> connectionIds;

    public SqlCancelCollectStatistic(SqlParserPos pos, List<SqlLiteral> connectionIds) {
        super(pos);
        this.connectionIds = connectionIds;
    }

    public List<SqlLiteral> getConnectionIds() {
        return connectionIds;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.CANCEL_COLLECT_STATISTIC;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CANCEL COLLECT STATISTIC");
        if (connectionIds != null) {
            for (int i = 0; i < connectionIds.size(); i++) {
                if (i != 0) {
                    writer.keyword(",");
                }
                connectionIds.get(i).unparse(writer, leftPrec, rightPrec);
            }
        }
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    public static class SqlCancelCollectStatisticOperator extends SqlSpecialOperator {
        public SqlCancelCollectStatisticOperator() {
            super("CANCEL_COLLECT_STATISTIC", SqlKind.CANCEL_COLLECT_STATISTIC);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("RESULT", 0, typeFactory.createSqlType(SqlTypeName.CHAR)));
            return typeFactory.createStructType(columns);
        }
    }
}
