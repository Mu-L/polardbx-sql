package org.apache.calcite.sql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeFieldImpl;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class SqlWarmup extends SqlDal {
    private final SqlSpecialOperator operator;

    // SQL AST
    private List<SqlNode> selects;

    // native SQL
    private List<String> sqlList;

    // SQL hint
    private List<String> hints;

    // Warmup cron expression. Maybe empty string.
    private String cronExpression;

    public SqlWarmup(SqlParserPos pos, List<SqlNode> selects) {
        super(pos);
        this.sqlList = new ArrayList<>();
        this.hints = new ArrayList<>();
        this.operands = new ArrayList<>(0);
        this.selects = selects;
        this.operator = new SqlWarmupOperator();
    }

    public List<String> getHint() {
        return hints;
    }

    public void setHint(String hint) {
        hints.add(hint);
    }

    public List<String> getSql() {
        return sqlList;
    }

    public void setSql(String sql) {
        this.sqlList.add(sql);
    }

    public String getCronExpression() {
        return cronExpression;
    }

    public void setCronExpression(String cronExpression) {
        this.cronExpression = cronExpression;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.sep("WARMUP");

        if (cronExpression != null) {
            writer.print("('");
            writer.print(cronExpression);
            writer.print("')");
        }

        if (selects.size() == 1) {
            selects.get(0).unparse(writer, leftPrec, rightPrec);
        } else {
            for (int i = 0; i < selects.size(); i++) {
                writer.print("{");
                selects.get(i).unparse(writer, leftPrec, rightPrec);
                writer.print("}");
            }
        }

        writer.endList(selectFrame);
    }

    @Override
    public SqlOperator getOperator() {
        return operator;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.WARMUP;
    }

    public static class SqlWarmupOperator extends SqlSpecialOperator {

        public SqlWarmupOperator() {
            super("WARMUP", SqlKind.WARMUP);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();

            columns.add(new RelDataTypeFieldImpl("START_TIME", 0, typeFactory.createSqlType(SqlTypeName.DATETIME)));
            columns.add(new RelDataTypeFieldImpl("FINISH_TIME", 1, typeFactory.createSqlType(SqlTypeName.DATETIME)));
            columns.add(new RelDataTypeFieldImpl("TIME_COST", 2, typeFactory.createSqlType(SqlTypeName.BIGINT)));
            columns.add(new RelDataTypeFieldImpl("IO_MESSAGE", 3, typeFactory.createSqlType(SqlTypeName.VARCHAR)));

            return typeFactory.createStructType(columns);
        }
    }
}
