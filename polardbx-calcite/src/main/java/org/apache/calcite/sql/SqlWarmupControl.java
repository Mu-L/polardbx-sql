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
import java.util.Objects;

public class SqlWarmupControl extends SqlDal {
    private final SqlSpecialOperator operator;

    public enum SqlWarmupControlType {
        DELETE, SUSPEND, RESUME
    }

    boolean isAll;
    long taskId;
    SqlWarmupControlType controlType;

    public SqlWarmupControl(SqlParserPos pos, boolean isAll, long taskId, String controlType) {
        super(pos);
        this.operands = new ArrayList<>(0);
        this.operator = new SqlWarmupControlOperator();
        this.isAll = isAll;
        this.taskId = taskId;

        // must be upper case.
        Objects.requireNonNull(controlType);
        this.controlType = SqlWarmupControlType.valueOf(controlType.toUpperCase());
    }

    public boolean isAll() {
        return isAll;
    }

    public long getTaskId() {
        return taskId;
    }

    public SqlWarmupControlType getControlType() {
        return controlType;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        final SqlWriter.Frame selectFrame = writer.startList(SqlWriter.FrameTypeEnum.SELECT);
        writer.sep("WARMUP " + controlType.name());

        if (isAll) {
            writer.print("ALL");
        } else {
            writer.print(String.valueOf(taskId));
        }

        writer.endList(selectFrame);
    }

    @Override
    public SqlOperator getOperator() {
        return operator;
    }

    @Override
    public SqlKind getKind() {
        return SqlKind.WARMUP_CONTROL;
    }

    public static class SqlWarmupControlOperator extends SqlSpecialOperator {

        public SqlWarmupControlOperator() {
            super("WARMUP_CONTROL", SqlKind.WARMUP_CONTROL);
        }

        @Override
        public RelDataType deriveType(SqlValidator validator, SqlValidatorScope scope, SqlCall call) {
            final RelDataTypeFactory typeFactory = validator.getTypeFactory();
            List<RelDataTypeFieldImpl> columns = new LinkedList<>();
            columns.add(new RelDataTypeFieldImpl("TASK_ID", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            columns.add(new RelDataTypeFieldImpl("STATUS", 0, typeFactory.createSqlType(SqlTypeName.VARCHAR)));
            return typeFactory.createStructType(columns);
        }
    }
}
