package org.apache.calcite.sql;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql.validate.SqlValidatorScope;

import java.util.List;

/**
 * SQL AST for: ALTER TABLE t EXPAND PARTITIONS TO N
 *              ALTER TABLE t EXPAND SUBPARTITIONS TO N
 */
public class SqlAlterTableExpandPartitions extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("EXPAND PARTITIONS", SqlKind.EXPAND_PARTITIONS);

    /** Whether this operates on subpartitions (true) or partitions (false). */
    private final boolean subPartitions;

    /** The target partition count, e.g. the N in "TO N". */
    private final SqlNode targetCount;

    public SqlAlterTableExpandPartitions(SqlParserPos pos, boolean subPartitions, SqlNode targetCount) {
        super(pos);
        this.subPartitions = subPartitions;
        this.targetCount = targetCount;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of(targetCount);
    }

    @Override
    public void validate(SqlValidator validator, SqlValidatorScope scope) {
        if (targetCount instanceof SqlNumericLiteral) {
            int count = ((SqlNumericLiteral) targetCount).intValue(true);
            if (count <= 0) {
                throw new TddlRuntimeException(ErrorCode.ERR_VALIDATE,
                    "EXPAND PARTITIONS target count must be a positive integer");
            }
        }
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword(subPartitions ? "EXPAND SUBPARTITIONS TO" : "EXPAND PARTITIONS TO");
        targetCount.unparse(writer, leftPrec, rightPrec);
    }

    public boolean isSubPartitions() {
        return subPartitions;
    }

    public SqlNode getTargetCount() {
        return targetCount;
    }
}
