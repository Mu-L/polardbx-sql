package org.apache.calcite.sql;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * SQL AST for: ALTER TABLE t CANCEL EXPAND
 */
public class SqlAlterTableCancelExpand extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("CANCEL EXPAND", SqlKind.CANCEL_EXPAND);

    public SqlAlterTableCancelExpand(SqlParserPos pos) {
        super(pos);
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return ImmutableList.of();
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("CANCEL EXPAND");
    }
}
