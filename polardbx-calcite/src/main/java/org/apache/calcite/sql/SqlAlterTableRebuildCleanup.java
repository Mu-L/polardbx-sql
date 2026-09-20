package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;

import java.util.Collections;
import java.util.List;

public class SqlAlterTableRebuildCleanup extends SqlAlterSpecification {

    private static final SqlOperator OPERATOR =
        new SqlSpecialOperator("REBUILD CLEANUP", SqlKind.REBUILD_CLEANUP);

    private final SqlNode cleanupPredicate;
    private final boolean dryRun;

    public SqlAlterTableRebuildCleanup(SqlParserPos pos, SqlNode cleanupPredicate, boolean dryRun) {
        super(pos);
        this.cleanupPredicate = cleanupPredicate;
        this.dryRun = dryRun;
    }

    @Override
    public SqlOperator getOperator() {
        return OPERATOR;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Collections.singletonList(cleanupPredicate);
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.keyword("REBUILD CLEANUP WHERE");
        cleanupPredicate.unparse(writer, leftPrec, rightPrec);
        if (dryRun) {
            writer.keyword("DRY RUN");
        }
    }

    @Override
    public String toString() {
        String result = "REBUILD CLEANUP WHERE " + cleanupPredicate;
        if (dryRun) {
            result += " DRY RUN";
        }
        return result;
    }

    @Override
    public SqlString toSqlString(SqlDialect dialect) {
        return new SqlString(dialect, toString());
    }

    public SqlNode getCleanupPredicate() {
        return cleanupPredicate;
    }

    public boolean isDryRun() {
        return dryRun;
    }
}
