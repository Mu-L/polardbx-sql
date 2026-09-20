package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.Arrays;
import java.util.List;

/**
 * Created by luoyanxin.
 *
 * @author luoyanxin
 */
public class SqlAlterTableToggleFullScan extends SqlCreate {

    private static final SqlOperator OPERATOR = new SqlSpecialOperator("ALTER TABLE", SqlKind.ALTER_TABLE);

    final String sourceSql;

    final List<SqlIdentifier> objectNames;
    final boolean enable;

    public SqlAlterTableToggleFullScan(List<SqlIdentifier> objectNames, SqlIdentifier tableName,
                                       String sql,  boolean enable) {
        super(OPERATOR, SqlParserPos.ZERO, false, false);
        this.name = tableName;
        this.sourceSql = sql;
        this.objectNames = objectNames;
        this.enable = enable;
    }


    public List<SqlIdentifier> getObjectNames() {
        return objectNames;
    }

    public String getSourceSql() {
        return sourceSql;
    }

    public boolean isEnable() {
        return enable;
    }

    @Override
    public List<SqlNode> getOperandList() {
        return Arrays.asList(name);
    }

    @Override
    public String toString() {
        return sourceSql;
    }

    @Override
    public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
        writer.print(toString());
    }

}
