package org.apache.calcite.sql;

import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlString;

import java.util.ArrayList;

/**
 * @author wumu
 */
public class SqlAlterTableRemoveAutoPartition extends SqlAlterTable {
    protected SqlNode parent;
    private final SqlIdentifier originTableName;

    public SqlAlterTableRemoveAutoPartition(SqlIdentifier tableName, String sql) {
        super(null, tableName, null, sql, null, new ArrayList<>(), SqlParserPos.ZERO);
        this.name = tableName;
        this.originTableName = tableName;
    }

    public String getSchemaName() {
        return originTableName.getComponent(0).getLastName();
    }

    public String getPrimaryTableName() {
        return originTableName.getComponent(1).getLastName();
    }

    public SqlNode getParent() {
        return parent;
    }

    public void setParent(SqlNode parent) {
        this.parent = parent;
    }

    @Override
    public String toString() {
        return "REMOVE AUTO PARTITION";
    }

    @Override
    public SqlString toSqlString(SqlDialect dialect) {
        return new SqlString(dialect ,toString());
    }
}
