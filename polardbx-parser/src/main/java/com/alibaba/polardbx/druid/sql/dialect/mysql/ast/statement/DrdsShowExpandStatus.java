package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

/**
 * SHOW EXPAND STATUS [FOR table_name]
 */
public class DrdsShowExpandStatus extends MySqlStatementImpl implements SQLShowStatement {

    /**
     * Optional table name: SHOW EXPAND STATUS FOR <tableName>
     */
    private SQLExpr tableName;

    public SQLExpr getTableName() {
        return tableName;
    }

    public void setTableName(SQLExpr tableName) {
        this.tableName = tableName;
    }

    @Override
    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, tableName);
        }
        visitor.endVisit(this);
    }
}
