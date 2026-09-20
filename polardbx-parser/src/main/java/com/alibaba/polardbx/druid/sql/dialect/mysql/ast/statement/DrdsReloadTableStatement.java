package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLPropertyExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

/**
 * @author wumu
 */
public class DrdsReloadTableStatement extends MySqlStatementImpl {

    private SQLName tableName = null;
    private Boolean preemptive = false;

    public void setTableName(SQLName tableName) {
        this.tableName = tableName;
    }

    public SQLName getTableName() {
        return tableName;
    }

    public Boolean getPreemptive() {
        return preemptive;
    }

    public void setPreemptive(Boolean preemptive) {
        this.preemptive = preemptive;
    }

    public String getTableNameStr() {
        if (tableName != null) {
            return tableName.getSimpleName();
        }
        return null;
    }

    public String getTableSchemaName() {
        if (tableName == null) {
            return null;
        }

        if (tableName instanceof SQLPropertyExpr) {
            return ((SQLPropertyExpr) tableName).getOwnernName();
        }

        return null;
    }

    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, tableName);
        }
        visitor.endVisit(this);
    }

    @Override
    public SqlType getSqlType() {
        return null;
    }
}
