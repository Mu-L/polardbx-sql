package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

import java.util.List;

/**
 * @author pangzhaoxing
 */
public class DrdsCancelCollectStatisticStatement extends MySqlStatementImpl {

    private List<SQLExpr> connectionIds;

    public DrdsCancelCollectStatisticStatement() {
    }

    public List<SQLExpr> getConnectionIds() {
        return connectionIds;
    }

    public void setConnectionIds(List<SQLExpr> connectionIds) {
        this.connectionIds = connectionIds;
    }

    @Override
    public void accept0(MySqlASTVisitor v) {
        if (v.visit(this)) {
            acceptChild(v, connectionIds);
        }
    }

    @Override
    public SqlType getSqlType() {
        return null;
    }
}
