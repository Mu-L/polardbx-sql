package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

public class MySqlShowExternalCatalogsStatement extends MySqlStatementImpl implements MySqlShowStatement {
    private SQLExpr like;

    public MySqlShowExternalCatalogsStatement() {
        setDbType(DbType.mysql);
    }

    public SQLExpr getLike() {
        return like;
    }

    public void setLike(SQLExpr like) {
        if (like != null) {
            like.setParent(this);
        }
        this.like = like;
    }

    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, like);
        }
        visitor.endVisit(this);
    }
}
