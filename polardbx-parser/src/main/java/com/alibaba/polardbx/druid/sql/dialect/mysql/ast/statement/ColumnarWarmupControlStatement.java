package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

public class ColumnarWarmupControlStatement extends MySqlStatementImpl implements SQLStatement {

    private boolean isAll = false;

    private long taskId;

    private String token;

    public ColumnarWarmupControlStatement(String tokenName) {
        this.token = tokenName;
    }

    public void accept0(MySqlASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    public String getToken() {
        return token;
    }

    public boolean isAll() {
        return isAll;
    }

    public void setAll(boolean all) {
        isAll = all;
    }

    public long taskId() {
        return taskId;
    }

    public void setTaskId(long taskId) {
        this.taskId = taskId;
    }

    @Override
    public SqlType getSqlType() {
        return null;
    }
}
