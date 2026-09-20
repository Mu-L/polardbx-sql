package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLStatement;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLSelect;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

import java.util.ArrayList;
import java.util.List;

public class ColumnarWarmupStatement extends MySqlStatementImpl implements SQLStatement {

    private List<SQLSelect> selects = new ArrayList<>();

    private String cronExpression;

    public ColumnarWarmupStatement() {
    }

    public void accept0(MySqlASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    public List<SQLSelect> getSelect() {
        return selects;
    }

    public void setSelect(SQLSelect select) {
        selects.add(select);
    }

    public String getCronExpression() {
        return cronExpression;
    }

    public void setCronExpression(String cronExpression) {
        this.cronExpression = cronExpression;
    }

    @Override
    public SqlType getSqlType() {
        return null;
    }
}
