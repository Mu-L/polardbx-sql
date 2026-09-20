package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

public class SQLNativeQueryTableSource extends SQLTableSourceImpl {
    private String catalogName;
    private String sql;

    public SQLNativeQueryTableSource() {
    }

    public String getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(String catalogName) {
        this.catalogName = catalogName;
    }

    public String getSql() {
        return sql;
    }

    public void setSql(String sql) {
        this.sql = sql;
    }

    @Override
    protected void accept0(SQLASTVisitor v) {
        v.visit(this);
        v.endVisit(this);
    }

    public void cloneTo(SQLNativeQueryTableSource x) {
        x.alias = alias;
        x.catalogName = catalogName;
        x.sql = sql;
    }

    @Override
    public SQLNativeQueryTableSource clone() {
        SQLNativeQueryTableSource x = new SQLNativeQueryTableSource();
        cloneTo(x);
        return x;
    }
}
