package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

import java.util.LinkedHashMap;
import java.util.Map;

public class SQLFilesTableSource extends SQLTableSourceImpl {
    private final Map<String, String> properties = new LinkedHashMap<>();

    public SQLFilesTableSource() {
    }

    public Map<String, String> getProperties() {
        return properties;
    }

    @Override
    protected void accept0(SQLASTVisitor v) {
        v.visit(this);
        v.endVisit(this);
    }

    public void cloneTo(SQLFilesTableSource x) {
        x.alias = alias;
        x.properties.putAll(properties);
    }

    @Override
    public SQLFilesTableSource clone() {
        SQLFilesTableSource x = new SQLFilesTableSource();
        cloneTo(x);
        return x;
    }
}
