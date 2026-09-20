package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

import java.util.LinkedHashMap;
import java.util.Map;

public class MySqlAlterSecretStatement extends MySqlStatementImpl implements SQLAlterStatement {
    private SQLName name;
    private Map<String, String> setProperties = new LinkedHashMap<>();

    public MySqlAlterSecretStatement() {
        setDbType(DbType.mysql);
    }

    public SQLName getName() {
        return name;
    }

    public void setName(SQLName name) {
        if (name != null) {
            name.setParent(this);
        }
        this.name = name;
    }

    public Map<String, String> getSetProperties() {
        return setProperties;
    }

    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, name);
        }
        visitor.endVisit(this);
    }
}
