package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.DbType;
import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLAlterStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

public class MySqlRefreshExternalCatalogStatement extends MySqlStatementImpl implements SQLAlterStatement {

    private SQLName catalogName;
    private SQLName dbName;
    private SQLName tableName;
    private boolean isTable;

    public MySqlRefreshExternalCatalogStatement() {
        setDbType(DbType.mysql);
    }

    public SQLName getCatalogName() {
        return catalogName;
    }

    public void setCatalogName(SQLName catalogName) {
        if (catalogName != null) {
            catalogName.setParent(this);
        }
        this.catalogName = catalogName;
    }

    public SQLName getDbName() {
        return dbName;
    }

    public void setDbName(SQLName dbName) {
        if (dbName != null) {
            dbName.setParent(this);
        }
        this.dbName = dbName;
    }

    public SQLName getTableName() {
        return tableName;
    }

    public void setTableName(SQLName tableName) {
        if (tableName != null) {
            tableName.setParent(this);
        }
        this.tableName = tableName;
    }

    public boolean isTable() {
        return isTable;
    }

    public void setTable(boolean table) {
        isTable = table;
    }

    public void accept0(MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, catalogName);
            acceptChild(visitor, dbName);
            acceptChild(visitor, tableName);
        }
        visitor.endVisit(this);
    }
}
