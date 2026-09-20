package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLShowStatement;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

import java.util.ArrayList;
import java.util.List;

/**
 * @version 1.0
 */
public class DrdsCheckTableRouting extends MySqlStatementImpl implements SQLShowStatement {

    private Boolean checkIndexRouting = false;
    private Boolean explain = false;
    private SQLName indexName = null;
    private SQLName tableName = null;
    private List<SQLName> partitions = new ArrayList<>();

    public DrdsCheckTableRouting() {
    }

    public void accept0(MySqlASTVisitor visitor) {
        visitor.visit(this);
        visitor.endVisit(this);
    }

    public SQLName getIndexName() {
        return indexName;
    }

    public void setIndexName(SQLName indexName) {
        this.indexName = indexName;
    }

    public SQLName getTableName() {
        return tableName;
    }

    public void setTableName(SQLName tableName) {
        this.tableName = tableName;
    }


    public Boolean getCheckIndexRouting() {
        return checkIndexRouting;
    }

    public void setCheckIndexRouting(Boolean checkIndexRouting) {
        this.checkIndexRouting = checkIndexRouting;
    }

    public List<SQLName> getPartitions() {
        return partitions;
    }

    public void setPartitions(List<SQLName> partitions) {
        this.partitions = partitions;
    }

    public Boolean getExplain() {
        return explain;
    }

    public void setExplain(Boolean explain) {
        this.explain = explain;
    }
}
