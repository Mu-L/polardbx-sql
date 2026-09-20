package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLObjectImpl;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLIntegerExpr;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

/**
 * ALTER TABLE ... EXPAND PARTITIONS/SUBPARTITIONS TO N
 */
public class DrdsExpandPartitions extends SQLObjectImpl implements SQLAlterTableItem {

    private boolean subPartitions;
    private SQLIntegerExpr targetCount;

    public boolean isSubPartitions() {
        return subPartitions;
    }

    public void setSubPartitions(boolean subPartitions) {
        this.subPartitions = subPartitions;
    }

    public SQLIntegerExpr getTargetCount() {
        return targetCount;
    }

    public void setTargetCount(SQLIntegerExpr targetCount) {
        this.targetCount = targetCount;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, targetCount);
        }
        visitor.endVisit(this);
    }
}
