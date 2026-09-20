package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.SQLObjectImpl;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

public class DrdsAlterTableRebuildCleanup extends SQLObjectImpl implements SQLAlterTableItem {

    private SQLExpr cleanupPredicate;
    private boolean dryRun;

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {
            acceptChild(visitor, cleanupPredicate);
        }
        visitor.endVisit(this);
    }

    public SQLExpr getCleanupPredicate() {
        return cleanupPredicate;
    }

    public void setCleanupPredicate(SQLExpr cleanupPredicate) {
        this.cleanupPredicate = cleanupPredicate;
    }

    public boolean isDryRun() {
        return dryRun;
    }

    public void setDryRun(boolean dryRun) {
        this.dryRun = dryRun;
    }
}
