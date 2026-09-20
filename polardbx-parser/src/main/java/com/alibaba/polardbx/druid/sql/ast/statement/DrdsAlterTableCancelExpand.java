package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLObjectImpl;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

/**
 * ALTER TABLE ... CANCEL EXPAND
 */
public class DrdsAlterTableCancelExpand extends SQLObjectImpl
    implements SQLAlterTableItem {

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        // no-op: no children and no specific visitor callbacks required
    }
}
