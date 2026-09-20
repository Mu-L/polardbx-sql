package com.alibaba.polardbx.druid.sql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement.MySqlStatementImpl;
import com.alibaba.polardbx.druid.sql.dialect.mysql.visitor.MySqlASTVisitor;

import java.util.List;

/**
 * @author pangzhaoxing
 */
public class DrdsSQLCollectStatisticStatement extends MySqlStatementImpl {

    private List<SQLName> schemas;

    public DrdsSQLCollectStatisticStatement() {
    }

    @Override
    public void accept0(final MySqlASTVisitor visitor) {
        if (visitor.visit(this)) {
            if (null != this.schemas) {
                for (SQLName labelName : schemas) {
                    labelName.accept(visitor);
                }
            }

        }
        visitor.endVisit(this);
    }

    public List<SQLName> getSchemas() {
        return schemas;
    }

    public void setSchemas(List<SQLName> schemas) {
        this.schemas = schemas;
    }

    @Override
    public SqlType getSqlType() {
        return null;
    }
}
