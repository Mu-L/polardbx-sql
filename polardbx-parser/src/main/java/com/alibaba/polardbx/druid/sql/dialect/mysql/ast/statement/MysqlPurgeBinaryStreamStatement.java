package com.alibaba.polardbx.druid.sql.dialect.mysql.ast.statement;

import com.alibaba.polardbx.druid.sql.ast.SQLName;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.druid.sql.visitor.SQLASTVisitor;

public class MysqlPurgeBinaryStreamStatement extends MySqlStatementImpl {

    private final SQLName streamName;

    public MysqlPurgeBinaryStreamStatement(SQLName streamName) {
        this.streamName = streamName;
    }

    @Override
    public SqlType getSqlType() {
        return null;
    }

    public SQLName getStreamName() {
        return streamName;
    }

    @Override
    protected void accept0(SQLASTVisitor visitor) {
        if (visitor.visit(this)) {

        }
        visitor.endVisit(this);
    }
}
