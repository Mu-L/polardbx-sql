package com.alibaba.polardbx.server.handler.pl.inner;

import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.ServerConnection;

import java.util.List;

public class JdbcUrlAddProcedure extends BaseInnerProcedure {

    @Override
    public void execute(ServerConnection c, SQLCallStatement statement, ArrayResultCursor cursor) {
        String jdbcUrl = checkParamter(statement.getParameters(), statement);
        c.setJdbcUrl(jdbcUrl);
        cursor.addColumn("ID", DataTypes.LongType);
        cursor.addColumn("USER", DataTypes.StringType);
        cursor.addColumn("HOST", DataTypes.StringType);
        cursor.addColumn("DB", DataTypes.StringType);
        cursor.addColumn("TIME", DataTypes.LongType);
        cursor.addColumn("JDBC_CONNECTION", DataTypes.StringType);
        long time = (System.nanoTime() - c.getLastActiveTime()) / 1000000000;
        cursor.addRow(new Object[] {
            c.getId(),
            c.getUser(),
            c.getHost() + ":" + c.getPort(),
            c.getSchema(),
            time,
            jdbcUrl
        });
    }

    private String checkParamter(List<SQLExpr> params, SQLCallStatement statement) {
        if (params.size() != 1) {
            throw new IllegalArgumentException(statement.toString() + " parameters is not match 1  parameters");
        }

        if (!(params.get(0) instanceof SQLCharExpr)) {
            throw new IllegalArgumentException(
                statement.toString() + " first parameters need String");
        }
        return ((SQLCharExpr) params.get(0)).getText();
    }

}
