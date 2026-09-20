package com.alibaba.polardbx.server.handler.pl.inner;

import com.alibaba.polardbx.druid.sql.SQLUtils;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.GlobalChainUtils;

import java.util.List;

public class CheckChainGlobalProcedure extends BaseInnerProcedure {
    @Override
    public void execute(ServerConnection c, SQLCallStatement statement, ArrayResultCursor cursor) {
        GlobalChainUtils.checkPrivilege(c);
        List<SQLExpr> params = statement.getParameters();
        // schemaName and tableName
        // TODO: support check the whole schema
        if (params.size() != 2) {
            throw new IllegalArgumentException("Expects two parameters: schema_name and table_name."
                + " Example: call polardbx.check_chain_global('db1', 'tb1')");
        }
        final String schemaName = SQLUtils.normalizeNoTrim(params.get(0).toString());
        final String tableName = SQLUtils.normalizeNoTrim(params.get(1).toString());

        String result;
        StringBuilder sb = new StringBuilder();
        if (GlobalChainUtils.checkGlobal(schemaName, tableName, sb)) {
            result = "OK";
        } else {
            result = "FAIL";
        }
        cursor.addColumn("result", DataTypes.StringType);
        cursor.addColumn("details", DataTypes.StringType);
        cursor.addRow(new Object[] {result, sb.toString()});
    }
}
