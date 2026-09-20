package com.alibaba.polardbx.server.handler.pl.inner;

import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.server.response.JdbcUrlRecordSyncAction;
import com.alibaba.polardbx.gms.sync.GmsSyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.ServerConnection;

import java.util.List;
import java.util.Map;

public class JdbcUrlShowProcedure extends BaseInnerProcedure {
    @Override
    public void execute(ServerConnection c, SQLCallStatement statement, ArrayResultCursor cursor) {
        List<List<Map<String, Object>>> metrics =
            GmsSyncManagerHelper.sync(new JdbcUrlRecordSyncAction(), "polardbx", SyncScope.CURRENT_ONLY);
        cursor.addColumn("ID", DataTypes.LongType);
        cursor.addColumn("USER", DataTypes.StringType);
        cursor.addColumn("HOST", DataTypes.StringType);
        cursor.addColumn("DB", DataTypes.StringType);
        cursor.addColumn("TIME", DataTypes.LongType);
        cursor.addColumn("JDBC_CONNECTION", DataTypes.StringType);

        for (List<Map<String, Object>> metric : metrics) {
            for (Map<String, Object> metricMap : metric) {
                cursor.addRow(new Object[] {
                    metricMap.get("ID"),
                    metricMap.get("USER"),
                    metricMap.get("HOST"),
                    metricMap.get("DB"),
                    metricMap.get("TIME"),
                    metricMap.get("JDBC_CONNECTION")
                });
            }
        }
    }
}
