package com.alibaba.polardbx.server.handler.pl.inner;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.expr.SQLCharExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.gms.metadb.table.BlockChainHistoryTable;
import com.alibaba.polardbx.gms.metadb.table.BlockChainHistoryTableAccessor;
import com.alibaba.polardbx.gms.metadb.table.BlockChainHistoryTableRecord;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.conn.InnerConnectionManager;
import com.alibaba.polardbx.server.util.GlobalChainUtils;

import java.sql.Connection;
import java.util.List;

public class ChainHistArchiveProcedure extends BaseInnerProcedure {
    @Override
    public void execute(ServerConnection c, SQLCallStatement statement, ArrayResultCursor cursor) {
        GlobalChainUtils.checkPrivilege(c);
        List<SQLExpr> params = statement.getParameters();
        // schemaName and tableName
        if (params.size() != 2) {
            throw new IllegalArgumentException("Expects two parameters: schema_name and table_name."
                    + " Example: call polardbx.chain_hist_archive('db1', 'tb1')");
        }
        if (!(params.get(0) instanceof SQLCharExpr && params.get(1) instanceof SQLCharExpr)) {
            throw new IllegalArgumentException("Expects two string parameters: schema_name and table_name."
                    + " Example: call polardbx.chain_hist_archive('db1', 'tb1')");
        }
        final String schemaName = ((SQLCharExpr)params.get(0)).getText();
        final String tableName = ((SQLCharExpr)params.get(1)).getText();

        BlockChainHistoryTableRecord result = chainHistArchive(schemaName, tableName);

        cursor.addColumn("status", DataTypes.StringType);
        cursor.addColumn("delete_num", DataTypes.LongType);
        cursor.addColumn("last_trace_id", DataTypes.StringType);

        if (result == null) {
            cursor.addRow(new Object[]{"No Need", 0L, "null"});
        } else {
            cursor.addRow(new Object[]{"OK", result.longPk, result.traceId});
        }
    }

    public static BlockChainHistoryTableRecord chainHistArchive(String schemaName, String tableName) {
        String histTableName = String.format(BlockChainHistoryTable.tableNameFormat, tableName);
        try (Connection connection = InnerConnectionManager.getInstance().getConnection(schemaName)) {
            BlockChainHistoryTableAccessor accessor = new BlockChainHistoryTableAccessor();
            accessor.setConnection(connection);
            List<BlockChainHistoryTableRecord> records = accessor.queryLastTraceIdRecord(schemaName, histTableName);
            if (records.isEmpty()) {
                //无记录，返回null
                return null;
            }
            BlockChainHistoryTableRecord lastRecord = records.get(0);

            if (!DynamicConfig.getInstance().isIgnoreCheckGlobalWhenArchiveChain()
                && !GlobalChainUtils.checkHistChainValid(schemaName, tableName, GlobalChainUtils.getTso(),
                new StringBuilder())) {
                throw new RuntimeException("Check global failed, can not archive.");
            }

            //不使用事务，错误了下次再继续
            //删除
            int delete = accessor.deleteById(schemaName, histTableName, lastRecord.blockId);
            //修改
            accessor.updateByTraceId(schemaName, histTableName, lastRecord.traceId, "ARCHIVE");
            //修改GlobalChain？？

            //删除行数暂时放到longPk传递
            lastRecord.longPk = (long) delete;
            return lastRecord;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
