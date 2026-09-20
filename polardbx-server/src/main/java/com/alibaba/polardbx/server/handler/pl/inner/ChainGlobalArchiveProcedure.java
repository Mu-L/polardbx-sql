package com.alibaba.polardbx.server.handler.pl.inner;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.druid.sql.ast.SQLExpr;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.gms.metadb.chain.GlobalChainAccessor;
import com.alibaba.polardbx.gms.metadb.chain.GlobalChainSimpleRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.util.GlobalChainUtils;

import java.sql.Connection;
import java.util.List;

public class ChainGlobalArchiveProcedure extends BaseInnerProcedure {
    @Override
    public void execute(ServerConnection c, SQLCallStatement statement, ArrayResultCursor cursor) {
        GlobalChainUtils.checkPrivilege(c);
        List<SQLExpr> params = statement.getParameters();
        // schemaName and tableName
        if (params.size() != 0) {
            throw new IllegalArgumentException("Expects No Parameters");
        }

        long result = globalChainArchive();

        cursor.addColumn("status", DataTypes.StringType);
        cursor.addColumn("delete_num", DataTypes.LongType);

        if (result == -1) {
            cursor.addRow(new Object[] {"No Need", 0L});
        } else {
            cursor.addRow(new Object[] {"OK", result});
        }
    }

    public static long globalChainArchive() {
        try (Connection metaDbConn = MetaDbUtil.getConnection()) {
            metaDbConn.setAutoCommit(false);
            try {
                GlobalChainAccessor accessor = new GlobalChainAccessor();
                accessor.setConnection(metaDbConn);
                List<GlobalChainSimpleRecord> records = accessor.queryLastRecordForEachTable();
                if (records.isEmpty()) {
                    //无记录，返回null
                    return -1;
                }

                long maxBlockId = 0;
                // Calculate op hash for each table.
                for (GlobalChainSimpleRecord record : records) {
                    if (record.schemaName == null) {
                        continue;
                    }
                    record.opHash = GlobalChainUtils.accumulateOpHash(metaDbConn, record.schemaName, record.tableName,
                        record.blockId);
                    if (record.blockId > maxBlockId) {
                        maxBlockId = record.blockId;
                    }
                }

                // Validate
                if (!DynamicConfig.getInstance().isIgnoreCheckGlobalWhenArchiveChain()
                    && !GlobalChainUtils.checkGlobalChainValid(GlobalChainUtils.getTso(), new StringBuilder())) {
                    throw new RuntimeException("Check global failed, can not archive.");
                }

                //删除
                int delete = accessor.deleteByBlockId(records, maxBlockId);
                //打标
                accessor.updateArchiveByBlockId(records);
                metaDbConn.commit();
                return delete;
            } catch (Exception e) {
                metaDbConn.rollback();
                throw new RuntimeException(e);
            } finally {
                metaDbConn.setAutoCommit(true);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
