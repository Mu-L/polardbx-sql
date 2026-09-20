package com.alibaba.polardbx.server.handler.pl.inner;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.ClearAllDnCclSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.node.LeaderStatusBridge;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.ServerConnection;

import java.util.List;
import java.util.Map;

/**
 * @author liugaoji
 */
public class ClearAllDnCclProcedure extends BaseInnerProcedure {

    protected static final Logger logger = LoggerFactory.getLogger(ClearAllDnCclProcedure.class);

    @Override
    public void execute(ServerConnection c, SQLCallStatement statement, ArrayResultCursor cursor) {

        // Use sync action to clear all DN CCL rules across all CN nodes
        ClearAllDnCclSyncAction syncAction = new ClearAllDnCclSyncAction();
        List<List<Map<String, Object>>> results = SyncManagerHelper.syncIgnoreExceptions(
            syncAction, SystemDbHelper.DEFAULT_DB_NAME, SyncScope.ALL);

        boolean overallSuccess = true;
        StringBuilder messageBuilder = new StringBuilder();

        // Process results from all nodes
        for (List<Map<String, Object>> nodeRows : results) {
            if (nodeRows == null) {
                continue;
            }
            for (Map<String, Object> row : nodeRows) {
                String computeNode = (String) row.get("COMPUTE_NODE");
                String status = (String) row.get("STATUS");
                String message = (String) row.get("MESSAGE");

                if (!"SUCCESS".equals(status)) {
                    overallSuccess = false;
                }

                if (computeNode != null && message != null && !message.trim().isEmpty()) {
                    messageBuilder.append("Node ").append(computeNode).append(": ").append(message).append(" ");
                }
            }
        }

        cursor.addColumn("Status", DataTypes.VarcharType);
        if (overallSuccess) {
            cursor.addRow(new Object[] {
                "Success"
            });
        } else {
            cursor.addRow(new Object[] {
                "Fail, Please Check Log. Details: " + messageBuilder.toString()
            });
        }
    }
}