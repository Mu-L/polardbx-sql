package com.alibaba.polardbx.server.response;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.utils.StorageNodeHelper;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Sync action to clear index usage statistics on all DN nodes
 * Executes: TRUNCATE TABLE performance_schema.table_io_waits_summary_by_index_usage
 */
public class ClearIndexUsageSyncAction implements ISyncAction {

    private static final Logger logger = LoggerFactory.getLogger(ClearIndexUsageSyncAction.class);

    private String db;

    public ClearIndexUsageSyncAction() {
    }

    public ClearIndexUsageSyncAction(String db) {
        this.db = db;
    }

    public String getDb() {
        return db;
    }

    public void setDb(String db) {
        this.db = db;
    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor result = new ArrayResultCursor("CLEAR_INDEX_USAGE");
        result.addColumn("DN_NODE", DataTypes.StringType);
        result.addColumn("STATUS", DataTypes.StringType);
        result.initMeta();

        try {
            List<StorageNodeHelper.StorageNode> storageNodes = StorageNodeHelper.getUniqueStorageNodes(db);

            if (storageNodes.isEmpty()) {
                logger.warn("No DN nodes found for schema: " + db);
                result.addRow(new Object[] {"N/A", "No DN nodes found"});
                return result;
            }

            Map<String, String> nodeResults = new LinkedHashMap<>();
            int successCount = 0;
            int failCount = 0;

            for (StorageNodeHelper.StorageNode node : storageNodes) {
                String address = node.getAddress();
                IDataSource dataSource = node.getDataSource();

                try (IConnection conn = dataSource.getConnection();
                    Statement stmt = conn.createStatement()) {

                    stmt.execute("TRUNCATE TABLE performance_schema.table_io_waits_summary_by_index_usage");
                    nodeResults.put(address, "SUCCESS");
                    successCount++;
                } catch (Exception e) {
                    String errorMsg = e.getMessage();
                    if (errorMsg != null && errorMsg.length() > 100) {
                        errorMsg = errorMsg.substring(0, 100) + "...";
                    }
                    nodeResults.put(address, "FAILED: " + errorMsg);
                    failCount++;
                    logger.error("Failed to clear index usage on DN: " + address, e);
                }
            }

            logger.info("CLEAR INDEX_USAGE completed: " + successCount + "/" + storageNodes.size() + " succeeded");

            result.addRow(new Object[] {
                "Summary",
                String.format("Cleared %d/%d DN nodes (Success: %d, Failed: %d)",
                    successCount, storageNodes.size(), successCount, failCount)
            });

            for (Map.Entry<String, String> entry : nodeResults.entrySet()) {
                result.addRow(new Object[] {entry.getKey(), entry.getValue()});
            }

        } catch (Exception e) {
            logger.error("Failed to clear index usage statistics", e);
            throw new TddlRuntimeException(ErrorCode.ERR_EXECUTE_ON_MYSQL, e,
                "Failed to clear index usage statistics");
        }

        return result;
    }
}
