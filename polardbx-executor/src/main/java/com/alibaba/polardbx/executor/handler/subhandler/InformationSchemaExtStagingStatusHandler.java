package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.sync.FetchExtStagingStatusSyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaExtStagingStatus;
import com.alibaba.polardbx.optimizer.view.VirtualView;

import java.util.List;
import java.util.Map;

/**
 * Handler for INFORMATION_SCHEMA.EXT_STAGING_STATUS.
 * Broadcasts to all CNs and returns one row per node (no aggregation).
 */
public class InformationSchemaExtStagingStatusHandler extends BaseVirtualViewSubClassHandler {

    public InformationSchemaExtStagingStatusHandler(VirtualViewHandler virtualViewHandler) {
        super(virtualViewHandler);
    }

    @Override
    public boolean isSupport(VirtualView virtualView) {
        return virtualView instanceof InformationSchemaExtStagingStatus;
    }

    @Override
    public Cursor handle(VirtualView virtualView, ExecutionContext executionContext, ArrayResultCursor cursor) {
        List<List<Map<String, Object>>> results = SyncManagerHelper.syncIgnoreExceptions(
            new FetchExtStagingStatusSyncAction(),
            SystemDbHelper.INFO_SCHEMA_DB_NAME, SyncScope.ALL);

        for (List<Map<String, Object>> nodeRows : results) {
            if (nodeRows == null || nodeRows.isEmpty()) {
                continue;
            }
            Map<String, Object> row = nodeRows.get(0);
            cursor.addRow(new Object[] {
                row.get("COMPUTE_NODE"),
                row.get("ACTIVE_SEQ_ID"),
                row.get("ACTIVE_DN_ID"),
                row.get("ACTIVE_BY_DN"),
                row.get("FLUSHED_WATERMARK"),
                row.get("ACTIVE_TABLE_COUNT"),
                row.get("CANDIDATE_DNS"),
                row.get("DRAINING_DNS"),
                row.get("SEQ_DN_CACHE"),
                row.get("WRITERS_IN_FLIGHT"),
                row.get("TRANSACTION_LEASES"),
                row.get("LOCAL_ROW_COUNTS")
            });
        }
        return cursor;
    }
}
