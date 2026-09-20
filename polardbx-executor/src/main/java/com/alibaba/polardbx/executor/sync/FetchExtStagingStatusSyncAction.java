package com.alibaba.polardbx.executor.sync;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.TddlNode;
import com.alibaba.polardbx.executor.columnar.ExtStagingDnRouter;
import com.alibaba.polardbx.executor.columnar.StagingTableManager;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;

/**
 * Per-CN snapshot of staging runtime state.
 * Used by INFORMATION_SCHEMA.EXT_STAGING_STATUS virtual view.
 */
public class FetchExtStagingStatusSyncAction implements ISyncAction {

    public FetchExtStagingStatusSyncAction() {
    }

    @Override
    public ResultCursor sync() {
        ArrayResultCursor result = new ArrayResultCursor("EXT_STAGING_STATUS");
        result.addColumn("COMPUTE_NODE", DataTypes.StringType);
        result.addColumn("ACTIVE_SEQ_ID", DataTypes.LongType);
        result.addColumn("ACTIVE_DN_ID", DataTypes.StringType);
        result.addColumn("ACTIVE_BY_DN", DataTypes.StringType);
        result.addColumn("FLUSHED_WATERMARK", DataTypes.LongType);
        result.addColumn("ACTIVE_TABLE_COUNT", DataTypes.LongType);
        result.addColumn("CANDIDATE_DNS", DataTypes.StringType);
        result.addColumn("DRAINING_DNS", DataTypes.StringType);
        result.addColumn("SEQ_DN_CACHE", DataTypes.StringType);
        result.addColumn("WRITERS_IN_FLIGHT", DataTypes.StringType);
        result.addColumn("TRANSACTION_LEASES", DataTypes.StringType);
        result.addColumn("LOCAL_ROW_COUNTS", DataTypes.StringType);

        StagingTableManager mgr = StagingTableManager.getInstance();
        int activeSeq = mgr.getActiveSeqId();
        int watermark = mgr.flushedWatermark();

        result.addRow(new Object[] {
            TddlNode.getHost() + ":" + TddlNode.getPort(),
            (long) activeSeq,
            mgr.getActiveDnId(),
            JSON.toJSONString(mgr.getActiveSeqByDnSnapshot()),
            (long) watermark,
            (long) mgr.getActiveStagingTableCount(),
            JSON.toJSONString(ExtStagingDnRouter.getInstance().listCandidates()),
            JSON.toJSONString(ExtStagingDnRouter.getInstance().getDrainingDnSet()),
            JSON.toJSONString(mgr.getSeqDnCacheSnapshot()),
            JSON.toJSONString(mgr.getWritersInFlightSnapshot()),
            JSON.toJSONString(mgr.getTransactionLeasesSnapshot()),
            JSON.toJSONString(mgr.getLocalRowCountsSnapshot())
        });

        return result;
    }
}
