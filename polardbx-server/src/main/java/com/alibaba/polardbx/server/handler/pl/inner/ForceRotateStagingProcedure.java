package com.alibaba.polardbx.server.handler.pl.inner;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.executor.columnar.StagingTableManager;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.server.ServerConnection;

import java.util.Map;

/**
 * Force rotate the current CN's active staging seq, then synchronously flush
 * the sealed seq to OSS before returning.
 *
 * <p>Usage: {@code CALL polardbx.force_rotate_staging()}
 *
 * <p>This is the preferred alternative to SET GLOBAL EXT_STAGING_FORCE_ROTATE
 * for testing, because it guarantees the flush path (including renewLease)
 * has completed by the time control returns to the caller.
 */
public class ForceRotateStagingProcedure extends BaseInnerProcedure {

    @Override
    void execute(ServerConnection c, SQLCallStatement statement, ArrayResultCursor cursor) {
        cursor.addColumn("PHASE", DataTypes.StringType);
        cursor.addColumn("RESULT", DataTypes.StringType);
        cursor.addColumn("MESSAGE", DataTypes.StringType);

        StagingTableManager mgr = StagingTableManager.getInstance();
        Map<String, Integer> oldSeqByDn = mgr.getActiveSeqByDnSnapshot();

        if (oldSeqByDn.isEmpty()) {
            cursor.addRow(new Object[] {"DONE", "OK", "Nothing to flush"});
            return;
        }

        try {
            int uploaded = mgr.forceRotateAndFlush();
            // One CN owns one ACTIVE seq per DN. Keep ROTATE as a single phase row for compatibility,
            // but report the complete pre-rotation snapshot instead of the legacy maximum seqId.
            cursor.addRow(new Object[] {"ROTATE", "OK", "Sealed seqByDn=" + oldSeqByDn});
            cursor.addRow(new Object[] {"FLUSH", "OK", "Uploaded " + uploaded + " rows to OSS"});
            cursor.addRow(new Object[] {"DONE", "OK", "Force rotate + flush completed"});
        } catch (Exception e) {
            throw new RuntimeException("force_rotate_staging failed", e);
        }
    }
}
