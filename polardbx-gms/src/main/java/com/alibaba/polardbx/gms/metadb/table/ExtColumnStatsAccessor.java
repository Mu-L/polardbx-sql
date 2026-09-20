package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.List;

/**
 * Accessor for the ext_column_table_stats table in MetaDB.
 * Uses UPSERT (INSERT ... ON DUPLICATE KEY UPDATE) for delta accumulation.
 */
public class ExtColumnStatsAccessor extends AbstractAccessor {

    private static final Logger LOGGER = LoggerFactory.getLogger("EXT_COLUMN");

    private static final String TABLE_NAME = "`ext_column_table_stats`";

    private static final String UPSERT_DELTA =
        "INSERT INTO " + TABLE_NAME
            + " (`table_id`, `total_raw_bytes`, `total_compressed_bytes`, `total_blob_count`, `skipped_count`, `total_row_count`)"
            + " VALUES (?, ?, ?, ?, ?, ?)"
            + " ON DUPLICATE KEY UPDATE"
            + "   `total_raw_bytes` = `total_raw_bytes` + VALUES(`total_raw_bytes`),"
            + "   `total_compressed_bytes` = `total_compressed_bytes` + VALUES(`total_compressed_bytes`),"
            + "   `total_blob_count` = `total_blob_count` + VALUES(`total_blob_count`),"
            + "   `skipped_count` = `skipped_count` + VALUES(`skipped_count`),"
            + "   `total_row_count` = `total_row_count` + VALUES(`total_row_count`),"
            + "   `gmt_modified` = NOW()";

    private static final String UPSERT_V2_DELTA =
        "INSERT INTO " + TABLE_NAME
            + " (`table_id`, `raw_bytes_written`, `stored_payload_bytes_written`, `total_page_bytes_written`,"
            + " `page_count`, `value_count`, `stats_created_time`, `stats_updated_time`)"
            + " VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
            + " ON DUPLICATE KEY UPDATE"
            + "   `raw_bytes_written` = `raw_bytes_written` + VALUES(`raw_bytes_written`),"
            + "   `stored_payload_bytes_written` = `stored_payload_bytes_written`"
            + "     + VALUES(`stored_payload_bytes_written`),"
            + "   `total_page_bytes_written` = `total_page_bytes_written`"
            + "     + VALUES(`total_page_bytes_written`),"
            + "   `page_count` = `page_count` + VALUES(`page_count`),"
            + "   `value_count` = `value_count` + VALUES(`value_count`),"
            + "   `stats_created_time` = CASE"
            + "     WHEN `stats_created_time` IS NULL THEN VALUES(`stats_created_time`)"
            + "     ELSE LEAST(`stats_created_time`, VALUES(`stats_created_time`)) END,"
            + "   `stats_updated_time` = CASE"
            + "     WHEN `stats_updated_time` IS NULL THEN VALUES(`stats_updated_time`)"
            + "     ELSE GREATEST(`stats_updated_time`, VALUES(`stats_updated_time`)) END,"
            + "   `gmt_modified` = NOW()";

    /**
     * Upsert accumulated statistics for a table.
     */
    public void upsertDelta(long tableId, long rawBytes, long compressedBytes,
                            long blobCount, long skippedCount, long rowCount) {
        try (PreparedStatement ps = connection.prepareStatement(UPSERT_DELTA)) {
            ps.setLong(1, tableId);
            ps.setLong(2, rawBytes);
            ps.setLong(3, compressedBytes);
            ps.setLong(4, blobCount);
            ps.setLong(5, skippedCount);
            ps.setLong(6, rowCount);
            ps.executeUpdate();
        } catch (Exception e) {
            LOGGER.warn("Failed to upsert ext_column_table_stats: tableId=" + tableId, e);
        }
    }

    /**
     * Atomically add V2 Blob Page write deltas for multiple external column objects.
     *
     * <p>The caller owns the transaction. Unlike the legacy API above, failures are deliberately propagated so the
     * in-memory accumulator can roll the transaction back and retain the deltas for retry.</p>
     */
    public int[] upsertV2Deltas(List<ExtColumnPageStatsDelta> deltas) throws SQLException {
        if (deltas == null || deltas.isEmpty()) {
            return new int[0];
        }
        try (PreparedStatement ps = connection.prepareStatement(UPSERT_V2_DELTA)) {
            for (ExtColumnPageStatsDelta delta : deltas) {
                ps.setLong(1, delta.getTableId());
                ps.setLong(2, delta.getRawBytesWritten());
                ps.setLong(3, delta.getStoredPayloadBytesWritten());
                ps.setLong(4, delta.getTotalPageBytesWritten());
                ps.setLong(5, delta.getPageCount());
                ps.setLong(6, delta.getValueCount());
                ps.setTimestamp(7, new Timestamp(delta.getStatsCreatedTimeMillis()));
                ps.setTimestamp(8, new Timestamp(delta.getStatsUpdatedTimeMillis()));
                ps.addBatch();
            }
            return ps.executeBatch();
        }
    }

}
