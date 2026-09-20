package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.oss.blob.BlobObjectId;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.Connection;
import java.util.List;

/**
 * Allocates globally unique {@code blob_addr} sequence values for v5 externalized columns.
 *
 * <p>Uses the {@code columnar_file_id_info} table as a CAS-backed batched counter, similar
 * to a group sequence: each CN pre-fetches a batch of {@code step} IDs from MetaDB (default
 * 10000), then dispenses them locally without further MetaDB access until the batch is
 * exhausted.
 *
 * <p>Singleton with a fixed type key {@code __POLARDBX_BLOB_ADDR__} — all schemas and
 * tables share the same global sequence so that every Blob Page ID is universally unique.
 *
 * <p>The address format reserves 41 bits for the global Page ID. Exhaustion is failed closed
 * before an invalid slot address can be published.
 */
public class BlobFileIdAllocator {

    private static final Logger LOGGER = LoggerFactory.getLogger("EXT_COLUMN");

    /**
     * Single global type key used in {@code columnar_file_id_info}.
     */
    private static final String GLOBAL_TYPE = "__POLARDBX_BLOB_ADDR__";

    /**
     * Number of sequence values fetched per MetaDB roundtrip.
     */
    private static final int DEFAULT_STEP = 10000;

    private static final int MAX_RETRY = 5;

    private static final long ALLOCATION_LIMIT_EXCLUSIVE = BlobObjectId.MAX_BLOB_PAGE_ID + 1;

    private static final BlobFileIdAllocator INSTANCE = new BlobFileIdAllocator();

    public static BlobFileIdAllocator getInstance() {
        return INSTANCE;
    }

    private long currentId;
    private long maxId;

    private BlobFileIdAllocator() {
        this.currentId = 0;
        this.maxId = 0;
    }

    /**
     * Allocate the next globally unique sequence value.
     *
     * @return sequence value in {@code [1, 2^41)}
     */
    public synchronized long allocate() {
        if (currentId >= maxId) {
            fetchBatch();
        }
        long allocated = currentId++;
        if (allocated <= 0 || allocated > BlobObjectId.MAX_BLOB_PAGE_ID) {
            throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE,
                "Blob Page ID space exhausted: " + allocated);
        }
        return allocated;
    }

    private void fetchBatch() {
        for (int retry = 0; retry < MAX_RETRY; retry++) {
            try (Connection connection = MetaDbUtil.getConnection()) {
                ColumnarFileIdInfoAccessor accessor = new ColumnarFileIdInfoAccessor();
                accessor.setConnection(connection);

                List<ColumnarFileIdInfoRecord> records = accessor.query(GLOBAL_TYPE);

                if (records == null || records.isEmpty()) {
                    // Only the CN that wins INSERT IGNORE owns the initial half-open range.
                    // A concurrent loser must re-query and reserve its own range through CAS.
                    if (insertGlobalRecord(connection) > 0) {
                        this.currentId = 1;
                        this.maxId = DEFAULT_STEP;
                        return;
                    }
                    continue;
                }

                ColumnarFileIdInfoRecord record = records.get(0);
                long oldMaxId = record.getMaxId();
                if (oldMaxId >= ALLOCATION_LIMIT_EXCLUSIVE) {
                    throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE,
                        "Blob Page ID space exhausted: " + oldMaxId);
                }
                long newMaxId = Math.min(ALLOCATION_LIMIT_EXCLUSIVE,
                    oldMaxId + Math.max(1L, record.getStep()));

                int updated = accessor.update(
                    record.getId(), newMaxId, oldMaxId, record.getVersion(), GLOBAL_TYPE
                );

                if (updated > 0) {
                    this.currentId = Math.max(oldMaxId, 1); // skip 0
                    this.maxId = newMaxId;
                    return;
                }
                LOGGER.warn("BlobFileIdAllocator CAS failed for type=" + GLOBAL_TYPE + ", retry=" + retry);
            } catch (Exception e) {
                throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE, e,
                    "allocate blob addr", "columnar_file_id_info", e.getMessage());
            }
        }
        throw new TddlRuntimeException(ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE,
            "Failed to allocate blob addr after " + MAX_RETRY + " retries");
    }

    private int insertGlobalRecord(Connection connection) throws Exception {
        // Direct INSERT IGNORE bypassing the (schema, tableName) -> type composition
        // assumption in the legacy accessor.insert.
        String sql = "insert ignore into `columnar_file_id_info` (`type`, `max_id`, `step`, `version`) "
            + "values (?, ?, ?, ?)";
        try (java.sql.PreparedStatement ps = connection.prepareStatement(sql)) {
            ps.setString(1, GLOBAL_TYPE);
            ps.setLong(2, (long) DEFAULT_STEP);
            ps.setInt(3, DEFAULT_STEP);
            ps.setLong(4, 0L);
            return ps.executeUpdate();
        }
    }
}
