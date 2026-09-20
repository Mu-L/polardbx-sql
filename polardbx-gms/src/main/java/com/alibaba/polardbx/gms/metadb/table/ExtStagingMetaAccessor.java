package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Accessor for the ext_staging_meta table in MetaDB.
 *
 * <p>v1: seq → DN mapping is recorded so any CN can locate the physical staging
 * table for any given seq. Mapping is immutable per seq.
 */
public class ExtStagingMetaAccessor extends AbstractAccessor {

    private static final String TABLE_NAME = "`ext_staging_meta`";

    public static final String DEFAULT_PHY_DB = "__polarx_ext_staging";

    // --- Queries ---

    private static final String QUERY_BY_OWNER_AND_STATUS =
        "SELECT * FROM " + TABLE_NAME + " WHERE `owner_cn` = ? AND `status` = ?";

    private static final String QUERY_BY_SEQ =
        "SELECT * FROM " + TABLE_NAME + " WHERE `seq_id` = ?";

    private static final String QUERY_MIN_SEQ_AND_COUNT =
        "SELECT MIN(`seq_id`) AS min_seq, COUNT(*) AS cnt FROM " + TABLE_NAME;

    private static final String QUERY_SEALED_FOR_FLUSH =
        "SELECT * FROM " + TABLE_NAME + " WHERE `owner_cn` = ? AND `status` = 'SEALED' ORDER BY `seq_id` LIMIT 1";

    private static final String QUERY_SEALED_SNAPSHOT =
        "SELECT * FROM " + TABLE_NAME + " WHERE `owner_cn` = ? AND `status` = 'SEALED' ORDER BY `seq_id`";

    private static final String QUERY_ORPHANS =
        "SELECT * FROM " + TABLE_NAME
            + " WHERE `owner_cn` NOT IN (%s) AND `status` IN ('ACTIVE','DRAINING','SEALED')";

    private static final String QUERY_ALL_SEQ_DN =
        "SELECT `seq_id`, `dn_id` FROM " + TABLE_NAME;

    private static final String QUERY_ALL_SEQS =
        "SELECT `seq_id` FROM " + TABLE_NAME;

    /**
     * Count rows still alive (CREATING/ACTIVE/DRAINING/SEALED/FLUSHING/FAILED) on the given DNs.
     * Used by drain wait task — `%s` is filled with a quoted IN list.
     */
    private static final String COUNT_ALIVE_ON_DNS =
        "SELECT COUNT(*) FROM " + TABLE_NAME
            + " WHERE `dn_id` IN (%s)"
            + " AND `status` IN ('CREATING','ACTIVE','DRAINING','SEALED','FLUSHING','FAILED')";

    // --- Mutations ---

    /**
     * Insert binds (owner_cn, status, dn_id, phy_db) and returns the AUTO_INCREMENT seq_id.
     */
    private static final String INSERT_RECORD =
        "INSERT INTO " + TABLE_NAME + " (`owner_cn`, `status`, `dn_id`, `phy_db`) VALUES (?, ?, ?, ?)";

    private static final String SET_ROW_COUNT =
        "UPDATE " + TABLE_NAME
            + " SET `row_count` = ? WHERE `seq_id` = ? AND `status` IN ('ACTIVE','DRAINING')";

    private static final String DRAIN_WITH_ROW_COUNT =
        "UPDATE " + TABLE_NAME
            + " SET `status` = 'DRAINING', `row_count` = ? WHERE `seq_id` = ? AND `status` = 'ACTIVE'";

    private static final String SEAL_WITH_ROW_COUNT =
        "UPDATE " + TABLE_NAME
            + " SET `status` = 'SEALED', `row_count` = ? WHERE `seq_id` = ? AND `status` = 'DRAINING'";

    private static final String DELETE_RECORD =
        "DELETE FROM " + TABLE_NAME + " WHERE `seq_id` = ?";

    private static final String CLAIM_FOR_FLUSH =
        "UPDATE " + TABLE_NAME
            + " SET `status` = 'FLUSHING', `gmt_modified` = NOW()"
            + " WHERE `seq_id` = ? AND `status` = 'SEALED' AND `owner_cn` = ?";

    private static final String DELETE_CLAIMED =
        "DELETE FROM " + TABLE_NAME
            + " WHERE `seq_id` = ? AND `status` = 'FLUSHING' AND `owner_cn` = ?";

    private static final String MARK_FLUSH_FAILED =
        "UPDATE " + TABLE_NAME + " SET `status` = 'FAILED', `gmt_modified` = NOW()"
            + " WHERE `seq_id` = ? AND `status` = 'FLUSHING' AND `owner_cn` = ?";

    private static final String RECLAIM_TIMED_OUT =
        "UPDATE " + TABLE_NAME
            + " SET `status` = 'SEALED', `owner_cn` = ?"
            + " WHERE `status` = 'FLUSHING'"
            + " AND `gmt_modified` < NOW() - INTERVAL ? SECOND";

    private static final String UPDATE_OWNER =
        "UPDATE " + TABLE_NAME + " SET `owner_cn` = ? WHERE `seq_id` = ?";

    private static final String RENEW_LEASE =
        "UPDATE " + TABLE_NAME + " SET `gmt_modified` = NOW()"
            + " WHERE `seq_id` = ? AND `status` = 'FLUSHING' AND `owner_cn` = ?";

    private static final String DELETE_TIMED_OUT_CREATING =
        "DELETE FROM " + TABLE_NAME + " WHERE `status` = 'CREATING'"
            + " AND `gmt_modified` < NOW() - INTERVAL ? SECOND"
            + " AND `owner_cn` NOT IN (%s)";

    // ==================== Allocate + Register (one operation) ====================

    /**
     * Allocate a new seq_id via AUTO_INCREMENT and register it for the given CN
     * with the chosen DN. Uses default phy_db {@link #DEFAULT_PHY_DB}.
     *
     * @return the newly allocated seq_id
     */
    public int allocateSeq(String ownerCn, String status, String dnId) {
        return allocateSeq(ownerCn, status, dnId, DEFAULT_PHY_DB);
    }

    /**
     * Allocate a new seq_id via AUTO_INCREMENT and register it for the given CN
     * with the specified DN and physical DB.
     *
     * @return the newly allocated seq_id
     */
    public int allocateSeq(String ownerCn, String status, String dnId, String phyDb) {
        try (PreparedStatement ps = connection.prepareStatement(INSERT_RECORD, Statement.RETURN_GENERATED_KEYS)) {
            ps.setString(1, ownerCn);
            ps.setString(2, status);
            ps.setString(3, dnId == null ? "" : dnId);
            ps.setString(4, phyDb == null ? DEFAULT_PHY_DB : phyDb);
            ps.executeUpdate();
            try (ResultSet rs = ps.getGeneratedKeys()) {
                if (rs.next()) {
                    return rs.getInt(1);
                }
            }
            throw new RuntimeException("Failed to get generated seq_id");
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    // ==================== Queries ====================

    /**
     * Query records for a specific CN with given status.
     */
    public List<ExtStagingMetaRecord> queryByOwnerAndStatus(String ownerCn, String status) {
        Map<Integer, ParameterContext> params = MetaDbUtil.buildParameters(
            ParameterMethod.setObject1, new Object[] {ownerCn, status});
        try {
            return MetaDbUtil.query(QUERY_BY_OWNER_AND_STATUS, params, ExtStagingMetaRecord.class, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    public ExtStagingMetaRecord queryBySeq(int seqId) {
        Map<Integer, ParameterContext> params = MetaDbUtil.buildParameters(
            ParameterMethod.setObject1, new Object[] {seqId});
        try {
            List<ExtStagingMetaRecord> records =
                MetaDbUtil.query(QUERY_BY_SEQ, params, ExtStagingMetaRecord.class, connection);
            return records == null || records.isEmpty() ? null : records.get(0);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Query minimum seq_id and total row count in one round-trip.
     * Returns [minSeq, count]. minSeq is null if table is empty.
     */
    public long[] queryMinSeqAndCount() {
        try (Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(QUERY_MIN_SEQ_AND_COUNT)) {
            if (rs.next()) {
                int minSeq = rs.getInt("min_seq");
                long count = rs.getLong("cnt");
                if (rs.wasNull()) {
                    return new long[] {0, 0};
                }
                return new long[] {minSeq, count};
            }
            return new long[] {0, 0};
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Find one SEALED record owned by this CN, eligible for flush.
     */
    public ExtStagingMetaRecord querySealedForFlush(String ownerCn) {
        Map<Integer, ParameterContext> params = MetaDbUtil.buildParameters(
            ParameterMethod.setObject1, new Object[] {ownerCn});
        try {
            List<ExtStagingMetaRecord> list = MetaDbUtil.query(
                QUERY_SEALED_FOR_FLUSH, params, ExtStagingMetaRecord.class, connection);
            return (list != null && !list.isEmpty()) ? list.get(0) : null;
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Snapshot all currently SEALED records owned by one CN in seq order.
     */
    public List<ExtStagingMetaRecord> querySealedSnapshot(String ownerCn) {
        Map<Integer, ParameterContext> params = MetaDbUtil.buildParameters(
            ParameterMethod.setObject1, new Object[] {ownerCn});
        try {
            return MetaDbUtil.query(QUERY_SEALED_SNAPSHOT, params, ExtStagingMetaRecord.class, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Find orphan seqs (owner CN not in alive list).
     */
    public List<ExtStagingMetaRecord> queryOrphans(List<String> aliveCnIds) {
        if (aliveCnIds == null || aliveCnIds.isEmpty()) {
            return java.util.Collections.emptyList();
        }
        String sql = String.format(QUERY_ORPHANS, buildQuotedInList(aliveCnIds));
        try {
            return MetaDbUtil.query(sql, ExtStagingMetaRecord.class, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Load all seq → dn mappings in one shot (for full cache refresh).
     * ext_staging_meta is small (typically < 20 rows), so this is cheap.
     */
    public Map<Integer, String> queryAllSeqDnMappings() {
        Map<Integer, String> map = new java.util.HashMap<>();
        try (Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(QUERY_ALL_SEQ_DN)) {
            while (rs.next()) {
                int seq = rs.getInt("seq_id");
                String dn = rs.getString("dn_id");
                if (dn != null && !dn.isEmpty()) {
                    map.put(seq, dn);
                }
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
        return map;
    }

    /**
     * Count alive (CREATING/ACTIVE/DRAINING/SEALED/FLUSHING/FAILED) records on the given DN list.
     * Used by drain wait task to confirm staging is fully drained.
     */
    public long countAliveOnDns(Collection<String> dnIds) {
        if (dnIds == null || dnIds.isEmpty()) {
            return 0L;
        }
        String sql = String.format(COUNT_ALIVE_ON_DNS, buildQuotedInList(dnIds));
        try (Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(sql)) {
            if (rs.next()) {
                return rs.getLong(1);
            }
            return 0L;
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Query ACTIVE seqs on the given DNs — used by force takeover during drain.
     */
    public List<ExtStagingMetaRecord> queryActiveOnDns(Collection<String> dnIds) {
        if (dnIds == null || dnIds.isEmpty()) {
            return new ArrayList<>();
        }
        String sql = "SELECT * FROM " + TABLE_NAME
            + " WHERE `dn_id` IN (" + buildQuotedInList(dnIds) + ") AND `status` = 'ACTIVE'";
        try {
            return MetaDbUtil.query(sql, ExtStagingMetaRecord.class, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * List every seq_id that still has a meta record. Orphan cleanup must use
     * the global set because multiple logical dnIds may resolve to one physical DN.
     */
    public List<Integer> queryAllSeqs() {
        List<Integer> result = new ArrayList<>();
        try (Statement stmt = connection.createStatement();
            ResultSet rs = stmt.executeQuery(QUERY_ALL_SEQS)) {
            while (rs.next()) {
                result.add(rs.getInt(1));
            }
            return result;
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    // ==================== Mutations ====================

    /**
     * Set row_count to an absolute value. Used when local counter is monotonic
     * (not reset between flushes).
     */
    public int setRowCount(int seqId, long count) {
        Map<Integer, ParameterContext> params = MetaDbUtil.buildParameters(
            ParameterMethod.setObject1, new Object[] {count, seqId});
        try {
            return MetaDbUtil.update(SET_ROW_COUNT, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    private static final String UPDATE_STATUS =
        "UPDATE " + TABLE_NAME + " SET `status` = ? WHERE `seq_id` = ? AND `status` = ?";

    /**
     * CAS update status (e.g., ACTIVE → SEALED, SEALED → FLUSHING).
     *
     * @return rows affected (1 = success, 0 = CAS failed)
     */
    public int casUpdateStatus(int seqId, String expectedStatus, String newStatus) {
        Map<Integer, ParameterContext> params = MetaDbUtil.buildParameters(
            ParameterMethod.setObject1, new Object[] {newStatus, seqId, expectedStatus});
        try {
            return MetaDbUtil.update(UPDATE_STATUS, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Claim a SEALED seq for this owner.
     *
     * @return rows affected (1 = claimed, 0 = state or owner changed)
     */
    public int claimForFlush(int seqId, String ownerCn) {
        Map<Integer, ParameterContext> params = MetaDbUtil.buildParameters(
            ParameterMethod.setObject1, new Object[] {seqId, ownerCn});
        try {
            return MetaDbUtil.update(CLAIM_FOR_FLUSH, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Atomically move an ACTIVE seq to DRAINING and persist its current row count.
     */
    public int drainWithRowCount(int seqId, long rowCount) {
        Map<Integer, ParameterContext> params = MetaDbUtil.buildParameters(
            ParameterMethod.setObject1, new Object[] {rowCount, seqId});
        try {
            return MetaDbUtil.update(DRAIN_WITH_ROW_COUNT, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Atomically seal a DRAINING seq after its final transaction lease is released.
     *
     * @return rows affected (1 = success, 0 = already sealed)
     */
    public int sealWithRowCount(int seqId, long rowCount) {
        Map<Integer, ParameterContext> params = MetaDbUtil.buildParameters(
            ParameterMethod.setObject1, new Object[] {rowCount, seqId});
        try {
            return MetaDbUtil.update(SEAL_WITH_ROW_COUNT, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Delete a record after successful flush + DROP TABLE.
     */
    public int delete(int seqId) {
        Map<Integer, ParameterContext> params = MetaDbUtil.buildParameters(
            ParameterMethod.setObject1, new Object[] {seqId});
        try {
            return MetaDbUtil.delete(DELETE_RECORD, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Publish a claimed seq only when the caller still owns its FLUSHING lease.
     */
    public int deleteClaimed(int seqId, String ownerCn) {
        Map<Integer, ParameterContext> params = MetaDbUtil.buildParameters(
            ParameterMethod.setObject1, new Object[] {seqId, ownerCn});
        try {
            return MetaDbUtil.delete(DELETE_CLAIMED, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Move a corrupt, non-retryable flush to a terminal state.
     */
    public int markFlushFailed(int seqId, String ownerCn) {
        Map<Integer, ParameterContext> params = MetaDbUtil.buildParameters(
            ParameterMethod.setObject1, new Object[] {seqId, ownerCn});
        try {
            return MetaDbUtil.update(MARK_FLUSH_FAILED, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Reclaim FLUSHING records that have timed out (CN crashed).
     * Sets them back to SEALED with a new owner so they can be re-flushed.
     */
    public int reclaimTimedOut(String newOwnerCn, long claimTimeoutSec) {
        Map<Integer, ParameterContext> params = MetaDbUtil.buildParameters(
            ParameterMethod.setObject1, new Object[] {newOwnerCn, claimTimeoutSec});
        try {
            return MetaDbUtil.update(RECLAIM_TIMED_OUT, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Update owner_cn for a specific seq. Used when adopting orphan seqs from dead CNs.
     */
    public int updateOwner(int seqId, String newOwnerCn) {
        Map<Integer, ParameterContext> params = MetaDbUtil.buildParameters(
            ParameterMethod.setObject1, new Object[] {newOwnerCn, seqId});
        try {
            return MetaDbUtil.update(UPDATE_OWNER, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Renew flush lease by touching gmt_modified. Prevents reclaimTimedOut from
     * stealing a long-running but healthy flush task.
     *
     * @return 1 if renewed, 0 if seq is no longer in FLUSHING status
     */
    public int renewLease(int seqId, String ownerCn) {
        Map<Integer, ParameterContext> params = MetaDbUtil.buildParameters(
            ParameterMethod.setObject1, new Object[] {seqId, ownerCn});
        try {
            return MetaDbUtil.update(RENEW_LEASE, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Delete CREATING records stuck longer than the given timeout. These are
     * leftovers from init/rotate that allocated the seq but failed (and failed
     * to roll back) before the physical DB+table were confirmed created — there
     * is no data to lose. Acts as a cross-CN safety net.
     *
     * @return rows deleted
     */
    public int deleteTimedOutCreating(long timeoutSec, Collection<String> aliveCnIds) {
        if (aliveCnIds == null || aliveCnIds.isEmpty()) {
            return 0;
        }
        String sql = String.format(DELETE_TIMED_OUT_CREATING, buildQuotedInList(aliveCnIds));
        Map<Integer, ParameterContext> params = MetaDbUtil.buildParameters(
            ParameterMethod.setObject1, new Object[] {timeoutSec});
        try {
            return MetaDbUtil.delete(sql, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    // ==================== Helpers ====================

    private static String buildQuotedInList(Collection<String> values) {
        StringBuilder sb = new StringBuilder();
        Iterator<String> it = values.iterator();
        boolean first = true;
        while (it.hasNext()) {
            String v = it.next();
            if (!first) {
                sb.append(",");
            }
            sb.append("'").append(v == null ? "" : v.replace("'", "''")).append("'");
            first = false;
        }
        return sb.toString();
    }
}
