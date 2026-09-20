/*
 * Copyright [2013-2021], Alibaba Group Holding Limited
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.gms.metadb.cache;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.utils.GeneralUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.gms.metadb.accessor.AbstractAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;

import java.sql.SQLIntegrityConstraintViolationException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.alibaba.polardbx.gms.metadb.GmsSystemTables.CACHE_PEER;

public class CachePeerAccessor extends AbstractAccessor {
    private static final Logger LOGGER = LoggerFactory.getLogger(CachePeerAccessor.class);
    private static final String CACHE_PEER_TABLE = wrap(CACHE_PEER);

    // Clock drift tolerance in milliseconds between nodes
    private static final long CLOCK_DRIFT_MS = 500;

    // Insert a new peer node (leader defaults to 0)
    private static final String INSERT_PEER = "insert into " + CACHE_PEER_TABLE
        + " (`peer_name`, `host`, `node_hash`, `lease`, `role`, `leader`, `status_json`) values (?, ?, ?, ?, ?, 0, ?)";

    // Update lease for an existing peer
    private static final String UPDATE_LEASE = "update " + CACHE_PEER_TABLE
        + " set `lease` = ?, `host` = ?, `node_hash` = ?, `role` = ?, `status_json` = ?"
        + " where `peer_name` = ?";

    // Remove expired peers
    private static final String CLEANUP = "delete from " + CACHE_PEER_TABLE
        + " where `lease` < ?";

    // Get active peers (lease not expired)
    private static final String GET_PEERS = "select * from " + CACHE_PEER_TABLE
        + " where `lease` >= ?";

    // Get all peers
    private static final String GET_ALL = "select * from " + CACHE_PEER_TABLE;

    // Delete a peer node
    private static final String DELETE_PEER = "delete from " + CACHE_PEER_TABLE
        + " where `peer_name` = ?";

    /**
     * Refresh: update own peer info (lease, host, nodeHash, role).
     * If the peer does not exist, insert it.
     * <p>
     * NOTE: cleanup of expired peers is intentionally NOT performed here.
     * The range DELETE on `lease` acquires gap/next-key locks that, combined
     * with concurrent self-row UPDATEs from other peers, cause InnoDB
     * deadlocks (ER_LOCK_DEADLOCK). Caller should invoke {@link #cleanup(long)}
     * in a separate, short transaction instead.
     */
    public void refresh(String peerName, String host, long nodeHash, String peerRole,
                        long nowUTC, long leaseMs, String statusJson) {
        try {
            // Update myself first
            final Map<Integer, ParameterContext> params = new HashMap<>(6);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, nowUTC + leaseMs);
            MetaDbUtil.setParameter(2, params, ParameterMethod.setString, host);
            MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, nodeHash);
            MetaDbUtil.setParameter(4, params, ParameterMethod.setString, peerRole);
            MetaDbUtil.setParameter(5, params, ParameterMethod.setString, statusJson);
            MetaDbUtil.setParameter(6, params, ParameterMethod.setString, peerName);

            final int updates = MetaDbUtil.update(UPDATE_LEASE, params, connection);
            if (0 == updates) {
                // Need to insert myself
                params.clear();
                MetaDbUtil.setParameter(1, params, ParameterMethod.setString, peerName);
                MetaDbUtil.setParameter(2, params, ParameterMethod.setString, host);
                MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, nodeHash);
                MetaDbUtil.setParameter(4, params, ParameterMethod.setLong, nowUTC + leaseMs);
                MetaDbUtil.setParameter(5, params, ParameterMethod.setString, peerRole);
                MetaDbUtil.setParameter(6, params, ParameterMethod.setString, statusJson);

                try {
                    MetaDbUtil.insert(INSERT_PEER, params, connection);
                } catch (SQLIntegrityConstraintViolationException e) {
                    // Race condition: another node inserted the same peer_name, just ignore
                }
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Get all active peers whose lease has not expired.
     */
    public List<CachePeerRecord> getActivePeers(long nowUTC) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, nowUTC);

            return MetaDbUtil.query(GET_PEERS, params, CachePeerRecord.class, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Get all peer records regardless of lease status.
     */
    public List<CachePeerRecord> getAllPeers() {
        try {
            return MetaDbUtil.query(GET_ALL, CachePeerRecord.class, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Delete a peer node by name.
     */
    public boolean deletePeer(String peerName) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setString, peerName);

            final int deletes = MetaDbUtil.delete(DELETE_PEER, params, connection);
            return deletes > 0;
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Cleanup expired peers.
     *
     * @return the number of removed peers
     */
    public int cleanup(long nowUTC) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(1);
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, nowUTC);

            return MetaDbUtil.update(CLEANUP, params, connection);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    // --- Leader Election Methods ---

    // Get current leader (if exists and not expired)
    private static final String GET_LEADER = "select * from " + CACHE_PEER_TABLE
        + " where `leader` = 1 and `lease` >= ? limit 1";

    // Demote expired leaders (set leader=0 for expired peers that are still marked as leader)
    private static final String DEMOTE_EXPIRED_LEADERS = "update " + CACHE_PEER_TABLE
        + " set `leader` = 0 where `leader` = 1 and `lease` < ?";

    // Try to promote self to leader (only succeeds if no other active leader exists)
    // Uses optimistic locking: only update if leader=0 and lease is still valid
    private static final String PROMOTE_SELF = "update " + CACHE_PEER_TABLE
        + " set `leader` = 1, `lease` = ? where `peer_name` = ? and `leader` = 0 and `lease` >= ?"
        + " and not exists (select 1 from (select `peer_name` from " + CACHE_PEER_TABLE
        + " where `leader` = 1 and `lease` >= ?) as tmp)";

    // Renew leader lease (only if still leader and lease not expired)
    private static final String RENEW_LEADER = "update " + CACHE_PEER_TABLE
        + " set `lease` = ? where `peer_name` = ? and `leader` = 1 and `lease` >= ?";

    /**
     * Attempt leader election.
     * Returns true if this peer becomes/remains the leader, false otherwise.
     * <p>
     * Algorithm:
     * 1. Demote any expired leaders
     * 2. Try to get current leader
     * 3. If no leader exists, try to promote self
     * 4. If already leader, renew lease
     */
    public boolean elect(String peerName, long nowUTC, long leaseMs) {
        try {
            // Apply clock drift tolerance: treat a lease as valid even if it appears
            // expired by up to CLOCK_DRIFT_MS, preventing false leader demotion or
            // split-brain caused by clock skew between nodes.
            final long driftAdjusted = nowUTC - CLOCK_DRIFT_MS;
            final Map<Integer, ParameterContext> params = new HashMap<>(4);

            // Step 1: Demote expired leaders (tolerating clock drift)
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, driftAdjusted);
            MetaDbUtil.update(DEMOTE_EXPIRED_LEADERS, params, connection);

            // Step 2: Check if there's an active leader (tolerating clock drift)
            params.clear();
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, driftAdjusted);
            List<CachePeerRecord> leaders = MetaDbUtil.query(GET_LEADER, params, CachePeerRecord.class, connection);

            if (leaders.isEmpty()) {
                // Step 3: No leader exists, try to promote self
                params.clear();
                MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, nowUTC + leaseMs);
                MetaDbUtil.setParameter(2, params, ParameterMethod.setString, peerName);
                MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, driftAdjusted);
                MetaDbUtil.setParameter(4, params, ParameterMethod.setLong, driftAdjusted);

                int promoted = MetaDbUtil.update(PROMOTE_SELF, params, connection);
                return promoted > 0;
            } else {
                // Step 4: Leader exists, check if it's me
                CachePeerRecord leader = leaders.get(0);
                if (leader.peerName.equals(peerName)) {
                    // I'm the leader, renew my lease (tolerating clock drift)
                    params.clear();
                    MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, nowUTC + leaseMs);
                    MetaDbUtil.setParameter(2, params, ParameterMethod.setString, peerName);
                    MetaDbUtil.setParameter(3, params, ParameterMethod.setLong, driftAdjusted);

                    int renewed = MetaDbUtil.update(RENEW_LEADER, params, connection);
                    return renewed > 0;
                } else {
                    // Someone else is the leader
                    return false;
                }
            }
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }

    /**
     * Get the current leader peer (if exists and not expired).
     * Returns null if no active leader exists.
     */
    public CachePeerRecord getLeader(long nowUTC) {
        try {
            final Map<Integer, ParameterContext> params = new HashMap<>(1);
            // Tolerate clock drift when checking leader validity
            MetaDbUtil.setParameter(1, params, ParameterMethod.setLong, nowUTC - CLOCK_DRIFT_MS);

            List<CachePeerRecord> leaders = MetaDbUtil.query(GET_LEADER, params, CachePeerRecord.class, connection);
            return leaders.isEmpty() ? null : leaders.get(0);
        } catch (Exception e) {
            throw GeneralUtil.nestedException(e);
        }
    }
}
