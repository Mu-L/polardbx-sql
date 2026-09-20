package com.alibaba.polardbx.executor.columnar;

import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.gms.metadb.table.ExtStagingMetaAccessor;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Helper that hands out connections to the physical staging library
 * (`__polarx_ext_staging`) on a target DN.
 *
 * <p>v1 design — <b>do NOT build a private connection pool</b>. We reuse the
 * topology connection pool of the {@code __cdc__} schema; that pool already
 * covers every DN in the cluster.
 *
 * <p><b>Hard rule</b>: never call {@code setCatalog}/{@code USE db} on the
 * borrowed connection. After the connection returns to the pool, CDC code
 * relies on it being on the original physical DB. All staging SQL must use
 * the fully-qualified form
 * {@code `__polarx_ext_staging`.`polarx_ext_staging_<seqId>`}.
 */
public class ExtStagingDnConnector {

    private static final Logger LOGGER = LoggerFactory.getLogger("EXT_COLUMN");

    public static final String STAGING_PHY_DB = ExtStagingMetaAccessor.DEFAULT_PHY_DB;

    private static final ExtStagingDnConnector INSTANCE = new ExtStagingDnConnector();

    /**
     * dnId → groupName(in __cdc__ schema). Refreshed lazily on miss.
     */
    private volatile Map<String, String> dnGroupMap = Collections.emptyMap();

    /**
     * Set of dnIds where the physical staging DB has been ensured this lifetime.
     */
    private final Set<String> dbCreated = ConcurrentHashMap.newKeySet();

    private ExtStagingDnConnector() {
    }

    public static ExtStagingDnConnector getInstance() {
        return INSTANCE;
    }

    /**
     * Borrow a pooled connection to the given DN by reusing {@code __cdc__}
     * schema's group data source.
     *
     * <p>The returned connection's "current database" remains whatever
     * {@code __cdc__} configured — callers MUST NOT switch it. Use fully
     * qualified table names in every statement.
     */
    public Connection getConnection(String dnId) throws SQLException {
        TGroupDataSource ds = getDataSource(dnId);
        if (ds == null) {
            throw new SQLException("ExtStagingDnConnector: no __cdc__ group found for dnId=" + dnId);
        }
        return ds.getConnection();
    }

    public TGroupDataSource getDataSource(String dnId) {
        if (dnId == null || dnId.isEmpty()) {
            return null;
        }
        Map<String, String> map = dnGroupMap;
        String groupName = map.get(dnId);
        if (groupName == null) {
            map = refreshDnGroupMap();
            groupName = map.get(dnId);
            if (groupName == null) {
                return null;
            }
        }
        try {
            return DdlHelper.getPhyDataSource(SystemDbHelper.CDC_DB_NAME, groupName);
        } catch (Throwable e) {
            // Group might have been removed between map refresh and lookup —
            // wipe map and let the next call try again.
            dnGroupMap = Collections.emptyMap();
            throw new RuntimeException("ExtStagingDnConnector.getDataSource failed: dnId=" + dnId
                + ", group=" + groupName, e);
        }
    }

    /**
     * Lazily ensure the physical staging DB exists on the given DN.
     * Idempotent — first caller pays one DDL roundtrip; subsequent calls
     * are pure local hashset checks.
     */
    public void ensurePhysicalDatabase(String dnId) throws SQLException {
        if (dbCreated.contains(dnId)) {
            return;
        }
        synchronized (dbCreated) {
            if (dbCreated.contains(dnId)) {
                return;
            }
            try (Connection conn = getConnection(dnId);
                Statement stmt = conn.createStatement()) {
                // Check first to avoid the overhead of CREATE DATABASE IF NOT EXISTS
                try (ResultSet rs = stmt.executeQuery(
                    "SELECT 1 FROM information_schema.SCHEMATA WHERE SCHEMA_NAME = '" + STAGING_PHY_DB + "'")) {
                    if (!rs.next()) {
                        stmt.executeUpdate(
                            "CREATE DATABASE IF NOT EXISTS `" + STAGING_PHY_DB + "` DEFAULT CHARSET utf8mb4");
                    }
                }
            }
            dbCreated.add(dnId);
            LOGGER.warn("EXT_STAGING: ensured physical DB on dn=" + dnId);
        }
    }

    /**
     * Force a refresh of the dnId → groupName map from __cdc__ topology.
     * Called on miss and on draining.
     */
    public Map<String, String> refreshDnGroupMap() {
        Map<String, String> map = buildDnGroupMap();
        dnGroupMap = map;
        return map;
    }

    /**
     * Snapshot current map (read-only). Callers should treat it as immutable.
     */
    public Map<String, String> getDnGroupMapSnapshot() {
        Map<String, String> map = dnGroupMap;
        if (map.isEmpty()) {
            map = refreshDnGroupMap();
        }
        return map;
    }

    private Map<String, String> buildDnGroupMap() {
        Map<String, String> map = new HashMap<>();
        ExecutorContext ec = ExecutorContext.getContext(SystemDbHelper.CDC_DB_NAME);
        if (ec == null) {
            return map;
        }
        TopologyHandler topo = ec.getTopologyHandler();
        if (topo == null || topo.getMatrix() == null) {
            return map;
        }
        for (Group g : topo.getMatrix().getGroups()) {
            String groupName = g.getName();
            IGroupExecutor ge = topo.get(groupName);
            if (ge == null || !(ge.getDataSource() instanceof TGroupDataSource)) {
                continue;
            }
            TGroupDataSource ds = (TGroupDataSource) ge.getDataSource();
            String dnId = DdlHelper.getDnId(ds);
            if (dnId == null || dnId.isEmpty()) {
                continue;
            }
            // First write wins — the cdc schema usually has only one group per DN,
            // but if not, any group on the same DN works equally.
            map.putIfAbsent(dnId, groupName);
        }
        return map;
    }
}
