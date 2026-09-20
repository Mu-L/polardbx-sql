package com.alibaba.polardbx.executor.columnar;

import com.alibaba.polardbx.common.model.Group;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.logger.LoggerFactory;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Random DN router for staging tables.
 *
 * <p>Candidate DNs are sourced from the {@code __cdc__} topology — exactly the
 * same data source as {@link ExtStagingDnConnector#getDataSource(String)}.
 * That guarantees every router-picked DN can be connected to via the cdc pool.
 *
 * <p>Drain: when a DN is being removed from the cluster, callers add its dnId
 * via {@link #markDraining(Set)}; subsequent picks skip drained DNs.
 */
public class ExtStagingDnRouter {

    private static final Logger LOGGER = LoggerFactory.getLogger("EXT_COLUMN");

    private static final ExtStagingDnRouter INSTANCE = new ExtStagingDnRouter();

    private volatile Set<String> drainingDnSet = Collections.emptySet();

    private ExtStagingDnRouter() {
    }

    public static ExtStagingDnRouter getInstance() {
        return INSTANCE;
    }

    /**
     * Pick a random DN for a new staging seq. Returns {@code null} only when
     * no candidate DNs are available (e.g. cdc topology not ready or all DNs
     * marked draining).
     */
    public String pickDn() {
        List<String> list = buildCandidates();
        if (list.isEmpty()) {
            return null;
        }
        return list.get(ThreadLocalRandom.current().nextInt(list.size()));
    }

    /**
     * Snapshot of current candidate DN list (draining-filtered).
     * Useful for diagnostic / cluster status views.
     */
    public List<String> listCandidates() {
        return buildCandidates();
    }

    /**
     * Mark a set of DNs as draining. New seqs will not land on them. Calling
     * this with the same set is idempotent.
     */
    public synchronized void markDraining(Set<String> dnIds) {
        if (dnIds == null || dnIds.isEmpty()) {
            return;
        }
        Set<String> merged = new HashSet<>(drainingDnSet);
        merged.addAll(dnIds);
        this.drainingDnSet = Collections.unmodifiableSet(merged);
        LOGGER.warn("EXT_STAGING_DRAIN: markDraining=" + this.drainingDnSet);
    }

    public synchronized void clearDraining() {
        this.drainingDnSet = Collections.emptySet();
    }

    public synchronized void clearDraining(Set<String> dnIds) {
        if (dnIds == null || dnIds.isEmpty() || drainingDnSet.isEmpty()) {
            return;
        }
        Set<String> remaining = new HashSet<>(drainingDnSet);
        remaining.removeAll(dnIds);
        this.drainingDnSet = remaining.isEmpty()
            ? Collections.emptySet()
            : Collections.unmodifiableSet(remaining);
    }

    public Set<String> getDrainingDnSet() {
        return drainingDnSet;
    }

    private List<String> buildCandidates() {
        List<String> list = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        ExecutorContext ec = ExecutorContext.getContext(SystemDbHelper.CDC_DB_NAME);
        if (ec == null) {
            return list;
        }
        TopologyHandler topo = ec.getTopologyHandler();
        if (topo == null || topo.getMatrix() == null) {
            return list;
        }
        Set<String> draining = drainingDnSet;
        for (Group g : topo.getMatrix().getGroups()) {
            IGroupExecutor ge = topo.get(g.getName());
            if (ge == null || !(ge.getDataSource() instanceof TGroupDataSource)) {
                continue;
            }
            TGroupDataSource ds = (TGroupDataSource) ge.getDataSource();
            String dnId = DdlHelper.getDnId(ds);
            if (dnId == null || dnId.isEmpty()) {
                continue;
            }
            if (draining.contains(dnId)) {
                continue;
            }
            if (seen.add(dnId)) {
                list.add(dnId);
            }
        }
        return list;
    }
}
