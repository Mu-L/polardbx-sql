package com.alibaba.polardbx.optimizer.core.rel.dml;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * Routed INSERT parameters.
 *
 * <p>The route map is computed from the original logical values. A write hook may then replace or append concrete
 * parameters before the original Writer snapshots them into physical plans.
 *
 * <p>Example: batch rows {@code (1, 'a')} and {@code (200000, 'b')} may route to row-index entries
 * {@code 0 -&gt; g0/t_0000} and {@code 1 -&gt; g1/t_0007}. Externalization replaces only the body parameters; the deferred
 * Writer reuses those shard results, after which a replication Writer can still expand the primary plans normally.
 */
public final class RoutedInsertInput implements RoutedWriteInput {
    private final Map<Integer, PhysicalRoute> routeByRowIndex;

    public RoutedInsertInput(Map<Integer, PhysicalRoute> routeByRowIndex) {
        this.routeByRowIndex = Collections.unmodifiableMap(new HashMap<>(routeByRowIndex));
    }

    @Override
    public Map<Integer, PhysicalRoute> getRouteByRowIndex() {
        return routeByRowIndex;
    }

}
