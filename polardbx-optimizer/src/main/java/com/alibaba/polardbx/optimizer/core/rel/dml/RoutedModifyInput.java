package com.alibaba.polardbx.optimizer.core.rel.dml;

import org.apache.calcite.util.Pair;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableInsertSharder;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Primary UPDATE rows and their physical owner routes.
 *
 * <p>Example: {@code UPDATE t SET body=? WHERE id IN (1, 200000)} produces branch-local rows owned by two physical
 * tables. The statement context copies/materializes each row against its entry in
 * {@link #getRouteByRowIndex()}, then the existing modify Writer continues unchanged.
 */
public final class RoutedModifyInput implements RoutedWriteInput {

    private final List<List<Object>> rows;
    private final Map<Integer, PhysicalRoute> routeByRowIndex;

    public RoutedModifyInput(List<List<Object>> rows, Map<Integer, PhysicalRoute> routeByRowIndex) {
        this.rows = Objects.requireNonNull(rows, "routed modify rows are null");
        this.routeByRowIndex = Collections.unmodifiableMap(new HashMap<>(routeByRowIndex));
    }

    public List<List<Object>> getRows() {
        return rows;
    }

    @Override
    public Map<Integer, PhysicalRoute> getRouteByRowIndex() {
        return routeByRowIndex;
    }

    public static Map<Integer, PhysicalRoute> buildRoutes(
        String schemaName,
        Map<String, Map<String, List<Pair<Integer, List<Object>>>>> shardResult,
        int rowCount) {
        Map<Integer, PhysicalRoute> routes = new HashMap<>();
        for (Map.Entry<String, Map<String, List<Pair<Integer, List<Object>>>>> group : shardResult.entrySet()) {
            for (Map.Entry<String, List<Pair<Integer, List<Object>>>> table : group.getValue().entrySet()) {
                PhysicalRoute route = new PhysicalRoute(schemaName, group.getKey(), table.getKey());
                for (Pair<Integer, List<Object>> row : table.getValue()) {
                    Integer rowIndex = row.getKey();
                    if (rowIndex == null || rowIndex < 0 || rowIndex >= rowCount
                        || routes.put(rowIndex, route) != null) {
                        throw new IllegalStateException("Invalid routed UPDATE row index " + rowIndex);
                    }
                }
            }
        }
        if (routes.size() != rowCount) {
            throw new IllegalStateException("Incomplete routed UPDATE input: rows=" + rowCount
                + ", routes=" + routes.size());
        }
        return routes;
    }

    public static Map<Integer, PhysicalRoute> buildInsertRoutes(
        String schemaName, List<PhyTableInsertSharder.PhyTableShardResult> shardResults, int rowCount) {
        Map<Integer, PhysicalRoute> routes = new HashMap<>();
        for (PhyTableInsertSharder.PhyTableShardResult result : shardResults) {
            PhysicalRoute route = new PhysicalRoute(schemaName, result.getGroupName(), result.getPhyTableName());
            List<Integer> indices = result.getValueIndices();
            if (indices == null) {
                if (shardResults.size() != 1) {
                    throw new IllegalStateException("Cannot derive routed INSERT rows from multiple shards");
                }
                for (int rowIndex = 0; rowIndex < rowCount; rowIndex++) {
                    routes.put(rowIndex, route);
                }
            } else {
                for (Integer rowIndex : indices) {
                    if (rowIndex == null || rowIndex < 0 || rowIndex >= rowCount
                        || routes.put(rowIndex, route) != null) {
                        throw new IllegalStateException("Invalid routed INSERT row index " + rowIndex);
                    }
                }
            }
        }
        if (routes.size() != rowCount) {
            throw new IllegalStateException("Incomplete routed INSERT input: rows=" + rowCount
                + ", routes=" + routes.size());
        }
        return routes;
    }
}
