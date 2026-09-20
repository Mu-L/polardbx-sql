package com.alibaba.polardbx.optimizer.core.rel.dml;

import java.util.Map;

/**
 * A DML input whose primary physical owner routes are fixed before its physical plans are built.
 *
 * <p>For two logical rows routed to {@code g0/t_0000} and {@code g1/t_0007}, the map keeps those row-index owners
 * authoritative while the statement context replaces plaintext with route-owned BlobRefs. The original Writer then
 * resumes its own physical-plan builder with the already selected route.
 */
public interface RoutedWriteInput {

    Map<Integer, PhysicalRoute> getRouteByRowIndex();
}
