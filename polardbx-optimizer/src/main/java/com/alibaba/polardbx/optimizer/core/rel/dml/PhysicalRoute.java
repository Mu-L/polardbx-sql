package com.alibaba.polardbx.optimizer.core.rel.dml;

import java.util.Locale;
import java.util.Objects;

/**
 * Primary physical branch selected for one logical DML row.
 *
 * <p>The route identifies the business owner before physical plans are built. External write hooks use it to bind
 * generated values to the same schema, group and physical table that will own the primary business write.
 * For example, {@code db1.g0.t_0003} means the staging row and primary DML must share the connection branch for
 * {@code g0/t_0003}; a scale-out target replica is deliberately not another owner.
 */
public final class PhysicalRoute {
    private final String schemaName;
    private final String groupName;
    private final String physicalTableName;

    public PhysicalRoute(String schemaName, String groupName, String physicalTableName) {
        this.schemaName = Objects.requireNonNull(schemaName, "route schema is null");
        this.groupName = Objects.requireNonNull(groupName, "route group is null");
        this.physicalTableName = Objects.requireNonNull(physicalTableName, "route physical table is null");
    }

    public String getSchemaName() {
        return schemaName;
    }

    public String getGroupName() {
        return groupName;
    }

    public String getPhysicalTableName() {
        return physicalTableName;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof PhysicalRoute)) {
            return false;
        }
        PhysicalRoute other = (PhysicalRoute) obj;
        return schemaName.equalsIgnoreCase(other.schemaName)
            && groupName.equalsIgnoreCase(other.groupName)
            && physicalTableName.equalsIgnoreCase(other.physicalTableName);
    }

    @Override
    public int hashCode() {
        return Objects.hash(schemaName.toLowerCase(Locale.ROOT), groupName.toLowerCase(Locale.ROOT),
            physicalTableName.toLowerCase(Locale.ROOT));
    }

    @Override
    public String toString() {
        return schemaName + "." + groupName + "." + physicalTableName;
    }
}
