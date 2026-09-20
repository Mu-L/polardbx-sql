package com.alibaba.polardbx.optimizer.core.rel;

import org.apache.calcite.plan.RelOptTable;

/**
 * Abstract base for external data sinks (write side).
 * <p>
 * Mirrors {@link TableSource} on the read side. Each external connector that
 * supports INSERT supplies a concrete sub-class encapsulating connector-specific
 * write logic (schema validation, serialisation, target path, etc.).
 * <p>
 * The sub-class is embedded inside {@link LogicalExternalInsert} and passed
 * down to the executor layer, where {@code LogicalExternalInsertHandler}
 * dispatches to the appropriate {@code ExternalTableInsertHandler} based on the
 * runtime type of the {@code TableSink}.
 */
public abstract class TableSink {

    /**
     * The RelOptTable that carries the meta-info (row type, table name, etc.)
     * for the external table being written.
     */
    protected final RelOptTable table;

    protected TableSink(RelOptTable table) {
        this.table = table;
    }

    public RelOptTable getTable() {
        return table;
    }

    public abstract String connectorType();

    // -------------------------------------------------------------------------
    // Display / explain
    // -------------------------------------------------------------------------

    /**
     * Produces a human-readable summary of the sink configuration.
     * Used by {@link LogicalExternalInsert#explainTermsForDisplay}.
     *
     * @return display string; never {@code null}
     */
    public abstract String display();

    public static class DefaultTableSink extends TableSink {
        public DefaultTableSink(RelOptTable table) {
            super(table);
        }

        @Override
        public String connectorType() {
            return "default";
        }

        @Override
        public String display() {
            return "DefaultTableSink{table=" + table.getQualifiedName() + "}";
        }
    }
}
