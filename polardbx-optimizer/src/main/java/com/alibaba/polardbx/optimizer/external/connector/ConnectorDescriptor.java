package com.alibaba.polardbx.optimizer.external.connector;

import com.alibaba.polardbx.common.secret.PropertyDefinition;
import com.alibaba.polardbx.common.secret.SecretBundle;
import com.alibaba.polardbx.optimizer.core.rel.TableSink;
import com.alibaba.polardbx.optimizer.core.rel.TableSource;
import com.alibaba.polardbx.optimizer.external.schema.InferredSchema;
import org.apache.calcite.plan.RelOptTable;

import java.io.IOException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Connector SPI entry point, loaded via ServiceLoader and registered in ConnectorRegistry.
 *
 * <p>Contract — timeout guarantee:
 * every method that performs remote IO (notably {@link #createMetadata} client handshakes
 * and {@link #inferFilesSchema}) MUST be bounded by client-level timeouts configured inside
 * the connector implementation (e.g. connect/read timeouts on the underlying client).
 * The framework invokes these methods on parse threads, DDL validation threads and the sync
 * broadcast thread WITHOUT its own timeout wrapper, so a call that blocks indefinitely will
 * hang SQL parsing, DDL execution or cluster-wide sync on every CN.
 * The same guarantee applies to all remote-IO methods of the returned {@link ConnectorMetadata}.
 *
 * <p>Contract — lifecycle and resource ownership:
 * descriptors are instantiated via ServiceLoader at load/reload time and must be
 * STATELESS: they must not create or hold any process-level resources (no threads,
 * executors, connection pools or open connections) at construction or over their
 * lifetime. The framework has no shutdown hook for descriptors — on RELOAD CONNECTORS
 * it only swaps the registry reference and closes the jar file handle, which cannot
 * stop plugin-owned threads or pools, so any such resource would pin the old
 * classloader and leak permanently. All per-catalog resources belong to the
 * {@link ConnectorMetadata} instances returned by {@link #createMetadata}, which are
 * owned and closed by the caller; some call sites keep them for the process lifetime,
 * others close them per statement. Implementations must not assume a lifecycle, and
 * instances must not share mutable state unless internally synchronized.
 */
public interface ConnectorDescriptor {

    /**
     * Whether one {@link ConnectorMetadata} instance may serve concurrent lookups.
     * <p>
     * False by default: every metadata lookup then creates, uses and closes its own
     * instance, so an implementation never needs internal locking. Returning true lets
     * ExternalSchemaManager cache a single instance per schema and share it across
     * concurrent queries until idle reclaim closes it, and requires every method of
     * {@link ConnectorMetadata} to be thread-safe.
     */
    default boolean isThreadSafe() {
        return false;
    }

    /**
     * Connector type identifier and registry key. Must be stable, unique per connector
     * and lowercase; used to match catalogs, secret types and loaded runtimes.
     */
    String type();

    /**
     * Create a metadata handle for a catalog. May perform remote handshakes
     * (see the timeout contract above). The returned instance is owned by the caller.
     */
    ConnectorMetadata createMetadata(Map<String, String> catalogProps, SecretBundle secret);

    /**
     * Returns the secret property definitions supported by this connector.
     * A single connector may support multiple secret types (e.g., files connector
     * supports oss, s3, gcs each with different required parameters).
     * Must be stateless and idempotent; called on every CREATE SECRET validation.
     *
     * @return list of PropertyDefinitions, or empty list if not applicable
     */
    default List<PropertyDefinition> secretDefinitions() {
        return Collections.emptyList();
    }

    /**
     * Create the scan-side table source for planning. Return null only when this
     * connector cannot scan the given table (scan will then be rejected).
     */
    default TableSource createTableSource(Map<String, String> options, RelOptTable table) {
        return null;
    }

    default Optional<TableSink> createTableSink(Map<String, String> options, RelOptTable table) {
        return Optional.empty();
    }

    /**
     * Infer the schema of a FILES() table source. Performs remote IO on the parse
     * thread; must honor the timeout contract above.
     */
    default InferredSchema inferFilesSchema(Map<String, String> options) throws IOException {
        throw new UnsupportedOperationException("Schema inference not supported by connector: " + type());
    }

    /**
     * Declared pushdown capabilities drive the optimizer pushdown rules directly.
     * Declaring a capability is a promise that the corresponding TableSource pushdown
     * is implemented correctly (including residual-filter semantics); the framework
     * does not verify this at runtime, and a wrong declaration silently produces
     * wrong plans. When in doubt, declare fewer capabilities.
     */
    default EnumSet<PushdownCapability> capabilities() {
        return EnumSet.of(PushdownCapability.PROJECT, PushdownCapability.FILTER);
    }
}
