package com.alibaba.polardbx.optimizer.core.rel.dml;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dml.writer.InsertWriter;
import org.apache.calcite.rel.RelNode;

import java.util.List;
import java.util.Map;

/**
 * Statement-scoped extension points embedded in the ordinary Writer flow.
 *
 * <p>The context lives on {@link ExecutionContext}, not on a cached Writer. Ordinary DML keeps this field
 * {@code null}; route, plan construction, virtual dispatch and replica expansion therefore remain unchanged.
 * Externalized DML installs a context after the Handler has completed statement semantics such as expression
 * evaluation and conflict handling. The original Writer still selects the rows for each leaf. Preparation callbacks
 * run before routing, {@code before*Plans} callbacks run after routing but before physical-plan construction, and
 * {@code after*Plans} callbacks run after the primary business plans have been built.</p>
 */
public interface DmlWriteContext {

    /**
     * Prepare the rows selected for one distinct UPDATE, DELETE, or relocate-INSERT leaf.
     *
     * <p>The caller has already evaluated the statement, classified the action, and applied this Writer's
     * deduplication. No physical route exists yet. An implementation may copy rows, replace their logical values with
     * an execution-local physical representation, and register work that must wait for the owner route. The returned
     * rows are used by both routing and physical-plan construction, so their order and cardinality must match
     * {@code logicalRows}.</p>
     *
     * @param writer exact leaf Writer consuming the rows
     * @param logicalRows Writer-selected rows before execution-local representation changes
     * @param executionContext execution context used by this Writer invocation
     * @return exact rows to route and place in physical plans
     */
    List<List<Object>> prepareModifyRows(DistinctWriter writer, List<List<Object>> logicalRows,
                                         ExecutionContext executionContext);

    /**
     * Return the rows previously produced by {@link #prepareModifyRows} for the same Writer invocation.
     *
     * <p>Replication Writers use this lookup after their primary Writer has run so replicas consume the identical
     * physical values, including any canonical external addresses, without preparing the logical rows again.</p>
     *
     * @param writer exact Writer whose prepared rows are requested
     * @param executionContext execution context used to prepare the rows
     * @return prepared rows, or {@code null} if this Writer has not prepared a batch in this context
     */
    List<List<Object>> getPreparedModifyRows(DistinctWriter writer, ExecutionContext executionContext);

    /**
     * Prepare an INSERT branch whose parent Writer has already selected concrete rows.
     *
     * <p>This variant is used when the child {@link InsertWriter} consumes JDBC batch parameters rather than a
     * row-generator callback. The implementation may replace those parameters with prepared row copies and register
     * route-dependent work. Routing and physical-plan construction still happen later in the ordinary InsertWriter
     * flow.</p>
     *
     * @param writer exact INSERT leaf that will consume the prepared parameters
     * @param logicalRows selected INSERT rows in their logical representation
     * @param executionContext execution context whose batch parameters belong to this invocation
     */
    void prepareInsertRows(InsertWriter writer, List<List<Object>> logicalRows,
                           ExecutionContext executionContext);

    /**
     * Materialize route-dependent INSERT state after every row has an owner route and before plans are built.
     *
     * <p>An implementation may replace values or parameters using the fixed route and retain a hook for companion
     * plan construction. It must not calculate a different route.</p>
     *
     * @param writer exact INSERT leaf being built
     * @param routedInput owner route for every input row
     * @param executionContext execution context used by this Writer invocation
     */
    void beforeInsertPlans(InsertWriter writer, RoutedInsertInput routedInput,
                           ExecutionContext executionContext);

    /**
     * Materialize route-dependent UPDATE, DELETE, or relocate-INSERT state after routing and before plans are built.
     *
     * <p>The rows in {@code routedInput} are the exact rows returned by {@link #prepareModifyRows}. Implementations
     * may finish their physical representation and retain a hook for companion plans, but must preserve the supplied
     * owner routes.</p>
     *
     * @param writer exact distinct Writer being built
     * @param routedInput prepared rows and their owner routes
     * @param executionContext execution context used by this Writer invocation
     */
    void beforeModifyPlans(DistinctWriter writer, RoutedModifyInput routedInput,
                           ExecutionContext executionContext);

    /**
     * Compose route-owned companion plans with the physical plans built by an INSERT Writer.
     *
     * <p>The returned list is the executable Writer phase. Implementations may prepend staging plans, but must retain
     * all primary business plans and must not count companion plans as business writes.</p>
     *
     * @param writer exact INSERT leaf that built {@code primaryPlans}
     * @param primaryPlans physical business plans built by the ordinary Writer flow
     * @param executionContext execution context used by this Writer invocation
     * @return executable plans for this Writer phase
     */
    List<RelNode> afterInsertPlans(InsertWriter writer, List<RelNode> primaryPlans,
                                   ExecutionContext executionContext);

    /**
     * Compose route-owned companion plans with the physical plans built by a distinct modify Writer.
     *
     * <p>The returned list is the executable Writer phase. Implementations may prepend staging plans, but must retain
     * all primary UPDATE, DELETE, or relocate-INSERT plans.</p>
     *
     * @param writer exact distinct Writer that built {@code primaryPlans}
     * @param primaryPlans physical business plans built by the ordinary Writer flow
     * @param executionContext execution context used by this Writer invocation
     * @return executable plans for this Writer phase
     */
    List<RelNode> afterModifyPlans(DistinctWriter writer, List<RelNode> primaryPlans,
                                   ExecutionContext executionContext);

    /**
     * Materialize one already-pruned {@code LogicalModifyView} branch before its JDBC parameters are finalized.
     *
     * <p>The supplied route is fixed. Implementations may return a branch-local parameter map containing physical
     * values such as external addresses, but must not reroute the branch or retain the cached relational node.</p>
     *
     * @param route concrete owner route selected for this physical branch
     * @param parameters logical JDBC parameters for the branch
     * @param executionContext statement execution context
     * @return parameters to use when constructing the physical branch
     */
    Map<Integer, ParameterContext> materializeModifyViewParameters(
        PhysicalRoute route, Map<Integer, ParameterContext> parameters, ExecutionContext executionContext);

    /**
     * Build companion plans for all unmerged primary {@code LogicalModifyView} branches.
     *
     * <p>This callback runs before group-node merging so implementations can preserve the association between each
     * owner branch and its staging write. It returns companion plans only; the caller prepends them to the primary
     * plans.</p>
     *
     * @param unmergedPrimaryPlans physical business plans before group-node merging
     * @param executionContext statement execution context
     * @return companion plans to execute before the primary plans, or an empty list
     */
    List<RelNode> buildModifyViewStagingPlans(List<RelNode> unmergedPrimaryPlans,
                                              ExecutionContext executionContext);

    /**
     * Complete statement-scoped write state after all supplied physical plans have executed successfully.
     *
     * <p>Handlers call this only after lazy physical cursors have been consumed. Implementations may validate that a
     * companion batch was not split and record its successful execution. This is an execution-phase notification,
     * not a transaction-commit notification.</p>
     *
     * @param physicalPlans complete physical plan set executed in this phase
     */
    void afterExecutionSuccess(List<RelNode> physicalPlans);
}
