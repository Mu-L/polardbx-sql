package com.alibaba.polardbx.executor.columnar;

import com.alibaba.polardbx.common.columnar.ExternalColumnMetrics;
import com.alibaba.polardbx.common.columnar.ExternalColumnStatistics;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.BytesSql;
import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.common.jdbc.ParameterMethod;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.BaseQueryOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOpBuildParams;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperation;
import com.alibaba.polardbx.optimizer.core.rel.PhyTableOperationFactory;
import com.alibaba.polardbx.optimizer.utils.PhyTableOperationUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlKind;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * Statement-execution-scoped collection of phase-1 transactional staging writes.
 *
 * <p>One logical DML normally assembles one effective batch, although handlers may create temporary batches and
 * merge or split them while classifying the final business writes. Within the batch, values are grouped by
 * {@link OwnerRoute}: one route represents one primary physical-table branch, and each element in that group
 * represents one externalized column value that must be written to staging.
 *
 * <p>The staging lease is not batch-scoped. Its {@code seqId} is bound to the distributed transaction and target DN,
 * so separate statement batches, and separate owner routes on the same DN, may share the same sequence.
 */
public final class TransactionalStagingWriteBatch {

    private final Map<OwnerRoute, RouteRows> rowsByRoute = new LinkedHashMap<>();
    private boolean executionRecorded;
    private ExternalColumnStatistics externalColumnStatistics;

    public static String resolveDnId(String schemaName, String groupName) {
        ExecutorContext executorContext = ExecutorContext.getContext(schemaName);
        if (executorContext == null || executorContext.getTopologyHandler() == null) {
            throw error("Missing topology for schema " + schemaName);
        }
        IGroupExecutor groupExecutor = executorContext.getTopologyHandler().get(groupName);
        if (groupExecutor == null || !(groupExecutor.getDataSource() instanceof TGroupDataSource)) {
            throw error("Missing group data source for " + schemaName + "." + groupName);
        }
        String dnId = DdlHelper.getDnId((TGroupDataSource) groupExecutor.getDataSource());
        if (dnId == null || dnId.isEmpty()) {
            throw error("Cannot resolve DN for " + schemaName + "." + groupName);
        }
        return dnId;
    }

    public void add(OwnerRoute route, int seqId, String dnId, StagingTableManager.StagingRow row) {
        RouteRows routeRows = rowsByRoute.computeIfAbsent(route, ignored -> new RouteRows(seqId, dnId));
        if (routeRows.seqId != seqId || !routeRows.dnId.equals(dnId)) {
            throw error("One owner route resolved to different staging leases: " + route);
        }
        routeRows.rows.add(row);
    }

    public void addAll(TransactionalStagingWriteBatch other) {
        if (other == null || other.isEmpty()) {
            return;
        }
        for (Map.Entry<OwnerRoute, RouteRows> entry : other.rowsByRoute.entrySet()) {
            for (StagingTableManager.StagingRow row : entry.getValue().rows) {
                add(entry.getKey(), entry.getValue().seqId, entry.getValue().dnId, row);
            }
        }
    }

    public boolean isEmpty() {
        return rowsByRoute.isEmpty();
    }

    public int size() {
        int size = 0;
        for (RouteRows rows : rowsByRoute.values()) {
            size += rows.rows.size();
        }
        return size;
    }

    /**
     * Build one internal staging INSERT per primary business owner route. The SQL targets the fully qualified
     * staging table, while the routing tuple is copied from the primary plan so group parallelism selects the same
     * transaction write connection.
     */
    public List<RelNode> buildPhysicalPlans(List<RelNode> primaryBusinessPlans,
                                            ExecutionContext executionContext) {
        if (isEmpty()) {
            return Collections.emptyList();
        }
        externalColumnStatistics = executionContext.getOrCreateExtColStats();
        long planStart = System.nanoTime();
        try {
            return buildPhysicalPlansInternal(primaryBusinessPlans, executionContext);
        } finally {
            externalColumnStatistics.addStagingPlan(System.nanoTime() - planStart);
        }
    }

    private List<RelNode> buildPhysicalPlansInternal(List<RelNode> primaryBusinessPlans,
                                                     ExecutionContext executionContext) {
        if (executionContext.getTransaction() == null
            || !executionContext.getTransaction().isDistributedWriteTrx()) {
            throw error("Transactional staging reached execution without an XA/TSO transaction");
        }
        Map<OwnerRoute, PhyTableOperation> plans = indexPrimaryPlans(primaryBusinessPlans);
        List<RelNode> stagingPlans = new ArrayList<>(rowsByRoute.size());
        for (Map.Entry<OwnerRoute, RouteRows> entry : rowsByRoute.entrySet()) {
            PhyTableOperation ownerPlan = plans.get(entry.getKey());
            if (ownerPlan == null) {
                throw error("No primary business plan for staging owner route " + entry.getKey());
            }
            stagingPlans.add(buildRoutePlan(entry.getKey(), entry.getValue(), ownerPlan, executionContext));
        }
        return stagingPlans;
    }

    private static Map<OwnerRoute, PhyTableOperation> indexPrimaryPlans(List<RelNode> plans) {
        Map<OwnerRoute, PhyTableOperation> indexed = new LinkedHashMap<>();
        for (RelNode relNode : plans) {
            if (!(relNode instanceof PhyTableOperation)) {
                throw error("Primary business plan is not a PhyTableOperation: " + relNode.getClass().getName());
            }
            PhyTableOperation operation = (PhyTableOperation) relNode;
            if (!operation.isPrimaryWriteRelNode()) {
                continue;
            }
            OwnerRoute route = ownerRoute(operation);
            indexed.putIfAbsent(route, operation);
        }
        return indexed;
    }

    public static OwnerRoute ownerRoute(RelNode relNode) {
        if (!(relNode instanceof BaseQueryOperation)) {
            throw error("Business plan is not a BaseQueryOperation: " + relNode.getClass().getName());
        }
        BaseQueryOperation operation = (BaseQueryOperation) relNode;
        return new OwnerRoute(operation.getSchemaName(), operation.getDbIndex(), physicalTable(operation));
    }

    private static String physicalTable(BaseQueryOperation operation) {
        if (!(operation instanceof PhyTableOperation)) {
            throw error("Transactional staging currently requires PhyTableOperation, got "
                + operation.getClass().getName());
        }
        List<List<String>> tableNames = ((PhyTableOperation) operation).getTableNames();
        if (tableNames == null || tableNames.size() != 1 || tableNames.get(0).size() != 1) {
            throw error("Transactional staging requires exactly one primary physical table per plan");
        }
        return tableNames.get(0).get(0);
    }

    private static PhyTableOperation buildRoutePlan(OwnerRoute route, RouteRows routeRows,
                                                    PhyTableOperation ownerPlan,
                                                    ExecutionContext executionContext) {
        ExecutorContext executorContext = ExecutorContext.getContext(route.schemaName);
        IGroupExecutor groupExecutor = executorContext.getTopologyHandler().get(route.groupName);
        TGroupDataSource dataSource = (TGroupDataSource) groupExecutor.getDataSource();
        String actualDnId = DdlHelper.getDnId(dataSource);
        if (!routeRows.dnId.equals(actualDnId)) {
            throw error("Primary route DN changed before staging execution: expected=" + routeRows.dnId
                + ", actual=" + actualDnId + ", route=" + route);
        }

        String sql = "INSERT INTO " + StagingTableManager.fullyQualifiedTableName(routeRows.seqId)
            + " (`blob_addr`, `table_id`, `data`) VALUES (?, ?, ?)";
        List<Map<Integer, ParameterContext>> batchParameters = new ArrayList<>(routeRows.rows.size());
        for (StagingTableManager.StagingRow row : routeRows.rows) {
            Map<Integer, ParameterContext> parameters = new HashMap<>(3);
            parameters.put(1,
                new ParameterContext(ParameterMethod.setLong, new Object[] {1, row.blobAddr}));
            parameters.put(2,
                new ParameterContext(ParameterMethod.setLong, new Object[] {2, row.tableId}));
            parameters.put(3,
                new ParameterContext(ParameterMethod.setBytes, new Object[] {3, row.data}));
            batchParameters.add(parameters);
        }

        PhyTableOpBuildParams buildParams = new PhyTableOpBuildParams();
        buildParams.setSchemaName(ownerPlan.getSchemaName());
        buildParams.setLogTables(ownerPlan.getLogicalTableNames());
        buildParams.setGroupName(ownerPlan.getDbIndex());
        buildParams.setPhyTables(ownerPlan.getTableNames());
        buildParams.setSqlKind(SqlKind.INSERT);
        buildParams.setLockMode(ownerPlan.getLockMode());
        buildParams.setCluster(ownerPlan.getCluster());
        buildParams.setTraitSet(ownerPlan.getTraitSet());
        buildParams.setRowType(ownerPlan.getRowType());
        buildParams.setCursorMeta(ownerPlan.getCursorMeta());
        buildParams.setLogicalPlan(ownerPlan.getParent());
        buildParams.setBytesSql(BytesSql.getBytesSql(sql));
        buildParams.setBatchParameters(batchParameters);

        PhyTableOperation stagingPlan =
            PhyTableOperationFactory.getInstance().buildPhyTblOpByParams(buildParams);
        stagingPlan.setReplicateRelNode(false);
        stagingPlan.setStagingRelNode(true);

        if (DynamicConfig.getInstance().isExtStagingValidateGroupConnId()) {
            Long ownerGroupConnId = PhyTableOperationUtil.computeGrpConnIdByPhyOp(
                ownerPlan, route.groupName, null, executionContext);
            Long stagingGroupConnId = PhyTableOperationUtil.computeGrpConnIdByPhyOp(
                stagingPlan, route.groupName, null, executionContext);
            if (!Objects.equals(ownerGroupConnId, stagingGroupConnId)) {
                throw error("Staging and primary plans resolved to different GroupConnIds: staging="
                    + stagingGroupConnId + ", primary=" + ownerGroupConnId + ", route=" + route);
            }
        }
        return stagingPlan;
    }

    /**
     * Record committed-to-the-transaction staging requests after their physical plans execute successfully.
     */
    public void recordExecutionSuccess() {
        if (isEmpty()) {
            return;
        }
        long postStart = System.nanoTime();
        try {
            if (executionRecorded) {
                throw error("Transactional staging batch execution was recorded more than once");
            }
            executionRecorded = true;
            for (RouteRows routeRows : rowsByRoute.values()) {
                for (int i = 0; i < routeRows.rows.size(); i++) {
                    ExternalColumnMetrics.recordStagingWrite();
                }
                StagingTableManager.getInstance().recordStagedRows(routeRows.seqId, routeRows.rows.size());
            }
        } finally {
            if (externalColumnStatistics != null) {
                externalColumnStatistics.addStagingPost(System.nanoTime() - postStart);
            }
        }
    }

    /**
     * Identity of the primary business physical branch that owns a group of staging values.
     *
     * <p>Its granularity is exactly one {@code (schema, group, physical table)} tuple, not one logical row, DN, or
     * transaction. All values whose final primary write targets the same physical table share this key in a batch.
     * The key locates the owner {@link PhyTableOperation}, whose routing tuple is copied to the staging INSERT so both
     * plans select the same transaction write connection.
     */
    public static final class OwnerRoute {
        private final String schemaName;
        private final String groupName;
        private final String physicalTableName;

        public OwnerRoute(String schemaName, String groupName, String physicalTableName) {
            this.schemaName = require(schemaName, "schema");
            this.groupName = require(groupName, "group");
            this.physicalTableName = require(physicalTableName, "physical table");
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
            if (!(obj instanceof OwnerRoute)) {
                return false;
            }
            OwnerRoute other = (OwnerRoute) obj;
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

    /**
     * All staging values in the current batch that belong to one {@link OwnerRoute}.
     *
     * <p>{@code seqId} and {@code dnId} identify the transaction-scoped staging lease selected for the target DN;
     * multiple owner routes on that DN may share the same sequence. Each list element represents one externalized
     * column value, so one logical business row may contribute zero, one, or multiple elements.
     */
    private static final class RouteRows {
        private final int seqId;
        private final String dnId;
        private final List<StagingTableManager.StagingRow> rows = new ArrayList<>();

        private RouteRows(int seqId, String dnId) {
            this.seqId = seqId;
            this.dnId = dnId;
        }
    }

    private static String require(String value, String role) {
        if (value == null || value.isEmpty()) {
            throw error("Staging owner " + role + " is empty");
        }
        return value;
    }

    private static TddlRuntimeException error(String message) {
        return new TddlRuntimeException(ErrorCode.ERR_EXECUTOR, message);
    }
}
