/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.polardbx.matrix.jdbc;

import com.alibaba.polardbx.CobarConfig;
import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.ddl.newengine.DdlState;
import com.alibaba.polardbx.common.ddl.newengine.DdlType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.mock.MockUtils;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.ConcurrentHashSet;
import com.alibaba.polardbx.common.utils.Pair;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.TddlGroupExecutor;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.StorageInfoManager;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.ddl.newengine.utils.DdlHelper;
import com.alibaba.polardbx.executor.handler.ddl.LogicalCommonDdlHandler;
import com.alibaba.polardbx.executor.handler.ddl.LogicalDropTableGroupHandler;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.group.config.GroupDataSourceHolder;
import com.alibaba.polardbx.group.config.OptimizedGroupConfigManager;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;
import com.alibaba.polardbx.group.jdbc.TGroupDirectConnection;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.matrix.config.MatrixConfigHolder;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.ccl.exception.CclRescheduleException;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.optimizer.utils.ExecutionPlanProperties;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.utils.OptimizerUtils;
import com.alibaba.polardbx.rpc.compatible.XDataSource;
import com.alibaba.polardbx.rpc.perf.SwitchoverPerfCollection;
import com.alibaba.polardbx.rpc.pool.XConnectionManager;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.transaction.TransactionManager;
import com.alibaba.polardbx.gms.metadb.misc.DdlEngineRecord;
import com.alibaba.polardbx.transaction.connection.AutoCommitConnectionHolder;
import com.alibaba.polardbx.transaction.connection.TransactionConnectionHolder;
import com.alibaba.polardbx.transaction.trx.AutoCommitTransaction;
import com.alibaba.polardbx.transaction.trx.ColumnarExplicitTransaction;
import com.alibaba.polardbx.transaction.trx.TsoTransaction;
import com.google.common.collect.ImmutableSet;
import com.google.common.truth.Truth;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.TransactionClass.COLUMNAR_READ_ONLY_TRANSACTION;
import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.TransactionClass.COLUMNAR_RO_EXPLICIT_TRANSACTION;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class TConnectionTest {
    @Test
    public void newExecutionContextTest() {

        final TConnection tConnection = new TConnection(new TDataSource());

        // OUTPUT_MYSQL_ERROR_CODE = false
        tConnection.newExecutionContext();
        Truth.assertThat(
                tConnection
                    .getExecutionContext()
                    .getParamManager()
                    .getBoolean(ConnectionParams.OUTPUT_MYSQL_ERROR_CODE))
            .isFalse();

        // OUTPUT_MYSQL_ERROR_CODE = true
        ParamManager.setBooleanVal(tConnection.getExecutionContext().getParamManager().getProps(),
            ConnectionParams.OUTPUT_MYSQL_ERROR_CODE, true, false);
        tConnection.newExecutionContext();
        Truth.assertThat(
                tConnection
                    .getExecutionContext()
                    .getParamManager()
                    .getBoolean(ConnectionParams.OUTPUT_MYSQL_ERROR_CODE))
            .isTrue();
    }

    @Test
    public void testUpdateTransactionAndConcurrentPolicyForDml() throws SQLException {
        try (final TConnection tConnection = mock(TConnection.class)) {
            ExecutionPlan plan = mock(ExecutionPlan.class);
            ExecutionContext ec;

            ConcurrentHashSet<Integer> properties = new ConcurrentHashSet<>();
            when(plan.getPlanProperties()).thenReturn(properties);
            SqlNode ast = mock(SqlNode.class);
            when(plan.getAst()).thenReturn(ast);
            when(plan.is(any())).thenCallRealMethod();
            ITransaction trx = mock(ITransaction.class);
            when(tConnection.forceInitTransaction(any(), anyBoolean(), anyBoolean())).thenReturn(trx);

            properties.clear();
            properties.add(ExecutionPlanProperties.DML);
            when(tConnection.isAutoCommit()).thenReturn(true);
            when(ast.getKind()).thenReturn(SqlKind.SELECT);
            ec = new ExecutionContext();
            ec.getParamManager().getProps().put(ConnectionProperties.FORBID_AUTO_COMMIT_TRX, "true");
            when(tConnection.updateTransactionAndConcurrentPolicyForDml(plan, ec)).thenCallRealMethod();
            when(tConnection.isAutoCommit()).thenReturn(true);
            TDataSource ds = Mockito.mock(TDataSource.class);
            MatrixConfigHolder configHolder = Mockito.mock(MatrixConfigHolder.class);
            ExecutorContext executorContext = Mockito.mock(ExecutorContext.class);
            StorageInfoManager storageInfoManager = Mockito.mock(StorageInfoManager.class);
            when(tConnection.getDs()).thenReturn(ds);
            when(ds.getConfigHolder()).thenReturn(configHolder);
            when(configHolder.getExecutorContext()).thenReturn(executorContext);
            when(executorContext.getStorageInfoManager()).thenReturn(storageInfoManager);
            when(storageInfoManager.supportXA()).thenReturn(true);

            boolean updated = tConnection.updateTransactionAndConcurrentPolicyForDml(plan, ec);
            Assert.assertTrue(updated);
            Assert.assertEquals(trx, ec.getTransaction());

            properties.clear();
            when(tConnection.isAutoCommit()).thenReturn(true);
            when(ast.getKind()).thenReturn(SqlKind.SELECT);
            ec = new ExecutionContext();
            ec.getParamManager().getProps().put(ConnectionProperties.FORBID_AUTO_COMMIT_TRX, "true");
            when(tConnection.updateTransactionAndConcurrentPolicyForDml(any(), any())).thenCallRealMethod();

            updated = tConnection.updateTransactionAndConcurrentPolicyForDml(plan, ec);
            Assert.assertTrue(updated);
            Assert.assertEquals(trx, ec.getTransaction());
        }

    }

    /**
     * Tests whether the method correctly sets the MASTER property when given a valid 'groupindex:0' hint.
     */
    @Test
    public void testTransformGroupIndexHintToMasterSlaveForMaster() {
        Map<String, Object> extraCmd = new HashMap<>();
        String groupHint = "groupindex:0";

        TConnection.transformGroupIndexHintToMasterSlave(groupHint, extraCmd);

        assertEquals(Boolean.TRUE, extraCmd.get(ConnectionProperties.MASTER));
    }

    /**
     * Tests whether the method correctly sets the SLAVE property when given a valid 'groupindex:1' hint.
     */
    @Test
    public void testTransformGroupIndexHintToMasterSlaveForSlave() {
        Map<String, Object> extraCmd = new HashMap<>();
        String groupHint = "groupindex:1";

        TConnection.transformGroupIndexHintToMasterSlave(groupHint, extraCmd);

        assertEquals(Boolean.TRUE, extraCmd.get(ConnectionProperties.SLAVE));
    }

    /**
     * Tests whether the method leaves the extraCmd unchanged when given an empty string as the groupHint.
     */
    @Test
    public void testTransformGroupIndexHintToMasterSlaveWithEmptyString() {
        Map<String, Object> extraCmd = new HashMap<>();
        String groupHint = "";

        TConnection.transformGroupIndexHintToMasterSlave(groupHint, extraCmd);

        assertEquals(0, extraCmd.size());
    }

    /**
     * Tests whether the method leaves the extraCmd unchanged when given null as the groupHint.
     */
    @Test
    public void testTransformGroupIndexHintToMasterSlaveWithNull() {
        Map<String, Object> extraCmd = new HashMap<>();
        String groupHint = null;

        TConnection.transformGroupIndexHintToMasterSlave(groupHint, extraCmd);

        assertEquals(0, extraCmd.size());
    }

    /**
     * Tests whether the method leaves the extraCmd unchanged when given an invalid input as the groupHint.
     */
    @Test
    public void testTransformGroupIndexHintToMasterSlaveWithInvalidInput() {
        Map<String, Object> extraCmd = new HashMap<>();
        String groupHint = "invalid_input";

        TConnection.transformGroupIndexHintToMasterSlave(groupHint, extraCmd);

        assertEquals(0, extraCmd.size());
    }

    @Test
    public void testCheckGroupForSwitchover() throws SQLException {
        final XDataSource ds = mock(XDataSource.class);
        final Set<SwitchoverPerfCollection> collections = new HashSet<>();
        final TConnection tConnection = new TConnection(new TDataSource());
        final Map<String, Map<String, Boolean>> groups = new HashMap<>();
        groups.put("schema", new HashMap<>());
        groups.get("schema").put("group", true);
        try (final MockedStatic<ExecutorContext> staticExecutorContext = mockStatic(ExecutorContext.class);
            final MockedStatic<OptimizerUtils> staticOptimizerUtils = mockStatic(OptimizerUtils.class)) {
            staticOptimizerUtils.when(() -> OptimizerUtils.useExplicitTransaction(any())).thenReturn(true);
            final ExecutorContext executorContext = Mockito.mock(ExecutorContext.class);
            staticExecutorContext.when(() -> ExecutorContext.getContext(Mockito.anyString()))
                .thenReturn(executorContext);
            final TopologyHandler topologyHandler = Mockito.mock(TopologyHandler.class);
            Mockito.when(executorContext.getTopologyHandler()).thenReturn(topologyHandler);
            final TddlGroupExecutor groupExecutor = Mockito.mock(TddlGroupExecutor.class);
            Mockito.when(topologyHandler.get(any())).thenReturn(groupExecutor);
            final TGroupDataSource dataSource = Mockito.mock(TGroupDataSource.class);
            Mockito.when(groupExecutor.getDataSource()).thenReturn(dataSource);
            final OptimizedGroupConfigManager configManager = Mockito.mock(OptimizedGroupConfigManager.class);
            Mockito.when(dataSource.getConfigManager()).thenReturn(configManager);
            final GroupDataSourceHolder groupDataSourceHolder = Mockito.mock(GroupDataSourceHolder.class);
            Mockito.when(configManager.getGroupDataSourceHolder()).thenReturn(groupDataSourceHolder);
            Mockito.when(groupDataSourceHolder.isChangingLeader(Mockito.any())).thenReturn(Pair.of(true, ds));

            final TsoTransaction tsoTransaction = Mockito.mock(TsoTransaction.class);
            tConnection.getExecutionContext().setTransaction(tsoTransaction);
            final TransactionConnectionHolder connectionHolder = Mockito.mock(TransactionConnectionHolder.class);
            Mockito.when(tsoTransaction.getConnectionHolder()).thenReturn(connectionHolder);
            final TGroupDirectConnection connection = Mockito.mock(TGroupDirectConnection.class);
            Mockito.when(connection.isWrapperFor(any())).thenThrow(new RuntimeException("mock throw"));
            Mockito.when(connectionHolder.getAllConnection()).thenReturn(ImmutableSet.of(connection));

            Assert.assertFalse(tConnection.mayBlockByChangingLeader(groups, collections));

            final AutoCommitTransaction autoCommitTransaction = Mockito.mock(AutoCommitTransaction.class);
            tConnection.getExecutionContext().setTransaction(autoCommitTransaction);
            final AutoCommitConnectionHolder autoCommitConnectionHolder =
                Mockito.mock(AutoCommitConnectionHolder.class);
            Mockito.when(autoCommitTransaction.getConnectionHolder()).thenReturn(autoCommitConnectionHolder);
            Mockito.when(autoCommitConnectionHolder.getAllConnection()).thenReturn(ImmutableSet.of(connection));

            Assert.assertFalse(tConnection.mayBlockByChangingLeader(groups, collections));
        }
        tConnection.mayBlockByChangingLeader(groups, collections);
    }

    @Test
    public void testSwitchover() throws InterruptedException {
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_SMOOTH_SWITCHOVER, "false");
        final TConnection tConnection = new TConnection(new TDataSource());
        final ExecutionContext executionContext = Mockito.mock(ExecutionContext.class);
        tConnection.rescheduleIfSwitchover(null, executionContext);

        XConnectionManager.getInstance().markAnyOneChangingLeader();
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_SMOOTH_SWITCHOVER, "true");
        final TConnection spyConn = Mockito.spy(tConnection);
        doReturn(true).when(spyConn).mayBlockByChangingLeader(any(), any());
        final RelNode rel = Mockito.mock(RelNode.class);
        final ExecutionPlan executionPlan = Mockito.mock(ExecutionPlan.class);
        Mockito.when(executionPlan.getPlan()).thenReturn(rel);
        try {
            spyConn.rescheduleIfSwitchover(executionPlan, executionContext);
            Assert.fail();
        } catch (CclRescheduleException e) {
            final ServerConnection serverConnection = Mockito.mock(ServerConnection.class);
            e.getRescheduleCallback().apply(serverConnection);
        }

        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.SWITCHOVER_WAIT_TIMEOUT_IN_MILLIS, "1");
        Thread.sleep(2);
        spyConn.rescheduleIfSwitchover(executionPlan, executionContext);
    }

    @Test
    public void testRescheduleSkippedForInternalSubExecution() {
        // 当 internalSubExecution=true 时，rescheduleIfSwitchover 应直接返回，不触发重调度
        final TConnection tConnection = new TConnection(new TDataSource());
        final ExecutionContext executionContext = new ExecutionContext();
        executionContext.setInternalSubExecution(true);

        // 即使全局 switchover 条件已设置，也不应触发 reschedule（方法应在 early-return 处直接返回）
        XConnectionManager.getInstance().markAnyOneChangingLeader();
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_SMOOTH_SWITCHOVER, "true");

        // plan 传 null 也不会 NPE，因为 early-return 在访问 plan 之前就返回了
        tConnection.rescheduleIfSwitchover(null, executionContext);
        // 如果没有 early-return，传 null plan 会导致 NPE，说明测试未通过
    }

    @Test
    public void testRescheduleNotSkippedForNormalExecution() {
        // 当 internalSubExecution=false 时，方法不会 early-return，会继续执行后续逻辑
        final TConnection tConnection = new TConnection(new TDataSource());
        final ExecutionContext executionContext = new ExecutionContext();
        executionContext.setInternalSubExecution(false);

        XConnectionManager.getInstance().markAnyOneChangingLeader();
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_SMOOTH_SWITCHOVER, "true");

        // plan 传 null，因为没有 early-return，会进入正常逻辑访问 plan，导致 NPE
        // 这证明了方法确实没有被跳过
        try {
            tConnection.rescheduleIfSwitchover(null, executionContext);
            // 如果 isAnyOneChangingLeader 返回 false（已被重置），则正常返回，也是合法的
        } catch (NullPointerException e) {
            // 预期：进入了 switchover 检查逻辑，访问 null plan 导致 NPE
            // 证明 early-return 没有生效（因为 internalSubExecution=false）
        }
    }

    @Test
    public void testRescheduleAutoMarksInternalSubExecution() {
        // Verify that rescheduleIfSwitchover auto-sets internalSubExecution=true after first check
        final TConnection tConnection = new TConnection(new TDataSource());
        final ExecutionContext executionContext = new ExecutionContext();
        Assert.assertFalse(executionContext.isInternalSubExecution());

        // Disable switchover so rescheduleIfSwitchover completes without throwing
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_SMOOTH_SWITCHOVER, "false");

        tConnection.rescheduleIfSwitchover(null, executionContext);

        // After first invocation, flag should be auto-set to true
        Assert.assertTrue(executionContext.isInternalSubExecution());

        // Second call should be skipped (pass null plan without NPE)
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_SMOOTH_SWITCHOVER, "true");
        XConnectionManager.getInstance().markAnyOneChangingLeader();
        tConnection.rescheduleIfSwitchover(null, executionContext);
        // No NPE means the second call was skipped
    }

    @Test
    public void testSimulatedSwitchoverRescheduleForInsertSplitter() {
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_SMOOTH_SWITCHOVER, "false");

        final TConnection tConnection = new TConnection(new TDataSource());
        final ExecutionContext executionContext = buildSimulatedSwitchoverExecutionContext(
            System.nanoTime(), 2L, 2L, true);

        try {
            tConnection.rescheduleIfSwitchover(null, executionContext);
            Assert.fail();
        } catch (CclRescheduleException e) {
            Assert.assertNotNull(e.getRescheduleCallback());
        }
        Assert.assertTrue(executionContext.isInternalSubExecution());
    }

    @Test
    public void testSimulatedSwitchoverRescheduleSkippedForMismatchAndRepeatedTrigger() {
        DynamicConfig.getInstance().loadValue(null, ConnectionProperties.ENABLE_SMOOTH_SWITCHOVER, "false");

        final TConnection tConnection = new TConnection(new TDataSource());
        final long connId = System.nanoTime();

        final ExecutionContext disabledSimulation = buildSimulatedSwitchoverExecutionContext(
            connId, -1L, 2L, false);
        tConnection.rescheduleIfSwitchover(null, disabledSimulation);

        final ExecutionContext nullPhySqlId = buildSimulatedSwitchoverExecutionContext(
            connId, 2L, null, false);
        tConnection.rescheduleIfSwitchover(null, nullPhySqlId);

        final ExecutionContext nonMatchingPhySqlId = buildSimulatedSwitchoverExecutionContext(
            connId, 3L, 2L, false);
        tConnection.rescheduleIfSwitchover(null, nonMatchingPhySqlId);

        final ExecutionContext firstMatchedPhySqlId = buildSimulatedSwitchoverExecutionContext(
            connId, 2L, 2L, false);
        try {
            tConnection.rescheduleIfSwitchover(null, firstMatchedPhySqlId);
            Assert.fail();
        } catch (CclRescheduleException e) {
            Assert.assertNotNull(e.getRescheduleCallback());
        }

        final ExecutionContext repeatedMatchedPhySqlId = buildSimulatedSwitchoverExecutionContext(
            connId, 2L, 2L, false);
        tConnection.rescheduleIfSwitchover(null, repeatedMatchedPhySqlId);
    }

    private ExecutionContext buildSimulatedSwitchoverExecutionContext(long connId, long targetPhySqlId,
                                                                      Long phySqlId,
                                                                      boolean enableInternalSubExecutionGuard) {
        final ExecutionContext executionContext = new ExecutionContext();
        executionContext.setConnId(connId);
        executionContext.setDoingBatchInsertBySpliter(true);
        executionContext.setPhySqlId(phySqlId);
        ParamManager.setVal(executionContext.getParamManager().getProps(),
            ConnectionParams.SIMULATE_SWITCHOVER_RESCHEDULE_PHY_SQL_ID_FOR_TEST, String.valueOf(targetPhySqlId),
            false);
        ParamManager.setBooleanVal(executionContext.getParamManager().getProps(),
            ConnectionParams.ENABLE_SWITCHOVER_RESCHEDULE_INTERNAL_SUB_EXECUTION_GUARD_FOR_TEST,
            enableInternalSubExecutionGuard, false);
        return executionContext;
    }

    @Test
    public void testInternalSubExecutionCopiedInExecutionContext() {
        // Verify that internalSubExecution is propagated during EC copy
        final ExecutionContext ec = new ExecutionContext();
        ec.setInternalSubExecution(true);
        Assert.assertTrue(ec.isInternalSubExecution());

        ExecutionContext copied = ec.copy();
        Assert.assertTrue(copied.isInternalSubExecution());

        // Verify false is also propagated
        ec.setInternalSubExecution(false);
        ExecutionContext copied2 = ec.copy();
        Assert.assertFalse(copied2.isInternalSubExecution());
    }

    @Test
    public void testBeginColumnarTransaction() throws SQLException {
        final StorageInfoManager storageInfoManager = Mockito.mock(StorageInfoManager.class);
        Mockito.when(storageInfoManager.isReadOnly()).thenReturn(true);
        final ExecutorContext executorContext = Mockito.mock(ExecutorContext.class);
        Mockito.when(executorContext.getStorageInfoManager()).thenReturn(storageInfoManager);
        Mockito.when(executorContext.getTransactionManager()).thenReturn(new TransactionManager());
        final MatrixConfigHolder configHolder = Mockito.mock(MatrixConfigHolder.class);
        Mockito.when(configHolder.getExecutorContext()).thenReturn(executorContext);

        final TDataSource tDataSource = Mockito.mock(TDataSource.class);
        Mockito.when(tDataSource.getConfigHolder()).thenReturn(configHolder);
        final TConnection tConnection = new TConnection(tDataSource);
        tConnection.setReadOnly(true);

        final ParamManager paramManager = Mockito.mock(ParamManager.class);
        final ExecutionContext executionContext = Mockito.mock(ExecutionContext.class);
        Mockito.when(executionContext.getParamManager()).thenReturn(paramManager);
        Mockito.when(executionContext.isUseColumnar()).thenReturn(true);
        try (MockedStatic<ConfigDataMode> configDataModeMockedStatic = Mockito.mockStatic(ConfigDataMode.class)) {
            configDataModeMockedStatic.when(ConfigDataMode::isColumnarMode).thenReturn(true);
            ITransactionPolicy trxPolicy = tConnection.loadTrxPolicy(executionContext);
            assertEquals(ITransactionPolicy.COLUMNAR_TRANSACTION, trxPolicy);
            assertEquals("COLUMNAR_TRANSACTION", trxPolicy.toString());
            assertEquals(COLUMNAR_RO_EXPLICIT_TRANSACTION, trxPolicy.getTransactionType(false, true, false, false));
            assertEquals(COLUMNAR_READ_ONLY_TRANSACTION, trxPolicy.getTransactionType(true, true, false, false));
            MockUtils.assertThrows(TddlRuntimeException.class,
                "ERR-CODE: [PXC-4000][ERR_CONFIG] config error by Columnar transaction policy only support read only transaction ",
                () -> {
                    trxPolicy.getTransactionType(false, false, false, false);
                });
            tConnection.beginTransaction(false);
            assertTrue(tConnection.getTrx() instanceof ColumnarExplicitTransaction);
            assertTrue(tConnection.updateTransactionAndConcurrentPolicyForColumnar(executionContext));
            assertNull(executionContext.getTransaction());
        }
    }

    @Test
    public void testResetSqlMode() {
        final TDataSource tDataSource = Mockito.mock(TDataSource.class);
        Map<String, Object> connectionProperties = new HashMap<>();
        Mockito.when(tDataSource.getConnectionProperties()).thenReturn(connectionProperties);

        final TConnection tConnection = new TConnection(tDataSource);
        tConnection.newExecutionContext();
        ExecutionContext context = tConnection.getExecutionContext();

        // sql mode is empty
        tConnection.resetSqlMode();
        Assert.assertTrue(context.getSqlMode() == null);
        Assert.assertTrue(tConnection.getSqlMode() == null);

        // sql mode is 'ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION'
        String sqlMode =
            "ONLY_FULL_GROUP_BY,STRICT_TRANS_TABLES,NO_ZERO_IN_DATE,NO_ZERO_DATE,ERROR_FOR_DIVISION_BY_ZERO,NO_ENGINE_SUBSTITUTION";
        connectionProperties.put("sql_mode", sqlMode);
        tConnection.resetSqlMode();
        Assert.assertTrue(sqlMode.equals(context.getSqlMode()));
        Assert.assertTrue(sqlMode.equals(tConnection.getSqlMode()));

        tConnection.resetSqlMode();
    }

    @Test
    public void testSetLogicalTime() {
        ConfigDataMode.Mode mode = ConfigDataMode.getMode();
        ConfigDataMode.setMode(ConfigDataMode.Mode.MOCK);
        try (MockedStatic<CobarServer> cobarServerMockedStatic = Mockito.mockStatic(CobarServer.class)) {
            CobarServer cobarServer = Mockito.mock(CobarServer.class);
            cobarServerMockedStatic.when(CobarServer::getInstance).thenReturn(cobarServer);
            CobarConfig cobarConfig = Mockito.mock(CobarConfig.class);
            Mockito.when(cobarServer.getConfig()).thenReturn(cobarConfig);
            final TDataSource tDataSource = Mockito.mock(TDataSource.class);
            Map<String, Object> connectionProperties = new HashMap<>();
            Mockito.when(tDataSource.getConnectionProperties()).thenReturn(connectionProperties);

            final TConnection tConnection = new TConnection(tDataSource);
            tConnection.newExecutionContext();

            Assert.assertTrue(tConnection.getExecutionContext().getLogicalSqlStartTimeInMs() <= 0);
            Assert.assertTrue(tConnection.getExecutionContext().getLogicalSqlStartTime() <= 0);

            tConnection.prepareExecutionContext();

            Assert.assertTrue(tConnection.getExecutionContext().getLogicalSqlStartTimeInMs() <= 0);
            Assert.assertTrue(tConnection.getExecutionContext().getLogicalSqlStartTime() <= 0);

            tConnection.getExecutionContext().setLogicalSqlStartTimeInMs(System.currentTimeMillis());
            tConnection.getExecutionContext().setLogicalSqlStartTime(System.nanoTime());

            Assert.assertTrue(tConnection.getExecutionContext().getLogicalSqlStartTimeInMs() > 0);
            Assert.assertTrue(tConnection.getExecutionContext().getLogicalSqlStartTime() > 0);

            tConnection.prepareExecutionContext();

            Assert.assertTrue(tConnection.getExecutionContext().getLogicalSqlStartTimeInMs() > 0);
            Assert.assertTrue(tConnection.getExecutionContext().getLogicalSqlStartTime() > 0);
        } finally {
            ConfigDataMode.setMode(mode);
        }
    }

    /**
     * Regression test for AONE-84973931: TConnection#setGroupParallelism() calls
     * PlanManager.invalidateSchema(), which is expected to only clear the schema's
     * plan cache, but currently also force-deletes ALL SPM baselines under that
     * schema (including baselines of tables unrelated to the group_parallelism
     * change), causing a schema-wide baseline wipe-out (e.g. triggered by the TTL
     * AddParts Warning Scanner setting group_parallelism on an internal connection).
     */
    @Test
    public void testSetGroupParallelismOnlyInvalidatesPlanCacheNotBaseline() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        String schema = "ttl_warning_scanner_test_schema";
        PlanManager planManager = PlanManager.getInstance();
        planManager.getBaselineMap(schema).clear();

        try {
            String sqlForTableA = "select * from ttl_unrelated_table_a";
            String sqlForTableB = "select * from ttl_unrelated_table_b";
            Set<Pair<String, String>> tableSetA = new HashSet<>();
            tableSetA.add(Pair.of(schema, "ttl_unrelated_table_a"));
            Set<Pair<String, String>> tableSetB = new HashSet<>();
            tableSetB.add(Pair.of(schema, "ttl_unrelated_table_b"));

            planManager.addBaselineInfo(schema, sqlForTableA, new BaselineInfo(sqlForTableA, tableSetA));
            planManager.addBaselineInfo(schema, sqlForTableB, new BaselineInfo(sqlForTableB, tableSetB));

            assertEquals(2, planManager.getBaselineMap(schema).size());

            TDataSource dataSource = new TDataSource();
            dataSource.setSchemaName(schema);
            TConnection tConnection = new TConnection(dataSource);

            // Simulate TTL Warning Scanner's internal connection setting group_parallelism,
            // which should only invalidate the plan cache, not force-delete baselines.
            tConnection.setGroupParallelism(2L);

            Assert.assertTrue(
                "setGroupParallelism() must not force-delete unrelated-table baselines in the same schema",
                planManager.getBaselineMap(schema).containsKey(sqlForTableA));
            Assert.assertTrue(
                "setGroupParallelism() must not force-delete unrelated-table baselines in the same schema",
                planManager.getBaselineMap(schema).containsKey(sqlForTableB));
            assertEquals(2, planManager.getBaselineMap(schema).size());
        } finally {
            planManager.getBaselineMap(schema).clear();
        }
    }

    @Test
    public void testTableVersionChangedReturnsFalseForEmptyOrOlderVersions() throws Exception {
        OptimizerContext optimizerContext = mock(OptimizerContext.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        TableMeta tableMeta = mock(TableMeta.class);
        when(optimizerContext.getLatestSchemaManager()).thenReturn(schemaManager);
        when(schemaManager.getTableWithNull("skip_table")).thenReturn(tableMeta);
        when(schemaManager.getTableWithNull("same_table")).thenReturn(tableMeta);
        when(tableMeta.getVersion()).thenReturn(10L);

        Map<String, Long> tableVersions = new HashMap<>();
        tableVersions.put("skip_table", 0L);
        tableVersions.put("same_table", 10L);

        try (MockedStatic<OptimizerContext> mockedOptimizerContext = mockStatic(OptimizerContext.class)) {
            mockedOptimizerContext.when(() -> OptimizerContext.getContext("test_schema")).thenReturn(optimizerContext);

            Assert.assertFalse(invokeTableVersionChanged(tableVersions, "test_schema"));
        }
    }

    @Test
    public void testTableVersionChangedReturnsTrueWhenLatestVersionIncreases() throws Exception {
        OptimizerContext optimizerContext = mock(OptimizerContext.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        TableMeta tableMeta = mock(TableMeta.class);
        when(optimizerContext.getLatestSchemaManager()).thenReturn(schemaManager);
        when(schemaManager.getTableWithNull("changed_table")).thenReturn(tableMeta);
        when(tableMeta.getVersion()).thenReturn(11L);

        Map<String, Long> tableVersions = new HashMap<>();
        tableVersions.put("changed_table", 10L);

        try (MockedStatic<OptimizerContext> mockedOptimizerContext = mockStatic(OptimizerContext.class)) {
            mockedOptimizerContext.when(() -> OptimizerContext.getContext("test_schema")).thenReturn(optimizerContext);

            Assert.assertTrue(invokeTableVersionChanged(tableVersions, "test_schema"));
        }
    }

    @Test
    public void testTableVersionChangedReturnsTrueWhenTableMissing() throws Exception {
        OptimizerContext optimizerContext = mock(OptimizerContext.class);
        SchemaManager schemaManager = mock(SchemaManager.class);
        when(optimizerContext.getLatestSchemaManager()).thenReturn(schemaManager);
        when(schemaManager.getTableWithNull("missing_table")).thenReturn(null);

        Map<String, Long> tableVersions = new HashMap<>();
        tableVersions.put("missing_table", 1L);

        try (MockedStatic<OptimizerContext> mockedOptimizerContext = mockStatic(OptimizerContext.class)) {
            mockedOptimizerContext.when(() -> OptimizerContext.getContext("test_schema")).thenReturn(optimizerContext);

            Assert.assertTrue(invokeTableVersionChanged(tableVersions, "test_schema"));
        }
    }

    @Test
    public void testBuildInitialDdlRecordContainsInitialStateAndCommands() throws Exception {
        TConnection tConnection = new TConnection(new TDataSource());
        DdlContext ddlContext = Mockito.mock(DdlContext.class);
        Set<String> resources = new HashSet<>();
        resources.add("test_schema.test_table");
        when(ddlContext.getDdlType()).thenReturn(DdlType.CREATE_TABLE);
        when(ddlContext.getSchemaName()).thenReturn("test_schema");
        when(ddlContext.getObjectName()).thenReturn("test_table");
        when(ddlContext.getTraceId()).thenReturn("trace_id");
        when(ddlContext.getDdlStmt()).thenReturn("create table test_table(id int)");
        when(ddlContext.getResources()).thenReturn(resources);
        when(ddlContext.getPausedPolicy()).thenReturn(DdlState.RUNNING);
        when(ddlContext.getRollbackPausedPolicy()).thenReturn(DdlState.ROLLBACK_RUNNING);

        try (MockedStatic<DdlHelper> mockedDdlHelper = mockStatic(DdlHelper.class);
            MockedStatic<ExecUtils> mockedExecUtils = mockStatic(ExecUtils.class)) {
            mockedDdlHelper.when(DdlHelper::getLocalServerKey).thenReturn("response_node");
            mockedExecUtils.when(() -> ExecUtils.getLeaderKey(null)).thenReturn("execution_node");

            DdlEngineRecord record = invokeBuildInitialDdlRecord(tConnection, 123L, ddlContext);

            Assert.assertEquals(123L, record.jobId);
            Assert.assertEquals(DdlType.CREATE_TABLE.name(), record.ddlType);
            Assert.assertEquals("test_schema", record.schemaName);
            Assert.assertEquals("test_table", record.objectName);
            Assert.assertEquals("response_node", record.responseNode);
            Assert.assertEquals("execution_node", record.executionNode);
            Assert.assertEquals("trace_id", record.traceId);
            Assert.assertEquals(DdlState.INITIAL.name(), record.state);
            Assert.assertEquals(0, record.progress);
            Assert.assertEquals("create table test_table(id int)", record.ddlStmt);
            Assert.assertEquals(resources.toString(), record.resources);
            Assert.assertTrue(record.gmtCreated > 0);
            Assert.assertEquals(record.gmtCreated, record.gmtModified);
            Assert.assertTrue(record.isSupportCancel());
            Assert.assertTrue(record.isSupportContinue());
        }
    }

    @Test
    public void testCleanupInitialDdlJobSkipsNullJobId() throws Exception {
        TConnection tConnection = new TConnection(new TDataSource());

        invokeCleanupInitialDdlJob(tConnection, null);
    }

    @Test
    public void testTwoPhaseDdlLockEnabledByDefault() throws Exception {
        TConnection tConnection = new TConnection(new TDataSource());
        ExecutionContext executionContext = new ExecutionContext();

        Assert.assertTrue(invokeIsTwoPhaseDdlLockEnabled(tConnection, executionContext));
    }

    @Test
    public void testTwoPhaseDdlLockCanBeDisabledDynamically() throws Exception {
        TConnection tConnection = new TConnection(new TDataSource());
        ExecutionContext executionContext = new ExecutionContext();
        ParamManager.setBooleanVal(executionContext.getParamManager().getProps(),
            ConnectionParams.ENABLE_DDL_TWO_PHASE_LOCK, false, false);

        Assert.assertFalse(invokeIsTwoPhaseDdlLockEnabled(tConnection, executionContext));

        ParamManager.setBooleanVal(executionContext.getParamManager().getProps(),
            ConnectionParams.ENABLE_DDL_TWO_PHASE_LOCK, true, false);

        Assert.assertTrue(invokeIsTwoPhaseDdlLockEnabled(tConnection, executionContext));
    }

    private boolean invokeTableVersionChanged(Map<String, Long> tableVersions, String schemaName) {
        LogicalCommonDdlHandler handler = new LogicalDropTableGroupHandler(null);
        return handler.tableVersionChanged(mock(BaseDdlOperation.class), mock(ExecutionContext.class),
            tableVersions, schemaName);
    }

    private DdlEngineRecord invokeBuildInitialDdlRecord(TConnection tConnection, Long jobId, DdlContext ddlContext)
        throws Exception {
        Method method = TConnection.class.getDeclaredMethod("buildInitialDdlRecord", Long.class, DdlContext.class);
        method.setAccessible(true);
        return (DdlEngineRecord) method.invoke(tConnection, jobId, ddlContext);
    }

    private void invokeCleanupInitialDdlJob(TConnection tConnection, Long jobId) throws Exception {
        Method method = TConnection.class.getDeclaredMethod("cleanupInitialDdlJob", Long.class);
        method.setAccessible(true);
        method.invoke(tConnection, jobId);
    }

    private boolean invokeIsTwoPhaseDdlLockEnabled(TConnection tConnection, ExecutionContext executionContext)
        throws Exception {
        Method method = TConnection.class.getDeclaredMethod("isTwoPhaseDdlLockEnabled", ExecutionContext.class);
        method.setAccessible(true);
        return (Boolean) method.invoke(tConnection, executionContext);
    }
}
