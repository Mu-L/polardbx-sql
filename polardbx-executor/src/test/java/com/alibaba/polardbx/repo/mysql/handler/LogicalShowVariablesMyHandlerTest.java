package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.cdc.ICdcManager;
import com.alibaba.polardbx.common.constants.SequenceAttribute;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.BatchInsertPolicy;
import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.logical.ITConnection;
import com.alibaba.polardbx.common.logical.ITPrepareStatement;
import com.alibaba.polardbx.common.logical.ITStatement;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.type.TransactionType;
import com.alibaba.polardbx.common.utils.InstanceRole;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.spi.ITransactionManager;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.DBVariableRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.sequence.SequenceManagerProxy;
import com.alibaba.polardbx.optimizer.utils.IConnectionHolder;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.optimizer.utils.ITransactionManagerUtil;
import com.alibaba.polardbx.optimizer.utils.InventoryMode;
import com.alibaba.polardbx.repo.mysql.spi.MyRepository;
import com.alibaba.polardbx.stats.CurrentTransactionStatistics;
import com.alibaba.polardbx.stats.TransactionStatistics;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import javax.sql.DataSource;
import java.lang.reflect.Field;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class LogicalShowVariablesMyHandlerTest {
    private MockedStatic<MetaDbInstConfigManager> mockMetaDbInstConfigManager;

    @After
    public void cleanup() {
        if (mockMetaDbInstConfigManager != null) {
            mockMetaDbInstConfigManager.close();
        }
    }

    @Test
    public void testShowTransactionVariables() {
        MyRepository repository = new MyRepository();
        LogicalShowVariablesMyHandler handler = new LogicalShowVariablesMyHandler(repository);
        TreeMap<String, Object> variables = new TreeMap<>();

        ExecutorContext executorContext = mock(ExecutorContext.class);
        ExecutorContext.setContext("polardbx", executorContext);
        ITransactionManager transactionManager = mock(ITransactionManager.class);
        when(executorContext.getTransactionManager()).thenReturn(transactionManager);
        when(transactionManager.supportXaTso()).thenReturn(true);

        ExecutionContext executionContext = new ExecutionContext();
        ITConnection mockTConnection = new MockTConnection();
        executionContext.setConnection(mockTConnection);

        MetaDbInstConfigManager manager = new MockMetaDbInstConfigManager();
        mockMetaDbInstConfigManager = Mockito.mockStatic(MetaDbInstConfigManager.class);
        mockMetaDbInstConfigManager.when(MetaDbInstConfigManager::getInstance).thenAnswer(i -> manager);
        ITransaction trx = new MockTransaction();
        executionContext.setTransaction(trx);

        // Default value.
        handler.collectCnVariables(variables, executionContext);
        for (Map.Entry<String, Object> kv : variables.entrySet()) {
            if ("enable_auto_commit_tso".equalsIgnoreCase(kv.getKey())) {
                Assert.assertEquals(true, kv.getValue());
            }
            if ("enable_auto_savepoint".equalsIgnoreCase(kv.getKey())) {
                Assert.assertEquals(true, kv.getValue());
            }
            if ("enable_transaction_recover_task".equalsIgnoreCase(kv.getKey())) {
                Assert.assertEquals(true, kv.getValue());
            }
            if ("enable_x_proto_opt_for_auto_sp".equalsIgnoreCase(kv.getKey())) {
                Assert.assertEquals(false, kv.getValue());
            }
            if ("enable_xa_tso".equalsIgnoreCase(kv.getKey())) {
                Assert.assertEquals(true, kv.getValue());
            }
            if ("trx_log_method".equalsIgnoreCase(kv.getKey())) {
                Assert.assertEquals(1, kv.getValue());
            }
            if ("trx_class".equalsIgnoreCase(kv.getKey())) {
                Assert.assertTrue("MockTransaction".equalsIgnoreCase((String) kv.getValue()));
            }
        }
    }

    @Test
    public void testForeignKeyChecksBooleanFormat() throws NoSuchFieldException, IllegalAccessException {
        MyRepository repository = new MyRepository();
        LogicalShowVariablesMyHandler handler = new LogicalShowVariablesMyHandler(repository);
        TreeMap<String, Object> variables = new TreeMap<>();

        // Simulate the case where foreign_key_checks is stored as Boolean in serverVariables
        // (SetHandler.parseBool returns Boolean, not String)
        variables.put("foreign_key_checks", Boolean.TRUE);

        ExecutorContext executorContext = mock(ExecutorContext.class);
        ExecutorContext.setContext("polardbx", executorContext);
        ITransactionManager transactionManager = mock(ITransactionManager.class);
        when(executorContext.getTransactionManager()).thenReturn(transactionManager);
        when(transactionManager.supportXaTso()).thenReturn(true);

        ExecutionContext executionContext = new ExecutionContext();
        ITConnection mockTConnection = new MockTConnection();
        executionContext.setConnection(mockTConnection);
        executionContext.setSchemaName("polardbx");
        executionContext.setExtraServerVariables(new HashMap<>());

        MetaDbInstConfigManager manager = new MockMetaDbInstConfigManager();
        mockMetaDbInstConfigManager = Mockito.mockStatic(MetaDbInstConfigManager.class);
        mockMetaDbInstConfigManager.when(MetaDbInstConfigManager::getInstance).thenAnswer(i -> manager);
        ITransaction trx = new MockTransaction();
        executionContext.setTransaction(trx);

        SequenceManagerProxy proxy = mock(SequenceManagerProxy.class);
        when(proxy.areAllSequencesSameType("polardbx",
            new SequenceAttribute.Type[] {SequenceAttribute.Type.GROUP, SequenceAttribute.Type.TIME})).thenReturn(true);
        Field instaceField = SequenceManagerProxy.class.getDeclaredField("instance");
        instaceField.setAccessible(true);
        instaceField.set(null, proxy);

        handler.updateReturnVariables(variables, executionContext);

        // MySQL SHOW VARIABLES should return "ON" / "OFF" for boolean variables,
        // not Java's "true" / "false"
        Assert.assertEquals("ON", variables.get("foreign_key_checks"));

        // Test FALSE case
        variables.put("foreign_key_checks", Boolean.FALSE);
        handler.updateReturnVariables(variables, executionContext);
        Assert.assertEquals("OFF", variables.get("foreign_key_checks"));
    }

    @Test
    public void showSqlLogBinVariablesTest() throws NoSuchFieldException, IllegalAccessException {
        MyRepository repository = new MyRepository();
        LogicalShowVariablesMyHandler handler = new LogicalShowVariablesMyHandler(repository);
        TreeMap<String, Object> variables = new TreeMap<>();

        ExecutorContext executorContext = mock(ExecutorContext.class);
        ExecutorContext.setContext("polardbx", executorContext);
        ITransactionManager transactionManager = mock(ITransactionManager.class);
        when(executorContext.getTransactionManager()).thenReturn(transactionManager);
        when(transactionManager.supportXaTso()).thenReturn(true);
        ExecutionContext executionContext = new ExecutionContext();
        ITConnection mockTConnection = new MockTConnection();
        executionContext.setConnection(mockTConnection);
        Map<String, Object> extraServerVariables = new HashMap<>();
        SequenceManagerProxy proxy = mock(SequenceManagerProxy.class);
        when(proxy.areAllSequencesSameType("polardbx",
            new SequenceAttribute.Type[] {SequenceAttribute.Type.GROUP, SequenceAttribute.Type.TIME})).thenReturn(true);
        Field instaceField = SequenceManagerProxy.class.getDeclaredField("instance");
        instaceField.setAccessible(true);
        instaceField.set(null, proxy);

        executionContext.setExtraServerVariables(extraServerVariables);
        executionContext.setSchemaName("polardbx");
        // Default value.
        handler.updateReturnVariables(variables, executionContext);
        for (Map.Entry<String, Object> kv : variables.entrySet()) {
            if ("sql_log_bin_x".equalsIgnoreCase(kv.getKey())) {
                Assert.assertEquals("ON", kv.getValue());
            }
        }

        extraServerVariables.put(ICdcManager.SQL_LOG_BIN, false);
        executionContext.setExtraServerVariables(extraServerVariables);
        executionContext.setSchemaName("polardbx");
        // Default value.
        handler.updateReturnVariables(variables, executionContext);
        for (Map.Entry<String, Object> kv : variables.entrySet()) {
            if ("sql_log_bin_x".equalsIgnoreCase(kv.getKey())) {
                Assert.assertEquals("OFF", kv.getValue());
            }
        }
    }

    @Test
    public void testCollectDnVariablesNullSessionVars() throws Exception {
        InstanceRole originalRole = ConfigDataMode.getInstanceRole();
        ConfigDataMode.setInstanceRole(InstanceRole.FAST_MOCK);
        try {
            MyRepository repository = new MyRepository();
            LogicalShowVariablesMyHandler handler = new LogicalShowVariablesMyHandler(repository);
            TreeMap<String, Object> variables = new TreeMap<>();
            ExecutionContext executionContext = new ExecutionContext();
            // serverVariables and extraServerVariables are null by default

            MetaDbDataSource mockMetaDbDataSource = mock(MetaDbDataSource.class);
            DataSource mockDataSource = mock(DataSource.class);
            Connection mockConnection = mock(Connection.class);
            when(mockMetaDbDataSource.getDataSource()).thenReturn(mockDataSource);
            when(mockDataSource.getConnection()).thenReturn(mockConnection);

            try (MockedStatic<MetaDbDataSource> metaDbDataSourceMock = mockStatic(MetaDbDataSource.class);
                MockedStatic<MetaDbUtil> metaDbUtilMock = mockStatic(MetaDbUtil.class)) {
                metaDbDataSourceMock.when(MetaDbDataSource::getInstance).thenReturn(mockMetaDbDataSource);
                metaDbUtilMock.when(() -> MetaDbUtil.query(anyString(), any(Class.class), any(Connection.class)))
                    .thenReturn(new ArrayList<>());

                handler.collectDnVariables(null, variables, executionContext);
            }
            // null serverVariables and extraServerVariables should not cause NPE; only GMS vars (empty) in result
            Assert.assertTrue(variables.isEmpty());
        } finally {
            ConfigDataMode.setInstanceRole(originalRole);
        }
    }

    @Test
    public void testCollectDnVariablesServerVarsAreMerged() throws Exception {
        InstanceRole originalRole = ConfigDataMode.getInstanceRole();
        ConfigDataMode.setInstanceRole(InstanceRole.FAST_MOCK);
        try {
            MyRepository repository = new MyRepository();
            LogicalShowVariablesMyHandler handler = new LogicalShowVariablesMyHandler(repository);
            TreeMap<String, Object> variables = new TreeMap<>();
            ExecutionContext executionContext = new ExecutionContext();

            Map<String, Object> serverVariables = new HashMap<>();
            serverVariables.put("wait_timeout", "100");
            executionContext.setServerVariables(serverVariables);

            MetaDbDataSource mockMetaDbDataSource = mock(MetaDbDataSource.class);
            DataSource mockDataSource = mock(DataSource.class);
            Connection mockConnection = mock(Connection.class);
            when(mockMetaDbDataSource.getDataSource()).thenReturn(mockDataSource);
            when(mockDataSource.getConnection()).thenReturn(mockConnection);

            try (MockedStatic<MetaDbDataSource> metaDbDataSourceMock = mockStatic(MetaDbDataSource.class);
                MockedStatic<MetaDbUtil> metaDbUtilMock = mockStatic(MetaDbUtil.class)) {
                metaDbDataSourceMock.when(MetaDbDataSource::getInstance).thenReturn(mockMetaDbDataSource);
                metaDbUtilMock.when(() -> MetaDbUtil.query(anyString(), any(Class.class), any(Connection.class)))
                    .thenReturn(new ArrayList<>());

                handler.collectDnVariables(null, variables, executionContext);
            }
            Assert.assertEquals("100", variables.get("wait_timeout"));
        } finally {
            ConfigDataMode.setInstanceRole(originalRole);
        }
    }

    @Test
    public void testCollectDnVariablesExtraVarsIncludeLegacyWhenNotDisabled() throws Exception {
        InstanceRole originalRole = ConfigDataMode.getInstanceRole();
        ConfigDataMode.setInstanceRole(InstanceRole.FAST_MOCK);
        try {
            MyRepository repository = new MyRepository();
            LogicalShowVariablesMyHandler handler = new LogicalShowVariablesMyHandler(repository);
            TreeMap<String, Object> variables = new TreeMap<>();
            ExecutionContext executionContext = new ExecutionContext();

            Map<String, Object> extraServerVariables = new HashMap<>();
            extraServerVariables.put("transaction policy", "XA");
            executionContext.setExtraServerVariables(extraServerVariables);

            MetaDbDataSource mockMetaDbDataSource = mock(MetaDbDataSource.class);
            DataSource mockDataSource = mock(DataSource.class);
            Connection mockConnection = mock(Connection.class);
            when(mockMetaDbDataSource.getDataSource()).thenReturn(mockDataSource);
            when(mockDataSource.getConnection()).thenReturn(mockConnection);

            DynamicConfig mockDynamicConfig = mock(DynamicConfig.class);
            when(mockDynamicConfig.isDisableLegacyVariable()).thenReturn(false);

            try (MockedStatic<MetaDbDataSource> metaDbDataSourceMock = mockStatic(MetaDbDataSource.class);
                MockedStatic<MetaDbUtil> metaDbUtilMock = mockStatic(MetaDbUtil.class);
                MockedStatic<DynamicConfig> dynamicConfigMock = mockStatic(DynamicConfig.class)) {
                metaDbDataSourceMock.when(MetaDbDataSource::getInstance).thenReturn(mockMetaDbDataSource);
                metaDbUtilMock.when(() -> MetaDbUtil.query(anyString(), any(Class.class), any(Connection.class)))
                    .thenReturn(new ArrayList<>());
                dynamicConfigMock.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);

                handler.collectDnVariables(null, variables, executionContext);
            }
            // legacy variable should be included when isDisableLegacyVariable() = false
            Assert.assertTrue(variables.containsKey("transaction policy"));
            Assert.assertEquals("XA", variables.get("transaction policy"));
        } finally {
            ConfigDataMode.setInstanceRole(originalRole);
        }
    }

    @Test
    public void testCollectDnVariablesExtraVarsExcludeLegacyWhenDisabled() throws Exception {
        InstanceRole originalRole = ConfigDataMode.getInstanceRole();
        ConfigDataMode.setInstanceRole(InstanceRole.FAST_MOCK);
        try {
            MyRepository repository = new MyRepository();
            LogicalShowVariablesMyHandler handler = new LogicalShowVariablesMyHandler(repository);
            TreeMap<String, Object> variables = new TreeMap<>();
            ExecutionContext executionContext = new ExecutionContext();

            Map<String, Object> extraServerVariables = new HashMap<>();
            extraServerVariables.put("transaction policy", "XA");
            extraServerVariables.put("character_set_client", "utf8");
            executionContext.setExtraServerVariables(extraServerVariables);

            MetaDbDataSource mockMetaDbDataSource = mock(MetaDbDataSource.class);
            DataSource mockDataSource = mock(DataSource.class);
            Connection mockConnection = mock(Connection.class);
            when(mockMetaDbDataSource.getDataSource()).thenReturn(mockDataSource);
            when(mockDataSource.getConnection()).thenReturn(mockConnection);

            DynamicConfig mockDynamicConfig = mock(DynamicConfig.class);
            when(mockDynamicConfig.isDisableLegacyVariable()).thenReturn(true);

            try (MockedStatic<MetaDbDataSource> metaDbDataSourceMock = mockStatic(MetaDbDataSource.class);
                MockedStatic<MetaDbUtil> metaDbUtilMock = mockStatic(MetaDbUtil.class);
                MockedStatic<DynamicConfig> dynamicConfigMock = mockStatic(DynamicConfig.class)) {
                metaDbDataSourceMock.when(MetaDbDataSource::getInstance).thenReturn(mockMetaDbDataSource);
                metaDbUtilMock.when(() -> MetaDbUtil.query(anyString(), any(Class.class), any(Connection.class)))
                    .thenReturn(new ArrayList<>());
                dynamicConfigMock.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);

                handler.collectDnVariables(null, variables, executionContext);
            }
            // "transaction policy" is a LEGACY_VARIABLE, must be filtered
            Assert.assertFalse(variables.containsKey("transaction policy"));
            // "character_set_client" is not legacy, must be present
            Assert.assertTrue(variables.containsKey("character_set_client"));
        } finally {
            ConfigDataMode.setInstanceRole(originalRole);
        }
    }

    @Test
    public void testCollectDnVariablesGmsVarsOverrideServerVars() throws Exception {
        InstanceRole originalRole = ConfigDataMode.getInstanceRole();
        ConfigDataMode.setInstanceRole(InstanceRole.FAST_MOCK);
        try {
            MyRepository repository = new MyRepository();
            LogicalShowVariablesMyHandler handler = new LogicalShowVariablesMyHandler(repository);
            TreeMap<String, Object> variables = new TreeMap<>();
            ExecutionContext executionContext = new ExecutionContext();

            // CN session var
            Map<String, Object> serverVariables = new HashMap<>();
            serverVariables.put("wait_timeout", "100");
            executionContext.setServerVariables(serverVariables);

            // GMS returns a different value for the same variable
            DBVariableRecord record = new DBVariableRecord();
            record.variableName = "wait_timeout";
            record.value = "28800";
            List<DBVariableRecord> gmsRecords = new ArrayList<>();
            gmsRecords.add(record);

            MetaDbDataSource mockMetaDbDataSource = mock(MetaDbDataSource.class);
            DataSource mockDataSource = mock(DataSource.class);
            Connection mockConnection = mock(Connection.class);
            when(mockMetaDbDataSource.getDataSource()).thenReturn(mockDataSource);
            when(mockDataSource.getConnection()).thenReturn(mockConnection);

            try (MockedStatic<MetaDbDataSource> metaDbDataSourceMock = mockStatic(MetaDbDataSource.class);
                MockedStatic<MetaDbUtil> metaDbUtilMock = mockStatic(MetaDbUtil.class)) {
                metaDbDataSourceMock.when(MetaDbDataSource::getInstance).thenReturn(mockMetaDbDataSource);
                metaDbUtilMock.when(() -> MetaDbUtil.query(anyString(), any(Class.class), any(Connection.class)))
                    .thenReturn(gmsRecords);

                handler.collectDnVariables(null, variables, executionContext);
            }
            // GMS value should override CN session var
            Assert.assertEquals("28800", variables.get("wait_timeout"));
        } finally {
            ConfigDataMode.setInstanceRole(originalRole);
        }
    }

    private static class MockTransaction implements ITransaction {

        @Override
        public long getId() {
            return 0;
        }

        @Override
        public void commit() {

        }

        @Override
        public void rollback() {

        }

        @Override
        public ExecutionContext getExecutionContext() {
            return null;
        }

        @Override
        public IConnectionHolder getConnectionHolder() {
            return null;
        }

        @Override
        public void tryClose(IConnection conn, String groupName) throws SQLException {

        }

        @Override
        public void tryClose() throws SQLException {

        }

        @Override
        public IConnection getConnection(String schemaName, String group, IDataSource ds, RW rw) throws SQLException {
            return null;
        }

        @Override
        public IConnection getConnection(String schemaName, String group, IDataSource ds, RW rw, ExecutionContext ec)
            throws SQLException {
            return null;
        }

        @Override
        public IConnection getConnection(String schemaName, String group, Long grpConnId, IDataSource ds, RW rw,
                                         ExecutionContext ec) throws SQLException {
            return null;
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void close() {

        }

        @Override
        public void setTraceId(String traceId) {

        }

        @Override
        public void updateStatisticsWhenStatementFinished(AtomicLong rowCount) {

        }

        @Override
        public void setMdlWaitTime(long mdlWaitTime) {

        }

        @Override
        public void setStartTimeInMs(long startTime) {

        }

        @Override
        public void setStartTime(long startTime) {

        }

        @Override
        public void setSqlStartTime(long sqlStartTime) {

        }

        @Override
        public void setSqlFinishTime(long t) {

        }

        @Override
        public void kill() throws SQLException {

        }

        @Override
        public void savepoint(String savepoint) {

        }

        @Override
        public void rollbackTo(String savepoint) {

        }

        @Override
        public void release(String savepoint) {

        }

        @Override
        public void clearTrxContext() {

        }

        @Override
        public void setCrucialError(ErrorCode errorCode, String cause) {

        }

        @Override
        public ErrorCode getCrucialError() {
            return null;
        }

        @Override
        public void checkCanContinue() {

        }

        @Override
        public boolean isDistributed() {
            return false;
        }

        @Override
        public boolean isDistributedWriteTrx() {
            return false;
        }

        @Override
        public State getState() {
            return null;
        }

        @Override
        public ITransactionPolicy.TransactionClass getTransactionClass() {
            return null;
        }

        @Override
        public long getStartTimeInMs() {
            return 0;
        }

        @Override
        public boolean isBegun() {
            return false;
        }

        @Override
        public InventoryMode getInventoryMode() {
            return ITransaction.super.getInventoryMode();
        }

        @Override
        public void setInventoryMode(InventoryMode inventoryMode) {

        }

        @Override
        public ITransactionManagerUtil getTransactionManagerUtil() {
            return null;
        }

        @Override
        public boolean handleStatementError(Throwable t, String traceId) {
            return false;
        }

        @Override
        public void releaseAutoSavepoint(String traceId) {

        }

        @Override
        public boolean isUnderCommitting() {
            return false;
        }

        @Override
        public boolean isAsyncCommit() {
            return false;
        }

        @Override
        public void updateCurrentStatistics(CurrentTransactionStatistics stat, long durationTimeMs) {
            ITransaction.super.updateCurrentStatistics(stat, durationTimeMs);
        }

        @Override
        public TransactionStatistics getStat() {
            return null;
        }

        @Override
        public TransactionType getType() {
            return null;
        }

        @Override
        public boolean isRwTransaction() {
            return ITransaction.super.isRwTransaction();
        }

        @Override
        public void setLastActiveTime() {

        }

        @Override
        public long getLastActiveTime() {
            return 0;
        }

        @Override
        public void resetLastActiveTime() {

        }

        @Override
        public long getIdleTimeout() {
            return 0;
        }

        @Override
        public long getIdleROTimeout() {
            return 0;
        }

        @Override
        public long getIdleRWTimeout() {
            return 0;
        }

        @Override
        public void clearFlashbackArea() {
            ITransaction.super.clearFlashbackArea();
        }

        @Override
        public String getUser() {
            return "polardbx_root";
        }

        @Override
        public void releaseDirtyReadConnections() {
        }
    }

    private static class MockMetaDbInstConfigManager extends MetaDbInstConfigManager {
        @Override
        protected void doInit() {
        }

        public void setProperties(Properties properties) {
            this.propertiesInfoMap = properties;
        }
    }

    private static class MockTConnection implements ITConnection {
        @Override
        public ITStatement createStatement() throws SQLException {
            return null;
        }

        @Override
        public ITPrepareStatement prepareStatement(String sql) throws SQLException {
            return null;
        }

        @Override
        public boolean isClosed() {
            return false;
        }

        @Override
        public void close() throws SQLException {

        }

        @Override
        public long getLastInsertId() {
            return 0;
        }

        @Override
        public void setLastInsertId(long id) {

        }

        @Override
        public long getReturnedLastInsertId() {
            return 0;
        }

        @Override
        public void setReturnedLastInsertId(long id) {

        }

        @Override
        public String getUser() {
            return null;
        }

        @Override
        public ITransactionPolicy getTrxPolicy() {
            return ITransactionPolicy.TSO;
        }

        @Override
        public void setTrxPolicy(ITransactionPolicy trxPolicy, boolean check) {

        }

        @Override
        public BatchInsertPolicy getBatchInsertPolicy(Map<String, Object> extraCmds) {
            return BatchInsertPolicy.NONE;
        }

        @Override
        public void setBatchInsertPolicy(BatchInsertPolicy policy) {

        }

        @Override
        public long getFoundRows() {
            return 0;
        }

        @Override
        public void setFoundRows(long foundRows) {

        }

        @Override
        public long getAffectedRows() {
            return 0;
        }

        @Override
        public void setAffectedRows(long affectedRows) {

        }

        @Override
        public List<Long> getGeneratedKeys() {
            return null;
        }

        @Override
        public void setGeneratedKeys(List<Long> ids) {

        }

        @Override
        public boolean isMppConnection() {
            return false;
        }
    }
}
