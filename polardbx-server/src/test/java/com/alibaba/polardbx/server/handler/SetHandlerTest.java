package com.alibaba.polardbx.server.handler;

import com.alibaba.polardbx.common.TddlConstants;
import com.alibaba.polardbx.common.cdc.ICdcManager;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.jdbc.ITransactionPolicy;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.InstanceRole;
import com.alibaba.polardbx.common.utils.TStringUtil;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingAccessor;
import com.alibaba.polardbx.gms.metadb.table.ColumnarTableMappingRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.net.buffer.ByteBufferHolder;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.server.ServerConnection;
import lombok.Data;
import org.apache.calcite.sql.SqlNode;
import org.hamcrest.Matchers;
import org.jetbrains.annotations.NotNull;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BiFunction;

import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.ALLOW_READ_CROSS_DB;
import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.ARCHIVE;
import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.BEST_EFFORT;
import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.COLUMNAR_TRANSACTION;
import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.FREE;
import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.IGNORE_BINLOG_TRANSACTION;
import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.NO_TRANSACTION;
import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.TSO;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class SetHandlerTest {
    @Test
    public void test0() {
        ServerConnection serverConnection = mock(ServerConnection.class);
        Map<String, Object> map = new HashMap<>();
        map.put(ConnectionProperties.IGNORE_TRANSACTION_POLICY_NO_TRANSACTION, "true");
        when(serverConnection.getConnectionVariables()).thenReturn(map);
        Assert.assertTrue(SetHandler.isIgnoreSettingNoTransaction(4, serverConnection));
    }

    @Test
    public void test1() {
        ServerConnection serverConnection = mock(ServerConnection.class);
        Map<String, Object> map = new HashMap<>();
        when(serverConnection.getConnectionVariables()).thenReturn(map);
        ExecutionContext ec = new ExecutionContext();
        when(serverConnection.getExecutionContext()).thenReturn(ec);
        try (MockedStatic<InstConfUtil> mockedStatic = mockStatic(InstConfUtil.class)) {
            mockedStatic.when(() -> InstConfUtil.getBool(ConnectionParams.IGNORE_TRANSACTION_POLICY_NO_TRANSACTION))
                .thenReturn(true);
            Assert.assertTrue(SetHandler.isIgnoreSettingNoTransaction(4, serverConnection));
        }
    }

    @Test
    public void test2() {
        ServerConnection serverConnection = mock(ServerConnection.class);
        Map<String, Object> map = new HashMap<>();
        when(serverConnection.getConnectionVariables()).thenReturn(map);
        ExecutionContext ec = new ExecutionContext();
        when(serverConnection.getExecutionContext()).thenReturn(ec);
        Map<String, String> props = new HashMap<>();
        props.put(ConnectionProperties.IGNORE_TRANSACTION_POLICY_NO_TRANSACTION, "true");
        ParamManager paramManager = new ParamManager(props);
        ec.setParamManager(paramManager);
        Assert.assertTrue(SetHandler.isIgnoreSettingNoTransaction(4, serverConnection));
    }

    @Test
    public void setPushDownRangeLimit() {
        try {
            ConfigDataMode.setInstanceRole(InstanceRole.FAST_MOCK);
            final ServerConnection serverConn = mockServerConnection();
            HashMap<String, Object> serverVariables = new HashMap<>();
            when(serverConn.getServerVariables()).thenReturn(serverVariables);
            ByteString sql = ByteString.from("set PUSHDOWN_RANGE_LIMIT=true");
            SetHandler.handleV2(sql, serverConn, 0, false, false);
            Assert.assertEquals(true,
                serverConn.getConnectionVariables().get(ConnectionProperties.PUSHDOWN_RANGE_LIMIT));
            Assert.assertEquals(true, serverConn.getServerVariables().get(
                ConnectionProperties.PUSHDOWN_RANGE_LIMIT.toLowerCase()));

            sql = ByteString.from("set global PUSHDOWN_RANGE_LIMIT=false");
            SetHandler.handleV2(sql, serverConn, 0, false, false);
            Assert.assertEquals(false,
                serverConn.getConnectionVariables().get(ConnectionProperties.PUSHDOWN_RANGE_LIMIT));
            Assert.assertEquals(false, serverConn.getServerVariables().get(
                ConnectionProperties.PUSHDOWN_RANGE_LIMIT.toLowerCase()));
        } finally {
            ConfigDataMode.setInstanceRole(InstanceRole.MASTER);
        }
    }

    @Test
    public void setSqlLogBinXTest() throws NoSuchFieldException, IllegalAccessException {
        try {
            ServerConnection serverConnection = mock(ServerConnection.class);
            when(serverConnection.initOptimizerContext()).thenReturn(true);
            ByteBuffer buffer = ByteBuffer.allocate(1024);
            ByteBufferHolder holder = new ByteBufferHolder(buffer);
            when(serverConnection.allocate()).thenReturn(holder);
            when(serverConnection.checkWriteBuffer(holder, 1)).thenReturn(holder);
            HashMap<String, Object> extraServerVariables = new HashMap<>();
            when(serverConnection.getExtraServerVariables()).thenReturn(extraServerVariables);
            ByteString byteString = new ByteString("set sql_log_bin=off".getBytes(), Charset.defaultCharset());
            ConfigDataMode.setInstanceRole(InstanceRole.FAST_MOCK);
            SetHandler.handleV2(byteString, serverConnection, 0, false);
            Assert.assertEquals(false, serverConnection.getExtraServerVariables().get(ICdcManager.SQL_LOG_BIN));

            byteString = new ByteString("set sql_log_bin=on".getBytes(), Charset.defaultCharset());
            ConfigDataMode.setInstanceRole(InstanceRole.FAST_MOCK);
            SetHandler.handleV2(byteString, serverConnection, 0, false);
            Assert.assertEquals(true, serverConnection.getExtraServerVariables().get(ICdcManager.SQL_LOG_BIN));
        } finally {
            ConfigDataMode.setInstanceRole(InstanceRole.MASTER);
        }

    }

    @Test
    public void setColumnarVersionChainPrunerTest() {
        final ServerConnection serverConnection = mockServerConnection();
        try (MockedStatic<MetaDbUtil> metaDbUtilMock = mockStatic(MetaDbUtil.class)) {
            Connection mockConnection = mock(Connection.class);
            metaDbUtilMock.when(MetaDbUtil::getConnection).thenReturn(mockConnection);
            List<ColumnarTableMappingRecord> mockRecords1 = new ArrayList<>();
            ColumnarTableMappingRecord record1 = new ColumnarTableMappingRecord();
            record1.tableId = 1L;
            mockRecords1.add(record1);

            List<ColumnarTableMappingRecord> mockRecords2 = new ArrayList<>();
            ColumnarTableMappingRecord record2 = new ColumnarTableMappingRecord();
            record2.tableId = 2L;
            mockRecords2.add(record2);
            ConfigDataMode.setInstanceRole(InstanceRole.FAST_MOCK);
            ByteString byteString =
                new ByteString(
                    "set global columnar_version_chain_pruner=\"schema1.1:7,schema2.2:14\"".getBytes(),
                    Charset.defaultCharset());
            try (MockedConstruction<ColumnarTableMappingAccessor> accessorConstruction =
                mockConstruction(ColumnarTableMappingAccessor.class, (mock, context) -> {
                    when(mock.querySchemaTableId(eq("schema1"), anyLong())).thenReturn(mockRecords1);
                    when(mock.querySchemaTableId(eq("schema2"), anyLong())).thenReturn(mockRecords2);
                })) {
                SetHandler.handleV2(byteString, serverConnection, 0, false);
            }
        } finally {
            ConfigDataMode.setInstanceRole(InstanceRole.MASTER);
        }
    }

    @Test
    public void strictSetGlobalTest() {

        try (MockedStatic<InstConfUtil> instUtilMockedStatic = Mockito.mockStatic(InstConfUtil.class)) {

            when(InstConfUtil.getValBool(TddlConstants.ENABLE_STRICT_SET_GLOBAL)).thenReturn(true);
            ServerConnection serverConnection = mock(ServerConnection.class);
            AtomicReference<String> msgRef = new AtomicReference<>(null);
            Mockito.doAnswer((invocation) -> {
                String msg = invocation.getArgument(1, String.class);
                msgRef.set(msg);
                return null;
            }).when(serverConnection).writeErrMessage(any(ErrorCode.class), anyString());

            List list = new ArrayList();
            SetHandler.handleGlobalVariable(serverConnection, list, list, list);

            Assert.assertNotNull(msgRef.get());
            Assert.assertTrue(msgRef.get(), msgRef.get().contains("is not allowed"));

        }
    }

    @Test
    public void setTransactionPolicyTest() {
        final ServerConnection serverConnection = mockServerConnection();
        try {
            ConfigDataMode.setInstanceRole(InstanceRole.FAST_MOCK);

            checkSetTransactionPolicyResult("drds_transaction_policy", (v, k) -> String.format("set %s=%s", k, v),
                serverConnection);
            checkSetTransactionPolicyResult("trans.policy", (v, k) -> String.format("set %s=%s", k, v),
                serverConnection);
            checkSetTransactionPolicyResult("transaction policy", (v, k) -> String.format("set %s %s", k, v),
                serverConnection);
            checkSetTransactionPolicyResult("\"transaction policy\"", (v, k) -> String.format("set %s=%s", k, v),
                serverConnection);

        } finally {
            ConfigDataMode.setInstanceRole(InstanceRole.MASTER);
        }
    }

    private static @NotNull ServerConnection mockServerConnection() {
        final ServerConnection serverConnection = mock(ServerConnection.class);
        when(serverConnection.initOptimizerContext()).thenReturn(true);
        when(serverConnection.getVarStringValue(any(SqlNode.class))).thenCallRealMethod();
        when(serverConnection.getVarIntegerValue(any())).thenCallRealMethod();
        Mockito.doCallRealMethod().when(serverConnection).setTrxPolicy(any(ITransactionPolicy.class));
        when(serverConnection.getTrxPolicy()).thenCallRealMethod();
        doCallRealMethod().when(serverConnection).writeErrMessage(any(ErrorCode.class), anyString());
        final Map<String, Object> connectionVariables = new HashMap<>();
        when(serverConnection.getConnectionVariables()).thenReturn(connectionVariables);
        connectionVariables.put(ConnectionProperties.IGNORE_TRANSACTION_POLICY_NO_TRANSACTION, "false");
        final ByteBuffer buffer = ByteBuffer.allocate(1024);
        final ByteBufferHolder holder = new ByteBufferHolder(buffer);
        when(serverConnection.allocate()).thenReturn(holder);
        when(serverConnection.checkWriteBuffer(holder, 1)).thenReturn(holder);
        final HashMap<String, Object> extraServerVariables = new HashMap<>();
        when(serverConnection.getExtraServerVariables()).thenReturn(extraServerVariables);
        when(serverConnection.setCharset(anyString())).thenReturn(true);
        when(serverConnection.setConnectionCharset(anyString())).thenReturn(true);
        when(serverConnection.setResultSetCharset(anyString())).thenReturn(true);
        return serverConnection;
    }

    @Data
    private static final class SetTransactionPolicyResult {
        // ServerConnection::trxPolicy
        final ITransactionPolicy trxPolicy;
        // TRANSACTION POLICY
        final int intPolicy;
        // TRANS.POLICY / DRDS_TRANSACTION_POLICY
        final String stringPolicy;

        final String setStatement;
    }

    private static SetTransactionPolicyResult expectedSetTransactionPolicyResult(String key,
                                                                                 String valueStringPolicy,
                                                                                 int valueIntPolicy,
                                                                                 String currentStringPolicy,
                                                                                 Integer currentIntPolicy,
                                                                                 BiFunction<String, String, String> setGenerator) {
        // SET TRANS.POLICY/DRDS_TRANSACTION_POLICY = TSO
        final boolean stringPolicy = !TStringUtil.containsIgnoreCase(key, "transaction policy");
        // SET TRANSACTION POLICY 4
        final boolean setTransactionPolicy = TStringUtil.equalsIgnoreCase(key, "transaction policy");
        // SET "TRANSACTION POLICY" = 4
        final boolean setTransactionBlankPolicy = TStringUtil.equalsIgnoreCase(key, "\"transaction policy\"");

        if (setTransactionPolicy) {
            // set with int policy
            // set ServerConnection::trxPolicy only
            final ITransactionPolicy trxPolicy = ITransactionPolicy.of(valueIntPolicy);
            return new SetTransactionPolicyResult(trxPolicy,
                currentIntPolicy,
                currentStringPolicy,
                setGenerator.apply(String.valueOf(valueIntPolicy), key));
        } else if (setTransactionBlankPolicy) {
            // set with int policy
            // set ServerConnection::trxPolicy, TRANSACTION POLICY, TRANS.POLICY, DRDS_TRANSACTION_POLICY
            final ITransactionPolicy trxPolicy = ITransactionPolicy.of(valueIntPolicy);
            return new SetTransactionPolicyResult(trxPolicy,
                valueIntPolicy, // set to origin int policy value
                trxPolicy.toString(),
                setGenerator.apply(String.valueOf(valueIntPolicy), key));
        } else if (stringPolicy) {
            // set with string policy
            // set ServerConnection::trxPolicy, TRANSACTION POLICY, TRANS.POLICY, DRDS_TRANSACTION_POLICY
            final ITransactionPolicy trxPolicy = ITransactionPolicy.of(valueStringPolicy);
            return new SetTransactionPolicyResult(trxPolicy,
                trxPolicy.getIntPolicy(),
                valueStringPolicy.toUpperCase(),
                setGenerator.apply(valueStringPolicy, key));
        }

        throw new IllegalArgumentException("Unexpected set with key: " + key);
    }

    private static void checkSetTransactionPolicyResult(String key,
                                                        BiFunction<String, String, String> setGenerator,
                                                        ServerConnection serverConnection) {
        // SET TRANS.POLICY/DRDS_TRANSACTION_POLICY = TSO
        final boolean stringPolicy = !TStringUtil.containsIgnoreCase(key, "transaction policy");
        // SET "TRANSACTION POLICY" = 4
        final boolean setTransactionBlankPolicy = TStringUtil.equalsIgnoreCase(key, "\"transaction policy\"");

        setAncCheckError(setGenerator.apply(stringPolicy ? "TDDL" : "10086", key),
            setTransactionBlankPolicy ? "transaction policy" : key);

        final Integer currentIntPolicy =
            Optional.ofNullable(serverConnection.getExtraServerVariables().get("TRANSACTION POLICY".toLowerCase()))
                .map(v -> Integer.parseInt(v.toString())).orElse(-1);
        final String currentTransDotPolicy =
            Optional.ofNullable(serverConnection.getExtraServerVariables().get("TRANS.POLICY".toLowerCase()))
                .map(Object::toString).orElse("");

        // IGNORE_TRANSACTION_POLICY_NO_TRANSACTION = true only works for set transaction policy 4
        serverConnection.getConnectionVariables()
            .put(ConnectionProperties.IGNORE_TRANSACTION_POLICY_NO_TRANSACTION, "true");
        String valueStringPolicy = "BEST_EFFORT";
        int valueIntPolicy = BEST_EFFORT.getIntPolicy();
        setAndCheckTransactionPolicyResult(expectedSetTransactionPolicyResult(key,
                valueStringPolicy,
                valueIntPolicy,
                currentTransDotPolicy,
                currentIntPolicy,
                setGenerator),
            serverConnection);

        valueStringPolicy = "2PC";
        valueIntPolicy = BEST_EFFORT.getIntPolicy();
        setAndCheckTransactionPolicyResult(expectedSetTransactionPolicyResult(key,
                valueStringPolicy,
                valueIntPolicy,
                currentTransDotPolicy,
                currentIntPolicy,
                setGenerator),
            serverConnection);

        valueStringPolicy = "FLEXIBLE";
        valueIntPolicy = 2;
        setAndCheckTransactionPolicyResult(expectedSetTransactionPolicyResult(key,
                valueStringPolicy,
                valueIntPolicy,
                currentTransDotPolicy,
                currentIntPolicy,
                setGenerator),
            serverConnection);

        valueStringPolicy = "TSO";
        valueIntPolicy = TSO.getIntPolicy();
        setAndCheckTransactionPolicyResult(expectedSetTransactionPolicyResult(key,
                valueStringPolicy,
                valueIntPolicy,
                currentTransDotPolicy,
                currentIntPolicy,
                setGenerator),
            serverConnection);

        valueStringPolicy = "FREE";
        valueIntPolicy = FREE.getIntPolicy();
        setAndCheckTransactionPolicyResult(expectedSetTransactionPolicyResult(key,
                valueStringPolicy,
                valueIntPolicy,
                currentTransDotPolicy,
                currentIntPolicy,
                setGenerator),
            serverConnection);

        valueStringPolicy = "ALLOW_READ_CROSS_DB";
        valueIntPolicy = ALLOW_READ_CROSS_DB.getIntPolicy();
        setAndCheckTransactionPolicyResult(expectedSetTransactionPolicyResult(key,
                valueStringPolicy,
                valueIntPolicy,
                currentTransDotPolicy,
                currentIntPolicy,
                setGenerator),
            serverConnection);

        valueStringPolicy = "ALLOW_READ";
        valueIntPolicy = ALLOW_READ_CROSS_DB.getIntPolicy();
        setAndCheckTransactionPolicyResult(expectedSetTransactionPolicyResult(key,
                valueStringPolicy,
                valueIntPolicy,
                currentTransDotPolicy,
                currentIntPolicy,
                setGenerator),
            serverConnection);

        // IGNORE_TRANSACTION_POLICY_NO_TRANSACTION = true only works for set transaction policy 4
        serverConnection.getConnectionVariables()
            .put(ConnectionProperties.IGNORE_TRANSACTION_POLICY_NO_TRANSACTION, "true");
        if (stringPolicy) {
            valueStringPolicy = "NO_TRANSACTION";
            valueIntPolicy = NO_TRANSACTION.getIntPolicy();
        }
        setAndCheckTransactionPolicyResult(expectedSetTransactionPolicyResult(key,
                valueStringPolicy,
                valueIntPolicy,
                currentTransDotPolicy,
                currentIntPolicy,
                setGenerator),
            serverConnection);

        serverConnection.getConnectionVariables()
            .put(ConnectionProperties.IGNORE_TRANSACTION_POLICY_NO_TRANSACTION, "false");
        valueStringPolicy = "NO_TRANSACTION";
        valueIntPolicy = NO_TRANSACTION.getIntPolicy();
        setAndCheckTransactionPolicyResult(expectedSetTransactionPolicyResult(key,
                valueStringPolicy,
                valueIntPolicy,
                currentTransDotPolicy,
                currentIntPolicy,
                setGenerator),
            serverConnection);

        valueStringPolicy = "ARCHIVE";
        valueIntPolicy = ARCHIVE.getIntPolicy();
        setAndCheckTransactionPolicyResult(expectedSetTransactionPolicyResult(key,
                valueStringPolicy,
                valueIntPolicy,
                currentTransDotPolicy,
                currentIntPolicy,
                setGenerator),
            serverConnection);

        valueStringPolicy = "IGNORE_BINLOG_TRANSACTION";
        valueIntPolicy = IGNORE_BINLOG_TRANSACTION.getIntPolicy();
        setAndCheckTransactionPolicyResult(expectedSetTransactionPolicyResult(key,
                valueStringPolicy,
                valueIntPolicy,
                currentTransDotPolicy,
                currentIntPolicy,
                setGenerator),
            serverConnection);

        valueStringPolicy = "COLUMNAR_TRANSACTION";
        valueIntPolicy = COLUMNAR_TRANSACTION.getIntPolicy();
        setAndCheckTransactionPolicyResult(expectedSetTransactionPolicyResult(key,
                valueStringPolicy,
                valueIntPolicy,
                currentTransDotPolicy,
                currentIntPolicy,
                setGenerator),
            serverConnection);
    }

    private static void setAncCheckError(String setSql, String errKey) {
        final ServerConnection serverConn = mockServerConnection();

        final ByteString sql = ByteString.from(setSql);
        SetHandler.handleV2(sql, serverConn, 0, false, false);

        verify(serverConn).writeErrMessage(eq(ErrorCode.ER_WRONG_VALUE_FOR_VAR), contains(errKey));
    }

    private static void setAndCheckTransactionPolicyResult(SetTransactionPolicyResult setResult,
                                                           ServerConnection serverConnection) {
        final ByteString sql = ByteString.from(setResult.getSetStatement());
        SetHandler.handleV2(sql, serverConnection, 0, false);

        Assert.assertThat(sql + " : ServerConnection::trxPolicy",
            serverConnection.getTrxPolicy(),
            Matchers.is(setResult.getTrxPolicy()));

        Assert.assertThat(sql + " : transaction policy",
            serverConnection.getExtraServerVariables().get("transaction policy"),
            Matchers.is(setResult.getIntPolicy()));

        Assert.assertThat(sql + " : trans.policy",
            serverConnection.getExtraServerVariables().get("trans.policy").toString(),
            Matchers.is(setResult.getStringPolicy()));

        Assert.assertThat(sql + " : drds_transaction_policy",
            serverConnection.getExtraServerVariables().get("drds_transaction_policy").toString(),
            Matchers.is(setResult.getStringPolicy()));
    }

    /**
     * Test for fix #72722208: CHARACTER_SET_RESULTS should be empty string when set to default/null
     */
    @Test
    public void testSetCharacterSetResultsWithDefaultValue() {
        try {
            ConfigDataMode.setInstanceRole(InstanceRole.FAST_MOCK);
            ServerConnection serverConnection = mockServerConnection();

            DynamicConfig mockDynamicConfig = mock(DynamicConfig.class);

            try (MockedStatic<DynamicConfig> mockedStatic = mockStatic(DynamicConfig.class)) {
                mockedStatic.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);
                when(mockDynamicConfig.isCompatibleCharsetVariables()).thenReturn(false);
                testSetCharacterSetResults(mockDynamicConfig, serverConnection);

                when(mockDynamicConfig.isCompatibleCharsetVariables()).thenReturn(true);
                testSetCharacterSetResults(mockDynamicConfig, serverConnection);
            }
        } finally {
            ConfigDataMode.setInstanceRole(InstanceRole.MASTER);
        }
    }

    private static void testSetCharacterSetResults(DynamicConfig mockDynamicConfig, ServerConnection serverConnection) {
        when(mockDynamicConfig.isCompatibleCharsetVariables()).thenReturn(false);

        // Test set CHARACTER_SET_RESULTS = default
        ByteString sql = ByteString.from("set character_set_results=default");
        SetHandler.handleV2(sql, serverConnection, 0, false);
        Assert.assertEquals("CHARACTER_SET_RESULTS should be utf8 when set to default",
            "utf8", serverConnection.getExtraServerVariables().get("character_set_results"));

        // Test set CHARACTER_SET_RESULTS = null
        sql = ByteString.from("set character_set_results=null");
        SetHandler.handleV2(sql, serverConnection, 0, false);
        Assert.assertEquals("CHARACTER_SET_RESULTS should be empty string when set to null",
            "", serverConnection.getExtraServerVariables().get("character_set_results"));

        // Test set CHARACTER_SET_RESULTS = utf8
        sql = ByteString.from("set character_set_results=utf8");
        SetHandler.handleV2(sql, serverConnection, 0, false);
        Assert.assertEquals("CHARACTER_SET_RESULTS should be utf8 when set to utf8",
            "utf8", serverConnection.getExtraServerVariables().get("character_set_results"));

        // Test set CHARACTER_SET_RESULTS = utf8mb4
        sql = ByteString.from("set character_set_results=utf8mb4");
        SetHandler.handleV2(sql, serverConnection, 0, false);
        Assert.assertEquals("CHARACTER_SET_RESULTS should be utf8mb4 when set to utf8mb4",
            "utf8mb4", serverConnection.getExtraServerVariables().get("character_set_results"));
    }

    /**
     * Test for fix #72722208: CHARACTER_SET_CONNECTION should be empty string when set to default/null
     */
    @Test
    public void testSetCharacterSetConnectionWithDefaultValue() {
        try {
            ConfigDataMode.setInstanceRole(InstanceRole.FAST_MOCK);
            ServerConnection serverConnection = mockServerConnection();

            DynamicConfig mockDynamicConfig = mock(DynamicConfig.class);

            try (MockedStatic<DynamicConfig> mockedStatic = mockStatic(DynamicConfig.class)) {
                mockedStatic.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);
                when(mockDynamicConfig.isCompatibleCharsetVariables()).thenReturn(false);
                testSetCharacterSetConnection(mockDynamicConfig, serverConnection);

                when(mockDynamicConfig.isCompatibleCharsetVariables()).thenReturn(true);
                testSetCharacterSetConnection(mockDynamicConfig, serverConnection);
            }
        } finally {
            ConfigDataMode.setInstanceRole(InstanceRole.MASTER);
        }
    }

    private static void testSetCharacterSetConnection(DynamicConfig mockDynamicConfig,
                                                      ServerConnection serverConnection) {
        // Test set CHARACTER_SET_CONNECTION = default
        ByteString sql = ByteString.from("set character_set_connection=default");
        SetHandler.handleV2(sql, serverConnection, 0, false);
        Assert.assertEquals("CHARACTER_SET_CONNECTION should be empty string when set to default",
            "utf8", serverConnection.getExtraServerVariables().get("character_set_connection"));

        // Test set CHARACTER_SET_CONNECTION = null
        sql = ByteString.from("set character_set_connection=null");
        SetHandler.handleV2(sql, serverConnection, 0, false);
        Assert.assertEquals("CHARACTER_SET_CONNECTION should be empty string when set to null",
            "", serverConnection.getExtraServerVariables().get("character_set_connection"));

        // Test set CHARACTER_SET_CONNECTION = utf8
        sql = ByteString.from("set character_set_connection=utf8");
        SetHandler.handleV2(sql, serverConnection, 0, false);
        Assert.assertEquals("CHARACTER_SET_CONNECTION should be utf8 when set to utf8",
            "utf8", serverConnection.getExtraServerVariables().get("character_set_connection"));

        // Test set CHARACTER_SET_CONNECTION = gbk
        sql = ByteString.from("set character_set_connection=gbk");
        SetHandler.handleV2(sql, serverConnection, 0, false);
        Assert.assertEquals("CHARACTER_SET_CONNECTION should be gbk when set to gbk",
            "gbk", serverConnection.getExtraServerVariables().get("character_set_connection"));
    }

    /**
     * Test for fix #72722208: Test both CHARACTER_SET_RESULTS and CHARACTER_SET_CONNECTION
     * are set to empty string when using set names default/null
     */
    @Test
    public void testSetNamesWithDefaultValue() {
        try {
            ConfigDataMode.setInstanceRole(InstanceRole.FAST_MOCK);
            ServerConnection serverConnection = mockServerConnection();

            DynamicConfig mockDynamicConfig = mock(DynamicConfig.class);

            try (MockedStatic<DynamicConfig> mockedStatic = mockStatic(DynamicConfig.class)) {
                mockedStatic.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);
                when(mockDynamicConfig.isCompatibleCharsetVariables()).thenReturn(false);
                testSetNames(mockDynamicConfig, serverConnection);

                when(mockDynamicConfig.isCompatibleCharsetVariables()).thenReturn(true);
                testSetNames(mockDynamicConfig, serverConnection);
            }
        } finally {
            ConfigDataMode.setInstanceRole(InstanceRole.MASTER);
        }
    }

    private static void testSetNames(DynamicConfig mockDynamicConfig, ServerConnection serverConnection) {
        // Test set names default
        ByteString sql = ByteString.from("set names default");
        SetHandler.handleV2(sql, serverConnection, 0, false);
        Assert.assertEquals("CHARACTER_SET_RESULTS should be empty string when set names default",
            "", serverConnection.getExtraServerVariables().get("character_set_results"));
        Assert.assertEquals("CHARACTER_SET_CONNECTION should be empty string when set names default",
            "", serverConnection.getExtraServerVariables().get("character_set_connection"));

        // Test set names utf8mb4
        sql = ByteString.from("set names utf8mb4");
        SetHandler.handleV2(sql, serverConnection, 0, false);
        Assert.assertEquals("CHARACTER_SET_RESULTS should be utf8mb4 when set names utf8mb4",
            "utf8mb4", serverConnection.getExtraServerVariables().get("character_set_results"));
        Assert.assertEquals("CHARACTER_SET_CONNECTION should be utf8mb4 when set names utf8mb4",
            "utf8mb4", serverConnection.getExtraServerVariables().get("character_set_connection"));

        // Test set names default again to verify it resets to empty string
        sql = ByteString.from("set names default");
        SetHandler.handleV2(sql, serverConnection, 0, false);
        Assert.assertEquals("CHARACTER_SET_RESULTS should be reset to empty string",
            "", serverConnection.getExtraServerVariables().get("character_set_results"));
        Assert.assertEquals("CHARACTER_SET_CONNECTION should be reset to empty string",
            "", serverConnection.getExtraServerVariables().get("character_set_connection"));
    }

    /**
     * Test fix for #78232207: When setting CHARACTER_SET_CONNECTION/CHARACTER_SET_RESULTS by numeric
     * charset ID (e.g. SET @@character_set_connection = 13), SHOW VARIABLES should return the resolved
     * charset name (e.g. "sjis"), not the raw numeric string ("13").
     */
    @Test
    public void testSetCharacterSetByNumericId() {
        try {
            ConfigDataMode.setInstanceRole(InstanceRole.FAST_MOCK);

            // Mock a connection where setConnectionCharset("13") fails (not a valid name),
            // but setConnectionCharsetIndex(13) succeeds and resolves to "sjis".
            final ServerConnection serverConnection = mock(ServerConnection.class);
            when(serverConnection.initOptimizerContext()).thenReturn(true);
            when(serverConnection.getVarStringValue(any(SqlNode.class))).thenCallRealMethod();
            when(serverConnection.getVarIntegerValue(any())).thenCallRealMethod();
            Mockito.doCallRealMethod().when(serverConnection).setTrxPolicy(any(ITransactionPolicy.class));
            when(serverConnection.getTrxPolicy()).thenCallRealMethod();
            doCallRealMethod().when(serverConnection).writeErrMessage(any(ErrorCode.class), anyString());
            when(serverConnection.setConnectionCharset(anyString())).thenReturn(false);
            when(serverConnection.setConnectionCharsetIndex(anyInt())).thenReturn(true);
            when(serverConnection.getConnectionCharset()).thenReturn("sjis");
            when(serverConnection.setResultSetCharset(anyString())).thenReturn(false);
            when(serverConnection.setResultSetCharsetIndex(anyInt())).thenReturn(true);
            when(serverConnection.getResultSetCharset()).thenReturn("sjis");
            when(serverConnection.setCharset(anyString())).thenReturn(false);
            when(serverConnection.setCharsetIndex(anyInt())).thenReturn(true);
            final Map<String, Object> connectionVariables = new HashMap<>();
            when(serverConnection.getConnectionVariables()).thenReturn(connectionVariables);
            connectionVariables.put(ConnectionProperties.IGNORE_TRANSACTION_POLICY_NO_TRANSACTION, "false");
            final ByteBuffer buffer = ByteBuffer.allocate(1024);
            final ByteBufferHolder holder = new ByteBufferHolder(buffer);
            when(serverConnection.allocate()).thenReturn(holder);
            when(serverConnection.checkWriteBuffer(holder, 1)).thenReturn(holder);
            final HashMap<String, Object> extraServerVariables = new HashMap<>();
            when(serverConnection.getExtraServerVariables()).thenReturn(extraServerVariables);

            DynamicConfig mockDynamicConfig = mock(DynamicConfig.class);
            when(mockDynamicConfig.isDisableLegacyVariable()).thenReturn(false);

            try (MockedStatic<DynamicConfig> mockedStatic = mockStatic(DynamicConfig.class)) {
                mockedStatic.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);

                // Test setConnectionCharset with isCompatibleCharsetVariables = true
                when(mockDynamicConfig.isCompatibleCharsetVariables()).thenReturn(true);
                ByteString sql = ByteString.from("set @@character_set_connection = 13");
                SetHandler.handleV2(sql, serverConnection, 0, false);
                Assert.assertEquals(
                    "CHARACTER_SET_CONNECTION should be 'sjis' (resolved from charset ID 13), not '13'",
                    "sjis", extraServerVariables.get("character_set_connection"));

                // Test setResultCharset with isCompatibleCharsetVariables = true
                extraServerVariables.clear();
                sql = ByteString.from("set @@character_set_results = 13");
                SetHandler.handleV2(sql, serverConnection, 0, false);
                Assert.assertEquals(
                    "CHARACTER_SET_RESULTS should be 'sjis' (resolved from charset ID 13), not '13'",
                    "sjis", extraServerVariables.get("character_set_results"));

                // Test setCharsets (old path) with isCompatibleCharsetVariables = false
                when(mockDynamicConfig.isCompatibleCharsetVariables()).thenReturn(false);
                extraServerVariables.clear();
                sql = ByteString.from("set @@character_set_connection = 13");
                SetHandler.handleV2(sql, serverConnection, 0, false);
                Assert.assertEquals(
                    "CHARACTER_SET_CONNECTION should be 'sjis' (resolved from charset ID 13), not '13'",
                    "sjis", extraServerVariables.get("character_set_connection"));
            }
        } finally {
            ConfigDataMode.setInstanceRole(InstanceRole.MASTER);
        }
    }

    @Test
    public void setEnableJavaUdfBlockedTest() {
        try {
            ServerConnection serverConnection = mockServerConnection();

            AtomicReference<String> msgRef = new AtomicReference<>(null);
            Mockito.doAnswer((invocation) -> {
                String msg = invocation.getArgument(1, String.class);
                msgRef.set(msg);
                return null;
            }).when(serverConnection).writeErrMessage(any(ErrorCode.class), anyString());

            ByteString byteString = new ByteString(
                "set ENABLE_JAVA_UDF=true".getBytes(), Charset.defaultCharset());
            ConfigDataMode.setInstanceRole(InstanceRole.FAST_MOCK);
            SetHandler.handleV2(byteString, serverConnection, 0, false);

            // Verify the SET was rejected with security message
            Assert.assertNotNull("Expected error message for ENABLE_JAVA_UDF SET", msgRef.get());
            Assert.assertTrue("Error message should mention security-sensitive",
                msgRef.get().contains("security-sensitive parameter"));
        } finally {
            ConfigDataMode.setInstanceRole(InstanceRole.MASTER);
        }
    }
}
