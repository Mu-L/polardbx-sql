package com.alibaba.polardbx.gms.metadb.accessor;

import com.alibaba.polardbx.common.jdbc.ParameterContext;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.metadb.cdc.CdcConfigAccessor;
import com.alibaba.polardbx.gms.topology.InstConfigAccessor;
import com.alibaba.polardbx.gms.topology.VariableConfigAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Map;
import java.util.Properties;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.anyMap;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class AbstractAccessorTest {
    private AbstractAccessor abstractAccessor;
    private Connection mockConnection;
    private PreparedStatement mockPreparedStatement;

    private CdcConfigAccessor cdcConfigAccessor;
    private InstConfigAccessor instConfigAccessor;
    private VariableConfigAccessor variableConfigAccessor;

    @Before
    public void setUp() throws Exception {
        abstractAccessor = new AbstractAccessor() {};
        mockConnection = mock(Connection.class);
        mockPreparedStatement = mock(PreparedStatement.class);

        abstractAccessor.setConnection(mockConnection);

        cdcConfigAccessor = new CdcConfigAccessor();
        cdcConfigAccessor.setConnection(mockConnection);
        instConfigAccessor = new InstConfigAccessor();
        instConfigAccessor.setConnection(mockConnection);
        variableConfigAccessor = new VariableConfigAccessor();
        variableConfigAccessor.setConnection(mockConnection);
    }

    @After
    public void tearDown() {
        mockConnection = null;
        mockPreparedStatement = null;
        abstractAccessor = null;
        cdcConfigAccessor = null;
        instConfigAccessor = null;
        variableConfigAccessor = null;
    }

    @Test
    public void testUpsertConfigValue_ReplaceSuccess() throws Exception {
        Properties props = new Properties();
        props.put("key1", "value1");
        String systemTable = "test_table";
        String replaceSql = "replace into test_table set param_val=?, param_key=?, inst_id=?";
        String dataId = "test_data_id";
        String instId = "test_inst_id";

        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = Mockito.mockStatic(MetaDbUtil.class);
            MockedStatic<MetaDbConfigManager> configManagerMockedStatic = Mockito.mockStatic(MetaDbConfigManager.class)) {

            MetaDbConfigManager mockConfigManager = mock(MetaDbConfigManager.class);
            configManagerMockedStatic.when(MetaDbConfigManager::getInstance).thenReturn(mockConfigManager);

            when(mockConnection.prepareStatement(anyString())).thenReturn(mockPreparedStatement);
            doNothing().when(mockPreparedStatement).setString(anyInt(), anyString());
            when(mockPreparedStatement.executeUpdate()).thenReturn(1);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyMap(), any(Connection.class)))
                .thenAnswer(invocation -> {
                    // 验证参数传递是否正确
                    Map<Integer, ParameterContext> params = invocation.getArgument(1);
                    assertEquals(3, params.size());
                    return 1;
                });
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.setParameter(anyInt(), anyMap(), any(), any())).thenCallRealMethod();

            abstractAccessor.upsertConfigValue(instId, props, systemTable, replaceSql, dataId);
        }
    }

    @Test
    public void testConfigAccessor() throws Exception {
        Properties props = new Properties();
        props.put("key1", "value1");
        String instId = "test_inst_id";

        SQLException duplicateException = mock(SQLException.class);
        when(duplicateException.getErrorCode()).thenReturn(1062);
        when(duplicateException.getSQLState()).thenReturn("23000");
        when(duplicateException.getMessage()).thenReturn("for key 'test_key'");
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = Mockito.mockStatic(MetaDbUtil.class);
            MockedStatic<MetaDbConfigManager> configManagerMockedStatic = Mockito.mockStatic(MetaDbConfigManager.class)) {

            MetaDbConfigManager mockConfigManager = mock(MetaDbConfigManager.class);
            configManagerMockedStatic.when(MetaDbConfigManager::getInstance).thenReturn(mockConfigManager);

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyMap(), any(Connection.class)))
                .thenAnswer(invocation -> 1);

            cdcConfigAccessor.updateInstConfigValue(props);
            instConfigAccessor.updateInstConfigValue(instId, props);
            variableConfigAccessor.updateParamsValue(props, instId);
        }
    }
}