package com.alibaba.polardbx.gms.metadb.table;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.topology.ServerInstIdManager;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.google.common.collect.Sets;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;

public class BaselineInfoAccessorTest {

    @BeforeClass
    public static void setUp() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
    }

    @Test
    public void testDeletePlans() throws Exception {
        ServerInstIdManager serverInstIdManager = mock(ServerInstIdManager.class);
        when(serverInstIdManager.getInstId()).thenReturn("test");
        String instId = "test_inst";

        Set<Integer> planInfoIds = Sets.newHashSet(1, 2, 3);
        try (BaselineInfoAccessor baselineInfoAccessor = new BaselineInfoAccessor(false);
            MockedStatic<ServerInstIdManager> serverInstIdManagerMockedStatic = mockStatic(ServerInstIdManager.class);
            MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(anyString(), anyMap(), any())).thenReturn(1);
            serverInstIdManagerMockedStatic.when(ServerInstIdManager::getInstance).thenReturn(serverInstIdManager);

            baselineInfoAccessor.deletePlans(instId, "test", 1, planInfoIds);

            metaDbUtilMockedStatic.verify(() -> MetaDbUtil.delete(anyString(), anyMap(), any()), times(3));
        }
    }

    @Test
    public void testDeleteBaselineWithInstNotExist() throws Exception {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        ServerInstIdManager serverInstIdManager = mock(ServerInstIdManager.class);
        when(serverInstIdManager.getInstId()).thenReturn("test");

        try (BaselineInfoAccessor baselineInfoAccessor = new BaselineInfoAccessor(false);
            MockedStatic<ServerInstIdManager> serverInstIdManagerMockedStatic = mockStatic(ServerInstIdManager.class);
            MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(anyString(), anyMap(), any())).thenReturn(1);
            serverInstIdManagerMockedStatic.when(ServerInstIdManager::getInstance).thenReturn(serverInstIdManager);

            try {
                baselineInfoAccessor.deleteBaselineWithInstNotExist(null);
                Assert.fail("should throw exception");
            } catch (TddlRuntimeException e) {
                assert e.getErrorCode() == ErrorCode.ERR_GMS_CHECK_ARGUMENTS.getCode();
            }

            try {
                baselineInfoAccessor.deleteBaselineWithInstNotExist(Sets.newHashSet());
                Assert.fail("should throw exception");
            } catch (TddlRuntimeException e) {
                assert e.getErrorCode() == ErrorCode.ERR_GMS_CHECK_ARGUMENTS.getCode();
            }

            baselineInfoAccessor.deleteBaselineWithInstNotExist(Sets.newHashSet("test"));
            metaDbUtilMockedStatic.verify(() -> MetaDbUtil.delete(anyString(), any()), times(1));

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(anyString(), any())).thenThrow(new RuntimeException());

            try {
                baselineInfoAccessor.deleteBaselineWithInstNotExist(Sets.newHashSet("test"));
                Assert.fail("should throw exception");
            } catch (TddlRuntimeException e) {
                assert e.getErrorCode() == ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE.getCode();
            }
        }
    }

    @Test
    public void testDeleteBaselineByInstSchema() throws Exception {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        ServerInstIdManager serverInstIdManager = mock(ServerInstIdManager.class);
        when(serverInstIdManager.getInstId()).thenReturn("test");

        try (BaselineInfoAccessor baselineInfoAccessor = new BaselineInfoAccessor(false);
            MockedStatic<ServerInstIdManager> serverInstIdManagerMockedStatic = mockStatic(ServerInstIdManager.class);
            MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(anyString(), anyMap(), any())).thenReturn(1);
            serverInstIdManagerMockedStatic.when(ServerInstIdManager::getInstance).thenReturn(serverInstIdManager);

            try {
                baselineInfoAccessor.deleteBaselineByInstSchema("", Sets.newHashSet());
                Assert.fail("should throw exception");
            } catch (TddlRuntimeException e) {
                assert e.getErrorCode() == ErrorCode.ERR_GMS_CHECK_ARGUMENTS.getCode();
            }

            try {
                baselineInfoAccessor.deleteBaselineByInstSchema("", Sets.newHashSet("test"));
                Assert.fail("should throw exception");
            } catch (TddlRuntimeException e) {
                assert e.getErrorCode() == ErrorCode.ERR_GMS_CHECK_ARGUMENTS.getCode();
            }

            baselineInfoAccessor.deleteBaselineByInstSchema("test", Sets.newHashSet("test"));
            metaDbUtilMockedStatic.verify(() -> MetaDbUtil.delete(anyString(), any()), times(1));

            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(anyString(), any())).thenThrow(new RuntimeException());

            try {
                baselineInfoAccessor.deleteBaselineByInstSchema("test", Sets.newHashSet("test"));
                Assert.fail("should throw exception");
            } catch (TddlRuntimeException e) {
                assert e.getErrorCode() == ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE.getCode();
            }
        }
    }

    @Test
    public void testDeleteDriftPlan() throws Exception {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        ServerInstIdManager serverInstIdManager = mock(ServerInstIdManager.class);
        when(serverInstIdManager.getInstId()).thenReturn("test");

        try (BaselineInfoAccessor baselineInfoAccessor = new BaselineInfoAccessor(false);
            MockedStatic<ServerInstIdManager> serverInstIdManagerMockedStatic = mockStatic(ServerInstIdManager.class);
            MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {

            serverInstIdManagerMockedStatic.when(ServerInstIdManager::getInstance).thenReturn(serverInstIdManager);

            baselineInfoAccessor.deleteDriftPlan();

            metaDbUtilMockedStatic.verify(() -> MetaDbUtil.delete(anyString(), any()), times(2));
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(anyString(), any())).thenThrow(new RuntimeException());

            try {
                baselineInfoAccessor.deleteDriftPlan();
                Assert.fail("should throw exception");
            } catch (TddlRuntimeException e) {
                assert e.getErrorCode() == ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE.getCode();
            }
        }
    }

    @Test
    public void testPersist() throws Exception {
        String schema = "test_schema";
        String instId = "test_inst";
        String sql = "select * from test";
        ServerInstIdManager serverInstIdManager = mock(ServerInstIdManager.class);
        when(serverInstIdManager.getInstId()).thenReturn(instId);

        BaselineInfoRecord baseline = new BaselineInfoRecord();
        baseline.setInstId(instId);
        baseline.setSchemaName(schema);
        baseline.setId(11);
        baseline.setSql(sql);
        baseline.setTableSet("t1");
        baseline.setExtendField("");

        List<BaselineInfoRecord> planInfos = new ArrayList<>();

        BaselineInfoRecord record1 = new BaselineInfoRecord();
        BaselineInfoRecord record2 = new BaselineInfoRecord();

        record1.setPlan("test plan1");
        record1.setPlanId(100000);
        record2.setPlan("test plan2");
        record2.setPlanId(5);

        planInfos.add(record1);
        planInfos.add(record2);

        try (BaselineInfoAccessor baselineInfoAccessor = new BaselineInfoAccessor(false);
            MockedStatic<ServerInstIdManager> serverInstIdManagerMockedStatic = mockStatic(ServerInstIdManager.class);
            MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.delete(anyString(), anyMap(), any())).thenReturn(1);

            List<CommonIntegerRecord> commonIntegerRecords = new ArrayList<>();
            CommonIntegerRecord c1 = new CommonIntegerRecord();
            c1.value = 100000;
            CommonIntegerRecord c2 = new CommonIntegerRecord();
            c2.value = 2;
            commonIntegerRecords.add(c1);
            commonIntegerRecords.add(c2);
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.query(anyString(), anyMap(), any(), any()))
                .thenReturn(commonIntegerRecords);
            serverInstIdManagerMockedStatic.when(ServerInstIdManager::getInstance).thenReturn(serverInstIdManager);

            baselineInfoAccessor.persist(schema, baseline, planInfos, true);

            metaDbUtilMockedStatic.verify(() -> MetaDbUtil.delete(anyString(), anyMap(), any()), times(1));
            metaDbUtilMockedStatic.verify(() -> MetaDbUtil.insert(anyString(), anyMap(), any()), times(3));
        }
    }

    @Test
    public void testUpdateExtendField() throws Exception {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        ServerInstIdManager serverInstIdManager = mock(ServerInstIdManager.class);
        when(serverInstIdManager.getInstId()).thenReturn("test");

        String instId = "test_inst";
        String schema = "test_schema";
        int baselineId = 1;
        int planId = 100;
        String extend = "{\"key\":\"value\"}";

        try (BaselineInfoAccessor baselineInfoAccessor = new BaselineInfoAccessor(false);
            MockedStatic<ServerInstIdManager> serverInstIdManagerMockedStatic = mockStatic(ServerInstIdManager.class);
            MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class)) {
            serverInstIdManagerMockedStatic.when(ServerInstIdManager::getInstance).thenReturn(serverInstIdManager);

            // Test successful update
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyMap(), any())).thenReturn(1);
            int result = baselineInfoAccessor.updateExtendField(instId, schema, baselineId, planId, extend);
            Assert.assertTrue(result == 1);
            metaDbUtilMockedStatic.verify(() -> MetaDbUtil.update(anyString(), anyMap(), any()), times(1));

            // Test with null instId
            try {
                baselineInfoAccessor.updateExtendField(null, schema, baselineId, planId, extend);
                Assert.fail("should throw exception when instId is null");
            } catch (TddlRuntimeException e) {
                assert e.getErrorCode() == ErrorCode.ERR_GMS_CHECK_ARGUMENTS.getCode();
            }

            // Test with empty instId
            try {
                baselineInfoAccessor.updateExtendField("", schema, baselineId, planId, extend);
                Assert.fail("should throw exception when instId is empty");
            } catch (TddlRuntimeException e) {
                assert e.getErrorCode() == ErrorCode.ERR_GMS_CHECK_ARGUMENTS.getCode();
            }

            // Test with null schema
            try {
                baselineInfoAccessor.updateExtendField(instId, null, baselineId, planId, extend);
                Assert.fail("should throw exception when schema is null");
            } catch (TddlRuntimeException e) {
                assert e.getErrorCode() == ErrorCode.ERR_GMS_CHECK_ARGUMENTS.getCode();
            }

            // Test with empty schema
            try {
                baselineInfoAccessor.updateExtendField(instId, "", baselineId, planId, extend);
                Assert.fail("should throw exception when schema is empty");
            } catch (TddlRuntimeException e) {
                assert e.getErrorCode() == ErrorCode.ERR_GMS_CHECK_ARGUMENTS.getCode();
            }

            // Test database exception
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyMap(), any()))
                .thenThrow(new RuntimeException("Database error"));
            try {
                baselineInfoAccessor.updateExtendField(instId, schema, baselineId, planId, extend);
                Assert.fail("should throw exception when database error occurs");
            } catch (TddlRuntimeException e) {
                assert e.getErrorCode() == ErrorCode.ERR_GMS_ACCESS_TO_SYSTEM_TABLE.getCode();
            }

            // Test with null extend field (should be allowed)
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.update(anyString(), anyMap(), any())).thenReturn(1);
            result = baselineInfoAccessor.updateExtendField(instId, schema, baselineId, planId, null);
            Assert.assertTrue(result == 1);
        }
    }
}
