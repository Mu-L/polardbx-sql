package com.alibaba.polardbx.gms.util;

import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.lang.reflect.Field;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

public class InstIdUtilTest {

    private static final String TEST_INST_ID = "test_inst_id";
    private static final String TEST_SUB_INST_ID = "test_sub_inst_id";
    private static final String TEST_MASTER_INST_ID = "test_master_inst_id";

    @Before
    public void setUp() {
        // 设置系统属性用于测试
        System.setProperty("instanceId", TEST_INST_ID);
        System.setProperty("masterInstanceId", TEST_MASTER_INST_ID);
    }

    @After
    public void tearDown() {
        // 清理系统属性
        System.clearProperty("instanceId");
        System.clearProperty("masterInstanceId");

        // 重置subInstId字段
        try {
            Field subInstIdField = InstIdUtil.class.getDeclaredField("subInstId");
            subInstIdField.setAccessible(true);
            subInstIdField.set(null, null);
        } catch (Exception e) {
            // 忽略异常
        }
    }

    @Test
    public void testGetInstId() {
        String instId = InstIdUtil.getInstId();
        assertEquals(TEST_INST_ID, instId);
    }

    @Test
    public void testGetSubInstIdWhenNotSet() {
        // 当subInstId未设置时，应该返回instId
        String subInstId = InstIdUtil.getSubInstId();
        assertEquals(TEST_INST_ID, subInstId);
    }

    @Test
    public void testGetSubInstIdWhenSet() {
        // 设置subInstId
        try (MockedStatic<MetaDbConfigManager> metaDbConfigManagerMockedStatic = mockStatic(MetaDbConfigManager.class);
            MockedStatic<MetaDbInstConfigManager> metaDbInstConfigManagerMockedStatic = mockStatic(
                MetaDbInstConfigManager.class)) {
            MetaDbConfigManager metaDbConfigManager = mock(MetaDbConfigManager.class);
            doNothing().when(metaDbConfigManager).register(anyString(), any());
            doNothing().when(metaDbConfigManager).bindListener(anyString(), any());
            metaDbConfigManagerMockedStatic.when(MetaDbConfigManager::getInstance).thenReturn(metaDbConfigManager);

            MetaDbInstConfigManager metaDbInstConfigManager = mock(MetaDbInstConfigManager.class);
            doNothing().when(metaDbInstConfigManager).reloadInstConfig();
            metaDbInstConfigManagerMockedStatic.when(MetaDbInstConfigManager::getInstance)
                .thenReturn(metaDbInstConfigManager);
            InstIdUtil.setSubInstId(TEST_SUB_INST_ID);
        }
        // 当subInstId已设置时，应该返回设置的值
        String subInstId = InstIdUtil.getSubInstId();
        assertEquals(TEST_SUB_INST_ID, subInstId);
    }

    @Test
    public void testIsClusterInstIdWhenEqual() {
        // 当instId和subInstId相等时，应该返回true

        try (MockedStatic<MetaDbConfigManager> metaDbConfigManagerMockedStatic = mockStatic(MetaDbConfigManager.class);
            MockedStatic<MetaDbInstConfigManager> metaDbInstConfigManagerMockedStatic = mockStatic(
                MetaDbInstConfigManager.class)) {
            MetaDbConfigManager metaDbConfigManager = mock(MetaDbConfigManager.class);
            doNothing().when(metaDbConfigManager).register(anyString(), any());
            doNothing().when(metaDbConfigManager).bindListener(anyString(), any());
            metaDbConfigManagerMockedStatic.when(MetaDbConfigManager::getInstance).thenReturn(metaDbConfigManager);

            MetaDbInstConfigManager metaDbInstConfigManager = mock(MetaDbInstConfigManager.class);
            doNothing().when(metaDbInstConfigManager).reloadInstConfig();
            metaDbInstConfigManagerMockedStatic.when(MetaDbInstConfigManager::getInstance)
                .thenReturn(metaDbInstConfigManager);
            InstIdUtil.setSubInstId(TEST_INST_ID);
        }
        assertTrue(InstIdUtil.isClusterInstId());
    }

    @Test
    public void testIsClusterInstIdWhenNotEqual() {
        // 当instId和subInstId不相等时，应该返回false

        try (MockedStatic<MetaDbConfigManager> metaDbConfigManagerMockedStatic = mockStatic(MetaDbConfigManager.class);
            MockedStatic<MetaDbInstConfigManager> metaDbInstConfigManagerMockedStatic = mockStatic(
                MetaDbInstConfigManager.class)) {
            MetaDbConfigManager metaDbConfigManager = mock(MetaDbConfigManager.class);
            doNothing().when(metaDbConfigManager).register(anyString(), any());
            doNothing().when(metaDbConfigManager).bindListener(anyString(), any());
            metaDbConfigManagerMockedStatic.when(MetaDbConfigManager::getInstance).thenReturn(metaDbConfigManager);

            MetaDbInstConfigManager metaDbInstConfigManager = mock(MetaDbInstConfigManager.class);
            doNothing().when(metaDbInstConfigManager).reloadInstConfig();
            metaDbInstConfigManagerMockedStatic.when(MetaDbInstConfigManager::getInstance)
                .thenReturn(metaDbInstConfigManager);
            InstIdUtil.setSubInstId(TEST_SUB_INST_ID);
        }
        assertFalse(InstIdUtil.isClusterInstId());
    }

    @Test
    public void testGetMasterInstId() {
        String masterInstId = InstIdUtil.getMasterInstId();
        assertEquals(TEST_MASTER_INST_ID, masterInstId);
    }

    @Test
    public void testGetInstIdFromStorageInfoDataId() {
        String storageInfoDataId = "polardbx.storage.info.test_inst_id";
        String instId = InstIdUtil.getInstIdFromStorageInfoDataId(storageInfoDataId);
        assertEquals(TEST_INST_ID, instId);
    }
}