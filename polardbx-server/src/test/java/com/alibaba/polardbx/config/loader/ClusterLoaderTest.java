package com.alibaba.polardbx.config.loader;

import com.alibaba.polardbx.CobarConfig;
import com.alibaba.polardbx.CobarServer;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.config.SystemConfig;
import com.alibaba.polardbx.executor.gms.ColumnarManager;
import com.alibaba.polardbx.executor.gms.DynamicColumnarManager;
import com.alibaba.polardbx.gms.config.impl.InstConfUtil;
import com.alibaba.polardbx.gms.listener.ConfigListener;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.topology.ServerInstSubManager;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Properties;

public class ClusterLoaderTest {
    private ClusterLoader clusterLoader;
    private MockedStatic<MetaDbUtil> metaDbUtilMockedStatic;
    private MockedStatic<CobarServer> cobarServerMockedStatic;
    private MockedConstruction<ServerLoader> serverLoaderMockedCtor;
    private MockedStatic<InstIdUtil> instIdUtilMockedStatic;
    private MockedStatic<MetaDbConfigManager> metaDbConfigManagerMockedStatic;
    private MockedStatic<ServerInstSubManager> serverInstSubManagerMockedStatic;
    private MockedStatic<InstConfUtil> instConfUtilMockedStatic;

    @Before
    public void setUp() throws Exception {
        SystemConfig systemConfig = new SystemConfig();
        systemConfig.setClusterName("test_cluster");
        systemConfig.setUnitName("test_unit");

        clusterLoader = new GmsClusterLoader(systemConfig);
        metaDbUtilMockedStatic = Mockito.mockStatic(MetaDbUtil.class);
        metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(null);

        // Close any existing ServerLoader mock construction
        if (serverLoaderMockedCtor != null) {
            serverLoaderMockedCtor.close();
        }

        serverLoaderMockedCtor = Mockito.mockConstruction(ServerLoader.class, (mock, context) -> {
            Mockito.when(mock.getSystem()).thenReturn(systemConfig);
        });

        CobarServer cobarServer = Mockito.mock(CobarServer.class);
        CobarConfig cobarConfig = Mockito.mock(CobarConfig.class);
        Mockito.when(cobarServer.getConfig()).thenReturn(cobarConfig);
        Mockito.when(cobarConfig.getSystem()).thenReturn(systemConfig);
        cobarServerMockedStatic = Mockito.mockStatic(CobarServer.class);
        cobarServerMockedStatic.when(CobarServer::getInstance).thenReturn(cobarServer);

        // Mock InstIdUtil
        instIdUtilMockedStatic = Mockito.mockStatic(InstIdUtil.class);
        instIdUtilMockedStatic.when(InstIdUtil::getInstId).thenReturn("test_inst_id");

        // Mock MetaDbConfigManager
        metaDbConfigManagerMockedStatic = Mockito.mockStatic(MetaDbConfigManager.class);

        // Mock ServerInstSubManager
        serverInstSubManagerMockedStatic = Mockito.mockStatic(ServerInstSubManager.class);

        instConfUtilMockedStatic = Mockito.mockStatic(InstConfUtil.class);
        instConfUtilMockedStatic.when(() -> InstConfUtil.getOriginVal(ConnectionParams.COLUMNAR_VERSION_CHAIN_PRUNER))
            .thenReturn("");
    }

    @After
    public void tearDown() throws Exception {
        if (clusterLoader != null) {
            clusterLoader.destroy();
        }
        if (metaDbUtilMockedStatic != null) {
            metaDbUtilMockedStatic.close();
        }
        if (cobarServerMockedStatic != null) {
            cobarServerMockedStatic.close();
        }
        if (instIdUtilMockedStatic != null) {
            instIdUtilMockedStatic.close();
        }
        if (metaDbConfigManagerMockedStatic != null) {
            metaDbConfigManagerMockedStatic.close();
        }
        if (serverInstSubManagerMockedStatic != null) {
            serverInstSubManagerMockedStatic.close();
        }
        if (serverLoaderMockedCtor != null) {
            serverLoaderMockedCtor.close();
        }
        if (instConfUtilMockedStatic != null) {
            instConfUtilMockedStatic.close();
        }
    }

    @Test
    public void testSetColumnarProperties() {
        Properties p = new Properties();
        p.setProperty(ConnectionProperties.CSV_CACHE_SIZE, "1024");
        clusterLoader.applyProperties(p);
        p.setProperty(ConnectionProperties.CSV_CACHE_SIZE, "2048");
        clusterLoader.applyProperties(p);
        p.setProperty(ConnectionProperties.COLUMNAR_VERSION_CHAIN_PRUNER, "");
        clusterLoader.applyProperties(p);
    }

    @Test
    public void testResetOssCrcCheck() {
        Properties p = new Properties();
        p.setProperty(ConnectionProperties.ENABLE_OSS_CLIENT_CRC_CHECK, "true");
        clusterLoader.applyProperties(p);
        p.setProperty(ConnectionProperties.ENABLE_OSS_CLIENT_CRC_CHECK, "false");
        clusterLoader.applyProperties(p);
    }

    @Test
    public void testServerSubClusterInfoListener() {
        // Create listener instance
        GmsClusterLoader.ServerSubClusterInfoListener listener =
            new GmsClusterLoader.ServerSubClusterInfoListener((GmsClusterLoader) clusterLoader);

        // Mock ServerInstSubManager.getInstance()
        ServerInstSubManager serverInstSubManager = Mockito.mock(ServerInstSubManager.class);
        serverInstSubManagerMockedStatic.when(ServerInstSubManager::getInstance).thenReturn(serverInstSubManager);

        // Call onHandleConfig method
        listener.onHandleConfig("test_data_id", 1L);

        // Verify that loadNodeSubCluster was called
        Mockito.verify(serverInstSubManager).loadNodeSubCluster();
    }

    @Test
    public void testServerLoadWeightInfoListener() {
        // Create listener instance
        GmsClusterLoader.ServerLoadWeightInfoListener listener =
            new GmsClusterLoader.ServerLoadWeightInfoListener((GmsClusterLoader) clusterLoader);

        // Mock ServerInstSubManager.getInstance()
        ServerInstSubManager serverInstSubManager = Mockito.mock(ServerInstSubManager.class);
        serverInstSubManagerMockedStatic.when(ServerInstSubManager::getInstance).thenReturn(serverInstSubManager);

        // Call onHandleConfig method
        listener.onHandleConfig("test_data_id", 1L);

        // Verify that loadNodeLoadWeight was called
        Mockito.verify(serverInstSubManager).loadNodeLoadWeight();
    }

    @Test
    public void testLoadServerSubClusterInfos() {
        // Mock MetaDbConfigManager.getInstance()
        MetaDbConfigManager metaDbConfigManager = Mockito.mock(MetaDbConfigManager.class);
        metaDbConfigManagerMockedStatic.when(MetaDbConfigManager::getInstance).thenReturn(metaDbConfigManager);

        // Mock InstIdUtil.getInstId()
        instIdUtilMockedStatic.when(InstIdUtil::getInstId).thenReturn("test_inst_id");

        // Mock MetaDbDataIdBuilder.getServerSubClusterDataId()
        String testDataId = "test_sub_cluster_data_id";
        MockedStatic<MetaDbDataIdBuilder> metaDbDataIdBuilderMockedStatic =
            Mockito.mockStatic(MetaDbDataIdBuilder.class);
        metaDbDataIdBuilderMockedStatic.when(() -> MetaDbDataIdBuilder.getServerSubClusterDataId("test_inst_id"))
            .thenReturn(testDataId);

        try {
            // Call loadServerSubClusterInfos method
            ((GmsClusterLoader) clusterLoader).loadServerSubClusterInfos();

            // Verify register was called with correct parameters
            Mockito.verify(metaDbConfigManager).register(testDataId, null);

            // Verify bindListener was called with correct parameters
            Mockito.verify(metaDbConfigManager).bindListener(Mockito.eq(testDataId),
                Mockito.any(GmsClusterLoader.ServerSubClusterInfoListener.class));
        } finally {
            metaDbDataIdBuilderMockedStatic.close();
        }
    }

    @Test
    public void testLoadServerLoadWeightInfos() {
        // Mock MetaDbConfigManager.getInstance()
        MetaDbConfigManager metaDbConfigManager = Mockito.mock(MetaDbConfigManager.class);
        metaDbConfigManagerMockedStatic.when(MetaDbConfigManager::getInstance).thenReturn(metaDbConfigManager);

        // Mock InstIdUtil.getInstId()
        instIdUtilMockedStatic.when(InstIdUtil::getInstId).thenReturn("test_inst_id");

        // Mock MetaDbDataIdBuilder.getServerLoadWeightDataId()
        String testDataId = "test_load_weight_data_id";
        MockedStatic<MetaDbDataIdBuilder> metaDbDataIdBuilderMockedStatic =
            Mockito.mockStatic(MetaDbDataIdBuilder.class);
        metaDbDataIdBuilderMockedStatic.when(() -> MetaDbDataIdBuilder.getServerLoadWeightDataId("test_inst_id"))
            .thenReturn(testDataId);

        try {
            // Call loadServerLoadWeightInfos method
            ((GmsClusterLoader) clusterLoader).loadServerLoadWeightInfos();

            // Verify register was called with correct parameters
            Mockito.verify(metaDbConfigManager).register(testDataId, null);

            // Verify bindListener was called with correct parameters
            Mockito.verify(metaDbConfigManager).bindListener(Mockito.eq(testDataId),
                Mockito.any(GmsClusterLoader.ServerLoadWeightInfoListener.class));
        } finally {
            metaDbDataIdBuilderMockedStatic.close();
        }
    }
}