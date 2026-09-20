package com.alibaba.polardbx.gms.metadb.encdb;

import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.util.Collections;

public class EncdbKeyManagerTest {

    private final EncdbKeyManager target = new EncdbKeyManager();

    @Test
    public void testReplaceEncKeysWithNullDeleteTypes() throws Exception {
        Connection mockConnection = Mockito.mock(Connection.class);
        MetaDbConfigManager mockMetaDbConfigManager = Mockito.mock(MetaDbConfigManager.class);

        try (MockedStatic<MetaDbUtil> mockMetaDbUtil = Mockito.mockStatic(MetaDbUtil.class);
            MockedStatic<MetaDbConfigManager> mockMetaDbConfigManagerStatic =
                Mockito.mockStatic(MetaDbConfigManager.class);
            MockedConstruction<EncdbKeyAccessor> mockEncdbKeyAccessorConstruction =
                Mockito.mockConstruction(EncdbKeyAccessor.class)) {
            mockMetaDbUtil.when(MetaDbUtil::getConnection).thenReturn(mockConnection);
            mockMetaDbConfigManagerStatic.when(MetaDbConfigManager::getInstance)
                .thenReturn(mockMetaDbConfigManager);

            target.replaceEncKeys(Collections.emptyList(), null);

            EncdbKeyAccessor mockEncdbKeyAccessor = mockEncdbKeyAccessorConstruction.constructed().get(0);
            Mockito.verify(mockEncdbKeyAccessor, Mockito.never()).deleteByType(Mockito.anyString());
            Mockito.verify(mockEncdbKeyAccessor).replace(Collections.emptyList());
            Mockito.verify(mockMetaDbConfigManager)
                .notify(MetaDbDataIdBuilder.ENCDB_KEY_DATA_ID, mockConnection);
            Mockito.verify(mockConnection).commit();
            Mockito.verify(mockMetaDbConfigManager).sync(MetaDbDataIdBuilder.ENCDB_KEY_DATA_ID);
        }
    }
}
