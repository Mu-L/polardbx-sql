package com.alibaba.polardbx.server.encdb.handler;

import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbKey;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbKeyManager;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

public class EncdbMekProvisionHandlerTest {

    @Test
    public void testRegisterKmsMekWithIv() throws Exception {
        String mockKmsEncMek = "kms-encrypted-mek";
        byte[] mockKmsPlainMek = "plain-mek".getBytes(StandardCharsets.UTF_8);
        String mockKmsIv = "kms-iv-not-base64";
        EncdbKeyManager mockEncdbKeyManager = Mockito.mock(EncdbKeyManager.class);

        try (MockedStatic<EncdbKeyManager> mockEncdbKeyManagerStatic =
            Mockito.mockStatic(EncdbKeyManager.class, Mockito.CALLS_REAL_METHODS);
            MockedStatic<SyncManagerHelper> mockSyncManagerHelper = Mockito.mockStatic(SyncManagerHelper.class)) {
            mockEncdbKeyManagerStatic.when(EncdbKeyManager::getInstance).thenReturn(mockEncdbKeyManager);

            EncdbMekProvisionHandler.registerKmsMek(mockKmsEncMek, mockKmsPlainMek, "cn-hangzhou",
                "dbstack-key-id", mockKmsIv);

            ArgumentCaptor<List> mockKeysCaptor = ArgumentCaptor.forClass(List.class);
            ArgumentCaptor<List> mockTypesToDeleteCaptor = ArgumentCaptor.forClass(List.class);
            Mockito.verify(mockEncdbKeyManager)
                .replaceEncKeys(mockKeysCaptor.capture(), mockTypesToDeleteCaptor.capture());

            Map<EncdbKey.KeyType, String> actualKeys = toKeyMap(mockKeysCaptor.getValue());
            Assert.assertEquals(mockKmsEncMek, actualKeys.get(EncdbKey.KeyType.KMS_ENC_MEK));
            Assert.assertEquals(mockKmsIv, actualKeys.get(EncdbKey.KeyType.KMS_IV));
            Assert.assertEquals("cn-hangzhou", actualKeys.get(EncdbKey.KeyType.KMS_REGION));
            Assert.assertEquals("dbstack-key-id", actualKeys.get(EncdbKey.KeyType.KMS_KEY_ID));
            Assert.assertTrue(mockTypesToDeleteCaptor.getValue().isEmpty());
        }
    }

    @Test
    public void testRegisterKmsMekWithoutIvDeletesStaleIv() throws Exception {
        String mockKmsEncMek = "kms-encrypted-mek";
        byte[] mockKmsPlainMek = "plain-mek".getBytes(StandardCharsets.UTF_8);
        EncdbKeyManager mockEncdbKeyManager = Mockito.mock(EncdbKeyManager.class);

        try (MockedStatic<EncdbKeyManager> mockEncdbKeyManagerStatic =
            Mockito.mockStatic(EncdbKeyManager.class, Mockito.CALLS_REAL_METHODS);
            MockedStatic<SyncManagerHelper> mockSyncManagerHelper = Mockito.mockStatic(SyncManagerHelper.class)) {
            mockEncdbKeyManagerStatic.when(EncdbKeyManager::getInstance).thenReturn(mockEncdbKeyManager);

            EncdbMekProvisionHandler.registerKmsMek(mockKmsEncMek, mockKmsPlainMek, "cn-hangzhou",
                "legacy-key-id");

            ArgumentCaptor<List> mockKeysCaptor = ArgumentCaptor.forClass(List.class);
            ArgumentCaptor<List> mockTypesToDeleteCaptor = ArgumentCaptor.forClass(List.class);
            Mockito.verify(mockEncdbKeyManager)
                .replaceEncKeys(mockKeysCaptor.capture(), mockTypesToDeleteCaptor.capture());

            Map<EncdbKey.KeyType, String> actualKeys = toKeyMap(mockKeysCaptor.getValue());
            Assert.assertFalse(actualKeys.containsKey(EncdbKey.KeyType.KMS_IV));
            Assert.assertEquals(1, mockTypesToDeleteCaptor.getValue().size());
            Assert.assertEquals(EncdbKey.KeyType.KMS_IV, mockTypesToDeleteCaptor.getValue().get(0));
        }
    }

    private Map<EncdbKey.KeyType, String> toKeyMap(List<EncdbKey> encdbKeys) {
        Map<EncdbKey.KeyType, String> result = new EnumMap<>(EncdbKey.KeyType.class);
        for (EncdbKey encdbKey : encdbKeys) {
            result.put(EncdbKey.KeyType.valueOf(encdbKey.getType()), encdbKey.getKey());
        }
        return result;
    }
}
