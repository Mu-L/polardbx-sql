package com.alibaba.polardbx.server.encdb.handler;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants;
import com.alibaba.polardbx.gms.metadb.encdb.EncdbKeyManager;
import com.alibaba.polardbx.server.encdb.EncdbServer;
import com.alibaba.polardbx.server.encdb.EncdbUtils;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

public class EncdbServerInfoGetHandlerTest {

    private final EncdbServerInfoGetHandler target = new EncdbServerInfoGetHandler();

    @Test
    public void testKmsServerInfoContainsKeyIdAndIv() {
        String mockKmsEncMek = "kms-encrypted-mek";
        String mockKmsIv = "kms-iv-not-base64";
        EncdbKeyManager mockEncdbKeyManager = mockEncdbKeyManager(mockKmsEncMek, mockKmsIv);
        EncdbServer mockEncdbServer = mockEncdbServer();

        try (MockedStatic<EncdbKeyManager> mockEncdbKeyManagerStatic =
            Mockito.mockStatic(EncdbKeyManager.class);
            MockedStatic<EncdbServer> mockEncdbServerStatic = Mockito.mockStatic(EncdbServer.class);
            MockedStatic<EncdbUtils> mockEncdbUtilsStatic = Mockito.mockStatic(EncdbUtils.class)) {
            mockEncdbKeyManagerStatic.when(EncdbKeyManager::getInstance).thenReturn(mockEncdbKeyManager);
            mockEncdbServerStatic.when(EncdbServer::getInstance).thenReturn(mockEncdbServer);
            mockEncdbUtilsStatic.when(() -> EncdbUtils.checkEncjdbcKmsVersion("1.2.22")).thenReturn(true);

            JSONObject request = new JSONObject();
            request.put(MsgKeyConstants.VERSION, "1.2.22");
            JSONObject response = target.handle(request, null);
            JSONObject serverInfo = response.getJSONObject(MsgKeyConstants.SERVER_INFO);

            Assert.assertEquals("dbstack-key-id", serverInfo.getString(MsgKeyConstants.KMS_KEY_ID));
            Assert.assertEquals(mockKmsEncMek, serverInfo.getString(MsgKeyConstants.KMS_ENC_MEK));
            Assert.assertEquals(mockKmsIv, serverInfo.getString(MsgKeyConstants.KMS_IV));

            JSONObject serializedServerInfo =
                JSON.parseObject(response.toJSONString()).getJSONObject(MsgKeyConstants.SERVER_INFO);
            Assert.assertEquals(mockKmsEncMek, serializedServerInfo.getString(MsgKeyConstants.KMS_ENC_MEK));
            Assert.assertEquals(mockKmsIv, serializedServerInfo.getString(MsgKeyConstants.KMS_IV));
        }
    }

    @Test
    public void testKmsServerInfoOmitsMissingIv() {
        EncdbKeyManager mockEncdbKeyManager =
            mockEncdbKeyManager("kms-encrypted-mek", null);
        EncdbServer mockEncdbServer = mockEncdbServer();

        try (MockedStatic<EncdbKeyManager> mockEncdbKeyManagerStatic =
            Mockito.mockStatic(EncdbKeyManager.class);
            MockedStatic<EncdbServer> mockEncdbServerStatic = Mockito.mockStatic(EncdbServer.class);
            MockedStatic<EncdbUtils> mockEncdbUtilsStatic = Mockito.mockStatic(EncdbUtils.class)) {
            mockEncdbKeyManagerStatic.when(EncdbKeyManager::getInstance).thenReturn(mockEncdbKeyManager);
            mockEncdbServerStatic.when(EncdbServer::getInstance).thenReturn(mockEncdbServer);
            mockEncdbUtilsStatic.when(() -> EncdbUtils.checkEncjdbcKmsVersion("1.2.22")).thenReturn(true);

            JSONObject request = new JSONObject();
            request.put(MsgKeyConstants.VERSION, "1.2.22");
            JSONObject serverInfo =
                target.handle(request, null).getJSONObject(MsgKeyConstants.SERVER_INFO);

            Assert.assertEquals("dbstack-key-id", serverInfo.getString(MsgKeyConstants.KMS_KEY_ID));
            Assert.assertFalse(serverInfo.containsKey(MsgKeyConstants.KMS_IV));
        }
    }

    @Test
    public void testKmsServerInfoOmitsMissingKeyId() {
        EncdbKeyManager mockEncdbKeyManager =
            mockEncdbKeyManager("kms-encrypted-mek", null);
        Mockito.when(mockEncdbKeyManager.getKmsKeyId()).thenReturn(null);
        EncdbServer mockEncdbServer = mockEncdbServer();

        try (MockedStatic<EncdbKeyManager> mockEncdbKeyManagerStatic =
            Mockito.mockStatic(EncdbKeyManager.class);
            MockedStatic<EncdbServer> mockEncdbServerStatic = Mockito.mockStatic(EncdbServer.class);
            MockedStatic<EncdbUtils> mockEncdbUtilsStatic = Mockito.mockStatic(EncdbUtils.class)) {
            mockEncdbKeyManagerStatic.when(EncdbKeyManager::getInstance).thenReturn(mockEncdbKeyManager);
            mockEncdbServerStatic.when(EncdbServer::getInstance).thenReturn(mockEncdbServer);
            mockEncdbUtilsStatic.when(() -> EncdbUtils.checkEncjdbcKmsVersion("1.2.22")).thenReturn(true);

            JSONObject request = new JSONObject();
            request.put(MsgKeyConstants.VERSION, "1.2.22");
            JSONObject serverInfo =
                target.handle(request, null).getJSONObject(MsgKeyConstants.SERVER_INFO);

            Assert.assertFalse(serverInfo.containsKey(MsgKeyConstants.KMS_KEY_ID));
        }
    }

    private EncdbKeyManager mockEncdbKeyManager(String kmsEncMek, String kmsIv) {
        EncdbKeyManager mockEncdbKeyManager = Mockito.mock(EncdbKeyManager.class);
        Mockito.when(mockEncdbKeyManager.useKmsMode()).thenReturn(true);
        Mockito.when(mockEncdbKeyManager.getMekId()).thenReturn(1L);
        Mockito.when(mockEncdbKeyManager.getKmsRegion()).thenReturn("cn-hangzhou");
        Mockito.when(mockEncdbKeyManager.getKmsEncMek()).thenReturn(kmsEncMek);
        Mockito.when(mockEncdbKeyManager.getKmsKeyId()).thenReturn("dbstack-key-id");
        Mockito.when(mockEncdbKeyManager.getKmsIv()).thenReturn(kmsIv);
        return mockEncdbKeyManager;
    }

    private EncdbServer mockEncdbServer() {
        EncdbServer mockEncdbServer = Mockito.mock(EncdbServer.class);
        Mockito.when(mockEncdbServer.getCipherSuite()).thenReturn(EncdbServer.cipherSuite);
        Mockito.when(mockEncdbServer.getPemPublicKey()).thenReturn("public-key");
        Mockito.when(mockEncdbServer.getPublicKeyHash()).thenReturn("public-key-hash");
        Mockito.when(mockEncdbServer.getEnclaveId()).thenReturn("0");
        return mockEncdbServer;
    }
}
