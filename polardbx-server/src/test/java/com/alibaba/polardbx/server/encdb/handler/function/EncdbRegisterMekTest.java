package com.alibaba.polardbx.server.encdb.handler.function;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.polardbx.common.encdb.enums.MsgKeyConstants;
import com.alibaba.polardbx.common.encdb.utils.Utils;
import com.alibaba.polardbx.gms.privilege.AccountType;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import com.alibaba.polardbx.server.encdb.handler.EncdbMekProvisionHandler;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.nio.charset.StandardCharsets;

public class EncdbRegisterMekTest {

    private final EncdbRegisterMek target = new EncdbRegisterMek();

    @Test
    public void testRegisterKmsMekWithNullIv() throws Exception {
        String mockKmsEncMek = "kms-encrypted-mek";
        byte[] mockKmsPlainMek = "plain-mek".getBytes(StandardCharsets.UTF_8);
        ExecutionContext mockExecutionContext = Mockito.mock(ExecutionContext.class);
        PrivilegeContext mockPrivilegeContext = Mockito.mock(PrivilegeContext.class);
        PolarAccountInfo mockPolarAccountInfo = Mockito.mock(PolarAccountInfo.class);
        Mockito.when(mockExecutionContext.getPrivilegeContext()).thenReturn(mockPrivilegeContext);
        Mockito.when(mockPrivilegeContext.getPolarUserInfo()).thenReturn(mockPolarAccountInfo);
        Mockito.when(mockPolarAccountInfo.getAccountType()).thenReturn(AccountType.GOD);

        try (MockedStatic<EncdbMekProvisionHandler> mockEncdbMekProvisionHandler =
            Mockito.mockStatic(EncdbMekProvisionHandler.class)) {
            Object[] args = {
                mockKmsEncMek,
                Utils.bytesTobase64(mockKmsPlainMek),
                "cn-hangzhou",
                "dbstack-key-id",
                null
            };

            JSONObject result = (JSONObject) target.compute(args, mockExecutionContext);

            Assert.assertEquals(0, result.getIntValue(MsgKeyConstants.STATUS));
            ArgumentCaptor<String> mockKmsEncMekCaptor = ArgumentCaptor.forClass(String.class);
            ArgumentCaptor<byte[]> mockKmsPlainMekCaptor = ArgumentCaptor.forClass(byte[].class);
            mockEncdbMekProvisionHandler.verify(() -> EncdbMekProvisionHandler.registerKmsMek(
                mockKmsEncMekCaptor.capture(), mockKmsPlainMekCaptor.capture(), Mockito.eq("cn-hangzhou"),
                Mockito.eq("dbstack-key-id"), Mockito.<String>isNull()));
            Assert.assertEquals(mockKmsEncMek, mockKmsEncMekCaptor.getValue());
            Assert.assertArrayEquals(mockKmsPlainMek, mockKmsPlainMekCaptor.getValue());
        }
    }
}
