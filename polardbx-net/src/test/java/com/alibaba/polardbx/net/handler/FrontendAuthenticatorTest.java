package com.alibaba.polardbx.net.handler;

import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.net.FrontendConnection;
import com.alibaba.polardbx.net.NIOProcessor;
import com.alibaba.polardbx.net.packet.AuthPacket;
import com.alibaba.polardbx.net.util.PrivilegeUtil;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyByte;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.booleanThat;
import static org.mockito.ArgumentMatchers.charThat;
import static org.mockito.ArgumentMatchers.contains;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.matches;
import static org.mockito.Mockito.clearInvocations;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class FrontendAuthenticatorTest {

    @Test
    public void test() {
        String testUser = "test_user";
        try (MockedStatic<PrivilegeUtil> privilegeUtil = mockStatic(PrivilegeUtil.class);
            final MockedConstruction<AuthPacket> autoMockedConstruction =
                mockConstruction(AuthPacket.class, (mock, context) -> {
                    mock.user = testUser;
                    mock.password = new byte[] {1, 2, 3};
                });
        ) {
            privilegeUtil.when(() -> PrivilegeUtil.isPasswordEmpty(any())).thenReturn(false);
            Privileges privileges = mock(Privileges.class);
            when(privileges.userExists(testUser)).thenReturn(true);
            when(privileges.checkQuarantine(testUser, null)).thenReturn(true);
            FrontendConnection fc = mock(FrontendConnection.class);
            when(fc.getPrivileges()).thenReturn(privileges);
            when(fc.checkConnectionCount()).thenReturn(true);
            FrontendAuthenticator fa = new FrontendAuthenticator(fc);
            byte[] data = new byte[100];
            fa.handle(data);

            verify(fc).writeErrMessage(anyByte(), anyInt(), eq(null), contains("because password is not correct"));

            DynamicConfig.getInstance().loadValue(null, "ENABLE_CONSISTENT_ERRORCODE", "true");
            clearInvocations(fc);

            fa.handle(data);
            ArgumentCaptor<String> argumentCaptor = ArgumentCaptor.forClass(String.class);

            verify(fc).writeErrMessage(anyByte(), anyInt(), eq(null), argumentCaptor.capture());

            assert !argumentCaptor.getValue().contains("because password is not correct");
            clearInvocations(fc);
        } finally {
            DynamicConfig.getInstance().loadValue(null, "ENABLE_CONSISTENT_ERRORCODE", "false");
        }

    }

    @Test
    public void testClientAttributesAreSavedOnSuccessfulAuthentication() {
        FrontendConnection connection = mock(FrontendConnection.class);
        NIOProcessor processor = mock(NIOProcessor.class);
        when(connection.getProcessor()).thenReturn(processor);
        when(processor.getCommands()).thenReturn(mock(CommandCount.class));

        AuthPacket auth = mock(AuthPacket.class);
        when(auth.getConnectionAttribute(AuthPacket.ATTR_CLIENT_NAME)).thenReturn("MySQL Connector Java");
        when(auth.getConnectionAttribute(AuthPacket.ATTR_CLIENT_VERSION)).thenReturn("5.1.40.12");

        TestFrontendAuthenticator target = new TestFrontendAuthenticator(connection);
        target.successForTest(auth);

        verify(connection).setClientName("MySQL Connector Java");
        verify(connection).setClientVersion("5.1.40.12");
    }

    private static class TestFrontendAuthenticator extends FrontendAuthenticator {

        private TestFrontendAuthenticator(FrontendConnection source) {
            super(source);
        }

        private void successForTest(AuthPacket auth) {
            success(auth, true);
        }

        @Override
        protected void sendOk(com.alibaba.polardbx.net.buffer.ByteBufferHolder buffer) {
        }
    }
}
