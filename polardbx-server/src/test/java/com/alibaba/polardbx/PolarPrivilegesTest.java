package com.alibaba.polardbx;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.common.properties.DynamicConfig;
import com.alibaba.polardbx.gms.privilege.AccountType;
import com.alibaba.polardbx.gms.privilege.PolarAccount;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.net.FrontendConnection;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * Unit test for PolarPrivileges.checkAndGetMatchUser method
 */
public class PolarPrivilegesTest {

    private PolarPrivileges polarPrivileges;
    private FrontendConnection mockConnection;
    private PolarPrivManager mockPrivManager;
    private PolarQuarantineManager mockQuarantineManager;
    private DynamicConfig mockDynamicConfig;
    private MockedStatic<PolarQuarantineManager> quarantineStatic;
    private MockedStatic<PolarPrivManager> privManagerStatic;
    private MockedStatic<DynamicConfig> configStatic;

    @Before
    public void setUp() {
        mockConnection = mock(FrontendConnection.class);
        mockQuarantineManager = mock(PolarQuarantineManager.class);
        mockPrivManager = mock(PolarPrivManager.class);
        mockDynamicConfig = mock(DynamicConfig.class);

        // Mock singletons to avoid initialization issues
        quarantineStatic = mockStatic(PolarQuarantineManager.class);
        quarantineStatic.when(PolarQuarantineManager::getInstance).thenReturn(mockQuarantineManager);

        privManagerStatic = mockStatic(PolarPrivManager.class);
        privManagerStatic.when(PolarPrivManager::getInstance).thenReturn(mockPrivManager);

        configStatic = mockStatic(DynamicConfig.class);
        configStatic.when(DynamicConfig::getInstance).thenReturn(mockDynamicConfig);

        polarPrivileges = new PolarPrivileges(mockConnection);
    }

    @After
    public void tearDown() {
        if (quarantineStatic != null) {
            quarantineStatic.close();
        }
        if (privManagerStatic != null) {
            privManagerStatic.close();
        }
        if (configStatic != null) {
            configStatic.close();
        }
    }

    @Test
    public void testCheckAndGetMatchUser_UserExists_Success() {
        // Arrange
        String user = "testuser";
        String host = "192.168.1.1";

        PolarAccountInfo expectedUserInfo = new PolarAccountInfo(
            PolarAccount.newBuilder()
                .setUsername(user)
                .setHost(host)
                .setAccountType(AccountType.USER)
                .build()
        );

        when(mockConnection.getMatchPolarUserInfo()).thenReturn(null);
        when(mockPrivManager.getMatchUser(user, host)).thenReturn(expectedUserInfo);

        // Act
        PolarAccountInfo result = polarPrivileges.checkAndGetMatchUser(user, host);

        // Assert
        assertNotNull(result);
        assertEquals(user, result.getUsername());
        assertEquals(host, result.getHost());
    }

    @Test
    public void testCheckAndGetMatchUser_UserNotExists_ConsistentErrorCodeEnabled() {
        // Arrange
        String user = "nonexistent";
        String host = "192.168.1.1";

        when(mockConnection.getMatchPolarUserInfo()).thenReturn(null);
        when(mockPrivManager.getMatchUser(user, host)).thenReturn(null);
        when(mockDynamicConfig.isEnableConsistentErrorCode()).thenReturn(true);

        // Act & Assert
        try {
            polarPrivileges.checkAndGetMatchUser(user, host);
            fail("Expected TddlRuntimeException to be thrown");
        } catch (TddlRuntimeException e) {
            assertEquals(ErrorCode.ER_ACCESS_DENIED_ERROR, e.getErrorCodeType());
            assertTrue(e.getMessage().contains("Access denied for user"));
        }
    }

    @Test
    public void testCheckAndGetMatchUser_UserNotExists_ConsistentErrorCodeDisabled() {
        // Arrange
        String user = "nonexistent";
        String host = "192.168.1.1";

        when(mockConnection.getMatchPolarUserInfo()).thenReturn(null);
        when(mockPrivManager.getMatchUser(user, host)).thenReturn(null);
        when(mockDynamicConfig.isEnableConsistentErrorCode()).thenReturn(false);

        // Act & Assert
        try {
            polarPrivileges.checkAndGetMatchUser(user, host);
            fail("Expected RuntimeException to be thrown");
        } catch (RuntimeException e) {
            // Expected exception from GeneralUtil.nestedException
            assertNotNull(e.getMessage());
            assertTrue(e.getMessage().contains("does not exist"));
        }
    }

    @Test
    public void testCheckAndGetMatchUser_RoleAccount_ReturnsNull() {
        // Arrange
        String user = "roleuser";
        String host = "%";

        PolarAccountInfo roleUserInfo = new PolarAccountInfo(
            PolarAccount.newBuilder()
                .setUsername(user)
                .setHost(host)
                .setAccountType(AccountType.ROLE)
                .build()
        );

        when(mockConnection.getMatchPolarUserInfo()).thenReturn(null);
        when(mockPrivManager.getMatchUser(user, host)).thenReturn(roleUserInfo);
        when(mockDynamicConfig.isEnableConsistentErrorCode()).thenReturn(true);

        // Act & Assert
        try {
            polarPrivileges.checkAndGetMatchUser(user, host);
            fail("Expected TddlRuntimeException to be thrown");
        } catch (TddlRuntimeException e) {
            assertEquals(ErrorCode.ER_ACCESS_DENIED_ERROR, e.getErrorCodeType());
        }
    }

    @Test
    public void testCheckAndGetMatchUser_WithMatchedUserInfo_UsesExactUser() {
        // Arrange
        String user = "testuser";
        String host = "127.0.0.1";

        PolarAccountInfo matchedUserInfo = new PolarAccountInfo(
            PolarAccount.newBuilder()
                .setUsername(user)
                .setHost(host)
                .setAccountType(AccountType.USER)
                .build()
        );

        when(mockConnection.getMatchPolarUserInfo()).thenReturn(matchedUserInfo);
        when(mockPrivManager.getExactUser(user, host)).thenReturn(matchedUserInfo);

        // Act
        PolarAccountInfo result = polarPrivileges.checkAndGetMatchUser(user, host);

        // Assert
        assertNotNull(result);
        assertEquals(user, result.getUsername());
        assertEquals(host, result.getHost());
    }
}
