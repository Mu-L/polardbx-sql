package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.privilege.AccountType;
import com.alibaba.polardbx.gms.privilege.PolarAccount;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.privilege.PolarInstPriv;
import com.alibaba.polardbx.gms.privilege.PolarLoginErr;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import org.apache.calcite.sql.SqlAlterUser;
import org.apache.calcite.sql.SqlUserName;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyList;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * LogicalAlterUserHandler 类的单元测试类
 * 测试 handlerAlterUserLock 方法的各种场景
 */
public class LogicalAlterUserHandlerLockTest {

    @Mock
    private IRepository repository;

    @Mock
    private SqlAlterUser sqlAlterUser;

    @Mock
    private ExecutionContext executionContext;

    @Mock
    private SqlUserName userSpec;

    @Mock
    private PolarAccount polarAccount;

    @Mock
    private PolarAccountInfo polarAccountInfo;

    @Mock
    private PolarInstPriv polarInstPriv;

    @Mock
    private PolarPrivManager polarPrivManager;

    @Mock
    private Connection connection;

    @Mock
    private Statement statement;

    @Mock
    private PrivilegeContext privilegeContext;

    private LogicalAlterUserHandler handler;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        handler = new LogicalAlterUserHandler(repository);
        when(executionContext.getPrivilegeContext()).thenReturn(privilegeContext);
        when(privilegeContext.getPolarUserInfo()).thenReturn(polarAccountInfo);
        when(polarAccountInfo.getAccountType()).thenReturn(AccountType.SSO);

    }

    /**
     * 测试用例编号：TC000
     * 验证方法抛出 TddlRuntimeException 异常
     */
    @Test
    public void testHandlerAlterUserLock_Privilege() {
        // 准备：设置 mock 对象
        when(sqlAlterUser.getUser()).thenReturn(userSpec);
        when(userSpec.toPolarAccount()).thenReturn(polarAccount);
        when(polarAccount.getUsername()).thenReturn("testuser");
        when(polarAccount.getHost()).thenReturn("%");
        when(polarAccount.getIdentifier()).thenReturn("'testuser'@'%'");

        try {
            when(polarAccountInfo.getAccountType()).thenReturn(AccountType.USER);
            handler.handlerAlterUserLock(sqlAlterUser, executionContext);
            fail("应该抛出 TddlRuntimeException 异常");
        } catch (Exception e) {
            assertTrue("异常信息应该包含权限",
                e.getMessage().contains("ERR_CHECK_PRIVILEGE_FAILED"));
        }
    }

    /**
     * 测试用例编号：TC001
     * 测试场景：用户不存在的情况
     * 设计思路：模拟 getUser() 返回的用户在 PolarPrivManager 中不存在，
     * 验证方法抛出 TddlRuntimeException 异常
     */
    @Test
    public void testHandlerAlterUserLock_UserNotFound() {
        // 准备：设置 mock 对象
        when(sqlAlterUser.getUser()).thenReturn(userSpec);
        when(userSpec.toPolarAccount()).thenReturn(polarAccount);
        when(polarAccount.getUsername()).thenReturn("testuser");
        when(polarAccount.getHost()).thenReturn("%");
        when(polarAccount.getIdentifier()).thenReturn("'testuser'@'%'");

        try (MockedStatic<PolarPrivManager> mockedPrivManager = mockStatic(PolarPrivManager.class)) {
            mockedPrivManager.when(PolarPrivManager::getInstance).thenReturn(polarPrivManager);
            when(polarPrivManager.getExactUser(anyString(), anyString())).thenReturn(null);
            handler.handlerAlterUserLock(sqlAlterUser, executionContext);
            fail("应该抛出 TddlRuntimeException 异常");
        } catch (Exception e) {

            assertTrue("异常信息应该包含用户不存在的描述",
                e.getMessage().contains("User 'testuser'@'%' does not exist."));
        }
    }

    /**
     * 测试用例编号：TC002
     * 测试场景：用户存在且需要锁定的情况
     * 设计思路：模拟用户存在且当前未锁定，需要进行锁定操作，
     * 验证数据库更新操作和重新加载操作被执行
     */
    @Test
    public void testHandlerAlterUserLock_LockUser() throws Exception {
        // 准备：设置 mock 对象
        when(sqlAlterUser.getUser()).thenReturn(userSpec);
        when(userSpec.toPolarAccount()).thenReturn(polarAccount);
        when(polarAccount.getUsername()).thenReturn("testuser");
        when(polarAccount.getHost()).thenReturn("%");
        when(polarAccount.getIdentifier()).thenReturn("'testuser'@'%'");

        when(sqlAlterUser.getLock()).thenReturn(true);

        when(polarPrivManager.getExactUser(anyString(), anyString())).thenReturn(polarAccountInfo);
        when(polarAccountInfo.getInstPriv()).thenReturn(polarInstPriv);
        when(polarInstPriv.isAccountLocked()).thenReturn(false); // 当前未锁定
        when(polarAccountInfo.getAccount()).thenReturn(polarAccount);

        try (MockedStatic<PolarPrivManager> mockedPrivManager = mockStatic(PolarPrivManager.class)) {
            mockedPrivManager.when(PolarPrivManager::getInstance).thenReturn(polarPrivManager);

            // 模拟 runWithMetaDBConnection 方法的执行
            doAnswer(invocation -> {
                Consumer<Connection> consumer = invocation.getArgument(0);
                consumer.accept(connection);
                return null;
            }).when(polarPrivManager).runWithMetaDBConnection(any());

            when(connection.createStatement()).thenReturn(statement);
            when(statement.executeUpdate(anyString())).thenReturn(1);

            // 执行：调用 handlerAlterUserLock 方法
            Cursor cursor = handler.handlerAlterUserLock(sqlAlterUser, executionContext);

            // 验证：检查各个方法是否被正确调用
            verify(polarPrivManager).getExactUser("testuser", "%");
            verify(statement).executeUpdate(anyString());
            verify(connection).commit();
            verify(polarPrivManager).reloadAccounts(eq(connection), anyList());
            verify(polarPrivManager).triggerReload();
            assertTrue("应该返回 AffectRowCursor 实例", cursor instanceof AffectRowCursor);
        }
    }

    /**
     * 测试用例编号：TC003
     * 测试场景：用户存在且需要解锁的情况
     * 设计思路：模拟用户存在且当前已锁定，需要进行解锁操作，
     * 验证数据库更新操作、重新加载操作和清除登录错误计数操作被执行
     */
    @Test
    public void testHandlerAlterUserLock_UnlockUser() throws Exception {
        // 准备：设置 mock 对象
        when(sqlAlterUser.getUser()).thenReturn(userSpec);
        when(userSpec.toPolarAccount()).thenReturn(polarAccount);
        when(polarAccount.getUsername()).thenReturn("testuser");
        when(polarAccount.getHost()).thenReturn("%");
        when(polarAccount.getIdentifier()).thenReturn("'testuser'@'%'");

        when(sqlAlterUser.getLock()).thenReturn(false); // 解锁操作

        when(polarPrivManager.getExactUser(anyString(), anyString())).thenReturn(polarAccountInfo);
        when(polarAccountInfo.getInstPriv()).thenReturn(polarInstPriv);
        when(polarInstPriv.isAccountLocked()).thenReturn(true); // 当前已锁定
        when(polarAccountInfo.getAccount()).thenReturn(polarAccount);

        // 准备登录错误映射
        Map<String, PolarLoginErr> loginErrMap = new HashMap<>();
        PolarLoginErr loginErr = mock(PolarLoginErr.class);
        loginErrMap.put("'testuser'@'%'", loginErr);

        try (MockedStatic<PolarPrivManager> mockedPrivManager = mockStatic(PolarPrivManager.class)) {
            mockedPrivManager.when(PolarPrivManager::getInstance).thenReturn(polarPrivManager);
            when(polarPrivManager.getLoginErrMap()).thenReturn(loginErrMap);

            // 模拟 runWithMetaDBConnection 方法的执行
            doAnswer(invocation -> {
                Consumer<Connection> consumer = invocation.getArgument(0);
                consumer.accept(connection);
                return null;
            }).when(polarPrivManager).runWithMetaDBConnection(any());

            when(connection.createStatement()).thenReturn(statement);
            when(statement.executeUpdate(anyString())).thenReturn(1);

            // 执行：调用 handlerAlterUserLock 方法
            Cursor cursor = handler.handlerAlterUserLock(sqlAlterUser, executionContext);

            // 验证：检查各个方法是否被正确调用
            verify(polarPrivManager).getExactUser("testuser", "%");
            verify(statement).executeUpdate(anyString());
            verify(connection).commit();
            verify(polarPrivManager).reloadAccounts(eq(connection), anyList());
            verify(polarPrivManager).triggerReload();
            assertTrue("应该返回 AffectRowCursor 实例", cursor instanceof AffectRowCursor);
        }
    }

    /**
     * 测试用例编号：TC004
     * 测试场景：用户存在但锁定状态未变化的情况
     * 设计思路：模拟用户存在且当前锁定状态与目标状态相同，
     * 验证不执行任何数据库操作
     */
    @Test
    public void testHandlerAlterUserLock_NoStateChange() throws Exception {
        // 准备：设置 mock 对象
        when(sqlAlterUser.getUser()).thenReturn(userSpec);
        when(userSpec.toPolarAccount()).thenReturn(polarAccount);
        when(polarAccount.getUsername()).thenReturn("testuser");
        when(polarAccount.getHost()).thenReturn("%");

        when(sqlAlterUser.getLock()).thenReturn(true); // 要求锁定

        when(polarPrivManager.getExactUser(anyString(), anyString())).thenReturn(polarAccountInfo);
        when(polarAccountInfo.getInstPriv()).thenReturn(polarInstPriv);
        when(polarInstPriv.isAccountLocked()).thenReturn(true); // 已经锁定

        try (MockedStatic<PolarPrivManager> mockedPrivManager = mockStatic(PolarPrivManager.class)) {
            mockedPrivManager.when(PolarPrivManager::getInstance).thenReturn(polarPrivManager);

            // 执行：调用 handlerAlterUserLock 方法
            Cursor cursor = handler.handlerAlterUserLock(sqlAlterUser, executionContext);

            // 验证：检查 runWithMetaDBConnection 方法未被调用
            verify(polarPrivManager, never()).runWithMetaDBConnection(any());
            assertTrue("应该返回 AffectRowCursor 实例", cursor instanceof AffectRowCursor);
        }
    }

    /**
     * 测试用例编号：TC005
     * 测试场景：数据库更新失败的情况
     * 设计思路：模拟数据库更新操作抛出异常，
     * 验证方法抛出 TddlRuntimeException 异常
     */
    @Test
    public void testHandlerAlterUserLock_DatabaseUpdateFailure() throws Exception {
        // 准备：设置 mock 对象
        when(sqlAlterUser.getUser()).thenReturn(userSpec);
        when(userSpec.toPolarAccount()).thenReturn(polarAccount);
        when(polarAccount.getUsername()).thenReturn("testuser");
        when(polarAccount.getHost()).thenReturn("%");
        when(polarAccount.getIdentifier()).thenReturn("'testuser'@'%'");

        when(sqlAlterUser.getLock()).thenReturn(true);

        when(polarPrivManager.getExactUser(anyString(), anyString())).thenReturn(polarAccountInfo);
        when(polarAccountInfo.getInstPriv()).thenReturn(polarInstPriv);
        when(polarInstPriv.isAccountLocked()).thenReturn(false); // 当前未锁定
        when(polarAccountInfo.getAccount()).thenReturn(polarAccount);

        try (MockedStatic<PolarPrivManager> mockedPrivManager = mockStatic(PolarPrivManager.class)) {
            mockedPrivManager.when(PolarPrivManager::getInstance).thenReturn(polarPrivManager);

            // 模拟 runWithMetaDBConnection 方法的执行并抛出异常
            doAnswer(invocation -> {
                Consumer<Connection> consumer = invocation.getArgument(0);
                consumer.accept(connection);
                return null;
            }).when(polarPrivManager).runWithMetaDBConnection(any());

            when(connection.createStatement()).thenReturn(statement);
            when(statement.executeUpdate(anyString())).thenThrow(new SQLException("Database error"));

            handler.handlerAlterUserLock(sqlAlterUser, executionContext);
            fail("应该抛出 TddlRuntimeException 异常");
        } catch (Exception e) {
            assertTrue("异常信息应该包含持久化失败的描述",
                e.getMessage().contains("Failed to persist account data."));
        }
    }

    /**
     * 测试用例编号：TC006
     * 测试场景：清除登录错误计数的场景
     * 设计思路：模拟解锁操作时存在匹配的登录错误记录，
     * 验证 clearLoginErrorCount 方法被正确调用
     */
    @Test
    public void testHandlerAlterUserLock_ClearLoginErrors() throws Exception {
        // 准备：设置 mock 对象
        when(sqlAlterUser.getUser()).thenReturn(userSpec);
        when(userSpec.toPolarAccount()).thenReturn(polarAccount);
        when(polarAccount.getUsername()).thenReturn("testuser");
        when(polarAccount.getHost()).thenReturn("localhost");
        when(polarAccount.getIdentifier()).thenReturn("'testuser'@'localhost'");

        when(sqlAlterUser.getLock()).thenReturn(false); // 解锁操作

        when(polarPrivManager.getExactUser(anyString(), anyString())).thenReturn(polarAccountInfo);
        when(polarAccountInfo.getInstPriv()).thenReturn(polarInstPriv);
        when(polarInstPriv.isAccountLocked()).thenReturn(true); // 当前已锁定
        when(polarAccountInfo.getAccount()).thenReturn(polarAccount);
        when(polarAccountInfo.isMatch(anyString(), anyString())).thenReturn(true);

        // 准备多个登录错误映射，其中一个是匹配的
        Map<String, PolarLoginErr> loginErrMap = new HashMap<>();
        PolarLoginErr loginErr1 = mock(PolarLoginErr.class);
        PolarLoginErr loginErr2 = mock(PolarLoginErr.class);
        loginErrMap.put("'testuser'@'localhost'", loginErr1); // 匹配的
        loginErrMap.put("'otheruser'@'%'", loginErr2); // 不匹配的

        PolarAccount errAccount1 = mock(PolarAccount.class);
        PolarAccount errAccount2 = mock(PolarAccount.class);
        when(errAccount1.getUsername()).thenReturn("testuser");
        when(errAccount1.getHost()).thenReturn("localhost");
        when(errAccount2.getUsername()).thenReturn("otheruser");
        when(errAccount2.getHost()).thenReturn("%");

        try (MockedStatic<PolarPrivManager> mockedPrivManager = mockStatic(PolarPrivManager.class);
            MockedStatic<PolarAccount> mockedAccount = mockStatic(PolarAccount.class)) {
            mockedPrivManager.when(PolarPrivManager::getInstance).thenReturn(polarPrivManager);
            when(polarPrivManager.getLoginErrMap()).thenReturn(loginErrMap);

            mockedAccount.when(() -> PolarAccount.fromIdentifier("'testuser'@'localhost'")).thenReturn(errAccount1);
            mockedAccount.when(() -> PolarAccount.fromIdentifier("'otheruser'@'%'")).thenReturn(errAccount2);

            // 模拟 runWithMetaDBConnection 方法的执行
            doAnswer(invocation -> {
                Consumer<Connection> consumer = invocation.getArgument(0);
                consumer.accept(connection);
                return null;
            }).when(polarPrivManager).runWithMetaDBConnection(any());

            when(connection.createStatement()).thenReturn(statement);
            when(statement.executeUpdate(anyString())).thenReturn(1);

            // 执行：调用 handlerAlterUserLock 方法
            Cursor cursor = handler.handlerAlterUserLock(sqlAlterUser, executionContext);

            // 验证：检查 clearLoginErrorCount 方法被正确调用
            verify(polarPrivManager).clearLoginErrorCount("testuser", "localhost");
            assertTrue("应该返回 AffectRowCursor 实例", cursor instanceof AffectRowCursor);
        }
    }
}
