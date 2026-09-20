package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.handler.LogicalCreateRoutingRuleHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.metadb.htap.RoutingRuleAccessor;
import com.alibaba.polardbx.gms.metadb.htap.RoutingRuleRecord;
import com.alibaba.polardbx.gms.privilege.PolarAccountInfo;
import com.alibaba.polardbx.gms.privilege.PolarPrivManager;
import com.alibaba.polardbx.gms.privilege.PolarPrivUtil;
import com.alibaba.polardbx.gms.privilege.PolarPrivilegeData;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalRoutingRule;
import com.alibaba.polardbx.optimizer.htaprouting.RoutingRuleManager;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import com.google.common.collect.Lists;
import org.apache.calcite.sql.SqlCreateRoutingRule;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.Pair;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class LogicalCreateRoutingRuleHandlerTest {

    // 常量定义
    private static final String TEST_RULE_NAME = "testrule";
    private static final String TEST_USER_NAME = "testuser";
    private static final String TEST_ROUTING_TYPE_ROW = "row";
    private static final String TEST_ROUTING_TYPE_HTAP = "htap";
    private static final String TEST_ROUTING_TYPE_FOLLOWER = "FOLLOWER";
    private static final String TEST_ROUTING_TYPE_FOLLOWER_WEAK = "stale";
    private static final String TEST_ROUTING_TYPE_NON_FOLLOWER = "ROW";
    private static final String TEST_KEYWORD_1 = "keyword1";
    private static final String TEST_KEYWORD_2 = "keyword2";
    private static final String TEST_TYPE = "type";
    private static final String ALL_USER = "%";
    private static final String VALID_RULE_NAME = "validruleName";
    private static final String VALID_USER_NAME = "someuser";
    private static final String INVALID_RULE_NAME = "invalid_rule_name";
    private static final String DIFFERENT_USER = "different_user";
    private static final String TEST_RULE_SUFFIX = "test_user";
    private static final String MAX_LENGTH_RULE_NAME = generateMaxLengthString(100);
    private static final String TOO_LONG_RULE_NAME = generateMaxLengthString(101);
    private static final String LONG_TEMPLATE_ID = generateMaxLengthString(11);
    private static final String LONG_KEYWORD = generateMaxLengthString(501);

    // 内部测试类
    static class TestLogicalCreateRoutingRuleHandler extends LogicalCreateRoutingRuleHandler {

        public TestLogicalCreateRoutingRuleHandler(IRepository repo) {
            super(repo);
        }

        @Override
        protected Cursor commitRoutingRule(RoutingRuleRecord record, Boolean ifNotExists) {
            return super.commitRoutingRule(record, ifNotExists);
        }

        @Override
        protected void validateRoutingRule(String ruleName, String userName, String templateId, List<String> keywords,
                                           String routingType, ExecutionContext ec) {
            super.validateRoutingRule(ruleName, userName, templateId, keywords, routingType, ec);
        }

        @Override
        protected void validateRuleName(String ruleName) {
            super.validateRuleName(ruleName);
        }

        @Override
        protected void validatePrivilege(String userName, ExecutionContext ec) {
            super.validatePrivilege(userName, ec);
        }

        @Override
        protected void validateTemplateIdAndKeyWords(String templateId, List<String> keyWords) {
            super.validateTemplateIdAndKeyWords(templateId, keyWords);
        }

        @Override
        protected void validateRoutingType(String routingType) {
            super.validateRoutingType(routingType);
        }

        @Override
        protected void validateUser(String userName, String templateId, List<String> keyWords) {
            super.validateUser(userName, templateId, keyWords);
        }

        @Override
        protected void validateFollower(String ruleName, String userName, String templateId,
                                        List<String> keywords, String routingType) {
            super.validateFollower(ruleName, userName, templateId, keywords, routingType);
        }
    }

    @Mock
    private Connection metaDbConn;

    @Mock
    private ExecutionContext ec;

    @Mock
    private PrivilegeContext privilegeContext;

    @Mock
    private MetaDbConfigManager metaDbConfigManager;

    @Mock
    private PolarPrivManager polarPrivManager;

    @Mock
    private PolarPrivilegeData polarPrivilegeData;

    @InjectMocks
    private TestLogicalCreateRoutingRuleHandler handler;

    @Before
    public void setUp() throws SQLException {
        when(ec.getPrivilegeContext()).thenReturn(privilegeContext);
        doNothing().when(metaDbConn).setTransactionIsolation(anyInt());
        doNothing().when(metaDbConn).setAutoCommit(anyBoolean());
        doNothing().when(metaDbConn).commit();
        doNothing().when(metaDbConfigManager).sync(anyString());
        when(metaDbConfigManager.notify(anyString(), any())).thenReturn(1L);
    }

    @Test
    public void handle_ValidInputs_ShouldReturnAffectRowCursor() {
        String[] keywords = new String[] {TEST_KEYWORD_1, TEST_KEYWORD_2};

        SqlCreateRoutingRule sqlCreateRoutingRule = createSqlCreateRoutingRule(
            TEST_RULE_NAME, TEST_USER_NAME, keywords, TEST_TYPE, TEST_ROUTING_TYPE_ROW);

        final RoutingRuleRecord[] routingRuleRecord = {null};
        LogicalRoutingRule logicalRoutingRule = mock(LogicalRoutingRule.class);
        when(logicalRoutingRule.getSqlDal()).thenReturn(sqlCreateRoutingRule);
        new LogicalCreateRoutingRuleHandler(null) {
            protected Cursor commitRoutingRule(RoutingRuleRecord record, Boolean ifNotExists) {
                routingRuleRecord[0] = record;
                return new AffectRowCursor(0);
            }

            @Override
            protected void validateRoutingRule(String ruleName, String userName, String templateId,
                                               List<String> keywords, String routingType, ExecutionContext ec) {
            }
        }.handle(logicalRoutingRule, ec);

        Assert.assertEquals(InstIdUtil.getInstId(), routingRuleRecord[0].instId);
        Assert.assertEquals(TEST_RULE_NAME, routingRuleRecord[0].ruleName);
        Assert.assertEquals(TEST_USER_NAME, routingRuleRecord[0].userName);
        Assert.assertNull(routingRuleRecord[0].templateId);
        Assert.assertEquals(String.join(",", keywords), String.join(",", routingRuleRecord[0].keywords));
        Assert.assertEquals(TEST_ROUTING_TYPE_ROW, routingRuleRecord[0].routingType);
    }

    @Test
    public void testCommitRoutingRule() {
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = mockStatic(MetaDbUtil.class);
            MockedStatic<MetaDbConfigManager> metaDbConfigManagerMockedStatic = mockStatic(
                MetaDbConfigManager.class);
            MockedStatic<RoutingRuleAccessor> routingRuleAccessorMockedStatic = mockStatic(
                RoutingRuleAccessor.class);) {
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(metaDbConn);
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.beginTransaction(metaDbConn)).thenCallRealMethod();
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.commit(metaDbConn)).thenCallRealMethod();
            metaDbConfigManagerMockedStatic.when(MetaDbConfigManager::getInstance).thenReturn(metaDbConfigManager);

            RoutingRuleAccessor accessor = mock(RoutingRuleAccessor.class);
            when(accessor.insert(any())).thenReturn(1);
            routingRuleAccessorMockedStatic.when(() -> RoutingRuleAccessor.create(metaDbConn)).thenReturn(accessor);
            RoutingRuleRecord record = new RoutingRuleRecord();

            // already exists
            when(accessor.lockRule(null, null)).thenReturn(
                Collections.singletonList(mock(RoutingRuleRecord.class)));
            Cursor cursor = handler.commitRoutingRule(record, true);
            Assert.assertEquals(0L, (long) cursor.next().getInteger(0));
            try {
                handler.commitRoutingRule(record, false);
                Assert.fail("Unexpected TddlRuntimeException");
            } catch (TddlRuntimeException e) {
                Assert.assertTrue(e.getMessage().contains("already exists!"));
            }
            // insert
            when(accessor.lockRule(null, null)).thenReturn(null);
            cursor = handler.commitRoutingRule(record, true);
            Assert.assertEquals(1L, (long) cursor.next().getInteger(0));

            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenThrow(
                new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, "acquire meta db error"));

            try {
                handler.commitRoutingRule(record, true);
                Assert.fail("Unexpected TddlRuntimeException");
            } catch (TddlRuntimeException e) {
                Assert.assertTrue(e.getMessage().contains("acquire meta db error"));
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test(expected = TddlRuntimeException.class)
    public void validateRuleName_EmptyString_ThrowsException() {
        handler.validateRuleName("");
    }

    @Test(expected = TddlRuntimeException.class)
    public void validateRuleName_NullString_ThrowsException() {
        handler.validateRuleName(null);
    }

    @Test(expected = TddlRuntimeException.class)
    public void validateRuleName_TooLongString_ThrowsException() {
        handler.validateRuleName(TOO_LONG_RULE_NAME);
    }

    @Test()
    public void validateRuleName_InvalidString_ThrowsException() {
        handler.validateRuleName("$$_Ab12");
        try {
            handler.validateRuleName("``");
            Assert.fail("Expected TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("letters, digits, underscores, and dollar signs"));
        }
        try {
            handler.validateRuleName(";$\"123");
            Assert.fail("Expected TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("letters, digits, underscores, and dollar signs"));
        }
    }

    @Test()
    public void validateRoutingType_InvalidString_ThrowsException() {
        try {
            handler.validateRoutingType("hh");
            Assert.fail("Expected TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("is not valid"));
        }
    }

    @Test
    public void validateAllUser() {
        handler.validateUser(RoutingRuleManager.ALL_USER, "gg", new ArrayList<>());
        try {
            handler.validateUser(RoutingRuleManager.ALL_USER, "", new ArrayList<>());
            Assert.fail("Expected TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("can't be set for all user without templateId or keyWords"));
        }
    }

    @Test
    public void validateUser_KnownUser_NoException() {
        Mockito.when(ec.isSuperUser()).thenReturn(true);
        // 准备：模拟用户存在的情况
        List<PolarAccountInfo> accountInfos = new ArrayList<>();
        PolarAccountInfo accountInfo = mock(PolarAccountInfo.class);
        when(accountInfo.getUsername()).thenReturn(VALID_USER_NAME);
        accountInfos.add(accountInfo);

        // 执行：调用验证方法
        try (MockedStatic<PolarPrivManager> polarPrivManagerMockedStatic = mockStatic(PolarPrivManager.class)) {
            polarPrivManagerMockedStatic.when(PolarPrivManager::getInstance).thenReturn(polarPrivManager);
            when(polarPrivManager.getAccountPrivilegeData()).thenReturn(polarPrivilegeData);
            when(polarPrivilegeData.getAccountInfo()).thenReturn(accountInfos);
            handler.validateRoutingRule(VALID_RULE_NAME, VALID_USER_NAME, "", null, TEST_ROUTING_TYPE_HTAP, ec);
            // 验证：不抛出异常，方法正常返回
            assertTrue("方法应该正常返回，不抛出异常", true);
        }
    }

    @Test
    public void validateUser_UnknownUser_ThrowsException() {
        // 准备：模拟用户不存在的情况
        List<PolarAccountInfo> accountInfos = new ArrayList<>();
        PolarAccountInfo accountInfo1 = mock(PolarAccountInfo.class);
        when(accountInfo1.getUsername()).thenReturn("otheruser1");
        accountInfos.add(accountInfo1);

        PolarAccountInfo accountInfo2 = mock(PolarAccountInfo.class);
        when(accountInfo2.getUsername()).thenReturn("otheruser2");
        accountInfos.add(accountInfo2);

        try (MockedStatic<PolarPrivManager> polarPrivManagerMockedStatic = mockStatic(PolarPrivManager.class)) {
            polarPrivManagerMockedStatic.when(PolarPrivManager::getInstance).thenReturn(polarPrivManager);
            when(polarPrivManager.getAccountPrivilegeData()).thenReturn(polarPrivilegeData);
            when(polarPrivilegeData.getAccountInfo()).thenReturn(accountInfos);
            handler.validateUser("gege", "", new ArrayList<>());
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("unknown user gege"));
        }
    }

    @Test
    public void validateRuleName_ValidString_NoException() {
        handler.validateRuleName(VALID_RULE_NAME);
    }

    @Test
    public void validateRuleName_MaxLengthString_NoException() {
        handler.validateRuleName(MAX_LENGTH_RULE_NAME);
    }

    @Test
    public void validatePrivilege_EmptyUserName_ThrowsException() {
        try {
            handler.validatePrivilege("", ec);
            Assert.fail("Expected TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("user name is empty!"));
        }
    }

    @Test
    public void validatePrivilege_WildcardUserNameSuperUser_Passes() {
        Mockito.when(ec.isSuperUser()).thenReturn(true);
        handler.validatePrivilege(ALL_USER, ec);
        // 如果没有抛出异常，则测试通过
    }

    @Test
    public void validatePrivilege_WildcardUserNameNonSuperUser_ThrowsException() {
        Mockito.when(ec.isSuperUser()).thenReturn(false);
        try {
            handler.validatePrivilege(ALL_USER, ec);
            Assert.fail("Expected TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("only super user can assign user '%'!"));
        }
    }

    @Test
    public void validatePrivilege_NonWildcardUserNameSuperUser_Passes() {
        Mockito.when(ec.isSuperUser()).thenReturn(true);
        handler.validatePrivilege(VALID_USER_NAME, ec);
        // 如果没有抛出异常，则测试通过
    }

    @Test
    public void validatePrivilege_WildcardUserNameNonGodUser_ThrowsException() {
        Mockito.when(ec.isSuperUser()).thenReturn(true);
        Mockito.when(ec.isGod()).thenReturn(false);
        try {
            handler.validatePrivilege(PolarPrivUtil.POLAR_ROOT, ec);
            Assert.fail("Expected TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("only god can assign polar_root"));
        }
    }

    @Test
    public void validatePrivilege_NonWildcardUserNameGodUser_Passes() {
        Mockito.when(ec.isSuperUser()).thenReturn(true);
        Mockito.when(ec.isGod()).thenReturn(true);
        handler.validatePrivilege(PolarPrivUtil.POLAR_ROOT, ec);
        // 如果没有抛出异常，则测试通过
    }

    @Test
    public void validatePrivilege_NonWildcardUserNameMatchesCurrent_Passes() {
        Mockito.when(ec.isSuperUser()).thenReturn(false);
        Mockito.when(privilegeContext.getUser()).thenReturn(VALID_USER_NAME);
        handler.validatePrivilege(VALID_USER_NAME, ec);
        // 如果没有抛出异常，则测试通过
    }

    @Test
    public void validatePrivilege_NonWildcardUserNameDoesNotMatch_ThrowsException() {
        Mockito.when(ec.isSuperUser()).thenReturn(false);
        Mockito.when(privilegeContext.getUser()).thenReturn("otheruser");
        try {
            handler.validatePrivilege(VALID_USER_NAME, ec);
            Assert.fail("Expected TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            System.out.println(e.getMessage());
            Assert.assertTrue(e.getMessage().contains("user name is not current user!"));
        }
    }

    @Test
    public void validateTemplateIdAndKeyWords_BothEmpty_NoException() {
        handler.validateTemplateIdAndKeyWords("", null);
    }

    @Test
    public void validateTemplateIdAndKeyWords_TemplateIdTooLong_ExceptionThrown() {
        try {
            handler.validateTemplateIdAndKeyWords(LONG_TEMPLATE_ID, null);
            Assert.fail("Expected TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("templateId too long!"));
        }
    }

    @Test
    public void validateTemplateIdAndKeyWords_KeyWordsTooLong_ExceptionThrown() {
        try {
            handler.validateTemplateIdAndKeyWords("", Arrays.asList(LONG_KEYWORD));
            Assert.fail("Expected TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(e.getMessage().contains("keyword too long!"));
        }
    }

    @Test
    public void validateTemplateIdAndKeyWords_BothSet_ExceptionThrown() {
        try {
            handler.validateTemplateIdAndKeyWords("templateId", Arrays.asList("keyWords"));
            Assert.fail("Expected TddlRuntimeException");
        } catch (TddlRuntimeException e) {
            Assert.assertTrue(
                e.getMessage().contains("templateId and keyword can't be set at the same time!"));
        }
    }

    /**
     * 非FOLLOWER路由类型，应该直接返回不抛出异常
     */
    @Test
    public void validateFollower_NonFollowerType_NoException() {
        // 执行：调用验证方法
        // 验证：不抛出异常，方法正常返回
        try {
            handler.validateFollower(TEST_RULE_NAME, TEST_USER_NAME, null, Lists.newArrayList(),
                TEST_ROUTING_TYPE_NON_FOLLOWER);
        } catch (Exception e) {
            fail("非FOLLOWER路由类型应该直接返回，不应该抛出异常: " + e.getMessage());
        }
    }

    /**
     * FOLLOWER路由类型，用户名为ALL_USER，应该抛出异常
     */
    @Test
    public void validateFollower_FollowerTypeWithAllUser_ThrowsException() {
        // 执行和验证：应该抛出TddlRuntimeException异常
        boolean exceptionThrown = false;
        try {
            handler.validateFollower(TEST_RULE_NAME, RoutingRuleManager.ALL_USER, null, Lists.newArrayList(),
                TEST_ROUTING_TYPE_FOLLOWER);
        } catch (TddlRuntimeException e) {
            exceptionThrown = true;
            // 验证异常信息
            assertTrue(e.getMessage().contains("follower can't be set for all user"));
        } catch (Exception e) {
            fail("应该抛出TddlRuntimeException异常，而不是其他异常: " + e.getMessage());
        }

        assertTrue("应该抛出TddlRuntimeException异常", exceptionThrown);
    }

    /**
     * FOLLOWER_WEAK路由类型，用户名为ALL_USER，应该抛出异常
     */
    @Test
    public void validateFollowerWeak_FollowerTypeWithAllUser_ThrowsException() {
        // 执行和验证：应该抛出TddlRuntimeException异常
        boolean exceptionThrown = false;
        try {
            handler.validateFollower(TEST_RULE_NAME, RoutingRuleManager.ALL_USER, null, Lists.newArrayList(),
                TEST_ROUTING_TYPE_FOLLOWER_WEAK);
        } catch (TddlRuntimeException e) {
            exceptionThrown = true;
            // 验证异常信息
            assertTrue(e.getMessage().contains("follower can't be set for all user"));
        } catch (Exception e) {
            fail("应该抛出TddlRuntimeException异常，而不是其他异常: " + e.getMessage());
        }

        assertTrue("应该抛出TddlRuntimeException异常", exceptionThrown);
    }

    /**
     * FOLLOWER路由类型，ruleName不以INNER_RULE开头，应该抛出异常
     */
    @Test
    public void validateFollower_FollowerTypeWithInvalidRuleNamePrefix_ThrowsException() {
        // 执行和验证：应该抛出TddlRuntimeException异常
        boolean exceptionThrown = false;
        try {
            handler.validateFollower(INVALID_RULE_NAME, TEST_USER_NAME, null, Lists.newArrayList(),
                TEST_ROUTING_TYPE_FOLLOWER);
        } catch (TddlRuntimeException e) {
            exceptionThrown = true;
            // 验证异常信息
            assertTrue(
                e.getMessage().contains("rule name " + INVALID_RULE_NAME + " for follower is invalid"));
        } catch (Exception e) {
            fail("应该抛出TddlRuntimeException异常，而不是其他异常: " + e.getMessage());
        }

        assertTrue("应该抛出TddlRuntimeException异常", exceptionThrown);
    }

    /**
     * FOLLOWER路由类型，ruleName以INNER_RULE开头但后缀不匹配userName，应该抛出异常
     */
    @Test
    public void validateFollower_FollowerTypeWithMismatchedRuleNameSuffix_ThrowsException() {
        // 执行和验证：应该抛出TddlRuntimeException异常
        boolean exceptionThrown = false;
        try {
            handler.validateFollower(RoutingRuleManager.INNER_RULE + DIFFERENT_USER, TEST_USER_NAME, null,
                Lists.newArrayList(),
                TEST_ROUTING_TYPE_FOLLOWER);
        } catch (TddlRuntimeException e) {
            exceptionThrown = true;
            // 验证异常信息
            assertTrue(e.getMessage().contains("follower name must be " +
                RoutingRuleManager.INNER_RULE + " + userName"));
        } catch (Exception e) {
            fail("应该抛出TddlRuntimeException异常，而不是其他异常: " + e.getMessage());
        }

        assertTrue("应该抛出TddlRuntimeException异常", exceptionThrown);
    }

    /**
     * FOLLOWER路由类型，非主实例，应该抛出异常
     */
    @Test
    public void validateFollower_SlaveMode_ThrowsException() {
        // 执行和验证：应该抛出TddlRuntimeException异常
        boolean exceptionThrown = false;
        try (MockedStatic<ConfigDataMode> modeMockedStatic = Mockito.mockStatic(ConfigDataMode.class)) {
            modeMockedStatic.when(ConfigDataMode::isMasterMode).thenReturn(false);
            handler.validateFollower(RoutingRuleManager.INNER_RULE + TEST_RULE_SUFFIX, "TEST_USER",
                "", Lists.newArrayList(), TEST_ROUTING_TYPE_FOLLOWER);
        } catch (TddlRuntimeException e) {
            exceptionThrown = true;
            // 验证异常信息
            assertTrue(e.getMessage().contains("follower can't be set for non-master instance"));
        } catch (Exception e) {
            fail("应该抛出TddlRuntimeException异常，而不是其他异常: " + e.getMessage());
        }

        assertTrue("应该抛出TddlRuntimeException异常", exceptionThrown);
    }

    /**
     * FOLLOWER路由类型，ruleName以INNER_RULE开头且后缀匹配userName（小写匹配），应该成功通过验证
     */
    @Test
    public void validateFollower_FollowerTypeWithValidRuleNameLowercase_NoException() {
        // 执行：调用验证方法
        // 验证：不抛出异常，方法正常返回
        try {
            handler.validateFollower(RoutingRuleManager.INNER_RULE + TEST_RULE_SUFFIX, "TEST_USER",
                "", Lists.newArrayList(), TEST_ROUTING_TYPE_FOLLOWER);
        } catch (Exception e) {
            fail("有效的FOLLOWER路由规则应该通过验证，不应该抛出异常: " + e.getMessage());
        }
    }

    @Test
    public void validateFollower_FollowerTypeWithInvalidTemplateId_ThrowsException() {
        // 执行和验证：应该抛出TddlRuntimeException异常
        boolean exceptionThrown = false;
        try {
            handler.validateFollower(RoutingRuleManager.INNER_RULE + TEST_RULE_SUFFIX, "TEST_USER",
                "fe", Lists.newArrayList(), TEST_ROUTING_TYPE_FOLLOWER);
        } catch (TddlRuntimeException e) {
            exceptionThrown = true;
            // 验证异常信息
            assertTrue(
                e.getMessage().contains("templateId for follower can't be set!"));
        } catch (Exception e) {
            fail("应该抛出TddlRuntimeException异常，而不是其他异常: " + e.getMessage());
        }

        assertTrue("应该抛出TddlRuntimeException异常", exceptionThrown);
    }

    @Test
    public void validateFollower_FollowerTypeWithInvalidKeyword_ThrowsException() {
        // 执行和验证：应该抛出TddlRuntimeException异常
        boolean exceptionThrown = false;
        try {
            handler.validateFollower(RoutingRuleManager.INNER_RULE + TEST_RULE_SUFFIX, "TEST_USER",
                null, Lists.newArrayList("gg"), TEST_ROUTING_TYPE_FOLLOWER);
        } catch (TddlRuntimeException e) {
            exceptionThrown = true;
            // 验证异常信息
            assertTrue(
                e.getMessage().contains("keyword for follower can't be set!"));
        } catch (Exception e) {
            fail("应该抛出TddlRuntimeException异常，而不是其他异常: " + e.getMessage());
        }

        assertTrue("应该抛出TddlRuntimeException异常", exceptionThrown);
    }

    /**
     * FOLLOWER路由类型，ruleName以INNER_RULE开头但包含大小写差异，应该成功通过验证（toLowerCase处理）
     */
    @Test
    public void validateFollower_FollowerTypeWithCaseInsensitiveRuleName_NoException() {
        // 执行：调用验证方法
        // 验证：不抛出异常，方法正常返回（因为toLowerCase处理）
        try {
            handler.validateFollower(RoutingRuleManager.INNER_RULE.toUpperCase() + "TEST_USER", "test_user",
                null, Lists.newArrayList(), TEST_ROUTING_TYPE_FOLLOWER);
        } catch (Exception e) {
            fail("大小写不敏感的FOLLOWER路由规则应该通过验证，不应该抛出异常: " + e.getMessage());
        }
    }

    // 辅助方法
    private SqlCreateRoutingRule createSqlCreateRoutingRule(String ruleName, String userName, String[] keywords,
                                                            String type, String routingType) {
        return new SqlCreateRoutingRule(
            SqlParserPos.ZERO,
            false,
            new SqlIdentifier(ruleName, SqlParserPos.ZERO),
            SqlLiteral.createCharString(userName, SqlParserPos.ZERO),
            null,
            new SqlNodeList(
                Arrays.stream(keywords).map(keyword -> SqlLiteral.createCharString(keyword, SqlParserPos.ZERO))
                    .collect(Collectors.toList()), SqlParserPos.ZERO),
            Pair.of(
                new SqlIdentifier(type, SqlParserPos.ZERO),
                SqlLiteral.createCharString(routingType, SqlParserPos.ZERO)
            )
        );
    }

    private static String generateMaxLengthString(int length) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < length; i++) {
            sb.append('a');
        }
        return sb.toString();
    }
}