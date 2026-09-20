package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.AffectRowCursor;
import com.alibaba.polardbx.executor.handler.LogicalDropRoutingRuleHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.metadb.htap.RoutingRuleAccessor;
import com.alibaba.polardbx.gms.metadb.htap.RoutingRuleRecord;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalRoutingRule;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import org.apache.calcite.sql.SqlDropRoutingRule;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LogicalDropRoutingRuleHandlerTest {

    static class TestLogicalDropRoutingRuleHandler extends LogicalDropRoutingRuleHandler {
        public TestLogicalDropRoutingRuleHandler(IRepository repo) {
            super(repo);
        }

        @Override
        protected void validateUser(List<String> ruleNames, ExecutionContext ec) {
            super.validateUser(ruleNames, ec);
        }

        @Override
        protected Cursor commitDrop(List<String> ruleNames, boolean ifExists) {
            return super.commitDrop(ruleNames, ifExists);
        }
    }

    @Mock
    private LogicalRoutingRule logicalRoutingRule;

    @Mock
    private SqlDropRoutingRule sqlDropRoutingRule;

    @Mock
    private Connection metaDbConn;

    @Mock
    private RoutingRuleAccessor routingRuleAccessor;

    @Mock
    private MetaDbConfigManager metaDbConfigManager;

    @Mock
    private TestLogicalDropRoutingRuleHandler handler;

    @Before
    public void setUp() throws SQLException {
        MockitoAnnotations.initMocks(this);
        doNothing().when(metaDbConn).setTransactionIsolation(anyInt());
        doNothing().when(metaDbConn).setAutoCommit(anyBoolean());
        doNothing().when(metaDbConn).commit();
        doNothing().when(metaDbConfigManager).sync(anyString());
        when(metaDbConfigManager.notify(anyString(), any())).thenReturn(1L);
        doCallRealMethod().when(handler).handle(any(), any());
    }

    @Test
    public void handle_NonEmptyRuleNames() {
        Mockito.when(logicalRoutingRule.getSqlDal()).thenReturn(sqlDropRoutingRule);
        Mockito.when(sqlDropRoutingRule.getRuleNames()).thenReturn(Arrays.asList(
            new SqlIdentifier("rule1", SqlParserPos.ZERO),
            new SqlIdentifier("rule2", SqlParserPos.ZERO)));
        Mockito.when(sqlDropRoutingRule.isIfExists()).thenReturn(true);
        when(handler.commitDrop(anyList(), anyBoolean())).thenReturn(new AffectRowCursor(1));
        Cursor cursor = handler.handle(logicalRoutingRule, new ExecutionContext());
        Assert.assertEquals(1L, (long) cursor.next().getInteger(0));
    }

    @Test(expected = TddlRuntimeException.class)
    public void handle_EmptyRuleNames() {
        Mockito.when(logicalRoutingRule.getSqlDal()).thenReturn(sqlDropRoutingRule);
        Mockito.when(sqlDropRoutingRule.getRuleNames()).thenReturn(Collections.emptyList());
        when(handler.commitDrop(anyList(), anyBoolean())).thenReturn(new AffectRowCursor(1));

        handler.handle(logicalRoutingRule, new ExecutionContext());
    }

    @Test
    public void testValidateUser() {
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = Mockito.mockStatic(MetaDbUtil.class);
            MockedStatic<MetaDbConfigManager> metaDbConfigManagerMockedStatic = Mockito.mockStatic(
                MetaDbConfigManager.class);
            MockedStatic<RoutingRuleAccessor> routingRuleAccessorMockedStatic = Mockito.mockStatic(
                RoutingRuleAccessor.class);
            MockedStatic<InstIdUtil> instIdUtilMockedStatic = Mockito.mockStatic(InstIdUtil.class)) {
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(metaDbConn);
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.beginTransaction(metaDbConn)).thenCallRealMethod();
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.commit(metaDbConn)).thenCallRealMethod();
            metaDbConfigManagerMockedStatic.when(MetaDbConfigManager::getInstance).thenReturn(metaDbConfigManager);
            instIdUtilMockedStatic.when(InstIdUtil::getInstId).thenReturn("gg");
            RoutingRuleRecord routingRuleRecord1 = new RoutingRuleRecord();
            routingRuleRecord1.ruleName = "hh1";
            routingRuleRecord1.userName = "user1";
            RoutingRuleRecord routingRuleRecord2 = new RoutingRuleRecord();
            routingRuleRecord2.ruleName = "hh2";
            routingRuleRecord2.userName = "use2";
            when(routingRuleAccessor.query(anyString())).thenReturn(
                Arrays.asList(routingRuleRecord1, routingRuleRecord2));
            routingRuleAccessorMockedStatic.when(() -> RoutingRuleAccessor.create(metaDbConn))
                .thenReturn(routingRuleAccessor);

            ExecutionContext ec = mock(ExecutionContext.class);
            PrivilegeContext pr = mock(PrivilegeContext.class);
            when(ec.getPrivilegeContext()).thenReturn(pr);
            when(pr.getUser()).thenReturn("user1");
            doCallRealMethod().when(handler).validateUser(anyList(), any(ExecutionContext.class));

            when(ec.isSuperUser()).thenReturn(true);
            handler.validateUser(Arrays.asList("1"), ec);

            when(ec.isSuperUser()).thenReturn(false);
            handler.validateUser(Arrays.asList("hh1", "hh3"), ec);

            try {
                handler.validateUser(Arrays.asList("hh1", "hh2"), ec);
                Assert.fail("Unexpected TddlRuntimeException");
            } catch (TddlRuntimeException e) {
                Assert.assertTrue(e.getMessage().contains("which does not belong to you"));
            }
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenThrow(
                new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, "acquire meta db error"));
            try {
                handler.validateUser(Arrays.asList("hh1"), ec);
                Assert.fail("Unexpected TddlRuntimeException");
            } catch (TddlRuntimeException e) {
                Assert.assertTrue(e.getMessage().contains("acquire meta db error"));
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testCommitDropRoutingRule() {
        try (MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = Mockito.mockStatic(MetaDbUtil.class);
            MockedStatic<MetaDbConfigManager> metaDbConfigManagerMockedStatic = Mockito.mockStatic(
                MetaDbConfigManager.class);
            MockedStatic<RoutingRuleAccessor> routingRuleAccessorMockedStatic = Mockito.mockStatic(
                RoutingRuleAccessor.class);
            MockedStatic<InstIdUtil> instIdUtilMockedStatic = Mockito.mockStatic(InstIdUtil.class)) {
            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenReturn(metaDbConn);
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.beginTransaction(metaDbConn)).thenCallRealMethod();
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.commit(metaDbConn)).thenCallRealMethod();
            metaDbConfigManagerMockedStatic.when(MetaDbConfigManager::getInstance).thenReturn(metaDbConfigManager);
            instIdUtilMockedStatic.when(InstIdUtil::getInstId).thenReturn("gg");
            when(routingRuleAccessor.delete(anyString(), anyList())).thenReturn(1);
            routingRuleAccessorMockedStatic.when(() -> RoutingRuleAccessor.create(metaDbConn))
                .thenReturn(routingRuleAccessor);

            doCallRealMethod().when(handler).commitDrop(anyList(), anyBoolean());
            // drop success
            Cursor cursor = handler.commitDrop(Arrays.asList("1"), false);
            Assert.assertEquals(1L, (long) cursor.next().getInteger(0));
            cursor = handler.commitDrop(Arrays.asList("1"), true);
            Assert.assertEquals(1L, (long) cursor.next().getInteger(0));

            try {
                handler.commitDrop(Arrays.asList("1", "2"), false);
                Assert.fail("Unexpected TddlRuntimeException");
            } catch (TddlRuntimeException e) {
                Assert.assertTrue(e.getMessage().contains("does not exist"));
            }
            cursor = handler.commitDrop(Arrays.asList("1,2"), true);
            Assert.assertEquals(1L, (long) cursor.next().getInteger(0));

            metaDbUtilMockedStatic.when(MetaDbUtil::getConnection).thenThrow(
                new TddlRuntimeException(ErrorCode.ERR_GMS_GENERIC, "acquire meta db error"));
            try {
                handler.commitDrop(Arrays.asList("1", "2"), false);
                Assert.fail("Unexpected TddlRuntimeException");
            } catch (TddlRuntimeException e) {
                Assert.assertTrue(e.getMessage().contains("acquire meta db error"));
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
