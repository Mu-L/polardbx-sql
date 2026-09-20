package com.alibaba.polardbx.gms.metadb;

import com.alibaba.polardbx.gms.metadb.htap.RoutingRuleAccessor;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Arrays;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doCallRealMethod;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

public class RoutingRuleAccessorTest {

    @Test
    public void testDeleteByUser() {
        try (MockedStatic<MetaDbUtil> mockedStatic = mockStatic(MetaDbUtil.class)) {
            mockedStatic.when(() -> MetaDbUtil.delete(any(), any(Connection.class)))
                .thenReturn(1);
            RoutingRuleAccessor routingRuleAccessor = new RoutingRuleAccessor();
            routingRuleAccessor.setConnection(mock(Connection.class));
            routingRuleAccessor.deleteByUser(Arrays.asList("hh"));

            mockedStatic.when(() -> MetaDbUtil.delete(any(), any(Connection.class)))
                .thenThrow(new SQLException("gg"));
            try {
                routingRuleAccessor.deleteByUser(Arrays.asList("hh"));
                fail("应该抛出 TddlRuntimeException 异常");
            } catch (Throwable e) {
                assertTrue(e.getMessage().contains("gg"));
            }
        }
    }

    @Test
    public void testLockUser() {
        RoutingRuleAccessor routingRuleAccessor = mock(RoutingRuleAccessor.class);
        doCallRealMethod().when(routingRuleAccessor).lockRule(any(), any());
        routingRuleAccessor.lockRule("instId", "ruleName");
    }
}
