package com.alibaba.polardbx.manager.response;

import com.alibaba.fastjson.JSON;
import com.alibaba.polardbx.common.exception.NotSupportException;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.handler.LogicalShowRoutingRulesHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.executor.sync.ISyncAction;
import com.alibaba.polardbx.executor.sync.SyncManagerHelper;
import com.alibaba.polardbx.gms.metadb.htap.RoutingRuleAccessor;
import com.alibaba.polardbx.gms.metadb.htap.RoutingRuleRecord;
import com.alibaba.polardbx.gms.sync.SyncScope;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.server.response.ShowRoutingRulesSyncAction;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LogicalShowRoutingRulesTest {
    @Mock
    private IRepository repo;

    @Mock
    private RelNode logicalPlan;

    @Mock
    private ExecutionContext executionContext;

    @Mock
    private Connection connection;

    private AutoCloseable closeable;

    @Before
    public void setUp() throws Exception {
        closeable = MockitoAnnotations.openMocks(this);
        when(executionContext.getSchemaName()).thenReturn("test_schema");
    }

    @After
    public void tearDown() throws Exception {
        closeable.close();
    }

    /**
     * 测试handle(RelNode, ExecutionContext)方法正常执行情况
     */
    @Test
    public void testHandleWithRelNode_Normal() throws Exception {
        // 准备测试数据
        List<List<Map<String, Object>>> syncResult = new ArrayList<>();
        List<Map<String, Object>> innerList = new ArrayList<>();
        Map<String, Object> resultMap = new HashMap<>();
        resultMap.put("ID", 1L);
        resultMap.put("COUNT", 10L);
        innerList.add(resultMap);

        resultMap = new HashMap<>();
        resultMap.put("ID", 2L);
        resultMap.put("COUNT", -1L);
        innerList.add(resultMap);

        resultMap = new HashMap<>();
        resultMap.put("ID", 3L);
        resultMap.put("COUNT", 11L);
        innerList.add(resultMap);

        resultMap = new HashMap<>();
        resultMap.put("ID", 1L);
        resultMap.put("COUNT", -1L);
        innerList.add(resultMap);

        resultMap = new HashMap<>();
        resultMap.put("ID", 3L);
        resultMap.put("COUNT", 1L);
        innerList.add(resultMap);
        syncResult.add(innerList);

        try (
            MockedStatic<SyncManagerHelper> syncManagerHelperMockedStatic = Mockito.mockStatic(SyncManagerHelper.class);
            MockedStatic<MetaDbUtil> metaDbUtilMockedStatic = Mockito.mockStatic(MetaDbUtil.class);
            MockedStatic<InstIdUtil> instIdUtilMockedStatic = Mockito.mockStatic(InstIdUtil.class)) {
            syncManagerHelperMockedStatic.when(
                    () -> SyncManagerHelper.syncIgnoreExceptions(any(ISyncAction.class), anyString(), any(SyncScope.class)))
                .thenReturn(syncResult);
            metaDbUtilMockedStatic.when(() -> MetaDbUtil.getConnection()).thenReturn(connection);
            instIdUtilMockedStatic.when(() -> InstIdUtil.getInstId()).thenReturn("test_inst_id");

            // Mock RoutingRuleAccessor
            List<RoutingRuleRecord> records = new ArrayList<>();
            ResultSet rs = mock(ResultSet.class);
            when(rs.getLong("id")).thenReturn(1L);
            when(rs.getString("rule_name")).thenReturn("test_rule");
            when(rs.getString("user_name")).thenReturn("test_user");
            when(rs.getString("template_id")).thenReturn("test_template");
            when(rs.getString("keywords")).thenReturn(JSON.toJSONString(ImmutableList.of("test_keyword")));
            when(rs.getString("routing_type")).thenReturn("columnar");
            when(rs.getTimestamp("gmt_created")).thenReturn(new Timestamp(System.currentTimeMillis()));
            RoutingRuleRecord record = new RoutingRuleRecord();
            record.fill(rs);
            records.add(record);

            record = new RoutingRuleRecord();
            record.id = 2L;
            records.add(record);
            record = new RoutingRuleRecord();
            record.id = 3L;
            records.add(record);
            record = new RoutingRuleRecord();
            record.id = 4L;
            records.add(record);

            RoutingRuleAccessor accessor = Mockito.mock(RoutingRuleAccessor.class);
            when(accessor.query(anyString())).thenReturn(records);

            // 执行测试
            LogicalShowRoutingRulesHandler handler = new LogicalShowRoutingRulesHandler(repo) {
                @Override
                protected RoutingRuleAccessor getRoutingAccessor() {
                    return accessor;
                }
            };
            Cursor result = handler.handle(logicalPlan, executionContext);

            // 验证结果
            Row row = result.next();
            while (row != null) {
                long id = row.getLong(0);
                switch ((int) id) {
                case 1:
                    assertEquals((Long) 9L, row.getLong(7));
                    break;
                case 2:
                    assertEquals((Long) Long.MAX_VALUE, row.getLong(7));
                    break;
                case 3:
                    assertEquals((Long) 12L, row.getLong(7));
                    break;
                case 4:
                    assertEquals((Long) 0L, row.getLong(7));
                    break;
                default:
                    fail();
                }
                row = result.next();
            }
        }
    }

    /**
     * 测试handle方法当showRoutingRulesSyncActionClass为null时抛出NotSupportException
     */
    @Test(expected = NotSupportException.class)
    public void testHandle_ShowRoutingRulesSyncActionClassIsNull() {
        try {
            setClass(null);
            LogicalShowRoutingRulesHandler handler = new LogicalShowRoutingRulesHandler(repo);

            // 执行测试，期望抛出NotSupportException
            handler.handle(logicalPlan, executionContext);
        } finally {
            setClass(ShowRoutingRulesSyncAction.class);
        }
    }

    /**
     * 测试handle方法当创建ISyncAction实例失败时抛出TddlRuntimeException
     */
    @Test(expected = TddlRuntimeException.class)
    public void testHandle_CreateSyncActionFailed() {
        try {
            setClass(ISyncAction.class);
            LogicalShowRoutingRulesHandler handler = new LogicalShowRoutingRulesHandler(repo);
            // 执行测试，期望抛出TddlRuntimeException
            handler.handle(logicalPlan, executionContext);
        } finally {
            setClass(ShowRoutingRulesSyncAction.class);
        }
    }

    void setClass(Object value) {
        try {
            java.lang.reflect.Field field = LogicalShowRoutingRulesHandler.class.getDeclaredField(
                "showRoutingRulesSyncActionClass");
            field.setAccessible(true);
            field.set(null, value);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}