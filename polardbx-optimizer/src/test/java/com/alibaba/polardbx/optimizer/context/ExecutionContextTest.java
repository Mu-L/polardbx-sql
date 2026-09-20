package com.alibaba.polardbx.optimizer.context;

import com.alibaba.polardbx.common.async.GroupTaskExecutor;
import com.alibaba.polardbx.common.charset.CharsetName;
import com.alibaba.polardbx.common.columnar.ColumnarScanMetrics;
import com.alibaba.polardbx.common.columnar.ExternalColumnStatistics;
import com.alibaba.polardbx.common.columnar.VersionStorageStatistics;
import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.logical.ITConnection;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.common.utils.ExecutorMode;
import com.alibaba.polardbx.common.utils.LoggerUtil;
import com.alibaba.polardbx.common.utils.logger.Logger;
import com.alibaba.polardbx.common.utils.thread.NamedThreadFactory;
import com.alibaba.polardbx.common.utils.thread.ServerThreadPool;
import com.alibaba.polardbx.common.utils.timezone.InternalTimeZone;
import com.alibaba.polardbx.druid.sql.ast.SqlType;
import com.alibaba.polardbx.druid.sql.parser.ByteString;
import com.alibaba.polardbx.gms.metadb.columnar.ColumnarSnapshotCacheManager;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.optimizer.ccl.common.CclContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.rel.dml.DmlWriteContext;
import com.alibaba.polardbx.optimizer.core.profiler.RuntimeStat;
import com.alibaba.polardbx.optimizer.external.files.EphemeralFilesSchemaManager;
import com.alibaba.polardbx.optimizer.htaprouting.HtapTrace;
import com.alibaba.polardbx.optimizer.htaprouting.PlanType;
import com.alibaba.polardbx.optimizer.htaprouting.RoutingType;
import com.alibaba.polardbx.optimizer.htaprouting.WorkloadType;
import com.alibaba.polardbx.optimizer.memory.QueryMemoryPoolHolder;
import com.alibaba.polardbx.optimizer.parse.SqlParameterizeUtils;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.parse.privilege.PrivilegeContext;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.optimizer.planmanager.PreparedStmtCache;
import com.alibaba.polardbx.optimizer.planmanager.parametric.Point;
import com.alibaba.polardbx.optimizer.rule.MockSchemaManager;
import com.alibaba.polardbx.optimizer.spill.QuerySpillSpaceMonitor;
import com.alibaba.polardbx.optimizer.statis.ColumnarTracer;
import com.alibaba.polardbx.optimizer.statis.SQLRecorder;
import com.alibaba.polardbx.optimizer.statis.SQLTracer;
import com.alibaba.polardbx.optimizer.statis.XplanStat;
import com.alibaba.polardbx.optimizer.ttl.query.TtlQueryType;
import com.alibaba.polardbx.optimizer.utils.ExplainResult;
import com.alibaba.polardbx.optimizer.utils.ITransaction;
import com.alibaba.polardbx.stats.MatrixStatistics;
import com.clearspring.analytics.util.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.util.trace.CalcitePlanOptimizerTrace;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.InputStream;
import java.lang.reflect.Field;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.mockito.Mockito.mock;

/**
 * @author fangwu
 */
public class ExecutionContextTest {
    @Test
    public void subqueryRelatedIdTest() {
        ExecutionContext ec = new ExecutionContext();
        ec.getScalarSubqueryCtxMap().put(23342227, new ScalarSubQueryExecContext().setSubQueryResult(1));
        ec.getScalarSubqueryCtxMap().put(0, new ScalarSubQueryExecContext().setSubQueryResult(1));
        ec.getScalarSubqueryCtxMap().put(-1, new ScalarSubQueryExecContext().setSubQueryResult(1));
        ec.getScalarSubqueryCtxMap().put(-23342227, new ScalarSubQueryExecContext().setSubQueryResult(1));
        Assert.assertTrue(ec.getScalarSubqueryVal(23342227) != null);
        Assert.assertTrue(ec.getScalarSubqueryVal(0) != null);
        Assert.assertTrue(ec.getScalarSubqueryVal(-1) != null);
        Assert.assertTrue(ec.getScalarSubqueryVal(-23342227) != null);
    }

    /**
     * 正常情况下复制一个新的ExecutionContext实例
     */
    @Test
    public void testCopyForShardingNormalCase() {
        Parameters mockParams = mock(Parameters.class);
        InternalTimeZone mockTimeZone = mock(InternalTimeZone.class);

        String schema = "test_schema";
        ExecutionContext context = new ExecutionContext("initial_schema");
        Map<String, SchemaManager> schemaManagers = new HashMap<>();
        SchemaManager mockSchemaManager = mock(SchemaManager.class);
        schemaManagers.put(schema, mockSchemaManager);

        ExecutionContext copiedContext = context.copyForSharding(schema, mockParams, schemaManagers, mockTimeZone);

        assertEquals(mockSchemaManager, copiedContext.getSchemaManager(schema));
        assertSame(mockParams, copiedContext.getParams());
        assertSame(schemaManagers, copiedContext.getSchemaManagers());
        assertSame(mockTimeZone, copiedContext.getTimeZone());
    }

    @Test
    public void isEnableXaTsoTest() {
        ExecutionContext ec = new ExecutionContext();
        Map<String, String> properties = new HashMap<>();
        properties.put(ConnectionProperties.ENABLE_XA_TSO, "true");
        properties.put(ConnectionProperties.ENABLE_AUTO_COMMIT_TSO, "true");
        ParamManager paramManager = new ParamManager(properties);
        ec.setParamManager(paramManager);
        Assert.assertTrue(ec.isEnableXaTso());
        Assert.assertTrue(ec.isEnableAutoCommitTso());

        properties.put(ConnectionProperties.ENABLE_XA_TSO, "false");
        properties.put(ConnectionProperties.ENABLE_AUTO_COMMIT_TSO, "false");
        Assert.assertFalse(ec.isEnableXaTso());
        Assert.assertFalse(ec.isEnableAutoCommitTso());

        ec.setParamManager(null);
        Assert.assertFalse(ec.isEnableXaTso());
        Assert.assertFalse(ec.isEnableAutoCommitTso());
    }

    @Test
    public void isMarkSyncPointTest() {
        ExecutionContext ec = new ExecutionContext();
        ec.setExtraServerVariables(new HashMap<>());
        ec.getExtraServerVariables().put(ConnectionProperties.MARK_SYNC_POINT, "true");
        Assert.assertTrue(ec.isMarkSyncPoint());

        ec.getExtraServerVariables().put(ConnectionProperties.MARK_SYNC_POINT, "false");
        Assert.assertFalse(ec.isMarkSyncPoint());

        ec.getExtraServerVariables().put(ConnectionProperties.MARK_SYNC_POINT, null);
        Assert.assertFalse(ec.isMarkSyncPoint());

        ec.getExtraServerVariables().remove(ConnectionProperties.MARK_SYNC_POINT);
        Assert.assertFalse(ec.isMarkSyncPoint());

        ec.setExtraServerVariables(null);
        Assert.assertFalse(ec.isMarkSyncPoint());
    }

    @Test
    public void flashbackAreaTest() {
        ExecutionContext ec = new ExecutionContext();
        ec.setFlashbackArea(true);
        Assert.assertTrue(ec.isFlashbackArea());
        ec.clearContextInsideTrans();
        Assert.assertFalse(ec.isFlashbackArea());
    }

    /**
     * test if lifecycle of all properties are settled
     */
    @Test
    public void testIfAllPropertiesHasLifeCycle() {
        StringBuilder stringBuilder = new StringBuilder();
        Set<String> allPropertiesType = new HashSet<>();
        boolean missingAny = false;
        for (Field field : ExecutionContext.class.getDeclaredFields()) {
            if (!ExecutionContextPropertiesLifeCycle.ignoreFields.contains(field) &&
                !java.lang.reflect.Modifier.isStatic(field.getModifiers()) &&
                !ExecutionContextPropertiesLifeCycle.propertiesLife.containsKey(field)) {
                stringBuilder.append(field.getName()).append("\n");
                missingAny = true;
            } else {
                allPropertiesType.add(field.getType().getName());
            }
        }
        System.out.println("all properties type: " + allPropertiesType);

        if (stringBuilder.length() > 0) {
            System.out.println("miss life cycle preperties:\n" + stringBuilder);
        }
        Assert.assertTrue(!missingAny);

        StringBuilder staticBuilder = new StringBuilder();
        for (Field field : ExecutionContextPropertiesLifeCycle.propertiesLife.keySet()) {
            if (java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                staticBuilder.append(field.getName()).append("\n");
            }
        }
        if (staticBuilder.length() > 0) {
            System.out.println("static properties should not has life cycle:\n" + staticBuilder);
        }
        Assert.assertTrue(staticBuilder.length() == 0);
    }

    @Test
    public void testIfStmtLifeCyclePropertiesCleared() throws IllegalAccessException {
        ExecutionContext ec = prepareContext();

        ExecutionContextPropertiesLifeCycle.PROPERTIES_LIFE.STMT.clean(ec);

        assertPropertiesCleared(ec, ExecutionContextPropertiesLifeCycle.PROPERTIES_LIFE.STMT);

        ec = prepareContext();

        ExecutionContextPropertiesLifeCycle.PROPERTIES_LIFE.TRANS.clean(ec);

        assertPropertiesCleared(ec, ExecutionContextPropertiesLifeCycle.PROPERTIES_LIFE.TRANS);

        ec = prepareContext();

        ExecutionContextPropertiesLifeCycle.PROPERTIES_LIFE.SESSION.clean(ec);

        assertPropertiesCleared(ec, ExecutionContextPropertiesLifeCycle.PROPERTIES_LIFE.SESSION);
    }

    private void assertPropertiesCleared(ExecutionContext ec,
                                         ExecutionContextPropertiesLifeCycle.PROPERTIES_LIFE propertiesLife)
        throws IllegalAccessException {
        StringBuilder stringBuilder = new StringBuilder("properties not cleared: \n");
        int initSize = stringBuilder.length();
        for (Field field : ExecutionContextPropertiesLifeCycle.propertiesLife.keySet()) {
            if (ExecutionContextPropertiesLifeCycle.propertiesLife.get(field) == propertiesLife) {
                field.setAccessible(true);
                if (field.get(ec) != null) {
                    stringBuilder.append(field.getName()).append("\n");
                }
            }
        }
        if (stringBuilder.length() > initSize) {
            System.out.println(stringBuilder);
        }
        Assert.assertTrue(stringBuilder.length() == initSize);
    }

    private ExecutionContext prepareContext() throws IllegalAccessException {
        ExecutionContext ec = new ExecutionContext();
        for (Field field : ExecutionContextPropertiesLifeCycle.propertiesLife.keySet()) {
            if (!java.lang.reflect.Modifier.isStatic(field.getModifiers())) {
                field.setAccessible(true);
                if (field.getType().equals(ITransaction.class)) {
                    field.set(ec, new MockTransaction());
                } else if (field.getType().equals(Map.class)) {
                    field.set(ec, new HashMap<>());
                } else if (field.getType().equals(SchemaManager.class)) {
                    field.set(ec, new MockSchemaManager());
                } else if (field.getType().equals(ParamManager.class)) {
                    field.set(ec, new ParamManager(new HashMap<>()));
                } else if (field.getType().equals(Parameters.class)) {
                    field.set(ec, new Parameters());
                } else if (field.getType().equals(RuntimeStat.class)) {
                    field.set(ec, new MockRuntimeStat());
                } else if (field.getType().equals(TableInfoManager.class)) {
                    field.set(ec, new TableInfoManager());
                } else if (field.getType().equals(PlanManager.PLAN_SOURCE.class)) {
                    field.set(ec, PlanManager.PLAN_SOURCE.PLAN_CACHE);
                } else if (field.getType().equals(ColumnarTracer.class)) {
                    field.set(ec, new ColumnarTracer());
                } else if (field.getType().equals(Integer.class)) {
                    field.set(ec, 10);
                } else if (field.getType().equals(ServerThreadPool.class)) {
                    field.set(ec, new ServerThreadPool("mock", 1, 10));
                } else if (field.getType().equals(WorkloadType.class)) {
                    field.set(ec, WorkloadType.TP);
                } else if (field.getType().equals(CalcitePlanOptimizerTrace.class)) {
                    field.set(ec, new CalcitePlanOptimizerTrace());
                } else if (field.getType().equals(Long.class)) {
                    field.set(ec, 100L);
                } else if (field.getType().equals(PreparedStmtCache.class)) {
                    field.set(ec, new PreparedStmtCache(null));
                } else if (field.getType().equals(PrivilegeContext.class)) {
                    field.set(ec, new PrivilegeContext());
                } else if (field.getType().equals(DdlContext.class)) {
                    field.set(ec, new DdlContext());
                } else if (field.getType().equals(XplanStat.class)) {
                    field.set(ec, new XplanStat(false));
                } else if (field.getType().equals(Logger.class)) {
                    field.set(ec, LoggerUtil.statisticsLogger);
                } else if (field.getType().equals(LoadDataContext.class)) {
                    field.set(ec, Mockito.mock(LoadDataContext.class));
                } else if (field.getType().equals(Set.class)) {
                    field.set(ec, Sets.newHashSet());
                } else if (field.getType().equals(QuerySpillSpaceMonitor.class)) {
                    field.set(ec, new QuerySpillSpaceMonitor("tag"));
                } else if (field.getType().equals(CclContext.class)) {
                    field.set(ec, new CclContext(false));
                } else if (field.getType().equals(List.class)) {
                    field.set(ec, Lists.newArrayList());
                } else if (field.getType().equals(ExplainResult.class)) {
                    field.set(ec, new ExplainResult());
                } else if (field.getType().equals(Point.class)) {
                    field.set(ec, Mockito.mock(Point.class));
                } else if (field.getType().equals(QueryMemoryPoolHolder.class)) {
                    field.set(ec, new QueryMemoryPoolHolder());
                } else if (field.getType().equals(MatrixStatistics.class)) {
                    field.set(ec, new MatrixStatistics());
                } else if (field.getType().equals(InternalTimeZone.class)) {
                    field.set(ec, Mockito.mock(InternalTimeZone.class));
                } else if (field.getType().equals(Function.class)) {
                    field.set(ec, Mockito.mock(Function.class));
                } else if (field.getType().equals(SQLTracer.class)) {
                    field.set(ec, new SQLTracer());
                } else if (field.getType().equals(ExecutorMode.class)) {
                    field.set(ec, ExecutorMode.MPP);
                } else if (field.getType().equals(PhyDdlExecutionRecord.class)) {
                    field.set(ec, new PhyDdlExecutionRecord(1, 1, 2));
                } else if (field.getType().equals(Boolean.class)) {
                    field.set(ec, Boolean.TRUE);
                } else if (field.getType().equals(String.class)) {
                    field.set(ec, "test");
                } else if (field.getType().equals(SQLRecorder.class)) {
                    field.set(ec, new SQLRecorder(1024));
                } else if (field.getType().equals(ExecutionPlan.class)) {
                    field.set(ec, Mockito.mock(ExecutionPlan.class));
                } else if (field.getType().equals(ITConnection.class)) {
                    field.set(ec, Mockito.mock(ITConnection.class));
                } else if (field.getType().equals(RelNode.class)) {
                    field.set(ec, Mockito.mock(RelNode.class));
                } else if (field.getType().equals(MultiDdlContext.class)) {
                    field.set(ec, new MultiDdlContext());
                } else if (field.getType().equals(int.class)) {
                    field.set(ec, 1);
                } else if (field.getType().equals(long.class)) {
                    field.set(ec, 100L);
                } else if (field.getType().equals(boolean.class)) {
                    field.set(ec, true);
                } else if (field.getType().equals(ByteString.class)) {
                    field.set(ec, ByteString.from("test"));
                } else if (field.getType().equals(AsyncDDLContext.class)) {
                    field.set(ec, new AsyncDDLContext());
                } else if (field.getType().equals(CharsetName.class)) {
                    field.set(ec, CharsetName.UTF8MB4);
                } else if (field.getType().equals(SqlType.class)) {
                    field.set(ec, SqlType.SELECT);
                } else if (field.getType().equals(BitSet.class)) {
                    field.set(ec, new BitSet());
                } else if (field.getType().equals(InputStream.class)) {
                    field.set(ec, Mockito.mock(InputStream.class));
                } else if (field.getType().equals(int[].class)) {
                    field.set(ec, new int[] {1, 2, 3});
                } else if (field.getType().equals(String[].class)) {
                    field.set(ec, new String[] {"a", "b", "c"});
                } else if (field.getType().equals(ExecutorService.class)) {
                    field.set(ec, Executors.newFixedThreadPool(1, new NamedThreadFactory("test", true)));
                } else if (field.getType().equals(GroupTaskExecutor.class)) {
                    field.set(ec, new GroupTaskExecutor("test", Maps.newHashMap()));
                } else if (field.getType().equals(SqlParameterized.class)) {
                    field.set(ec, SqlParameterizeUtils.parameterize("select * from mock_tb"));
                } else if (field.getType().equals(RoutingType.class)) {
                    field.set(ec, RoutingType.getType("ROW"));
                } else if (field.getType().equals(PlanType.class)) {
                    field.set(ec, PlanType.ROW);
                } else if (field.getType().equals(HtapTrace.class)) {
                    field.set(ec, new HtapTrace());
                } else if (field.getType().equals(ColumnarSnapshotCacheManager.class)) {
                    field.set(ec, Mockito.mock(ColumnarSnapshotCacheManager.class));
                } else if (field.getType().equals(VersionStorageStatistics.class)) {
                    field.set(ec, Mockito.mock(VersionStorageStatistics.class));
                } else if (field.getType().equals(ColumnarScanMetrics.class)) {
                    field.set(ec, Mockito.mock(ColumnarScanMetrics.class));
                } else if (field.getType().equals(ExternalColumnStatistics.class)) {
                    field.set(ec, new ExternalColumnStatistics());
                } else if (field.getType().equals(DmlWriteContext.class)) {
                    field.set(ec, Mockito.mock(DmlWriteContext.class));
                } else if (field.getType().equals(TtlQueryType.class)) {
                    field.set(ec, TtlQueryType.HOT_AND_COLD);
                } else if (field.getType().equals(EphemeralFilesSchemaManager.class)) {
                    field.set(ec, new EphemeralFilesSchemaManager());
                } else {
                    throw new IllegalArgumentException("unsupported type: " + field.getType());
                }
            }
        }
        return ec;
    }

    @Test
    public void blockChainTest() {
        ExecutionContext context = new ExecutionContext();
        context.setContainsBlockChainTable(true);
        Assert.assertTrue(context.isContainsBlockChainTable());

        context.setBlockChainSchema("test");
        Assert.assertEquals("test", context.getBlockChainSchema());

        context.setBlockChainTable("test");
        Assert.assertEquals("test", context.getBlockChainTable());

        context.setUser("test");
        Assert.assertEquals("test", context.getUser());
        context.setPort(1000);
        Assert.assertEquals(1000, context.getPort());
    }

    @Test
    public void copyTest() {
        ExecutionContext context = new ExecutionContext();
        context.copy();
    }

    @Test
    public void copyShareQuerySpillMonitorByDefaultTest() {
        ExecutionContext source = new ExecutionContext();
        QuerySpillSpaceMonitor mockMonitor = Mockito.mock(QuerySpillSpaceMonitor.class);
        source.setQuerySpillSpaceMonitor(mockMonitor);

        ExecutionContext copiedByDefault = source.copy();
        Assert.assertSame(mockMonitor, copiedByDefault.getQuerySpillSpaceMonitor());

        ExecutionContext copiedWithDefaultOption = source.copy(new ExecutionContext.CopyOption());
        Assert.assertSame(mockMonitor, copiedWithDefaultOption.getQuerySpillSpaceMonitor());
    }

    @Test
    public void copyWithIndependentQuerySpillMonitorTest() {
        ExecutionContext source = new ExecutionContext();
        QuerySpillSpaceMonitor mockMonitor = Mockito.mock(QuerySpillSpaceMonitor.class);
        source.setQuerySpillSpaceMonitor(mockMonitor);

        ExecutionContext copied = source.copy(
            new ExecutionContext.CopyOption().setShareQuerySpillMonitor(false));

        Assert.assertNull(copied.getQuerySpillSpaceMonitor());
        Assert.assertSame(mockMonitor, source.getQuerySpillSpaceMonitor());
    }

    @Test
    public void copyOptionShareQuerySpillMonitorFlagTest() {
        ExecutionContext.CopyOption option = new ExecutionContext.CopyOption();
        Assert.assertTrue(option.isShareQuerySpillMonitor());
        Assert.assertSame(option, option.setShareQuerySpillMonitor(false));
        Assert.assertFalse(option.isShareQuerySpillMonitor());
        option.setShareQuerySpillMonitor(true);
        Assert.assertTrue(option.isShareQuerySpillMonitor());
    }

    @Test
    public void grayWorkloadTest() {
        ExecutionContext context = new ExecutionContext();

        // Test default value
        Assert.assertNull(context.getGrayWorkload());

        // Test setting to true
        context.setGrayWorkload(true);
        Assert.assertTrue(context.getGrayWorkload());

        // Test setting to false
        context.setGrayWorkload(false);
        Assert.assertFalse(context.getGrayWorkload());

        // Test setting to null
        context.setGrayWorkload(null);
        Assert.assertNull(context.getGrayWorkload());
    }

    @Test
    public void testInternalSubExecutionGetterSetter() {
        ExecutionContext ec = new ExecutionContext();
        // Default value should be false
        Assert.assertFalse(ec.isInternalSubExecution());

        ec.setInternalSubExecution(true);
        Assert.assertTrue(ec.isInternalSubExecution());

        ec.setInternalSubExecution(false);
        Assert.assertFalse(ec.isInternalSubExecution());
    }

    @Test
    public void testInternalSubExecutionCopy() {
        ExecutionContext ec = new ExecutionContext();
        ec.setInternalSubExecution(true);

        // Verify copy propagates the flag
        ExecutionContext copied = ec.copy();
        Assert.assertTrue(copied.isInternalSubExecution());

        // Verify false is also propagated
        ec.setInternalSubExecution(false);
        ExecutionContext copied2 = ec.copy();
        Assert.assertFalse(copied2.isInternalSubExecution());
    }

    @Test
    public void testInternalSubExecutionClearContext() {
        ExecutionContext ec = new ExecutionContext();
        ec.setInternalSubExecution(true);
        Assert.assertTrue(ec.isInternalSubExecution());

        ec.clearContextAfterTrans();
        Assert.assertFalse(ec.isInternalSubExecution());
    }
}
