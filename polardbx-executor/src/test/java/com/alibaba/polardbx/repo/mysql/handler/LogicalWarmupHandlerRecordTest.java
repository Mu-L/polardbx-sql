package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.executor.PlanExecutor;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.util.InstIdUtil;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.core.rel.dal.LogicalWarmup;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.statistics.RuntimeStatHelper;
import com.alibaba.polardbx.statistics.RuntimeStatistics;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.dal.Dal;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlWarmup;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.trace.RuntimeStatisticsSketch;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

public class LogicalWarmupHandlerRecordTest {
    LogicalWarmup logicalWarmup1;
    LogicalWarmup logicalWarmup2;


    ExecutionContext executionContext;
    private RuntimeStatistics runtimeStatistics;
    private MockedStatic<InstIdUtil> instIdUtilMock;
    private MockedStatic<MetaDbUtil> mockMetaDbUtil;
    private MockedStatic<PlanExecutor> mockedExecutor;
    private MockedStatic<Planner> plannerMockedStatic;
    private MockedStatic<RuntimeStatHelper> runtimeStatHelperMockedStatic;

    @Before
    public void setupExecutionContext() {
        executionContext = new ExecutionContext();
        executionContext.setSchemaName("test_db");
    }

    @Before
    public void setupCluster() {
        instIdUtilMock = Mockito.mockStatic(InstIdUtil.class);
        instIdUtilMock.when(() -> InstIdUtil.getInstId()).thenReturn("pxc-xxxxxxx");
    }

    @Before
    public void setupMetaDB() {
        // Mock connection and accessor.
        Connection conn = mock(Connection.class);
        mockMetaDbUtil = mockStatic(MetaDbUtil.class);

        final AtomicBoolean getConnectionFailed = new AtomicBoolean(false);
        final AtomicBoolean queryFailed = new AtomicBoolean(false);

        mockMetaDbUtil.when(MetaDbUtil::getConnection).thenAnswer(invocation -> {
            if (getConnectionFailed.get()) {
                throw new RuntimeException("Mock get connection failed");
            } else {
                return conn;
            }
        });

        mockMetaDbUtil.when(
            () -> MetaDbUtil.insert(anyString(), anyMap(), eq(conn))
        ).thenAnswer(invocation -> {
            if (queryFailed.get()) {
                throw new RuntimeException("Mock query failed");
            } else {
                return 1;
            }
        });
    }

    @Before
    public void setupPlan1() {
        // mock sqlWarmup
        List<SqlNode> selectList = new ArrayList<>();
        selectList.add(Mockito.mock(SqlNode.class));
        selectList.add(Mockito.mock(SqlNode.class));
        SqlWarmup sqlWarmup = new SqlWarmup(SqlParserPos.ZERO, selectList);

        // mock warmup() select
        sqlWarmup.setCronExpression("0 0 12 * * ?");

        sqlWarmup.setHint("/*+TDDL:cmd_extra()*/");
        sqlWarmup.setSql("select * from test1");

        sqlWarmup.setHint("/*+TDDL:cmd_extra()*/");
        sqlWarmup.setSql("select * from test2");

        Dal dal = Mockito.mock(Dal.class);
        Mockito.when(dal.getAst()).thenReturn(sqlWarmup);
        logicalWarmup1 = Mockito.mock(LogicalWarmup.class);
        Mockito.when(logicalWarmup1.getSqlWarmup()).thenReturn(sqlWarmup);
    }

    @Before
    public void setupPlan2() {
        // mock sqlWarmup
        List<SqlNode> selectList = new ArrayList<>();
        selectList.add(Mockito.mock(SqlNode.class));
        selectList.add(Mockito.mock(SqlNode.class));
        SqlWarmup sqlWarmup = new SqlWarmup(SqlParserPos.ZERO, selectList);

        // mock warmup() select
        sqlWarmup.setCronExpression("0 0 12 * * ?");

        sqlWarmup.setHint("/*+TDDL:cmd_extra()*/");
        sqlWarmup.setSql("select * from test1");

        Dal dal = Mockito.mock(Dal.class);
        Mockito.when(dal.getAst()).thenReturn(sqlWarmup);
        logicalWarmup2 = Mockito.mock(LogicalWarmup.class);
        Mockito.when(logicalWarmup2.getSqlWarmup()).thenReturn(sqlWarmup);
    }

    @Before
    public void setupExecution() {
        // mock PlanExecutor
        mockedExecutor = Mockito.mockStatic(PlanExecutor.class);
        PlanExecutor planExecutor = Mockito.mock(PlanExecutor.class);
        mockedExecutor.when(PlanExecutor::create).thenReturn(planExecutor);
        Mockito.doNothing().when(planExecutor).init();

        // mock Planner
        plannerMockedStatic = Mockito.mockStatic(Planner.class);
        Planner planner = Mockito.mock(Planner.class);
        plannerMockedStatic.when(Planner::getInstance).thenReturn(planner);

        ExecutionPlan executionPlan = Mockito.mock(ExecutionPlan.class);
        Mockito.when(planner.plan(anyString(), eq(executionContext))).thenReturn(executionPlan);

        // mock cursor
        ResultCursor cursor = Mockito.mock(ResultCursor.class);
        Mockito.when(cursor.next()).thenReturn(null);
        mockedExecutor.when(() -> PlanExecutor.execute(eq(executionPlan), eq(executionContext)))
            .thenReturn(cursor);
    }

    @Before
    public void setupStatistic() {
        runtimeStatistics = Mockito.mock(RuntimeStatistics.class);
        runtimeStatHelperMockedStatic = Mockito.mockStatic(RuntimeStatHelper.class);
        runtimeStatHelperMockedStatic.when(() -> RuntimeStatHelper.buildRuntimeStat(eq(executionContext)))
            .thenReturn(runtimeStatistics);

        Mockito.doNothing().when(runtimeStatistics).setPlanTree(any());

        Map<RelNode, RuntimeStatisticsSketch> runtimeStatistic = new HashMap<>();

        // node 1
        OSSTableScan relNode1 = Mockito.mock(OSSTableScan.class);
        RuntimeStatisticsSketch sketch1 = Mockito.mock(RuntimeStatisticsSketch.class);
        Mockito.when(sketch1.getIoBytesCount()).thenReturn(888L);

        // node 2
        Exchange relNode2 = Mockito.mock(Exchange.class);
        RuntimeStatisticsSketch sketch2 = Mockito.mock(RuntimeStatisticsSketch.class);
        Mockito.when(sketch2.getIoBytesCount()).thenReturn(999L);

        runtimeStatistic.put(relNode1, sketch1);
        runtimeStatistic.put(relNode2, sketch2);

        Mockito.when(runtimeStatistics.toMppSketch()).thenReturn(runtimeStatistic);
    }

    @Test
    public void test() {
        LogicalWarmupHandler logicalWarmupHandler1 = new LogicalWarmupHandler(Mockito.mock(IRepository.class));
        Cursor cursor1 = logicalWarmupHandler1.handle(logicalWarmup1, executionContext);

        Row row1;
        while ((row1 = cursor1.next()) != null) {
            System.out.println(row1);
        }

        LogicalWarmupHandler logicalWarmupHandler2 = new LogicalWarmupHandler(Mockito.mock(IRepository.class));
        Cursor cursor2 = logicalWarmupHandler2.handle(logicalWarmup2, executionContext);

        Row row2;
        while ((row2 = cursor2.next()) != null) {
            System.out.println(row2);
        }
    }

    @After
    public void tearDown() {
        if (instIdUtilMock != null) {
            instIdUtilMock.close();
        }
        if (mockMetaDbUtil != null) {
            mockMetaDbUtil.close();
        }
        if (mockedExecutor != null) {
            mockedExecutor.close();
        }
        if (plannerMockedStatic != null) {
            plannerMockedStatic.close();
        }
        if (runtimeStatHelperMockedStatic != null) {
            runtimeStatHelperMockedStatic.close();
        }
    }
}
