package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.executor.PlanExecutor;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;

public class LogicalWarmupHandlerTest {
    LogicalWarmup logicalWarmup;
    ExecutionContext executionContext;
    private RuntimeStatistics runtimeStatistics;
    private MockedStatic<PlanExecutor> mockedExecutor;
    private MockedStatic<Planner> plannerMockedStatic;
    private MockedStatic<RuntimeStatHelper> runtimeStatHelperMockedStatic;

    @Before
    public void setupExecutionContext() {
        executionContext = new ExecutionContext();
    }

    @Before
    public void setupPlan() {
        // mock sqlWarmup
        List<SqlNode> selectList = new ArrayList<>();
        selectList.add(Mockito.mock(SqlNode.class));
        selectList.add(Mockito.mock(SqlNode.class));
        SqlWarmup sqlWarmup = new SqlWarmup(SqlParserPos.ZERO, selectList);

        // mock warmup() select
        // sqlWarmup.setCronExpression("0 0 12 * * ?");
        sqlWarmup.setCronExpression("");

        sqlWarmup.setHint("/*+TDDL:cmd_extra()*/");
        sqlWarmup.setSql("select * from test1");

        sqlWarmup.setHint("/*+TDDL:cmd_extra()*/");
        sqlWarmup.setSql("select * from test2");

        Dal dal = Mockito.mock(Dal.class);
        Mockito.when(dal.getAst()).thenReturn(sqlWarmup);
        logicalWarmup = Mockito.mock(LogicalWarmup.class);
        Mockito.when(logicalWarmup.getSqlWarmup()).thenReturn(sqlWarmup);
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
        Mockito.when(planner.plan(anyString(), argThat(arg -> arg instanceof ExecutionContext)))
            .thenReturn(executionPlan);

        // mock cursor
        ResultCursor cursor = Mockito.mock(ResultCursor.class);
        Mockito.when(cursor.next()).thenReturn(null);
        mockedExecutor.when(
                () -> PlanExecutor.execute(eq(executionPlan), argThat(arg -> arg instanceof ExecutionContext)))
            .thenReturn(cursor);
    }

    @Before
    public void setupStatistic() {
        runtimeStatistics = Mockito.mock(RuntimeStatistics.class);
        runtimeStatHelperMockedStatic = Mockito.mockStatic(RuntimeStatHelper.class);
        runtimeStatHelperMockedStatic.when(
                () -> RuntimeStatHelper.buildRuntimeStat(argThat(arg -> arg instanceof ExecutionContext)))
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
        LogicalWarmupHandler logicalWarmupHandler = new LogicalWarmupHandler(Mockito.mock(IRepository.class));
        Cursor cursor = logicalWarmupHandler.handle(logicalWarmup, executionContext);

        Row row;
        while ((row = cursor.next()) != null) {
            System.out.println(row);
        }
    }

    @After
    public void tearDown() {
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