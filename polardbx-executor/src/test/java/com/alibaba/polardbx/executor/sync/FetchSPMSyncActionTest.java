package com.alibaba.polardbx.executor.sync;

import com.alibaba.polardbx.executor.cursor.ResultCursor;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.gms.topology.ServerInstIdManager;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.ExecutionPlan;
import com.alibaba.polardbx.optimizer.core.planner.PlanCache;
import com.alibaba.polardbx.optimizer.core.planner.Planner;
import com.alibaba.polardbx.optimizer.core.planner.rule.cte.CTEContext;
import com.alibaba.polardbx.optimizer.htaprouting.WorkloadType;
import com.alibaba.polardbx.optimizer.parse.bean.SqlParameterized;
import com.alibaba.polardbx.optimizer.planmanager.BaselineInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanInfo;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.optimizer.planmanager.PlanManagerUtil;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptCost;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.sql.SqlNode;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class FetchSPMSyncActionTest {
    @Test
    public void testSync() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        PlanManager planManager = PlanManager.getInstance();

        Map<String, BaselineInfo> baselineMap = new HashMap<>();
        BaselineInfo baselineInfo = new BaselineInfo("select * from t where c1 = 1", new HashSet<>());
        baselineInfo.setRebuildAtLoad(true);
        baselineInfo.setUsePostPlanner(true);
        baselineInfo.setHotEvolution(true);

        PlanInfo planInfo = new PlanInfo("", 1, 0.0D, "", "", 0);
        planInfo.setFixed(true);
        planInfo.setAccepted(true);
        planInfo.setOrigin("origin");
        planInfo.setFixHint("hint");
        planInfo.setVersion(0);

        baselineInfo.addAcceptedPlan(planInfo);
        String originSql = "select * from t where c1 = 1";
        baselineMap.put(originSql, baselineInfo);

        planManager.addBaselineInfo("test_schema", originSql, baselineInfo);

        Planner planner = mock(Planner.class);
        ExecutionPlan plan = mock(ExecutionPlan.class);
        RelNode rel = mock(RelNode.class);
        RelOptCluster cluster = mock(RelOptCluster.class);
        RelMetadataQuery mq = mock(RelMetadataQuery.class);
        WorkloadType workloadType = mock(WorkloadType.class);

        when(planner.doBuildPlan(any(), any())).thenReturn(plan);

        ExecutionPlan executionPlan = mock(ExecutionPlan.class);
        when(executionPlan.getCacheKey()).thenReturn(mock(PlanCache.CacheKey.class));
        when(executionPlan.getAst()).thenReturn(mock(SqlNode.class));

        when(plan.getPlan()).thenReturn(rel);
        when(rel.getCluster()).thenReturn(cluster);
        when(cluster.getMetadataQuery()).thenReturn(mq);
        when(mq.getCumulativeCost(rel)).thenReturn(mock(RelOptCost.class));
        ExecutionContext ec = new ExecutionContext();
        PlannerContext pc = new PlannerContext();
        pc.setWorkloadType(workloadType);
        ec.setOriginSql("test origin sql");
        RelOptPlanner relOptPlanner = mock(RelOptPlanner.class);
        when(cluster.getPlanner()).thenReturn(relOptPlanner);
        when(relOptPlanner.getContext()).thenReturn(mock(PlannerContext.class));

        try (MockedStatic<Planner> plannerMockedStatic = mockStatic(Planner.class);
            MockedStatic<PlanManagerUtil> planManagerUtilMockedStatic = mockStatic(PlanManagerUtil.class);
            MockedStatic<PlannerContext> plannerContextMockedStatic = mockStatic(PlannerContext.class);
            MockedStatic<ServerInstIdManager> serverInstIdManagerMockedStatic = mockStatic(ServerInstIdManager.class)) {

            // Mock ServerInstIdManager
            ServerInstIdManager serverInstIdManager = mock(ServerInstIdManager.class);
            when(serverInstIdManager.getInstId()).thenReturn("test_inst_id");
            serverInstIdManagerMockedStatic.when(ServerInstIdManager::getInstance).thenReturn(serverInstIdManager);

            plannerMockedStatic.when(Planner::getInstance).thenReturn(planner);
            planManagerUtilMockedStatic.when(() -> PlanManagerUtil.relNodeToJson(any())).thenReturn("test json");
            PlannerContext pc1 = mock(PlannerContext.class);
            plannerContextMockedStatic.when(() -> PlannerContext.getPlannerContext(any(RelNode.class)))
                .thenReturn(pc1);
            CTEContext cteContext = mock(CTEContext.class);
            when(pc1.getCteContext()).thenReturn(cteContext);
            doNothing().when(cteContext).reCollect(any(RelNode.class));

            PlanManager.Result r =
                planManager.buildNewPlan(mock(BaselineInfo.class), mock(SqlParameterized.class),
                    mock(ExecutionContext.class), 1);
            assertNotNull(r);
            assertEquals(r.plan, rel);

            FetchSPMSyncAction syncAction = new FetchSPMSyncAction("test_schema");
            // Act
            ResultCursor resultCursor = syncAction.sync();

            // Assert
            Assert.assertNotNull(resultCursor);
        }
    }

}
