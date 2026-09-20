package com.alibaba.polardbx.executor.fragmenter;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.deploy.MppServer;
import com.alibaba.polardbx.executor.mpp.deploy.Server;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.planner.PlanUtils;
import com.alibaba.polardbx.gms.node.AllNodes;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.NodeVersion;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.memory.MemoryPool;
import com.alibaba.polardbx.optimizer.utils.IMppReadOnlyTransaction;
import com.alibaba.polardbx.planner.common.PlanTestCommon;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.rel.RelNode;
import org.junit.Before;
import org.junit.runners.Parameterized;
import org.mockito.MockedStatic;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.alibaba.polardbx.common.model.RepoInst;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.group.jdbc.TGroupDataSource;

import static com.alibaba.polardbx.common.jdbc.ITransactionPolicy.TransactionClass.MPP_READ_ONLY_TRANSACTION;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class PhysicalPlanWithScheduleTest extends PlanTestCommon {

    private AllNodes allNodes;

    public PhysicalPlanWithScheduleTest(String caseName, int sqlIndex, String sql, String expectedPlan,
                                        String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(PhysicalPlanWithScheduleTest.class);
    }

    @Override
    protected void initBasePlannerTestEnv() {
        this.useNewPartDb = true;
    }

    @Before
    public void setUp() {
        InternalNode activeNode1 = new InternalNode("key1", "cluster1", "inst1", "11.11.11.11", 1234, 12345,
            NodeVersion.UNKNOWN, true, true, false, true);
        InternalNode activeNode2 = new InternalNode("key2", "cluster1", "inst1", "22.22.22.22", 5678, 54321,
            NodeVersion.UNKNOWN, true, true, false, true);

        Set<InternalNode> activeNodes = ImmutableSet.of(activeNode1, activeNode2);
        Set<InternalNode> otherActiveRowNodes = ImmutableSet.of();
        Set<InternalNode> otherActiveColumnarNodes = ImmutableSet.of();
        Set<InternalNode> inactiveNodes = ImmutableSet.of();
        Set<InternalNode> shuttingDownNodes = ImmutableSet.of();

        allNodes =
            new AllNodes(activeNodes, otherActiveRowNodes, otherActiveColumnarNodes, inactiveNodes, shuttingDownNodes);
    }

    @Override
    protected String returnPlanStr(ExecutionContext executionContext, RelNode plan) {
        Server server = null;
        try (final MockedStatic<ConfigDataMode> mockConfigDataMode = mockStatic(ConfigDataMode.class);
            final MockedStatic<ExecutorContext> mockExecutorContextStatic = mockStatic(ExecutorContext.class)) {
            server = new MppServer(1, true, true, "11.11.11.11", 1111);
            ServiceProvider.getInstance().setServer(server);
            server.run();
            server.getNodeManager().updateNodes(allNodes.getActiveNodes(),
                allNodes.getOtherActiveRowNodes(),
                allNodes.getOtherActiveColumnarNodes(),
                allNodes.getInactiveNodes(),
                allNodes.getShuttingDownNodes());

            when(ConfigDataMode.isMasterMode()).thenReturn(true);
            ExecutorContext executorContext = mock(ExecutorContext.class);
            TopologyHandler topologyHandler = mock(TopologyHandler.class);
            mockExecutorContextStatic.when(() -> ExecutorContext.getContext(anyString()))
                .thenReturn(executorContext);
            when(executorContext.getTopologyHandler()).thenReturn(topologyHandler);

            // Mock the get method of TopologyHandler
            IGroupExecutor mockGroupExecutor = mock(IGroupExecutor.class);
            when(topologyHandler.get(anyString())).thenReturn(mockGroupExecutor);

            // Mock the getDataSource method of IGroupExecutor
            TGroupDataSource mockDataSource = mock(TGroupDataSource.class);
            when(mockGroupExecutor.getDataSource()).thenReturn(mockDataSource);
            // Mock the getOneAtomAddress and getDbGroupKey methods of TGroupDataSource
            when(mockDataSource.getOneAtomAddress(anyBoolean())).thenReturn("192.168.1.100:3306");
            when(mockDataSource.getDbGroupKey()).thenReturn("OPTEST_GROUP");

            // Mock the getGroupRepoInstMapsForzigzag method
            Map<String, RepoInst> mockRepoInstMap = new HashMap<>();
            // Create and add the first RepoInst
            RepoInst repoInst1 = new RepoInst();
            repoInst1.setDnId("dn1");
            repoInst1.setAddress("192.168.1.1:3306");
            repoInst1.setRepoInstId("dn1@192.168.1.1:3306");
            mockRepoInstMap.put("OPTEST_P00000_GROUP", repoInst1);

            // Create and add the second RepoInst
            RepoInst repoInst2 = new RepoInst();
            repoInst2.setDnId("dn2");
            repoInst2.setAddress("192.168.1.2:3306");
            repoInst2.setRepoInstId("dn2@192.168.1.2:3306");
            mockRepoInstMap.put("OPTEST_P00001_GROUP", repoInst2);

            when(topologyHandler.getGroupRepoInstMapsForzigzag()).thenReturn(mockRepoInstMap);

            Session session = new Session("", executionContext);
            executionContext.setTraceId("1234");
            IMppReadOnlyTransaction trx = mock(IMppReadOnlyTransaction.class);
            when(trx.getTransactionClass()).thenReturn(MPP_READ_ONLY_TRANSACTION);
            executionContext.setTransaction(trx);

            MemoryPool mockMemoryPool = mock(MemoryPool.class);
            executionContext.setMemoryPool(mockMemoryPool);

            executionContext.getExtraCmds().put(ConnectionProperties.MPP_NODE_SIZE, 2);

            String mppPlanString = PlanUtils.textPlan(executionContext, session, plan, true);
            return mppPlanString;
        } finally {
            if (server != null) {
                server.stop();
            }
        }
    }
}