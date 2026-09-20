package com.alibaba.polardbx.executor.fragmenter;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.mpp.deploy.Server;
import com.alibaba.polardbx.executor.mpp.deploy.ServiceProvider;
import com.alibaba.polardbx.executor.mpp.planner.PlanUtils;
import com.alibaba.polardbx.gms.node.AllNodes;
import com.alibaba.polardbx.gms.node.InternalNode;
import com.alibaba.polardbx.gms.node.InternalNodeManager;
import com.alibaba.polardbx.gms.node.NodeVersion;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.planner.common.PlanTestCommon;
import com.google.common.collect.ImmutableSet;
import org.apache.calcite.rel.RelNode;
import org.junit.After;
import org.junit.Before;
import org.junit.runners.Parameterized;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.List;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class PhysicalPlanTest extends PlanTestCommon {

    private AllNodes allNodes;

    public PhysicalPlanTest(String caseName, int sqlIndex, String sql, String expectedPlan, String lineNum) {
        super(caseName, sqlIndex, sql, expectedPlan, lineNum);
    }

    @Parameterized.Parameters(name = "{0}:{1}")
    public static List<Object[]> prepare() {
        return loadSqls(PhysicalPlanTest.class);
    }

    @Before
    public void setUp() {
        InternalNode activeNode = mockNode();
        InternalNode otherActiveRowNode = mockNode();
        InternalNode otherActiveColumnarNode = mockNode();
        InternalNode inactiveNode = mockNode();
        InternalNode shuttingDownNode = mockNode();

        Set<InternalNode> activeNodes = ImmutableSet.of(activeNode);
        Set<InternalNode> otherActiveRowNodes = ImmutableSet.of(otherActiveRowNode);
        Set<InternalNode> otherActiveColumnarNodes = ImmutableSet.of(otherActiveColumnarNode);
        Set<InternalNode> inactiveNodes = ImmutableSet.of(inactiveNode);
        Set<InternalNode> shuttingDownNodes = ImmutableSet.of(shuttingDownNode);

        allNodes =
            new AllNodes(activeNodes, otherActiveRowNodes, otherActiveColumnarNodes, inactiveNodes, shuttingDownNodes);
    }

    private InternalNode mockNode() {
        InternalNode node = new InternalNode("key", "cluster1", "inst1", "11.11.11.11", 1234, 12345,
            NodeVersion.UNKNOWN, true, true, false, true);
        return node;
    }

    @Override
    protected String returnPlanStr(ExecutionContext executionContext, RelNode plan) {
        try (final MockedStatic<ConfigDataMode> mockConfigDataMode = mockStatic(ConfigDataMode.class);
            final MockedStatic<ServiceProvider> mockServiceProvider = mockStatic(ServiceProvider.class)
        ) {
            ServiceProvider serviceProvider = mock(ServiceProvider.class);
            when(ServiceProvider.getInstance()).thenReturn(serviceProvider);
            Server server = mock(Server.class);
            when(serviceProvider.getServer()).thenReturn(server);

            InternalNodeManager nodeManager = mock(InternalNodeManager.class);
            when(server.getNodeManager()).thenReturn(nodeManager);
            when(nodeManager.getAllNodes()).thenReturn(allNodes);
            when(ConfigDataMode.isMasterMode()).thenReturn(true);

            Session session = new Session("", executionContext);
            session.setIgnoreSplitInfo(true);
            executionContext.putIntoHintCmds(ConnectionProperties.SHOW_PIPELINE_INFO_UNDER_MPP, false);
            executionContext.putIntoHintCmds(ConnectionProperties.EXPLAIN_OUTPUT_FORMAT, "LEGACY");
            String mppPlanString = PlanUtils.textPlan(executionContext, session, plan, false);
            return mppPlanString;
        }
    }
}

