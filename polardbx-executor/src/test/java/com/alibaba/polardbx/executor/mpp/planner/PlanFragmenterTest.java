package com.alibaba.polardbx.executor.mpp.planner;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.executor.mpp.Session;
import com.alibaba.polardbx.executor.utils.ExecUtils;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.htaprouting.WorkloadUtil;
import org.apache.calcite.rel.RelNode;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.MockitoJUnitRunner;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PlanFragmenterTest {

    @Mock
    private Session mockSession;

    @Mock
    private ExecutionContext mockExecutionContext;

    @Mock
    private ParamManager mockParamManager;

    @Mock
    private RelNode mockRelNode;

    @Before
    public void setUp() {
        when(mockSession.getClientContext()).thenReturn(mockExecutionContext);
        when(mockExecutionContext.getParamManager()).thenReturn(mockParamManager);
    }

    @Test
    public void testCalcParallelismWithNegativeParallelism() throws Exception {
        try (MockedStatic<ExecUtils> mockedExecUtils = mockStatic(ExecUtils.class);
            MockedStatic<CBOUtil> mockedCBOUtil = mockStatic(CBOUtil.class);
            MockedStatic<WorkloadUtil> mockedWorkloadUtil = mockStatic(WorkloadUtil.class)) {

            // Setup test conditions for parallelism < 0 branch
            when(mockParamManager.getBoolean(ConnectionParams.MPP_PARALLELISM_AUTO_ENABLE)).thenReturn(false);
            when(mockParamManager.getInt(ConnectionParams.MPP_PARALLELISM)).thenReturn(-1);

            // Mock ExecUtils static methods
            mockedExecUtils.when(() -> ExecUtils.getMppMaxParallelism(any(ParamManager.class), anyBoolean()))
                .thenReturn(16);
            mockedExecUtils.when(() -> ExecUtils.getMppMinParallelism(any(ParamManager.class)))
                .thenReturn(1);

            // Mock CBOUtil to avoid NullPointerException in constructor
            mockedCBOUtil.when(() -> CBOUtil.isColumnarOptimizer(any(RelNode.class)))
                .thenReturn(false);

            // Mock WorkloadUtil to control lowConcurrencyQuery behavior
            mockedWorkloadUtil.when(() -> WorkloadUtil.isApWorkload(any()))
                .thenReturn(true);

            // Create Fragmenter instance directly (no reflection needed since it's now public)
            PlanFragmenter.Fragmenter fragmenter = new PlanFragmenter.Fragmenter(mockSession, mockRelNode);

            // Set lowConcurrencyQuery to false using reflection
            Field lowConcurrencyQueryField = PlanFragmenter.Fragmenter.class.getDeclaredField("lowConcurrencyQuery");
            lowConcurrencyQueryField.setAccessible(true);
            lowConcurrencyQueryField.set(fragmenter, false);

            // Create FragmentProperties instance directly (no reflection needed since it's now public)
            PlanFragmenter.FragmentProperties properties = new PlanFragmenter.FragmentProperties();

            // Create mock SubPlan children
            List<SubPlan> children = new ArrayList<>();

            // First SubPlan with partition count 4 and bka join parallelism 2
            SubPlan subPlan1 = mock(SubPlan.class);
            PlanFragment fragment1 = mock(PlanFragment.class);
            PartitionHandle partitioning1 = mock(PartitionHandle.class);
            when(subPlan1.getFragment()).thenReturn(fragment1);
            when(fragment1.getPartitioning()).thenReturn(partitioning1);
            when(partitioning1.getPartitionCount()).thenReturn(4);
            when(fragment1.getBkaJoinParallelism()).thenReturn(2);
            children.add(subPlan1);

            // Second SubPlan with partition count 6 and bka join parallelism 8
            SubPlan subPlan2 = mock(SubPlan.class);
            PlanFragment fragment2 = mock(PlanFragment.class);
            PartitionHandle partitioning2 = mock(PartitionHandle.class);
            when(subPlan2.getFragment()).thenReturn(fragment2);
            when(fragment2.getPartitioning()).thenReturn(partitioning2);
            when(partitioning2.getPartitionCount()).thenReturn(6);
            when(fragment2.getBkaJoinParallelism()).thenReturn(8);
            children.add(subPlan2);

            // Set children to properties using reflection (children field is private final)
            Field childrenField = PlanFragmenter.FragmentProperties.class.getDeclaredField("children");
            childrenField.setAccessible(true);
            childrenField.set(properties, children);

            // Call calcParallelism method directly
            int result = fragmenter.calcParallelism(properties);

            // Expected result: max(max(max(-1, 4), max(4, 2)), max(max(4, 6), max(6, 8))) = max(4, 8) = 8
            // Then apply min/max constraints: max(min(8, 16), 1) = 8
            assertEquals(8, result);
        }
    }
}