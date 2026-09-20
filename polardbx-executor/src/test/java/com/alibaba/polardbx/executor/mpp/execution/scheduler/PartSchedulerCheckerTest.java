package com.alibaba.polardbx.executor.mpp.execution.scheduler;

import com.alibaba.polardbx.executor.mpp.execution.SqlQueryExecution;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.core.rel.OSSTableScan;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.common.PartitionStrategy;
import com.alibaba.polardbx.optimizer.utils.TableTopologyUtil;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.RelOptTableImpl;
import org.apache.calcite.rel.core.TableScan;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class PartSchedulerCheckerTest {
    @Mock
    private TableMeta tableMeta;

    @Mock
    private PartitionInfo partitionInfo;

    @Mock
    private PartitionByDefinition partitionByDefinition;

    private TableScan tableScan;

    private RelOptTable relOptTable;

    @Before
    public void setUp() {
        tableScan = mock(OSSTableScan.class);
        relOptTable = mock(RelOptTableImpl.class);
        when(tableMeta.getPartitionInfo()).thenReturn(partitionInfo);
        when(partitionInfo.getPartitionBy()).thenReturn(partitionByDefinition);
        when(((RelOptTableImpl) relOptTable).getImplTable()).thenReturn(tableMeta);
        when(tableScan.getTable()).thenReturn(relOptTable);
    }

    @Test
    public void testVisitWhenShardIsSingleGroup() {
        when(((OSSTableScan) tableScan).isSingleGroupForExecutor()).thenReturn(true);
        test(false, PartitionStrategy.DIRECT_HASH);
        test(false, PartitionStrategy.RANGE);
    }

    @Test
    public void testVisitWhenShardIsNotSingleGroup() {
        try (MockedStatic<TableTopologyUtil> mockedUtils = Mockito.mockStatic(TableTopologyUtil.class)) {
            mockedUtils.when(() -> TableTopologyUtil.isShard(Mockito.any())).thenReturn(true);
            when(((OSSTableScan) tableScan).isSingleGroupForExecutor()).thenReturn(false);
            PartitionSpec mockPartitionSpec = mock(PartitionSpec.class);
            when(partitionByDefinition.getPartitions()).thenReturn(
                Arrays.asList(mockPartitionSpec, mockPartitionSpec, mockPartitionSpec, mockPartitionSpec));
            test(true, PartitionStrategy.DIRECT_HASH);
            test(false, PartitionStrategy.RANGE);
            test(false, PartitionStrategy.RANGE_COLUMNS);
        }
    }

    private void test(boolean partSchedule, PartitionStrategy strategy) {
        when(partitionByDefinition.getStrategy()).thenReturn(strategy);
        SqlQueryExecution.PartScheduleChecker checker = new SqlQueryExecution.PartScheduleChecker(2);
        checker.visit(tableScan);
        Assert.assertTrue(partSchedule == checker.canScheduleByPart());
    }
}