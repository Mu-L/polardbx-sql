package com.alibaba.polardbx.executor.ddl.job.factory;

import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.data.AlterTableGroupDropPartitionPreparedData;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AlterTableGroupDropPartitionJobFactoryTest {

    @Mock
    private AlterTableGroupDropPartitionPreparedData preparedData;

    @Mock
    private TableGroupConfig tableGroupConfig;

    @Mock
    private TableGroupRecord tableGroupRecord;

    @Mock
    private OptimizerContext optimizerContext;

    @Mock
    private ExecutionContext executionContext;

    @Mock
    private ExecutableDdlJob executableDdlJob;

    private AlterTableGroupDropPartitionJobFactory factory;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
        factory = new AlterTableGroupDropPartitionJobFactory(null, preparedData, null, null, null, null, null, null,
            executionContext);
    }

    @Test
    public void testTryAttachCdcFinalMarkTask_WithBroadcastTableGroup() {
        // Given
        when(preparedData.getTableGroupName()).thenReturn("broadcast_tg");

        // When
        factory.tryAttachCdcFinalMarkTask(executableDdlJob);

        // Then
        verify(executableDdlJob, never()).appendTask(any());
    }

    @Test
    public void testTryAttachCdcFinalMarkTask_WithNonBroadcastTableGroup() {
        // Given
        when(preparedData.getTableGroupName()).thenReturn("regular_tg");

        // When
        factory.tryAttachCdcFinalMarkTask(executableDdlJob);

        // Then
        verify(executableDdlJob, times(1)).appendTask(any());
    }
}