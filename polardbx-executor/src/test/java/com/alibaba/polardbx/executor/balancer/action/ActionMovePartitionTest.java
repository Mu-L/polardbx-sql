package com.alibaba.polardbx.executor.balancer.action;

import com.alibaba.polardbx.common.eventlogger.EventLogger;
import com.alibaba.polardbx.common.eventlogger.EventType;
import com.alibaba.polardbx.common.exception.TddlRuntimeException;
import com.alibaba.polardbx.common.exception.code.ErrorCode;
import com.alibaba.polardbx.executor.balancer.stats.BalanceStats;
import com.alibaba.polardbx.executor.balancer.stats.PartitionGroupStat;
import com.alibaba.polardbx.executor.balancer.stats.PartitionStat;
import com.alibaba.polardbx.executor.balancer.stats.TableGroupStat;
import com.alibaba.polardbx.executor.ddl.job.task.basic.SubJobTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlTask;
import com.alibaba.polardbx.executor.ddl.newengine.job.ExecutableDdlJob;
import com.alibaba.polardbx.gms.tablegroup.PartitionGroupRecord;
import com.alibaba.polardbx.gms.tablegroup.TableGroupConfig;
import com.alibaba.polardbx.gms.tablegroup.TableGroupRecord;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.*;

/**
 * ActionMovePartition.movesToDdlJob 方法的单元测试
 */
@RunWith(MockitoJUnitRunner.class)
public class ActionMovePartitionTest {

    @Mock
    private ExecutionContext executionContext;

    @Mock
    private BalanceStats balanceStats;

    @Mock
    private PartitionStat partitionStat;

    @Mock
    private PartitionStat partitionStat2;

    @Mock
    private PartitionGroupStat partitionGroupStat;

    @Mock
    private PartitionGroupRecord partitionGroupRecord;

    @Mock
    private TableGroupStat tableGroupStat;

    @Mock
    private TableGroupConfig tableGroupConfig;

    @Mock
    private TableGroupRecord tableGroupRecord;

    /**
     * 测试正常情况下生成DDL作业
     */
    @Test
    public void testMovesToDdlJob_NormalCase() {
        // 准备数据
        String tableGroupName = "test_table_group";
        String schema = "test_schema";
        String partitionName = "p1";
        String toGroup = "TO_GROUP_1";
        String toInst = null;

        ActionMovePartition move = new ActionMovePartition(schema, tableGroupName, partitionName, toGroup, toInst);
        move.setStats(balanceStats);
        move.setIsSubpartition(false);

        List<ActionMovePartition> moves = Arrays.asList(move);

        // Mock相关对象
        List<PartitionStat> partitionStats = Arrays.asList(partitionStat, partitionStat2);
        when(balanceStats.filterPartitionStat(eq(tableGroupName), anySet())).thenReturn(partitionStats);

        when(partitionStat.getPartitionName()).thenReturn(partitionName);
        when(partitionStat.getPartitionGroupRecord()).thenReturn(partitionGroupRecord);
        when(partitionStat.getPartitionRows()).thenReturn(100L);
        when(partitionStat.getPartitionDiskSize()).thenReturn(1024L);
        when(partitionStat.getDataRows()).thenReturn(100L);

        when(partitionStat2.getPartitionName()).thenReturn(partitionName);
        when(partitionStat2.getPartitionGroupRecord()).thenReturn(partitionGroupRecord);
        when(partitionStat2.getPartitionRows()).thenReturn(203L);
        when(partitionStat2.getPartitionDiskSize()).thenReturn(2049L);
        when(partitionStat2.getDataRows()).thenReturn(203L);

        when(partitionGroupRecord.getGroup_Name()).thenReturn("FROM_GROUP");

        // Mock TableGroupStat相关对象以避免IndexOutOfBoundsException
        when(tableGroupRecord.getTg_name()).thenReturn(tableGroupName);
        when(tableGroupConfig.getTableGroupRecord()).thenReturn(tableGroupRecord);
        when(tableGroupConfig.getTableCount()).thenReturn(1);
        when(tableGroupStat.getTableGroupConfig()).thenReturn(tableGroupConfig);
        when(balanceStats.getTableGroupStats()).thenReturn(Arrays.asList(tableGroupStat));

        Map<String, String> groupNameToInstMap = new HashMap<>();
        groupNameToInstMap.put("FROM_GROUP", "INST_1");
        groupNameToInstMap.put(toGroup, "INST_2");

        // Mock DbTopologyManager.getGroupNameToStorageInstIdMap
        try (MockedStatic<DbTopologyManager> mockedDbTopologyManager = Mockito.mockStatic(DbTopologyManager.class)) {
            mockedDbTopologyManager.when(() -> DbTopologyManager.getGroupNameToStorageInstIdMap(schema))
                .thenReturn(groupNameToInstMap);

            // 执行测试
            ExecutableDdlJob result = ActionMovePartition.movesToDdlJob(tableGroupName, moves, executionContext);
            // 验证结果
            assertNotNull(result);
            assertEquals(1, result.getAllTasks().size());
            DdlTask task = result.getAllTasks().get(0);
            assertTrue(task instanceof SubJobTask);
            SubJobTask subJobTask = (SubJobTask)task;
            assertEquals(partitionStat.getPartitionRows() + partitionStat2.getPartitionRows(), subJobTask.getCostInfo().rows);
            assertEquals(partitionStat.getPartitionDiskSize() + partitionStat2.getPartitionDiskSize(), subJobTask.getCostInfo().dataSize);
        }
    }

    /**
     * 测试moves列表为空的情况
     */
    @Test
    public void testMovesToDdlJob_EmptyMovesList() {
        String tableGroupName = "test_table_group";
        List<ActionMovePartition> moves = Collections.emptyList();

        // 验证抛出异常
        try {
            ActionMovePartition.movesToDdlJob(tableGroupName, moves, executionContext);
            fail("Expected TddlRuntimeException to be thrown");
        } catch (TddlRuntimeException e) {
            assertEquals(ErrorCode.ERR_REBALANCE.getCode(), e.getErrorCode());
            assertTrue(e.getMessage().contains(ErrorCode.ERR_REBALANCE.toString()));
        }
    }

    /**
     * 测试moves列表只有一个元素的情况
     */
    @Test
    public void testMovesToDdlJob_SingleMove() {
        // 准备数据
        String tableGroupName = "test_table_group";
        String schema = "test_schema";
        String partitionName = "p1";
        String toGroup = "TO_GROUP_1";
        String toInst = null;

        ActionMovePartition move = new ActionMovePartition(schema, tableGroupName, partitionName, toGroup, toInst);
        move.setStats(balanceStats);
        move.setIsSubpartition(false);

        List<ActionMovePartition> moves = Arrays.asList(move);

        // Mock相关对象
        List<PartitionStat> partitionStats = Arrays.asList(partitionStat);
        when(balanceStats.filterPartitionStat(eq(tableGroupName), anySet())).thenReturn(partitionStats);

        when(partitionStat.getPartitionName()).thenReturn(partitionName);
        when(partitionStat.getPartitionGroupRecord()).thenReturn(partitionGroupRecord);
        when(partitionStat.getPartitionDiskSize()).thenReturn(1024L);
        when(partitionStat.getDataRows()).thenReturn(100L);

        when(partitionGroupRecord.getGroup_Name()).thenReturn("FROM_GROUP");

        // Mock TableGroupStat相关对象以避免IndexOutOfBoundsException
        when(tableGroupRecord.getTg_name()).thenReturn(tableGroupName);
        when(tableGroupConfig.getTableGroupRecord()).thenReturn(tableGroupRecord);
        when(tableGroupConfig.getTableCount()).thenReturn(1);
        when(tableGroupStat.getTableGroupConfig()).thenReturn(tableGroupConfig);
        when(balanceStats.getTableGroupStats()).thenReturn(Arrays.asList(tableGroupStat));

        Map<String, String> groupNameToInstMap = new HashMap<>();
        groupNameToInstMap.put("FROM_GROUP", "INST_1");
        groupNameToInstMap.put(toGroup, "INST_2");

        // Mock DbTopologyManager.getGroupNameToStorageInstIdMap
        try (MockedStatic<DbTopologyManager> mockedDbTopologyManager = Mockito.mockStatic(DbTopologyManager.class)) {
            mockedDbTopologyManager.when(() -> DbTopologyManager.getGroupNameToStorageInstIdMap(schema))
                .thenReturn(groupNameToInstMap);

            // 执行测试
            ExecutableDdlJob result = ActionMovePartition.movesToDdlJob(tableGroupName, moves, executionContext);

            // 验证结果
            assertNotNull(result);
        }
    }

    /**
     * 测试使用toInst而不是toGroup的情况
     */
    @Test
    public void testMovesToDdlJob_WithToInst() {
        // 准备数据
        String tableGroupName = "test_table_group";
        String schema = "test_schema";
        String partitionName = "p1";
        String toInst = "TARGET_INST";

        ActionMovePartition move = new ActionMovePartition(schema, tableGroupName, partitionName, null, toInst);
        move.setStats(balanceStats);
        move.setIsSubpartition(false);

        List<ActionMovePartition> moves = Arrays.asList(move);

        // Mock相关对象
        List<PartitionStat> partitionStats = Arrays.asList(partitionStat);
        when(balanceStats.filterPartitionStat(eq(tableGroupName), anySet())).thenReturn(partitionStats);

        when(partitionStat.getPartitionName()).thenReturn(partitionName);
        when(partitionStat.getPartitionGroupRecord()).thenReturn(partitionGroupRecord);
        when(partitionStat.getPartitionDiskSize()).thenReturn(1024L);


        // Mock TableGroupStat相关对象以避免IndexOutOfBoundsException
        when(tableGroupRecord.getTg_name()).thenReturn(tableGroupName);
        when(tableGroupConfig.getTableGroupRecord()).thenReturn(tableGroupRecord);
        when(tableGroupConfig.getTableCount()).thenReturn(1);
        when(tableGroupStat.getTableGroupConfig()).thenReturn(tableGroupConfig);
        when(balanceStats.getTableGroupStats()).thenReturn(Arrays.asList(tableGroupStat));

        Map<String, String> groupNameToInstMap = new HashMap<>();
        groupNameToInstMap.put("FROM_GROUP", "INST_1");

        // Mock DbTopologyManager.getGroupNameToStorageInstIdMap
        try (MockedStatic<DbTopologyManager> mockedDbTopologyManager = Mockito.mockStatic(DbTopologyManager.class)) {
            mockedDbTopologyManager.when(() -> DbTopologyManager.getGroupNameToStorageInstIdMap(schema))
                .thenReturn(groupNameToInstMap);

            // 执行测试
            ExecutableDdlJob result = ActionMovePartition.movesToDdlJob(tableGroupName, moves, executionContext);

            // 验证结果
            assertNotNull(result);
        }
    }
}
