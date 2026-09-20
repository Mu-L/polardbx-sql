package com.alibaba.polardbx.executor.gms.util;

import com.alibaba.polardbx.common.jdbc.IConnection;
import com.alibaba.polardbx.common.jdbc.IDataSource;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.common.TopologyHandler;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.executor.statistic.ndv.NDVShardSketch;
import com.alibaba.polardbx.executor.utils.failpoint.FailPoint;
import com.alibaba.polardbx.gms.config.impl.MetaDbInstConfigManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticDataSource;
import com.alibaba.polardbx.optimizer.config.table.statistic.StatisticManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertType;
import com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertUtil;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.alibaba.polardbx.optimizer.optimizeralert.OptimizerAlertType.STATISTIC_PERSIST_FAIL;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyFloat;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class StatisticSubProcessUtilsTest {

    @Test
    public void testSampleTableDdl1() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        StatisticSubProcessUtils.sampleTableDdl("schema", "table", null);
    }

    @Test
    public void testSampleTableDdl2() {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);
        List<ColumnMeta> analyzeColumnList = Lists.newArrayList();
        analyzeColumnList.add(new ColumnMeta("schema", "table", "column", null));

        StatisticManager statisticManager = mock(StatisticManager.class);
        StatisticManager.CacheLine cacheLine = mock(StatisticManager.CacheLine.class);
        when(statisticManager.getCacheLine(anyString(), anyString())).thenReturn(cacheLine);

        try (MockedStatic<com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils> mock = mockStatic(
            com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.class);
            MockedStatic<StatisticManager> mockStatisticManager = mockStatic(StatisticManager.class);
            MockedStatic<OptimizerAlertUtil> optimizerAlertUtilMockedStatic = mockStatic(OptimizerAlertUtil.class);
            MockedStatic<StatisticUtils> statisticUtilsMockedStatic = mockStatic(StatisticUtils.class);
        ) {
            mock.when(
                () -> com.alibaba.polardbx.optimizer.config.table.statistic.StatisticUtils.getColumnMetas(anyBoolean(),
                    anyString(), anyString())).thenReturn(analyzeColumnList);
            mockStatisticManager.when(() -> StatisticManager.getInstance()).thenReturn(statisticManager);

            when(cacheLine.getRowCount()).thenReturn(Long.MAX_VALUE);
            statisticUtilsMockedStatic.when(() -> StatisticUtils.getSampleRate(anyLong())).thenReturn(1f);

            StatisticSubProcessUtils.sampleTableDdl("schema", "table", null);

            verify(cacheLine, times(1)).remainColumns(analyzeColumnList);
            optimizerAlertUtilMockedStatic.verify(
                () -> OptimizerAlertUtil.statisticsAlert(anyString(), anyString(),
                    eq(OptimizerAlertType.STATISTIC_SAMPLE_FAIL), any(), anyString()), times(1));

            when(cacheLine.getRowCount()).thenReturn(10L);
            statisticUtilsMockedStatic.when(
                () -> StatisticUtils.scanAnalyze(anyString(), anyString(), anyList(), anyFloat(), anyInt(), anyList(),
                    anyBoolean())).thenReturn(10D);

            StatisticSubProcessUtils.sampleTableDdl("schema", "table", null);

            optimizerAlertUtilMockedStatic.verify(
                () -> OptimizerAlertUtil.checkStatisticsMiss(anyString(), anyString(), eq(cacheLine), anyInt()),
                times(1));
        }
    }

    @Test
    public void testPersistStatistic() throws SQLException {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);

        StatisticManager statisticManager = mock(StatisticManager.class);
        StatisticManager.CacheLine cacheLine = mock(StatisticManager.CacheLine.class);
        StatisticDataSource statisticDataSource = mock(StatisticDataSource.class);
        when(statisticManager.getCacheLine(anyString(), anyString())).thenReturn(cacheLine);
        when(statisticManager.getSds()).thenReturn(statisticDataSource);

        Map<String, Long> cardinalityMap = Maps.newConcurrentMap();
        cardinalityMap.put("column", 10L);

        Map<String, Long> nullCountMap = Maps.newConcurrentMap();
        nullCountMap.put("column", 10L);

        when(cacheLine.getCardinalityMap()).thenReturn(cardinalityMap);
        try (MockedStatic<OptimizerAlertUtil> optimizerAlertUtilMockedStatic = mockStatic(OptimizerAlertUtil.class);
            MockedStatic<StatisticManager> mockStatisticManager = mockStatic(StatisticManager.class);
            MockedStatic<StatisticUtils> statisticUtilsMockedStatic = mockStatic(StatisticUtils.class);
        ) {
            mockStatisticManager.when(() -> StatisticManager.getInstance()).thenReturn(statisticManager);

            StatisticSubProcessUtils.persistStatistic("schema", "table", true, null);

            optimizerAlertUtilMockedStatic.verify(
                () -> OptimizerAlertUtil.statisticsAlert(anyString(), anyString(), eq(STATISTIC_PERSIST_FAIL), any(),
                    any()), times(1));
            when(cacheLine.getNullCountMap()).thenReturn(nullCountMap);

            StatisticSubProcessUtils.persistStatistic("schema", "table", true, null);

            statisticUtilsMockedStatic.verify(
                () -> StatisticUtils.updateMetaDbInformationSchemaTables(anyString(), anyString()),
                times(1));
        }
    }

    @Test
    public void testGetIndexInfoFromColumnarIndex_NoColumnarIndexes() {
        // Test case: No columnar indexes available
        String schema = "test_schema";
        String table = "test_table";
        ExecutionContext ec = mock(ExecutionContext.class);
        TableMeta tableMeta = mock(TableMeta.class);
        Set<String> colDoneSet = Sets.newHashSet();
        Map<String, Set<String>> indexColsMap = new HashMap<>();

        try (MockedStatic<CBOUtil> cboUtilMock = mockStatic(CBOUtil.class)) {
            // Mock CBOUtil to return null/empty list
            cboUtilMock.when(() -> CBOUtil.getColumnarIndexNamesWithoutArchive(table, schema, ec))
                .thenReturn(null);

            StatisticSubProcessUtils.getIndexInfoFromColumnarIndex(schema, table, ec, tableMeta, colDoneSet,
                indexColsMap);

            // Verify that indexColsMap remains empty
            assertTrue("IndexColsMap should be empty when no columnar indexes", indexColsMap.isEmpty());
        }
    }

    @Test
    public void testGetIndexInfoFromColumnarIndex_EmptyColumnarIndexes() {
        // Test case: Empty columnar indexes list
        String schema = "test_schema";
        String table = "test_table";
        ExecutionContext ec = mock(ExecutionContext.class);
        TableMeta tableMeta = mock(TableMeta.class);
        Set<String> colDoneSet = Sets.newHashSet();
        Map<String, Set<String>> indexColsMap = new HashMap<>();

        try (MockedStatic<CBOUtil> cboUtilMock = mockStatic(CBOUtil.class)) {
            // Mock CBOUtil to return empty list
            cboUtilMock.when(() -> CBOUtil.getColumnarIndexNamesWithoutArchive(table, schema, ec))
                .thenReturn(Lists.newArrayList());

            StatisticSubProcessUtils.getIndexInfoFromColumnarIndex(schema, table, ec, tableMeta, colDoneSet,
                indexColsMap);

            // Verify that indexColsMap remains empty
            assertTrue("IndexColsMap should be empty when columnar indexes list is empty", indexColsMap.isEmpty());
        }
    }

    @Test
    public void testGetIndexInfoFromColumnarIndex_NullAllColumns() {
        // Test case: TableMeta returns null for getAllColumns
        String schema = "test_schema";
        String table = "test_table";
        ExecutionContext ec = mock(ExecutionContext.class);
        TableMeta tableMeta = mock(TableMeta.class);
        Set<String> colDoneSet = Sets.newHashSet();
        Map<String, Set<String>> indexColsMap = new HashMap<>();

        List<String> columnarIndexNames = Lists.newArrayList("cci_index1");

        try (MockedStatic<CBOUtil> cboUtilMock = mockStatic(CBOUtil.class)) {
            cboUtilMock.when(() -> CBOUtil.getColumnarIndexNamesWithoutArchive(table, schema, ec))
                .thenReturn(columnarIndexNames);

            // Mock TableMeta to return null for getAllColumns
            when(tableMeta.getAllColumns()).thenReturn(null);

            StatisticSubProcessUtils.getIndexInfoFromColumnarIndex(schema, table, ec, tableMeta, colDoneSet,
                indexColsMap);

            // Verify that indexColsMap remains empty
            assertTrue("IndexColsMap should be empty when getAllColumns returns null", indexColsMap.isEmpty());
        }
    }

    @Test
    public void testGetIndexInfoFromColumnarIndex_EmptyAllColumns() {
        // Test case: TableMeta returns empty list for getAllColumns
        String schema = "test_schema";
        String table = "test_table";
        ExecutionContext ec = mock(ExecutionContext.class);
        TableMeta tableMeta = mock(TableMeta.class);
        Set<String> colDoneSet = Sets.newHashSet();
        Map<String, Set<String>> indexColsMap = new HashMap<>();

        List<String> columnarIndexNames = Lists.newArrayList("cci_index1");

        try (MockedStatic<CBOUtil> cboUtilMock = mockStatic(CBOUtil.class)) {
            cboUtilMock.when(() -> CBOUtil.getColumnarIndexNamesWithoutArchive(table, schema, ec))
                .thenReturn(columnarIndexNames);

            // Mock TableMeta to return empty list for getAllColumns
            when(tableMeta.getAllColumns()).thenReturn(Lists.newArrayList());

            StatisticSubProcessUtils.getIndexInfoFromColumnarIndex(schema, table, ec, tableMeta, colDoneSet,
                indexColsMap);

            // Verify that indexColsMap remains empty
            assertTrue("IndexColsMap should be empty when getAllColumns returns empty list", indexColsMap.isEmpty());
        }
    }

    @Test
    public void testGetIndexInfoFromColumnarIndex_AllColumnsAlreadyDone() {
        // Test case: All columns are already in colDoneSet
        String schema = "test_schema";
        String table = "test_table";
        ExecutionContext ec = mock(ExecutionContext.class);
        TableMeta tableMeta = mock(TableMeta.class);
        Set<String> colDoneSet = Sets.newHashSet("col1", "col2", "col3");
        Map<String, Set<String>> indexColsMap = new HashMap<>();

        List<String> columnarIndexNames = Lists.newArrayList("cci_index1");
        List<ColumnMeta> allColumns = Lists.newArrayList(
            new ColumnMeta("test_table", "col1", null, null),
            new ColumnMeta("test_table", "col2", null, null),
            new ColumnMeta("test_table", "col3", null, null)
        );

        try (MockedStatic<CBOUtil> cboUtilMock = mockStatic(CBOUtil.class)) {
            cboUtilMock.when(() -> CBOUtil.getColumnarIndexNamesWithoutArchive(table, schema, ec))
                .thenReturn(columnarIndexNames);

            when(tableMeta.getAllColumns()).thenReturn(allColumns);

            StatisticSubProcessUtils.getIndexInfoFromColumnarIndex(schema, table, ec, tableMeta, colDoneSet,
                indexColsMap);

            // Verify that indexColsMap remains empty since all columns are already done
            assertTrue("IndexColsMap should be empty when all columns are already done", indexColsMap.isEmpty());
        }
    }

    @Test
    public void testGetIndexInfoFromColumnarIndex_Success() {
        // Test case: Normal successful execution
        String schema = "test_schema";
        String table = "test_table";
        ExecutionContext ec = mock(ExecutionContext.class);
        TableMeta tableMeta = mock(TableMeta.class);
        Set<String> colDoneSet = Sets.newHashSet("col1"); // Only col1 is done
        Map<String, Set<String>> indexColsMap = new HashMap<>();

        List<String> columnarIndexNames = Lists.newArrayList("cci_index1", "cci_index2");
        List<ColumnMeta> allColumns = Lists.newArrayList(
            new ColumnMeta("test_table", "col1", null, null),
            new ColumnMeta("test_table", "col2", null, null),
            new ColumnMeta("test_table", "col3", null, null)
        );

        try (MockedStatic<CBOUtil> cboUtilMock = mockStatic(CBOUtil.class)) {
            cboUtilMock.when(() -> CBOUtil.getColumnarIndexNamesWithoutArchive(table, schema, ec))
                .thenReturn(columnarIndexNames);

            when(tableMeta.getAllColumns()).thenReturn(allColumns);

            StatisticSubProcessUtils.getIndexInfoFromColumnarIndex(schema, table, ec, tableMeta, colDoneSet,
                indexColsMap);

            // Verify that only the first columnar index is processed
            assertEquals("Should process exactly one columnar index", 1, indexColsMap.size());
            assertTrue("Should contain the first columnar index", indexColsMap.containsKey("cci_index1"));

            Set<String> expectedColumns = Sets.newHashSet("col2", "col3");
            assertEquals("Should contain available columns (excluding col1)", expectedColumns,
                indexColsMap.get("cci_index1"));
        }
    }

    @Test
    public void testGetIndexInfoFromColumnarIndex_SingleColumn() {
        // Test case: Only one available column
        String schema = "test_schema";
        String table = "test_table";
        ExecutionContext ec = mock(ExecutionContext.class);
        TableMeta tableMeta = mock(TableMeta.class);
        Set<String> colDoneSet = Sets.newHashSet();
        Map<String, Set<String>> indexColsMap = new HashMap<>();

        List<String> columnarIndexNames = Lists.newArrayList("cci_index1");
        List<ColumnMeta> allColumns = Lists.newArrayList(
            new ColumnMeta("test_table", "single_col", null, null)
        );

        try (MockedStatic<CBOUtil> cboUtilMock = mockStatic(CBOUtil.class)) {
            cboUtilMock.when(() -> CBOUtil.getColumnarIndexNamesWithoutArchive(table, schema, ec))
                .thenReturn(columnarIndexNames);

            when(tableMeta.getAllColumns()).thenReturn(allColumns);

            StatisticSubProcessUtils.getIndexInfoFromColumnarIndex(schema, table, ec, tableMeta, colDoneSet,
                indexColsMap);

            // Verify results
            assertEquals("Should process exactly one columnar index", 1, indexColsMap.size());
            assertTrue("Should contain the columnar index", indexColsMap.containsKey("cci_index1"));

            Set<String> expectedColumns = Sets.newHashSet("single_col");
            assertEquals("Should contain the single available column", expectedColumns, indexColsMap.get("cci_index1"));
        }
    }

    /**
     * Test that when DN returns null Cardinality, the rs.wasNull() check skips that column.
     */
    @Test
    public void testCollectCardinalityFromDn_NullCardinality() throws Exception {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);

        StatisticManager statisticManager = mock(StatisticManager.class);
        StatisticManager.CacheLine cacheLine = mock(StatisticManager.CacheLine.class);
        when(statisticManager.getCacheLine(anyString(), anyString(), anyBoolean())).thenReturn(cacheLine);

        // oldCardinalityMap is null to also cover the null-safe path
        when(cacheLine.getCardinalityMap()).thenReturn(null);

        OptimizerContext op = mock(OptimizerContext.class);
        TableMeta tableMeta = mock(TableMeta.class);
        when(op.getLatestSchemaManager()).thenReturn(
            mock(com.alibaba.polardbx.optimizer.config.table.SchemaManager.class));
        when(op.getLatestSchemaManager().getTable(anyString())).thenReturn(tableMeta);

        // Partition info with single column partition
        com.alibaba.polardbx.optimizer.partition.PartitionInfo partitionInfo =
            mock(com.alibaba.polardbx.optimizer.partition.PartitionInfo.class);
        List<List<String>> partCols = new ArrayList<>();
        partCols.add(Lists.newArrayList("col0"));
        when(partitionInfo.getAllLevelActualPartCols()).thenReturn(partCols);
        when(tableMeta.getPartitionInfo()).thenReturn(partitionInfo);

        // Topology map
        Map<String, Set<String>> topologyMap = new HashMap<>();
        topologyMap.put("group1", Sets.newHashSet("t1"));

        // JDBC mocks - simulate null Cardinality
        IDataSource dataSource = mock(IDataSource.class);
        IConnection connection = mock(IConnection.class);
        Statement statement = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(rs);

        // First row: col0 with NULL Cardinality (rs.wasNull() returns true)
        // Second row: col1 with valid Cardinality
        when(rs.next()).thenReturn(true, true, false);
        when(rs.getInt("Seq_in_index")).thenReturn(1, 1);
        when(rs.getString("Column_name")).thenReturn("col0", "col1");
        when(rs.getLong("Cardinality")).thenReturn(0L, 100L);
        // First call wasNull returns true (null Cardinality), second returns false
        when(rs.wasNull()).thenReturn(true, false);

        IGroupExecutor groupExecutor = mock(IGroupExecutor.class);
        when(groupExecutor.getDataSource()).thenReturn(dataSource);
        TopologyHandler topologyHandler = mock(TopologyHandler.class);
        when(topologyHandler.get(anyString())).thenReturn(groupExecutor);

        ExecutorContext executorContext = mock(ExecutorContext.class);
        when(executorContext.getTopologyHandler()).thenReturn(topologyHandler);

        try (MockedStatic<StatisticManager> mockStatMgr = mockStatic(StatisticManager.class);
            MockedStatic<OptimizerContext> mockOpCtx = mockStatic(OptimizerContext.class);
            MockedStatic<ExecutorContext> mockExecCtx = mockStatic(ExecutorContext.class);
            MockedStatic<NDVShardSketch> mockNdv = mockStatic(NDVShardSketch.class);
            MockedStatic<OptimizerAlertUtil> mockAlert = mockStatic(OptimizerAlertUtil.class);
            MockedStatic<StatisticUtils> mockStatUtils = mockStatic(StatisticUtils.class);
            MockedStatic<FailPoint> mockFailPoint = mockStatic(FailPoint.class);
            MockedStatic<com.alibaba.polardbx.gms.module.StatisticModuleLogUtil> mockLogUtil =
                mockStatic(com.alibaba.polardbx.gms.module.StatisticModuleLogUtil.class)) {

            mockStatMgr.when(() -> StatisticManager.getInstance()).thenReturn(statisticManager);
            mockOpCtx.when(() -> OptimizerContext.getContext(anyString())).thenReturn(op);
            mockExecCtx.when(() -> ExecutorContext.getContext(anyString())).thenReturn(executorContext);
            mockNdv.when(() -> NDVShardSketch.getTopology(anyString(), anyString(), any()))
                .thenReturn(topologyMap);
            mockStatUtils.when(() -> StatisticUtils.isFileStore(anyString(), anyString())).thenReturn(false);
            mockFailPoint.when(() -> FailPoint.isKeyEnable(anyString())).thenReturn(false);

            StatisticSubProcessUtils.collectCardinalityFromDn("schema", "table", null);

            // Verify: col0 was skipped due to null Cardinality, col1 was processed
            // Since oldCardinalityMap is null, setCardinality should be called for col1
            verify(cacheLine).setCardinality("col1", 100L);
            verify(cacheLine).setCardinalitySource("col1",
                com.alibaba.polardbx.optimizer.config.table.statistic.inf.StatisticResultSource.DN_STAT.name());
        }
    }

    /**
     * Test that when oldCardinalityMap is not null, existing higher values are preserved.
     */
    @Test
    public void testCollectCardinalityFromDn_OldCardinalityMapNotNull() throws Exception {
        MetaDbInstConfigManager.setConfigFromMetaDb(false);

        StatisticManager statisticManager = mock(StatisticManager.class);
        StatisticManager.CacheLine cacheLine = mock(StatisticManager.CacheLine.class);
        when(statisticManager.getCacheLine(anyString(), anyString(), anyBoolean())).thenReturn(cacheLine);

        // oldCardinalityMap with existing value higher than new
        Map<String, Long> oldCardinalityMap = new HashMap<>();
        oldCardinalityMap.put("col1", 200L); // existing is higher
        when(cacheLine.getCardinalityMap()).thenReturn(oldCardinalityMap);

        OptimizerContext op = mock(OptimizerContext.class);
        TableMeta tableMeta = mock(TableMeta.class);
        when(op.getLatestSchemaManager()).thenReturn(
            mock(com.alibaba.polardbx.optimizer.config.table.SchemaManager.class));
        when(op.getLatestSchemaManager().getTable(anyString())).thenReturn(tableMeta);

        com.alibaba.polardbx.optimizer.partition.PartitionInfo partitionInfo =
            mock(com.alibaba.polardbx.optimizer.partition.PartitionInfo.class);
        List<List<String>> partCols = new ArrayList<>();
        partCols.add(Lists.newArrayList("col0"));
        when(partitionInfo.getAllLevelActualPartCols()).thenReturn(partCols);
        when(tableMeta.getPartitionInfo()).thenReturn(partitionInfo);

        Map<String, Set<String>> topologyMap = new HashMap<>();
        topologyMap.put("group1", Sets.newHashSet("t1"));

        IDataSource dataSource = mock(IDataSource.class);
        IConnection connection = mock(IConnection.class);
        Statement statement = mock(Statement.class);
        ResultSet rs = mock(ResultSet.class);

        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement()).thenReturn(statement);
        when(statement.executeQuery(anyString())).thenReturn(rs);

        // One row: col1 with ndv=100 (less than old value 200)
        when(rs.next()).thenReturn(true, false);
        when(rs.getInt("Seq_in_index")).thenReturn(1);
        when(rs.getString("Column_name")).thenReturn("col1");
        when(rs.getLong("Cardinality")).thenReturn(100L);
        when(rs.wasNull()).thenReturn(false);

        IGroupExecutor groupExecutor = mock(IGroupExecutor.class);
        when(groupExecutor.getDataSource()).thenReturn(dataSource);
        TopologyHandler topologyHandler = mock(TopologyHandler.class);
        when(topologyHandler.get(anyString())).thenReturn(groupExecutor);

        ExecutorContext executorContext = mock(ExecutorContext.class);
        when(executorContext.getTopologyHandler()).thenReturn(topologyHandler);

        try (MockedStatic<StatisticManager> mockStatMgr = mockStatic(StatisticManager.class);
            MockedStatic<OptimizerContext> mockOpCtx = mockStatic(OptimizerContext.class);
            MockedStatic<ExecutorContext> mockExecCtx = mockStatic(ExecutorContext.class);
            MockedStatic<NDVShardSketch> mockNdv = mockStatic(NDVShardSketch.class);
            MockedStatic<OptimizerAlertUtil> mockAlert = mockStatic(OptimizerAlertUtil.class);
            MockedStatic<StatisticUtils> mockStatUtils = mockStatic(StatisticUtils.class);
            MockedStatic<FailPoint> mockFailPoint = mockStatic(FailPoint.class);
            MockedStatic<com.alibaba.polardbx.gms.module.StatisticModuleLogUtil> mockLogUtil =
                mockStatic(com.alibaba.polardbx.gms.module.StatisticModuleLogUtil.class)) {

            mockStatMgr.when(() -> StatisticManager.getInstance()).thenReturn(statisticManager);
            mockOpCtx.when(() -> OptimizerContext.getContext(anyString())).thenReturn(op);
            mockExecCtx.when(() -> ExecutorContext.getContext(anyString())).thenReturn(executorContext);
            mockNdv.when(() -> NDVShardSketch.getTopology(anyString(), anyString(), any()))
                .thenReturn(topologyMap);
            mockStatUtils.when(() -> StatisticUtils.isFileStore(anyString(), anyString())).thenReturn(false);
            mockFailPoint.when(() -> FailPoint.isKeyEnable(anyString())).thenReturn(false);

            StatisticSubProcessUtils.collectCardinalityFromDn("schema", "table", null);

            // Verify: oldNdv (200) > ndv (100), so setCardinality should NOT be called for col1
            // (old value is preserved)
            verify(cacheLine, times(0)).setCardinality("col1", 100L);
        }
    }
}
