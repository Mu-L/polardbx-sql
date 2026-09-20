package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.common.Engine;
import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexVisibility;
import com.alibaba.polardbx.gms.metadb.table.LackLocalIndexStatus;
import com.alibaba.polardbx.optimizer.PlannerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.index.IndexUtil;
import com.clearspring.analytics.util.Lists;
import org.apache.calcite.plan.RelOptSchema;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableLookup;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDal;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIndexHint;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.util.Pair;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.calcite.sql.parser.SqlParserPos.ZERO;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ToDrdsRelVisitorTest {
    private static final String schemaName = "testSchema";

    /**
     * Tests that null is returned when the index node of the scan is not an instance of SqlNodeList
     */
    @Test
    public void testGetForceIndexWithNonSqlNodeListNode() {
        TableScan mockTableScan = mock(TableScan.class);

        when(mockTableScan.getIndexNode()).thenReturn(mock(SqlNode.class));

        assertNull(new ToDrdsRelVisitor().getForceIndex(mockTableScan));
    }

    /**
     * Tests that null is returned when the index node's list is empty
     */
    @Test
    public void testGetForceIndexWithEmptySqlNodeList() {
        TableScan mockTableScan = mock(TableScan.class);
        SqlNodeList mockSqlNodeList = mock(SqlNodeList.class);

        when(mockTableScan.getIndexNode()).thenReturn(mockSqlNodeList);
        when(mockSqlNodeList.getList()).thenReturn(Collections.emptyList());

        assertNull(new ToDrdsRelVisitor().getForceIndex(mockTableScan));
    }

    /**
     * Tests that null is returned when the first element in the index node's list is not a SqlIndexHint
     */
    @Test
    public void testGetForceIndexWithoutSqlIndexHint() {
        TableScan mockTableScan = mock(TableScan.class);
        SqlNodeList mockSqlNodeList = mock(SqlNodeList.class);

        when(mockTableScan.getIndexNode()).thenReturn(mockSqlNodeList);
        when(mockSqlNodeList.getList()).thenReturn(Collections.singletonList(mock(SqlNode.class)));

        assertNull(new ToDrdsRelVisitor().getForceIndex(mockTableScan));
    }

    /**
     * Tests that null is returned when the SqlIndexHint is null
     */
    @Test
    public void testGetForceIndexWithNullSqlIndexHint() {
        TableScan mockTableScan = mock(TableScan.class);
        SqlNodeList mockSqlNodeList = mock(SqlNodeList.class);

        when(mockTableScan.getIndexNode()).thenReturn(mockSqlNodeList);
        when(mockSqlNodeList.getList()).thenReturn(null);

        assertNull(new ToDrdsRelVisitor().getForceIndex(mockTableScan));
    }

    /**
     * Tests that null is returned when the SqlIndexHint has a null index list
     */
    @Test
    public void testGetForceIndexWithNullIndexListInSqlIndexHint() {
        TableScan mockTableScan = mock(TableScan.class);
        SqlNodeList mockSqlNodeList = mock(SqlNodeList.class);
        SqlIndexHint mockSqlIndexHint = mock(SqlIndexHint.class);

        when(mockTableScan.getIndexNode()).thenReturn(mockSqlNodeList);
        when(mockSqlNodeList.getList()).thenReturn(Collections.singletonList(mockSqlIndexHint));
        when(mockSqlIndexHint.getIndexList()).thenReturn(null);

        assertNull(new ToDrdsRelVisitor().getForceIndex(mockTableScan));
    }

    // test no force index
    @Test
    public void testBuildForceIndexWithoutForceIndex() {
        Engine engine = mock(Engine.class);
        RelOptSchema catalog = mock(RelOptSchema.class);
        TableMeta tMeta = mock(TableMeta.class);
        LogicalTableScan scan = mock(LogicalTableScan.class);
        ToDrdsRelVisitor visitor = mock(ToDrdsRelVisitor.class);

        when(scan.getIndexNode()).thenReturn(null);

        RelNode result = visitor.buildForceIndexByForceIndex(catalog, scan, schemaName, tMeta, engine);

        assertNull(result);
    }

    // test force index cci
    @Test
    public void testBuildForceIndexWithCci() {
        try (MockedStatic<LogicalTableScan> scanMockedStatic = mockStatic(LogicalTableScan.class);
            MockedConstruction<OSSTableScan> mockedConstruction = mockConstruction(OSSTableScan.class);
            MockedStatic<PlannerContext> plannerContextMockedStatic = mockStatic(PlannerContext.class)) {
            scanMockedStatic.when(() -> LogicalTableScan.create(any(), any(), any(), any(), any(), any(), any()))
                .thenReturn(mock(LogicalTableScan.class));
            plannerContextMockedStatic.when(() -> PlannerContext.getPlannerContext(any(RelNode.class)))
                .thenReturn(mock(PlannerContext.class));
            Engine engine = mock(Engine.class);
            RelOptSchema catalog = mock(RelOptSchema.class);
            LogicalTableScan scan = mock(LogicalTableScan.class);
            TableMeta tMeta = mock(TableMeta.class);
            GsiMetaManager.GsiIndexMetaBean cciMeta =
                new GsiMetaManager.GsiIndexMetaBean(
                    "testIndex",
                    "testSchema",
                    "testTable",
                    false,
                    "index_schema",
                    "cci",
                    Lists.newArrayList(),
                    Lists.newArrayList(),
                    "indexType",
                    "",
                    "",
                    null,
                    "indexTableName",
                    IndexStatus.PUBLIC,
                    0,
                    true,
                    true,
                    IndexVisibility.VISIBLE,
                    LackLocalIndexStatus.NO_LACKIING);
            ToDrdsRelVisitor visitor = mock(ToDrdsRelVisitor.class);

            when(scan.getIndexNode()).thenReturn(null);
            when(tMeta.findGlobalSecondaryIndexByName("cci")).thenReturn(cciMeta);
            when(visitor.getForceIndex(Mockito.any())).thenReturn(
                Pair.of(new SqlIdentifier(Collections.singletonList("cci"), ZERO),
                    IndexUtil.IndexHintType.FORCE_INDEX));
            when(visitor.buildForceIndexByForceIndex(catalog, scan, schemaName, tMeta, engine)).thenCallRealMethod();
            when(
                visitor.buildForceIndex(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(), Mockito.any(),
                    Mockito.anyString(), Mockito.any()))
                .thenCallRealMethod();
            when(visitor.buildOSSTableScan(Mockito.any(), Mockito.any(), Mockito.anyString(), Mockito.any(),
                Mockito.any()))
                .thenCallRealMethod();

            RelNode result = visitor.buildForceIndexByForceIndex(catalog, scan, schemaName, tMeta, engine);

            assertTrue(result instanceof OSSTableScan);
        }
    }

    // test force index with two names, gis exists and local index exists
    @Test
    public void testBuildForceIndexWithTwoNamesGsiLocalIndexExists() {
        Engine engine = mock(Engine.class);
        RelOptSchema catalog = mock(RelOptSchema.class);
        LogicalTableScan scan = mock(LogicalTableScan.class);
        TableMeta tMeta = mock(TableMeta.class);
        GsiMetaManager.GsiIndexMetaBean gsi = mock(GsiMetaManager.GsiIndexMetaBean.class);
        TableMeta gsiMeta = mock(TableMeta.class);
        ToDrdsRelVisitor visitor = mock(ToDrdsRelVisitor.class);
        LogicalTableLookup target = mock(LogicalTableLookup.class);
        PlannerContext plannerContext = mock(PlannerContext.class);
        ExecutionContext executionContext = mock(ExecutionContext.class);
        SchemaManager schemaManager = mock(SchemaManager.class);

        when(scan.getIndexNode()).thenReturn(null);
        when(tMeta.findGlobalSecondaryIndexByName("tablePart")).thenReturn(gsi);
        when(visitor.getForceIndex(scan)).thenReturn(
            Pair.of(new SqlIdentifier(Arrays.asList("tablePart", "indexPart"), ZERO),
                IndexUtil.IndexHintType.FORCE_INDEX));
        when(visitor.buildForceIndexByForceIndex(catalog, scan, schemaName, tMeta, engine)).thenCallRealMethod();
        when(visitor.getPlannerContext()).thenReturn(plannerContext);
        when(plannerContext.getExecutionContext()).thenReturn(executionContext);
        when(executionContext.getSchemaManager(schemaName)).thenReturn(schemaManager);
        when(schemaManager.getTable(gsi.indexTableName)).thenReturn(gsiMeta);
        when(tMeta.findLocalIndexByName("tablePart")).thenReturn(null);
        when(gsiMeta.findLocalIndexByName("indexPart")).thenReturn(mock(IndexMeta.class));
        when(visitor.buildLogicalTableLookup(catalog, scan, schemaName, engine, gsi, "indexPart",
            IndexUtil.IndexHintType.FORCE_INDEX)).thenReturn(target);

        RelNode result = visitor.buildForceIndexByForceIndex(catalog, scan, schemaName, tMeta, engine);

        assertEquals(target, result);
    }

    // test gsi not exists
    @Test
    public void testBuildForceIndexWithErrorTablePartNotExist() {
        Engine engine = mock(Engine.class);
        RelOptSchema catalog = mock(RelOptSchema.class);
        LogicalTableScan scan = mock(LogicalTableScan.class);
        TableMeta tMeta = mock(TableMeta.class);
        TableMeta gsiMeta = mock(TableMeta.class);
        ToDrdsRelVisitor visitor = mock(ToDrdsRelVisitor.class);
        PlannerContext plannerContext = mock(PlannerContext.class);
        ExecutionContext executionContext = mock(ExecutionContext.class);
        SchemaManager schemaManager = mock(SchemaManager.class);

        when(scan.getIndexNode()).thenReturn(null);
        when(tMeta.findGlobalSecondaryIndexByName("tablePart")).thenReturn(null);
        when(visitor.getForceIndex(scan)).thenReturn(
            Pair.of(new SqlIdentifier(Arrays.asList("tablePart", "indexPart"), ZERO),
                IndexUtil.IndexHintType.FORCE_INDEX));
        when(visitor.buildForceIndexByForceIndex(catalog, scan, schemaName, tMeta, engine)).thenCallRealMethod();
        when(visitor.getPlannerContext()).thenReturn(plannerContext);
        when(plannerContext.getExecutionContext()).thenReturn(executionContext);
        when(executionContext.getSchemaManager(schemaName)).thenReturn(schemaManager);
        when(tMeta.findLocalIndexByName("tablePart")).thenReturn(null);
        when(gsiMeta.findLocalIndexByName("indexPart")).thenReturn(null);

        assertNull(visitor.buildForceIndexByForceIndex(catalog, scan, schemaName, tMeta, engine));
    }

    // gsi local index doesn't exist
    @Test
    public void testBuildForceIndexWithErrorIndexPartNotExist() {
        Engine engine = mock(Engine.class);
        RelOptSchema catalog = mock(RelOptSchema.class);
        LogicalTableScan scan = mock(LogicalTableScan.class);
        TableMeta tMeta = mock(TableMeta.class);
        GsiMetaManager.GsiIndexMetaBean gsi = mock(GsiMetaManager.GsiIndexMetaBean.class);
        TableMeta gsiMeta = mock(TableMeta.class);
        ToDrdsRelVisitor visitor = mock(ToDrdsRelVisitor.class);
        PlannerContext plannerContext = mock(PlannerContext.class);
        ExecutionContext executionContext = mock(ExecutionContext.class);
        SchemaManager schemaManager = mock(SchemaManager.class);

        when(scan.getIndexNode()).thenReturn(null);
        when(tMeta.findGlobalSecondaryIndexByName("tablePart")).thenReturn(gsi);
        when(visitor.getForceIndex(scan)).thenReturn(
            Pair.of(new SqlIdentifier(Arrays.asList("tablePart", "indexPart"), ZERO),
                IndexUtil.IndexHintType.FORCE_INDEX));
        when(visitor.buildForceIndexByForceIndex(catalog, scan, schemaName, tMeta, engine)).thenCallRealMethod();
        when(visitor.getPlannerContext()).thenReturn(plannerContext);
        when(plannerContext.getExecutionContext()).thenReturn(executionContext);
        when(executionContext.getSchemaManager(schemaName)).thenReturn(schemaManager);
        when(schemaManager.getTable(gsi.indexTableName)).thenReturn(gsiMeta);
        when(tMeta.findLocalIndexByName("tablePart")).thenReturn(null);
        when(gsiMeta.findLocalIndexByName("indexPart")).thenReturn(null);

        assertNull(visitor.buildForceIndexByForceIndex(catalog, scan, schemaName, tMeta, engine));
    }

    // test force index with invalid argument count
    @Test
    public void testBuildForceIndexWithErrorInvalidArgumentCount() {
        Engine engine = mock(Engine.class);
        RelOptSchema catalog = mock(RelOptSchema.class);
        TableMeta tMeta = mock(TableMeta.class);
        LogicalTableScan scan = mock(LogicalTableScan.class);
        ToDrdsRelVisitor visitor = mock(ToDrdsRelVisitor.class);

        Pair<SqlIdentifier, IndexUtil.IndexHintType> invalidIndexId =
            Pair.of(new SqlIdentifier(Arrays.asList("tablePart", "indexPart", "extraPart"), ZERO),
                IndexUtil.IndexHintType.FORCE_INDEX);
        doReturn(invalidIndexId).when(visitor).getForceIndex(scan);

        assertNull(visitor.buildForceIndexByForceIndex(catalog, scan, schemaName, tMeta, engine));
    }

    /**
     * 测试没有索引提示时返回null
     */
    @Test
    public void testBuildForceIndexByIndexHintWithoutHint() {
        Engine engine = mock(Engine.class);
        RelOptSchema catalog = mock(RelOptSchema.class);
        TableMeta tMeta = mock(TableMeta.class);
        LogicalTableScan scan = mock(LogicalTableScan.class);

        when(scan.getHints()).thenReturn(null);

        assertNull(new ToDrdsRelVisitor().buildForceIndexByIndexHint(catalog, scan, schemaName, tMeta, engine));
    }

    /**
     * 测试获取到完整索引提示，包括表和索引部分
     */
    @Test
    public void testBuildForceIndexByIndexHintWithFullHintAndIndexPart() {
        Engine engine = mock(Engine.class);
        RelOptSchema catalog = mock(RelOptSchema.class);
        TableMeta tMeta = mock(TableMeta.class);
        LogicalTableScan scan = mock(LogicalTableScan.class);
        SqlCall indexHintMock = mock(SqlCall.class);
        SqlNode tableNode = mock(SqlNode.class);
        SqlNode indexNode = mock(SqlNode.class);

        when(tMeta.getTableName()).thenReturn("tableName_test");
        when(tableNode.toString()).thenReturn("tableName");
        when(indexNode.toString()).thenReturn("indexName");
        when(indexHintMock.getOperandList()).thenReturn(Arrays.asList(mock(SqlNode.class), tableNode, indexNode));
        when(indexHintMock.getOperator()).thenReturn(new SqlSpecialOperator("index", SqlKind.INDEX_OPTION));
        when(scan.getHints()).thenReturn(SqlNodeList.of(indexHintMock));

        assertNull(new ToDrdsRelVisitor().buildForceIndexByIndexHint(catalog, scan, schemaName, tMeta, engine));
    }

    @Test
    /**
     * setIndexNode shouldn't be triggered when there is no index node in scan
     */
    public void testRemoveForceIndexWithoutIndexNode() {
        TableScan tableScan = mock(TableScan.class);
        when(tableScan.getIndexNode()).thenReturn(null);

        ToDrdsRelVisitor visitor = new ToDrdsRelVisitor();
        visitor.removeForceIndex(tableScan);

        verify(tableScan, never()).setIndexNode(any(SqlNodeList.class));
    }

    @Test
    /**
     * setIndexNode shouldn't be triggered when index node in scan is not one SqlNodeList
     */
    public void testRemoveForceIndexWithNonListNode() {
        TableScan tableScan = mock(TableScan.class);
        SqlNode indexNode = mock(SqlNode.class);
        when(tableScan.getIndexNode()).thenReturn(indexNode);

        ToDrdsRelVisitor visitor = new ToDrdsRelVisitor();
        visitor.removeForceIndex(tableScan);

        verify(tableScan, never()).setIndexNode(any(SqlNodeList.class));
    }

    @Test
    /**
     * setIndexNode shouldn't be triggered when index node in scan is an empty SqlNodeList
     */
    public void testRemoveForceIndexWithEmptyListNode() {
        TableScan tableScan = mock(TableScan.class);
        SqlNodeList nodeList = new SqlNodeList(ZERO);
        when(tableScan.getIndexNode()).thenReturn(nodeList);

        ToDrdsRelVisitor visitor = new ToDrdsRelVisitor();
        visitor.removeForceIndex(tableScan);

        verify(tableScan, never()).setIndexNode(any(SqlNodeList.class));
    }

    @Test
    /**
     * setIndexNode shouldn't be triggered when index node in scan had no force index node
     */
    public void testRemoveForceIndexWithOneNonForceNode() {
        TableScan tableScan = mock(TableScan.class);
        SqlNodeList nodeList = new SqlNodeList(ZERO);
        SqlNode nonForceNode = mock(SqlNode.class);
        nodeList.add(nonForceNode);
        when(tableScan.getIndexNode()).thenReturn(nodeList);

        ToDrdsRelVisitor visitor = new ToDrdsRelVisitor();
        visitor.removeForceIndex(tableScan);

        verify(tableScan, never()).setIndexNode(any(SqlNodeList.class));
    }

    @Test
    /**
     * setIndexNode should be triggered when index node in scan had a force index node
     */
    public void testRemoveForceIndexWithOneForceNode() {
        TableScan tableScan = mock(TableScan.class);
        SqlNodeList nodeList = new SqlNodeList(ZERO);
        SqlNode forceNode = buildForceIndexForTest();
        nodeList.add(forceNode);
        when(tableScan.getIndexNode()).thenReturn(nodeList);

        ToDrdsRelVisitor visitor = new ToDrdsRelVisitor();
        visitor.removeForceIndex(tableScan);

        ArgumentCaptor<SqlNode> argumentCaptor = ArgumentCaptor.forClass(SqlNode.class);
        verify(tableScan, times(1)).setIndexNode(argumentCaptor.capture());
        SqlNode indexNode = argumentCaptor.getValue();
        assertTrue(indexNode == null);
    }

    @Test
    /**
     * setIndexNode should be triggered when index node in scan had force/non-force index nodes
     */
    public void testRemoveForceIndexWithMixedNodes() {
        TableScan tableScan = mock(TableScan.class);
        SqlNodeList nodeList = new SqlNodeList(ZERO);
        SqlNode forceNode1 = buildForceIndexForTest();
        SqlNode forceNode2 = buildForceIndexForTest();
        SqlNode nonForceNode1 = mock(SqlNode.class);
        SqlNode nonForceNode2 = mock(SqlNode.class);
        nodeList.add(forceNode1);
        nodeList.add(nonForceNode1);
        nodeList.add(forceNode2);
        nodeList.add(nonForceNode2);
        when(tableScan.getIndexNode()).thenReturn(nodeList);

        ToDrdsRelVisitor visitor = new ToDrdsRelVisitor();
        visitor.removeForceIndex(tableScan);

        ArgumentCaptor<SqlNode> argumentCaptor = ArgumentCaptor.forClass(SqlNode.class);
        verify(tableScan, times(1)).setIndexNode(argumentCaptor.capture());
        SqlNode indexNode = argumentCaptor.getValue();
        assertTrue(indexNode instanceof SqlNodeList &&
            ((SqlNodeList) indexNode).getList().contains(nonForceNode1) &&
            ((SqlNodeList) indexNode).getList().contains(nonForceNode2) &&
            !((SqlNodeList) indexNode).getList().contains(forceNode1) &&
            !((SqlNodeList) indexNode).getList().contains(forceNode2)
        );
    }

    private SqlNode buildForceIndexForTest() {
        return new SqlIndexHint(SqlLiteral.createCharString("FORCE INDEX", ZERO), null, new SqlNodeList(ZERO), ZERO);
    }

    // -----------------------------------------------------------------------
    // isRandomShowKind – new private method added in commit 345cbc5e06b
    // -----------------------------------------------------------------------

    /**
     * Helper: create a real ToDrdsRelVisitor with its private plannerContext
     * field injected, then invoke the private isRandomShowKind method via
     * reflection so the real method body runs.
     */
    private boolean invokeIsRandomShowKind(ParamManager paramManager, SqlDal sqlDal) throws Exception {
        ToDrdsRelVisitor visitor = new ToDrdsRelVisitor();

        // Inject plannerContext into the private field.
        PlannerContext plannerContext = mock(PlannerContext.class);
        when(plannerContext.getParamManager()).thenReturn(paramManager);
        java.lang.reflect.Field plannerContextField =
            ToDrdsRelVisitor.class.getDeclaredField("plannerContext");
        plannerContextField.setAccessible(true);
        plannerContextField.set(visitor, plannerContext);

        // Invoke the private method via reflection.
        Method method = ToDrdsRelVisitor.class.getDeclaredMethod("isRandomShowKind", SqlDal.class);
        method.setAccessible(true);
        return (boolean) method.invoke(visitor, sqlDal);
    }

    /**
     * When SHOW_COMMAND_RAND_DISPATCH is false (default), isRandomShowKind must
     * return false regardless of the SqlDal kind.
     */
    @Test
    public void testIsRandomShowKindReturnsFalseWhenParamDisabled() throws Exception {
        ParamManager paramManager = mock(ParamManager.class);
        when(paramManager.getBoolean(ConnectionParams.SHOW_COMMAND_RAND_DISPATCH)).thenReturn(false);

        SqlDal sqlDal = mock(SqlDal.class);
        when(sqlDal.getKind()).thenReturn(SqlKind.SHOW);

        assertFalse("Should return false when SHOW_COMMAND_RAND_DISPATCH is disabled",
            invokeIsRandomShowKind(paramManager, sqlDal));
    }

    /**
     * When SHOW_COMMAND_RAND_DISPATCH is true but the SqlDal kind is not SHOW,
     * isRandomShowKind must return false.
     */
    @Test
    public void testIsRandomShowKindReturnsFalseWhenKindIsNotShow() throws Exception {
        ParamManager paramManager = mock(ParamManager.class);
        when(paramManager.getBoolean(ConnectionParams.SHOW_COMMAND_RAND_DISPATCH)).thenReturn(true);

        SqlDal sqlDal = mock(SqlDal.class);
        when(sqlDal.getKind()).thenReturn(SqlKind.SHOW_TABLES);

        assertFalse("Should return false when SqlDal kind is not SHOW",
            invokeIsRandomShowKind(paramManager, sqlDal));
    }

    /**
     * When SHOW_COMMAND_RAND_DISPATCH is true AND the SqlDal kind is SHOW,
     * isRandomShowKind must return true.
     */
    @Test
    public void testIsRandomShowKindReturnsTrueWhenParamEnabledAndKindIsShow() throws Exception {
        ParamManager paramManager = mock(ParamManager.class);
        when(paramManager.getBoolean(ConnectionParams.SHOW_COMMAND_RAND_DISPATCH)).thenReturn(true);

        SqlDal sqlDal = mock(SqlDal.class);
        when(sqlDal.getKind()).thenReturn(SqlKind.SHOW);

        assertTrue("Should return true when SHOW_COMMAND_RAND_DISPATCH is enabled and kind is SHOW",
            invokeIsRandomShowKind(paramManager, sqlDal));
    }

    // -----------------------------------------------------------------------
    // resolvePartitionedTableDbIndex – extracted from visit(RelNode) L1537-1544
    // -----------------------------------------------------------------------

    /**
     * Helper: build a visitor with plannerContext injected, then call
     * resolvePartitionedTableDbIndex directly (it is package-private).
     */
    private ToDrdsRelVisitor buildVisitorWithParamManager(ParamManager paramManager) throws Exception {
        ToDrdsRelVisitor visitor = new ToDrdsRelVisitor();
        PlannerContext plannerContext = mock(PlannerContext.class);
        when(plannerContext.getParamManager()).thenReturn(paramManager);
        java.lang.reflect.Field plannerContextField =
            ToDrdsRelVisitor.class.getDeclaredField("plannerContext");
        plannerContextField.setAccessible(true);
        plannerContextField.set(visitor, plannerContext);
        return visitor;
    }

    /**
     * When SHOW_COMMAND_RAND_DISPATCH is enabled and kind is SHOW,
     * resolvePartitionedTableDbIndex must call getRandomPhysicalPartition
     * and set modeHolder[0] to DB_INDEX_MODE_RANDOM.
     */
    @Test
    public void testResolvePartitionedTableDbIndexUsesRandomPartitionWhenParamEnabled() throws Exception {
        ParamManager paramManager = mock(ParamManager.class);
        when(paramManager.getBoolean(ConnectionParams.SHOW_COMMAND_RAND_DISPATCH)).thenReturn(true);

        SqlDal sqlDal = mock(SqlDal.class);
        when(sqlDal.getKind()).thenReturn(SqlKind.SHOW);

        com.alibaba.polardbx.optimizer.partition.PartitionInfoManager partInfoMgr =
            mock(com.alibaba.polardbx.optimizer.partition.PartitionInfoManager.class);
        com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo randomPartition =
            mock(com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo.class);
        when(partInfoMgr.getRandomPhysicalPartition("t1")).thenReturn(randomPartition);

        ToDrdsRelVisitor visitor = buildVisitorWithParamManager(paramManager);
        int[] modeHolder = new int[1];
        com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo result =
            visitor.resolvePartitionedTableDbIndex(sqlDal, partInfoMgr, "t1", modeHolder);

        assertEquals(randomPartition, result);
        assertEquals(com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow.DB_INDEX_MODE_RANDOM, modeHolder[0]);
        verify(partInfoMgr).getRandomPhysicalPartition("t1");
        verify(partInfoMgr, never()).getFirstPhysicalPartition("t1");
    }

    /**
     * When SHOW_COMMAND_RAND_DISPATCH is disabled,
     * resolvePartitionedTableDbIndex must call getFirstPhysicalPartition
     * and set modeHolder[0] to DB_INDEX_MODE_NORMAL.
     */
    @Test
    public void testResolvePartitionedTableDbIndexUsesFirstPartitionWhenParamDisabled() throws Exception {
        ParamManager paramManager = mock(ParamManager.class);
        when(paramManager.getBoolean(ConnectionParams.SHOW_COMMAND_RAND_DISPATCH)).thenReturn(false);

        SqlDal sqlDal = mock(SqlDal.class);
        when(sqlDal.getKind()).thenReturn(SqlKind.SHOW);

        com.alibaba.polardbx.optimizer.partition.PartitionInfoManager partInfoMgr =
            mock(com.alibaba.polardbx.optimizer.partition.PartitionInfoManager.class);
        com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo firstPartition =
            mock(com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo.class);
        when(partInfoMgr.getFirstPhysicalPartition("t1")).thenReturn(firstPartition);

        ToDrdsRelVisitor visitor = buildVisitorWithParamManager(paramManager);
        int[] modeHolder = new int[1];
        com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo result =
            visitor.resolvePartitionedTableDbIndex(sqlDal, partInfoMgr, "t1", modeHolder);

        assertEquals(firstPartition, result);
        assertEquals(com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow.DB_INDEX_MODE_NORMAL, modeHolder[0]);
        verify(partInfoMgr).getFirstPhysicalPartition("t1");
        verify(partInfoMgr, never()).getRandomPhysicalPartition("t1");
    }

    // -----------------------------------------------------------------------
    // resolveNoTableNameDbIndex – extracted from visit(RelNode) L1550-1556
    // -----------------------------------------------------------------------

    /**
     * When SHOW_COMMAND_RAND_DISPATCH is disabled, resolveNoTableNameDbIndex
     * must return the defaultDbIndex unchanged and leave modeHolder[0] as-is.
     */
    @Test
    public void testResolveNoTableNameDbIndexReturnsDefaultWhenParamDisabled() throws Exception {
        ParamManager paramManager = mock(ParamManager.class);
        when(paramManager.getBoolean(ConnectionParams.SHOW_COMMAND_RAND_DISPATCH)).thenReturn(false);

        SqlDal sqlDal = mock(SqlDal.class);
        when(sqlDal.getKind()).thenReturn(SqlKind.SHOW);

        ToDrdsRelVisitor visitor = buildVisitorWithParamManager(paramManager);
        int[] modeHolder = new int[] {com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow.DB_INDEX_MODE_NORMAL};
        String result = visitor.resolveNoTableNameDbIndex(sqlDal, "testSchema", "defaultGroup", modeHolder);

        assertEquals("defaultGroup", result);
        assertEquals(com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow.DB_INDEX_MODE_NORMAL, modeHolder[0]);
    }

    /**
     * When SHOW_COMMAND_RAND_DISPATCH is enabled and kind is SHOW,
     * resolveNoTableNameDbIndex must pick a group from the schema's group list,
     * set modeHolder[0] to DB_INDEX_MODE_RANDOM, and return a valid group name.
     */
    @Test
    public void testResolveNoTableNameDbIndexPicksRandomGroupWhenParamEnabled() throws Exception {
        ParamManager paramManager = mock(ParamManager.class);
        when(paramManager.getBoolean(ConnectionParams.SHOW_COMMAND_RAND_DISPATCH)).thenReturn(true);

        SqlDal sqlDal = mock(SqlDal.class);
        when(sqlDal.getKind()).thenReturn(SqlKind.SHOW);

        com.alibaba.polardbx.common.model.Group groupA = mock(com.alibaba.polardbx.common.model.Group.class);
        com.alibaba.polardbx.common.model.Group groupB = mock(com.alibaba.polardbx.common.model.Group.class);
        when(groupA.getName()).thenReturn("GROUP_A");
        when(groupB.getName()).thenReturn("GROUP_B");
        java.util.List<com.alibaba.polardbx.common.model.Group> groups = java.util.Arrays.asList(groupA, groupB);

        com.alibaba.polardbx.common.model.Matrix matrix = mock(com.alibaba.polardbx.common.model.Matrix.class);
        when(matrix.getGroups()).thenReturn(groups);

        com.alibaba.polardbx.optimizer.OptimizerContext optimizerContext =
            mock(com.alibaba.polardbx.optimizer.OptimizerContext.class);
        when(optimizerContext.getMatrix()).thenReturn(matrix);

        try (MockedStatic<com.alibaba.polardbx.optimizer.OptimizerContext> mockedOptimizerContext =
            mockStatic(com.alibaba.polardbx.optimizer.OptimizerContext.class)) {
            mockedOptimizerContext.when(
                    () -> com.alibaba.polardbx.optimizer.OptimizerContext.getContext("testSchema"))
                .thenReturn(optimizerContext);

            ToDrdsRelVisitor visitor = buildVisitorWithParamManager(paramManager);
            int[] modeHolder = new int[] {
                com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow.DB_INDEX_MODE_NORMAL};
            String result = visitor.resolveNoTableNameDbIndex(sqlDal, "testSchema", "defaultGroup", modeHolder);

            assertTrue("Result must be one of the schema groups",
                result.equals("GROUP_A") || result.equals("GROUP_B"));
            assertEquals(com.alibaba.polardbx.optimizer.core.rel.dal.LogicalShow.DB_INDEX_MODE_RANDOM, modeHolder[0]);
        }
    }
}
