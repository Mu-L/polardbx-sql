package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.constants.SystemTables;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.spi.IGroupExecutor;
import com.alibaba.polardbx.executor.spi.ITopologyExecutor;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.topology.SystemDbHelper;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.partition.PartitionSpec;
import com.alibaba.polardbx.optimizer.partition.PartitionByDefinition;
import com.alibaba.polardbx.optimizer.partition.common.PartitionLocation;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.rule.TableRule;
import com.alibaba.polardbx.rule.TddlRule;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * Unit tests for {@link PhysicalToLogicalTableMapping}.
 */
public class PhysicalToLogicalTableMappingTest {

    @Test
    public void testGetLogicalTableName() {
        Map<String, String> tableMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        tableMap.put("orders_v3gg_00001", "orders");
        PhysicalToLogicalTableMapping mapping = new PhysicalToLogicalTableMapping(tableMap, emptySchemaMap());

        // null
        assertNull(mapping.getLogicalTableName(null));
        // mapping hit
        assertEquals("orders", mapping.getLogicalTableName("orders_v3gg_00001"));
        // case-insensitive hit
        assertEquals("orders", mapping.getLogicalTableName("ORDERS_V3GG_00001"));
        // fallback to PhysicalNameExtractor
        PhysicalToLogicalTableMapping empty = createEmptyMapping();
        assertEquals("orders", empty.getLogicalTableName("orders_v3gg_00001"));
        assertEquals("my_table", empty.getLogicalTableName("my_table"));
    }

    @Test
    public void testGetLogicalSchemaName() {
        Map<String, String> schemaMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        schemaMap.put("mydb_p00000", "mydb");
        PhysicalToLogicalTableMapping mapping = new PhysicalToLogicalTableMapping(emptyTableMap(), schemaMap);

        // null
        assertNull(mapping.getLogicalSchemaName(null));
        // mapping hit
        assertEquals("mydb", mapping.getLogicalSchemaName("mydb_p00000"));
        // case-insensitive hit
        assertEquals("mydb", mapping.getLogicalSchemaName("MYDB_P00000"));
        // fallback to PhysicalNameExtractor
        PhysicalToLogicalTableMapping empty = createEmptyMapping();
        assertEquals("dxlauto", empty.getLogicalSchemaName("dxlauto_p00000"));
        assertEquals("slt", empty.getLogicalSchemaName("slt_000003"));
        assertEquals("plaindb", empty.getLogicalSchemaName("plaindb"));
    }

    @Test
    public void testBuildForAllSchemas_autoModeTable() {
        try (MockedStatic<DbInfoManager> dbInfoMock = mockStatic(DbInfoManager.class);
            MockedStatic<SystemDbHelper> sysDbMock = mockStatic(SystemDbHelper.class);
            MockedStatic<OptimizerContext> optMock = mockStatic(OptimizerContext.class);
            MockedStatic<ExecutorContext> execMock = mockStatic(ExecutorContext.class);
            MockedStatic<com.alibaba.polardbx.gms.util.GroupInfoUtil> groupInfoMock =
                mockStatic(com.alibaba.polardbx.gms.util.GroupInfoUtil.class);
            MockedStatic<SystemTables> sysTblMock = mockStatic(SystemTables.class)) {

            sysTblMock.when(() -> SystemTables.contains(anyString())).thenReturn(false);

            DbInfoManager dbInfoManager = mock(DbInfoManager.class);
            dbInfoMock.when(DbInfoManager::getInstance).thenReturn(dbInfoManager);
            when(dbInfoManager.getDbList()).thenReturn(Arrays.asList("testdb"));
            sysDbMock.when(() -> SystemDbHelper.isDBBuildIn("testdb")).thenReturn(false);

            OptimizerContext optimizerContext = mock(OptimizerContext.class);
            optMock.when(() -> OptimizerContext.getContext("testdb")).thenReturn(optimizerContext);

            ExecutorContext executorContext = mock(ExecutorContext.class);
            execMock.when(() -> ExecutorContext.getContext("testdb")).thenReturn(executorContext);

            TddlRuleManager ruleManager = mock(TddlRuleManager.class);
            when(optimizerContext.getRuleManager()).thenReturn(ruleManager);
            TddlRule tddlRule = mock(TddlRule.class);
            when(ruleManager.getTddlRule()).thenReturn(tddlRule);
            when(tddlRule.getTables()).thenReturn(Collections.emptyList());

            PartitionInfoManager partInfoMgr = mock(PartitionInfoManager.class);
            when(optimizerContext.getPartitionInfoManager()).thenReturn(partInfoMgr);

            ITopologyExecutor topoExec = mock(ITopologyExecutor.class);
            when(executorContext.getTopologyExecutor()).thenReturn(topoExec);

            PartitionInfo ordersPartInfo = mock(PartitionInfo.class);
            when(ordersPartInfo.getTableName()).thenReturn("orders");
            when(partInfoMgr.getPartitionInfos()).thenReturn(Arrays.asList(ordersPartInfo));
            when(partInfoMgr.isNewPartDbTable("orders")).thenReturn(true);
            when(partInfoMgr.getPartitionInfo("orders")).thenReturn(ordersPartInfo);

            PartitionByDefinition partBy = mock(PartitionByDefinition.class);
            when(ordersPartInfo.getPartitionBy()).thenReturn(partBy);
            PartitionSpec spec = mock(PartitionSpec.class);
            when(partBy.getPhysicalPartitions()).thenReturn(Arrays.asList(spec));
            PartitionLocation location = mock(PartitionLocation.class);
            when(spec.getLocation()).thenReturn(location);
            when(location.getGroupKey()).thenReturn("TESTDB_P00000_GROUP");
            when(location.getPhyTableName()).thenReturn("orders_8htn_00000");

            groupInfoMock.when(() ->
                com.alibaba.polardbx.gms.util.GroupInfoUtil.buildPhysicalDbNameFromGroupName("testdb",
                    "TESTDB_P00000_GROUP")
            ).thenReturn("testdb_p00000");

            SchemaManager schemaManager = mock(SchemaManager.class);
            when(optimizerContext.getLatestSchemaManager()).thenReturn(schemaManager);
            TableMeta tableMeta = mock(TableMeta.class);
            when(schemaManager.getTable("orders")).thenReturn(tableMeta);
            when(tableMeta.getGsiPublished()).thenReturn(null);

            PhysicalToLogicalTableMapping mapping = PhysicalToLogicalTableMapping.buildForAllSchemas();

            assertNotNull(mapping);
            assertEquals("orders", mapping.getLogicalTableName("orders_8htn_00000"));
            assertEquals("testdb", mapping.getLogicalSchemaName("testdb_p00000"));
        }
    }

    @Test
    public void testBuildForAllSchemas_withGsiTable() {
        try (MockedStatic<DbInfoManager> dbInfoMock = mockStatic(DbInfoManager.class);
            MockedStatic<SystemDbHelper> sysDbMock = mockStatic(SystemDbHelper.class);
            MockedStatic<OptimizerContext> optMock = mockStatic(OptimizerContext.class);
            MockedStatic<ExecutorContext> execMock = mockStatic(ExecutorContext.class);
            MockedStatic<com.alibaba.polardbx.gms.util.GroupInfoUtil> groupInfoMock =
                mockStatic(com.alibaba.polardbx.gms.util.GroupInfoUtil.class);
            MockedStatic<SystemTables> sysTblMock = mockStatic(SystemTables.class)) {

            sysTblMock.when(() -> SystemTables.contains(anyString())).thenReturn(false);

            DbInfoManager dbInfoManager = mock(DbInfoManager.class);
            dbInfoMock.when(DbInfoManager::getInstance).thenReturn(dbInfoManager);
            when(dbInfoManager.getDbList()).thenReturn(Arrays.asList("mydb"));
            sysDbMock.when(() -> SystemDbHelper.isDBBuildIn("mydb")).thenReturn(false);

            OptimizerContext optimizerContext = mock(OptimizerContext.class);
            optMock.when(() -> OptimizerContext.getContext("mydb")).thenReturn(optimizerContext);

            ExecutorContext executorContext = mock(ExecutorContext.class);
            execMock.when(() -> ExecutorContext.getContext("mydb")).thenReturn(executorContext);

            TddlRuleManager ruleManager = mock(TddlRuleManager.class);
            when(optimizerContext.getRuleManager()).thenReturn(ruleManager);
            TddlRule tddlRule = mock(TddlRule.class);
            when(ruleManager.getTddlRule()).thenReturn(tddlRule);
            when(tddlRule.getTables()).thenReturn(Collections.emptyList());

            PartitionInfoManager partInfoMgr = mock(PartitionInfoManager.class);
            when(optimizerContext.getPartitionInfoManager()).thenReturn(partInfoMgr);

            ITopologyExecutor topoExec = mock(ITopologyExecutor.class);
            when(executorContext.getTopologyExecutor()).thenReturn(topoExec);

            PartitionInfo usersPartInfo = mock(PartitionInfo.class);
            when(usersPartInfo.getTableName()).thenReturn("users");
            when(partInfoMgr.getPartitionInfos()).thenReturn(Arrays.asList(usersPartInfo));
            when(partInfoMgr.isNewPartDbTable("users")).thenReturn(true);
            when(partInfoMgr.getPartitionInfo("users")).thenReturn(usersPartInfo);

            PartitionByDefinition partBy = mock(PartitionByDefinition.class);
            when(usersPartInfo.getPartitionBy()).thenReturn(partBy);
            PartitionSpec spec = mock(PartitionSpec.class);
            when(partBy.getPhysicalPartitions()).thenReturn(Arrays.asList(spec));
            PartitionLocation loc = mock(PartitionLocation.class);
            when(spec.getLocation()).thenReturn(loc);
            when(loc.getGroupKey()).thenReturn("MYDB_P00000_GROUP");
            when(loc.getPhyTableName()).thenReturn("users_abcd_00000");

            groupInfoMock.when(() ->
                com.alibaba.polardbx.gms.util.GroupInfoUtil.buildPhysicalDbNameFromGroupName("mydb",
                    "MYDB_P00000_GROUP")
            ).thenReturn("mydb_p00000");

            SchemaManager schemaManager = mock(SchemaManager.class);
            when(optimizerContext.getLatestSchemaManager()).thenReturn(schemaManager);
            TableMeta tableMeta = mock(TableMeta.class);
            when(schemaManager.getTable("users")).thenReturn(tableMeta);

            Map<String, GsiMetaManager.GsiIndexMetaBean> gsiMap = new HashMap<>();
            gsiMap.put("gsi_uid", mock(GsiMetaManager.GsiIndexMetaBean.class));
            when(tableMeta.getGsiPublished()).thenReturn(gsiMap);

            when(partInfoMgr.isNewPartDbTable("gsi_uid")).thenReturn(true);
            PartitionInfo gsiPartInfo = mock(PartitionInfo.class);
            when(partInfoMgr.getPartitionInfo("gsi_uid")).thenReturn(gsiPartInfo);

            PartitionByDefinition gsiPartBy = mock(PartitionByDefinition.class);
            when(gsiPartInfo.getPartitionBy()).thenReturn(gsiPartBy);
            PartitionSpec gsiSpec = mock(PartitionSpec.class);
            when(gsiPartBy.getPhysicalPartitions()).thenReturn(Arrays.asList(gsiSpec));
            PartitionLocation gsiLoc = mock(PartitionLocation.class);
            when(gsiSpec.getLocation()).thenReturn(gsiLoc);
            when(gsiLoc.getGroupKey()).thenReturn("MYDB_P00000_GROUP");
            when(gsiLoc.getPhyTableName()).thenReturn("gsi_uid_efgh_00000");

            PhysicalToLogicalTableMapping mapping = PhysicalToLogicalTableMapping.buildForAllSchemas();

            assertEquals("users", mapping.getLogicalTableName("users_abcd_00000"));
            assertEquals("gsi_uid", mapping.getLogicalTableName("gsi_uid_efgh_00000"));
            assertEquals("mydb", mapping.getLogicalSchemaName("mydb_p00000"));
        }
    }

    @Test
    public void testBuildForAllSchemas_drdsMode() {
        try (MockedStatic<DbInfoManager> dbInfoMock = mockStatic(DbInfoManager.class);
            MockedStatic<SystemDbHelper> sysDbMock = mockStatic(SystemDbHelper.class);
            MockedStatic<OptimizerContext> optMock = mockStatic(OptimizerContext.class);
            MockedStatic<ExecutorContext> execMock = mockStatic(ExecutorContext.class);
            MockedStatic<SystemTables> sysTblMock = mockStatic(SystemTables.class)) {

            sysTblMock.when(() -> SystemTables.contains(anyString())).thenReturn(false);

            DbInfoManager dbInfoManager = mock(DbInfoManager.class);
            dbInfoMock.when(DbInfoManager::getInstance).thenReturn(dbInfoManager);
            when(dbInfoManager.getDbList()).thenReturn(Arrays.asList("drdsdb"));
            sysDbMock.when(() -> SystemDbHelper.isDBBuildIn("drdsdb")).thenReturn(false);

            OptimizerContext optimizerContext = mock(OptimizerContext.class);
            optMock.when(() -> OptimizerContext.getContext("drdsdb")).thenReturn(optimizerContext);

            ExecutorContext executorContext = mock(ExecutorContext.class);
            execMock.when(() -> ExecutorContext.getContext("drdsdb")).thenReturn(executorContext);

            TddlRuleManager ruleManager = mock(TddlRuleManager.class);
            when(optimizerContext.getRuleManager()).thenReturn(ruleManager);

            TddlRule tddlRule = mock(TddlRule.class);
            when(ruleManager.getTddlRule()).thenReturn(tddlRule);
            TableRule tableRule = mock(TableRule.class);
            when(tableRule.getVirtualTbName()).thenReturn("items");
            when(tddlRule.getTables()).thenReturn(Arrays.asList(tableRule));

            PartitionInfoManager partInfoMgr = mock(PartitionInfoManager.class);
            when(optimizerContext.getPartitionInfoManager()).thenReturn(partInfoMgr);
            when(partInfoMgr.getPartitionInfos()).thenReturn(Collections.emptyList());
            when(partInfoMgr.isNewPartDbTable("items")).thenReturn(false);

            ITopologyExecutor topoExec = mock(ITopologyExecutor.class);
            when(executorContext.getTopologyExecutor()).thenReturn(topoExec);

            when(ruleManager.getTableRule("items")).thenReturn(tableRule);
            Map<String, Set<String>> topology = new HashMap<>();
            Set<String> physTables = new HashSet<>(Arrays.asList("items_0000", "items_0001"));
            topology.put("DRDSDB_0000_GROUP", physTables);
            when(tableRule.getStaticTopology()).thenReturn(topology);

            IGroupExecutor groupExecutor = mock(IGroupExecutor.class);
            when(topoExec.getGroupExecutor("DRDSDB_0000_GROUP")).thenReturn(groupExecutor);
            when(groupExecutor.getDataSource()).thenReturn(null);

            SchemaManager schemaManager = mock(SchemaManager.class);
            when(optimizerContext.getLatestSchemaManager()).thenReturn(schemaManager);
            TableMeta tableMeta = mock(TableMeta.class);
            when(schemaManager.getTable("items")).thenReturn(tableMeta);
            when(tableMeta.getGsiPublished()).thenReturn(null);

            PhysicalToLogicalTableMapping mapping = PhysicalToLogicalTableMapping.buildForAllSchemas();

            assertEquals("items", mapping.getLogicalTableName("items_0000"));
            assertEquals("items", mapping.getLogicalTableName("items_0001"));
        }
    }

    @Test
    public void testBuildForAllSchemas_errorHandling() {
        // Case 1: DbInfoManager throws — should not propagate
        try (MockedStatic<DbInfoManager> dbInfoMock = mockStatic(DbInfoManager.class)) {
            DbInfoManager dbInfoManager = mock(DbInfoManager.class);
            dbInfoMock.when(DbInfoManager::getInstance).thenReturn(dbInfoManager);
            when(dbInfoManager.getDbList()).thenThrow(new RuntimeException("metadb unavailable"));

            PhysicalToLogicalTableMapping mapping = PhysicalToLogicalTableMapping.buildForAllSchemas();
            assertNotNull(mapping);
        }

        // Case 2: system DBs skipped, null contexts handled gracefully
        try (MockedStatic<DbInfoManager> dbInfoMock = mockStatic(DbInfoManager.class);
            MockedStatic<SystemDbHelper> sysDbMock = mockStatic(SystemDbHelper.class);
            MockedStatic<OptimizerContext> optMock = mockStatic(OptimizerContext.class)) {

            DbInfoManager dbInfoManager = mock(DbInfoManager.class);
            dbInfoMock.when(DbInfoManager::getInstance).thenReturn(dbInfoManager);
            when(dbInfoManager.getDbList()).thenReturn(Arrays.asList("information_schema", "userdb"));
            sysDbMock.when(() -> SystemDbHelper.isDBBuildIn("information_schema")).thenReturn(true);
            sysDbMock.when(() -> SystemDbHelper.isDBBuildIn("userdb")).thenReturn(false);
            optMock.when(() -> OptimizerContext.getContext("userdb")).thenReturn(null);

            PhysicalToLogicalTableMapping mapping = PhysicalToLogicalTableMapping.buildForAllSchemas();
            assertNotNull(mapping);
        }
    }

    // ==================== Helpers ====================

    private PhysicalToLogicalTableMapping createEmptyMapping() {
        return new PhysicalToLogicalTableMapping(emptyTableMap(), emptySchemaMap());
    }

    private Map<String, String> emptyTableMap() {
        return new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    }

    private Map<String, String> emptySchemaMap() {
        return new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
    }
}
