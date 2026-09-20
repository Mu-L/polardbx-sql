package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.gms.util.GroupInfoUtil;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.partition.PartitionInfoManager;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.alibaba.polardbx.rule.TableRule;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlDelete;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ReplaceDifferentDBSingleTblWithPhyTblVisitorTest {

    private static class TestableVisitor extends ReplaceDifferentDBSingleTblWithPhyTblVisitor {
        public TestableVisitor(String defaultSchemaName, ExecutionContext executionContext) {
            super(defaultSchemaName, executionContext);
        }

        public SqlNode callBuildSth(SqlNode sqlNode) {
            return super.buildSth(sqlNode);
        }

        public boolean callAddAliasForDelete(SqlNode delete) {
            return super.addAliasForDelete(delete);
        }
    }

    @Test
    public void testFastMockReturnsOriginalNode() {
        try (MockedStatic<ConfigDataMode> mockedConfig = Mockito.mockStatic(ConfigDataMode.class)) {
            mockedConfig.when(ConfigDataMode::isFastMock).thenReturn(true);

            TestableVisitor visitor = new TestableVisitor("db1", new ExecutionContext());
            SqlIdentifier input = new SqlIdentifier(ImmutableList.of("db1", "t"), SqlParserPos.ZERO);
            SqlNode result = visitor.callBuildSth(input);

            Assert.assertSame(input, result);
            Assert.assertTrue(visitor.getLogicalTables().isEmpty());
            Assert.assertTrue(visitor.getPhysicalTables().isEmpty());
            Assert.assertTrue(visitor.getSchemaPhysicalMapping().isEmpty());
        }
    }

    @Test
    public void testNonPartitionDbWithTableRuleProducesPhysicalNames_simpleIdentifier() {
        try (MockedStatic<ConfigDataMode> mockedConfig = Mockito.mockStatic(ConfigDataMode.class);
            MockedStatic<DbInfoManager> mockedDbInfoMgr = Mockito.mockStatic(DbInfoManager.class);
            MockedStatic<OptimizerContext> mockedOptCtx = Mockito.mockStatic(OptimizerContext.class);
            MockedStatic<GroupInfoUtil> mockedGroup = Mockito.mockStatic(GroupInfoUtil.class)) {

            mockedConfig.when(ConfigDataMode::isFastMock).thenReturn(false);

            DbInfoManager dbInfoManager = Mockito.mock(DbInfoManager.class);
            mockedDbInfoMgr.when(DbInfoManager::getInstance).thenReturn(dbInfoManager);
            Mockito.when(dbInfoManager.isNewPartitionDb("db1")).thenReturn(false);

            OptimizerContext ctx = Mockito.mock(OptimizerContext.class);
            TddlRuleManager ruleManager = Mockito.mock(TddlRuleManager.class);
            Mockito.when(ctx.getRuleManager()).thenReturn(ruleManager);
            mockedOptCtx.when(() -> OptimizerContext.getContext("db1")).thenReturn(ctx);

            TableRule tableRule = Mockito.mock(TableRule.class);
            Mockito.when(ruleManager.getTableRule("t")).thenReturn(tableRule);
            Mockito.when(tableRule.getDbNamePattern()).thenReturn("grp_pattern");
            Mockito.when(tableRule.getTbNamePattern()).thenReturn("phy_t");

            mockedGroup.when(() -> GroupInfoUtil.buildPhysicalDbNameFromGroupName("db1", "grp_pattern"))
                .thenReturn("phy_db");

            TestableVisitor visitor = new TestableVisitor("db1", new ExecutionContext());
            SqlIdentifier input = new SqlIdentifier("t", SqlParserPos.ZERO);
            SqlNode result = visitor.callBuildSth(input);

            Assert.assertTrue(result instanceof SqlIdentifier);
            SqlIdentifier out = (SqlIdentifier) result;
            Assert.assertEquals(2, out.names.size());
            Assert.assertEquals("phy_db", out.names.get(0));
            Assert.assertEquals("phy_t", out.names.get(1));

            Assert.assertEquals(1, visitor.getLogicalTables().size());
            Assert.assertEquals(new TableId("db1", "t"), visitor.getLogicalTables().get(0));
            Assert.assertEquals(1, visitor.getPhysicalTables().size());
            Assert.assertEquals(new TableId("phy_db", "phy_t"), visitor.getPhysicalTables().get(0));
            Assert.assertTrue(visitor.getSchemaPhysicalMapping().containsKey("db1"));
            Assert.assertTrue(visitor.getSchemaPhysicalMapping().get("db1").contains("phy_db"));
        }
    }

    @Test
    public void testNonPartitionDbWithTableRuleProducesPhysicalNames_qualifiedIdentifier() {
        try (MockedStatic<ConfigDataMode> mockedConfig = Mockito.mockStatic(ConfigDataMode.class);
            MockedStatic<DbInfoManager> mockedDbInfoMgr = Mockito.mockStatic(DbInfoManager.class);
            MockedStatic<OptimizerContext> mockedOptCtx = Mockito.mockStatic(OptimizerContext.class);
            MockedStatic<GroupInfoUtil> mockedGroup = Mockito.mockStatic(GroupInfoUtil.class)) {

            mockedConfig.when(ConfigDataMode::isFastMock).thenReturn(false);

            DbInfoManager dbInfoManager = Mockito.mock(DbInfoManager.class);
            mockedDbInfoMgr.when(DbInfoManager::getInstance).thenReturn(dbInfoManager);
            Mockito.when(dbInfoManager.isNewPartitionDb("db2")).thenReturn(false);

            OptimizerContext ctx = Mockito.mock(OptimizerContext.class);
            TddlRuleManager ruleManager = Mockito.mock(TddlRuleManager.class);
            Mockito.when(ctx.getRuleManager()).thenReturn(ruleManager);
            mockedOptCtx.when(() -> OptimizerContext.getContext("db2")).thenReturn(ctx);

            TableRule tableRule = Mockito.mock(TableRule.class);
            Mockito.when(ruleManager.getTableRule("t2")).thenReturn(tableRule);
            Mockito.when(tableRule.getDbNamePattern()).thenReturn("grp_p2");
            Mockito.when(tableRule.getTbNamePattern()).thenReturn("phy_t2");

            mockedGroup.when(() -> GroupInfoUtil.buildPhysicalDbNameFromGroupName("db2", "grp_p2"))
                .thenReturn("phy_db2");

            TestableVisitor visitor = new TestableVisitor("db1", new ExecutionContext());
            SqlIdentifier input = new SqlIdentifier(ImmutableList.of("db2", "t2"), SqlParserPos.ZERO);
            SqlNode result = visitor.callBuildSth(input);

            Assert.assertTrue(result instanceof SqlIdentifier);
            SqlIdentifier out = (SqlIdentifier) result;
            Assert.assertEquals(2, out.names.size());
            Assert.assertEquals("phy_db2", out.names.get(0));
            Assert.assertEquals("phy_t2", out.names.get(1));

            Assert.assertEquals(new TableId("db2", "t2"), visitor.getLogicalTables().get(0));
            Assert.assertEquals(new TableId("phy_db2", "phy_t2"), visitor.getPhysicalTables().get(0));
            Assert.assertTrue(visitor.getSchemaPhysicalMapping().containsKey("db2"));
            Assert.assertTrue(visitor.getSchemaPhysicalMapping().get("db2").contains("phy_db2"));
        }
    }

    @Test
    public void testPartitionDbCteNameWhenPartitionInfoNull() {
        try (MockedStatic<ConfigDataMode> mockedConfig = Mockito.mockStatic(ConfigDataMode.class);
            MockedStatic<DbInfoManager> mockedDbInfoMgr = Mockito.mockStatic(DbInfoManager.class);
            MockedStatic<OptimizerContext> mockedOptCtx = Mockito.mockStatic(OptimizerContext.class)) {

            mockedConfig.when(ConfigDataMode::isFastMock).thenReturn(false);

            DbInfoManager dbInfoManager = Mockito.mock(DbInfoManager.class);
            mockedDbInfoMgr.when(DbInfoManager::getInstance).thenReturn(dbInfoManager);
            Mockito.when(dbInfoManager.isNewPartitionDb("db1")).thenReturn(true);

            OptimizerContext ctx = Mockito.mock(OptimizerContext.class);
            PartitionInfoManager pim = Mockito.mock(PartitionInfoManager.class);
            Mockito.when(ctx.getPartitionInfoManager()).thenReturn(pim);
            Mockito.when(pim.getPartitionInfo("cte_tbl")).thenReturn(null);
            mockedOptCtx.when(() -> OptimizerContext.getContext("db1")).thenReturn(ctx);

            TestableVisitor visitor = new TestableVisitor("db1", new ExecutionContext());
            SqlIdentifier input = new SqlIdentifier("cte_tbl", SqlParserPos.ZERO);
            SqlNode result = visitor.callBuildSth(input);

            Assert.assertTrue(result instanceof SqlIdentifier);
            SqlIdentifier out = (SqlIdentifier) result;
            Assert.assertEquals(1, out.names.size());
            Assert.assertEquals("cte_tbl", out.names.get(0));

            Assert.assertTrue(visitor.getLogicalTables().isEmpty());
            Assert.assertTrue(visitor.getPhysicalTables().isEmpty());
            Assert.assertTrue(visitor.getSchemaPhysicalMapping().isEmpty());
        }
    }

    @Test
    public void testNonPartitionDbCteNameWhenTableRuleNull() {
        try (MockedStatic<ConfigDataMode> mockedConfig = Mockito.mockStatic(ConfigDataMode.class);
            MockedStatic<DbInfoManager> mockedDbInfoMgr = Mockito.mockStatic(DbInfoManager.class);
            MockedStatic<OptimizerContext> mockedOptCtx = Mockito.mockStatic(OptimizerContext.class)) {

            mockedConfig.when(ConfigDataMode::isFastMock).thenReturn(false);

            DbInfoManager dbInfoManager = Mockito.mock(DbInfoManager.class);
            mockedDbInfoMgr.when(DbInfoManager::getInstance).thenReturn(dbInfoManager);
            Mockito.when(dbInfoManager.isNewPartitionDb("db1")).thenReturn(false);

            OptimizerContext ctx = Mockito.mock(OptimizerContext.class);
            TddlRuleManager ruleManager = Mockito.mock(TddlRuleManager.class);
            Mockito.when(ctx.getRuleManager()).thenReturn(ruleManager);
            Mockito.when(ruleManager.getTableRule("cte_tbl")).thenReturn(null);
            mockedOptCtx.when(() -> OptimizerContext.getContext("db1")).thenReturn(ctx);

            TestableVisitor visitor = new TestableVisitor("db1", new ExecutionContext());
            SqlIdentifier input = new SqlIdentifier("cte_tbl", SqlParserPos.ZERO);
            SqlNode result = visitor.callBuildSth(input);

            Assert.assertTrue(result instanceof SqlIdentifier);
            SqlIdentifier out = (SqlIdentifier) result;
            Assert.assertEquals(1, out.names.size());
            Assert.assertEquals("cte_tbl", out.names.get(0));

            Assert.assertTrue(visitor.getLogicalTables().isEmpty());
            Assert.assertTrue(visitor.getPhysicalTables().isEmpty());
            Assert.assertTrue(visitor.getSchemaPhysicalMapping().isEmpty());
        }
    }

    @Test
    public void testAddAliasForDeleteTrueWhenContainsQueryKey() throws Exception {
        SqlDelete delete = Mockito.mock(SqlDelete.class);
        Map<SqlNode, List<SqlIdentifier>> map = new HashMap<>();
        SqlNode select = SqlParser.create("select 1").parseQuery();
        map.put(select, Collections.emptyList());
        Mockito.when(delete.getSubQueryTableMap()).thenReturn(map);

        TestableVisitor visitor = new TestableVisitor("db1", new ExecutionContext());
        boolean result = visitor.callAddAliasForDelete(delete);
        Assert.assertTrue(result);
    }

    @Test
    public void testAddAliasForDeleteFalseWhenNoQueryKey() {
        SqlDelete delete = Mockito.mock(SqlDelete.class);
        Map<SqlNode, List<SqlIdentifier>> map = new HashMap<>();
        SqlIdentifier id = new SqlIdentifier("t", SqlParserPos.ZERO);
        map.put(id, Collections.emptyList());
        Mockito.when(delete.getSubQueryTableMap()).thenReturn(map);

        TestableVisitor visitor = new TestableVisitor("db1", new ExecutionContext());
        boolean result = visitor.callAddAliasForDelete(delete);
        Assert.assertFalse(result);
    }

    // 新增：分区库且存在 PartitionInfo 的路径，验证物理库表名替换与统计集合
    @Test
    public void testPartitionDbWithPartitionInfoProducesPhysicalNames() {
        try (MockedStatic<ConfigDataMode> mockedConfig = Mockito.mockStatic(ConfigDataMode.class);
            MockedStatic<DbInfoManager> mockedDbInfoMgr = Mockito.mockStatic(DbInfoManager.class);
            MockedStatic<OptimizerContext> mockedOptCtx = Mockito.mockStatic(OptimizerContext.class);
            MockedStatic<GroupInfoUtil> mockedGroup = Mockito.mockStatic(GroupInfoUtil.class)) {

            mockedConfig.when(ConfigDataMode::isFastMock).thenReturn(false);

            DbInfoManager dbInfoManager = Mockito.mock(DbInfoManager.class);
            mockedDbInfoMgr.when(DbInfoManager::getInstance).thenReturn(dbInfoManager);
            Mockito.when(dbInfoManager.isNewPartitionDb("db1")).thenReturn(true);

            // mock OptimizerContext -> PartitionInfoManager -> PartitionInfo
            OptimizerContext ctx = Mockito.mock(OptimizerContext.class);
            PartitionInfoManager pim = Mockito.mock(PartitionInfoManager.class);
            Mockito.when(ctx.getPartitionInfoManager()).thenReturn(pim);
            mockedOptCtx.when(() -> OptimizerContext.getContext("db1")).thenReturn(ctx);

            // 使用 deep stubbing 简化链式调用
            com.alibaba.polardbx.optimizer.partition.PartitionInfo deepInfo =
                Mockito.mock(com.alibaba.polardbx.optimizer.partition.PartitionInfo.class, Mockito.RETURNS_DEEP_STUBS);
            Mockito.when(deepInfo.getTableSchema()).thenReturn("db1");
            Mockito.when(deepInfo.getPartitionBy().getPartitions().get(0).getLocation().getGroupKey())
                .thenReturn("grp1");
            Mockito.when(deepInfo.getPrefixTableName()).thenReturn("phy_t");

            Mockito.when(pim.getPartitionInfo("t")).thenReturn(deepInfo);

            mockedGroup.when(() -> GroupInfoUtil.buildPhysicalDbNameFromGroupName("db1", "grp1"))
                .thenReturn("phy_db");

            TestableVisitor visitor = new TestableVisitor("db1", new ExecutionContext());
            SqlIdentifier input = new SqlIdentifier("t", SqlParserPos.ZERO);
            SqlNode result = visitor.callBuildSth(input);

            Assert.assertTrue(result instanceof SqlIdentifier);
            SqlIdentifier out = (SqlIdentifier) result;
            Assert.assertEquals(2, out.names.size());
            Assert.assertEquals("phy_db", out.names.get(0));
            Assert.assertEquals("phy_t", out.names.get(1));

            Assert.assertEquals(1, visitor.getLogicalTables().size());
            Assert.assertEquals(new TableId("db1", "t"), visitor.getLogicalTables().get(0));
            Assert.assertEquals(1, visitor.getPhysicalTables().size());
            Assert.assertEquals(new TableId("phy_db", "phy_t"), visitor.getPhysicalTables().get(0));
            Assert.assertTrue(visitor.getSchemaPhysicalMapping().containsKey("db1"));
            Assert.assertTrue(visitor.getSchemaPhysicalMapping().get("db1").contains("phy_db"));
        }
    }
}