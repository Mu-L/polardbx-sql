package com.alibaba.polardbx.optimizer.core.rel;

import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.exception.TableNotFoundException;
import com.alibaba.polardbx.optimizer.partition.PartitionInfo;
import com.alibaba.polardbx.optimizer.partition.pruning.PhysicalPartitionInfo;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for ReplaceTblWithPhyTblVisitor
 * Focuses on partition hint scenarios and table metadata handling
 *
 * @author fangwu
 */
public class ReplaceTblWithPhyTblVisitorTest {

    private static class TestableVisitor extends ReplaceTblWithPhyTblVisitor {
        public TestableVisitor(String defaultSchemaName, ExecutionContext executionContext, boolean hasDirectHint) {
            super(defaultSchemaName, executionContext, hasDirectHint);
        }

        public SqlNode callBuildSth(SqlNode sqlNode) {
            return super.buildSth(sqlNode);
        }
    }

    /**
     * 测试用例1：当使用分区提示且表元数据为 null 时，应抛出 TableNotFoundException
     * 场景：执行上下文中包含分区提示，但表在 SchemaManager 中不存在
     * 预期：抛出 TableNotFoundException，错误码为 ERR_TABLE_NOT_EXIST
     * <p>
     * 覆盖代码行：
     * TableMeta tb = OptimizerContext.getContext(schemaName).getLatestSchemaManager().getTableWithNull(logicalTableName);
     * if (tb == null) {
     * throw new TableNotFoundException(ErrorCode.ERR_TABLE_NOT_EXIST, logicalTableName);
     * }
     */
    @Test
    public void testPartitionHintWithNullTableMeta_shouldThrowTableNotFoundException() {
        try (MockedStatic<ConfigDataMode> mockedConfig = Mockito.mockStatic(ConfigDataMode.class);
            MockedStatic<OptimizerContext> mockedOptCtx = Mockito.mockStatic(OptimizerContext.class)) {

            // 准备：设置非 FastMock 模式
            mockedConfig.when(ConfigDataMode::isFastMock).thenReturn(false);

            // 准备：创建 ExecutionContext 并设置分区提示
            ExecutionContext ec = new ExecutionContext();
            ec.setPartitionHint("p0");

            // 准备：Mock OptimizerContext 相关对象
            OptimizerContext ctx = Mockito.mock(OptimizerContext.class);
            TddlRuleManager ruleManager = Mockito.mock(TddlRuleManager.class);
            SchemaManager schemaManager = Mockito.mock(SchemaManager.class);

            Mockito.when(ctx.getRuleManager()).thenReturn(ruleManager);
            Mockito.when(ctx.getLatestSchemaManager()).thenReturn(schemaManager);
            mockedOptCtx.when(() -> OptimizerContext.getContext("test_db")).thenReturn(ctx);

            // 准备：设置 TableRule 为 null（触发分区表逻辑）
            Mockito.when(ruleManager.getTableRule("test_table")).thenReturn(null);

            // 准备：设置 TableMeta 为 null（核心测试场景 - 覆盖选中的代码）
            Mockito.when(schemaManager.getTableWithNull("test_table")).thenReturn(null);

            // 执行
            TestableVisitor visitor = new TestableVisitor("test_db", ec, false);
            SqlIdentifier input = new SqlIdentifier("test_table", SqlParserPos.ZERO);
            try {
                visitor.callBuildSth(input);
                Assert.fail("Expected TableNotFoundException to be thrown");
            } catch (TableNotFoundException e) {
                // 验证：应抛出包含 ERR_TABLE_NOT_EXIST 的异常
                Assert.assertTrue(e.getMessage().contains("ERR_TABLE_NOT_EXIST"));
            }
        }
    }

    /**
     * 测试用例2：当使用分区提示且表元数据不为 null 且为广播表时
     * 场景：执行上下文中包含分区提示，表在 SchemaManager 中存在且为广播表
     * 预期：返回物理表名而非逻辑表名
     * <p>
     * 覆盖代码行：验证 tb != null 的分支
     */
    @Test
    public void testPartitionHintWithBroadcastTable_shouldReturnPhysicalTableName() {
        try (MockedStatic<ConfigDataMode> mockedConfig = Mockito.mockStatic(ConfigDataMode.class);
            MockedStatic<OptimizerContext> mockedOptCtx = Mockito.mockStatic(OptimizerContext.class)) {

            // 准备：设置非 FastMock 模式
            mockedConfig.when(ConfigDataMode::isFastMock).thenReturn(false);

            // 准备：创建 ExecutionContext 并设置分区提示
            ExecutionContext ec = new ExecutionContext();
            ec.setPartitionHint("p0");

            // 准备：Mock OptimizerContext 相关对象
            OptimizerContext ctx = Mockito.mock(OptimizerContext.class);
            TddlRuleManager ruleManager = Mockito.mock(TddlRuleManager.class);
            SchemaManager schemaManager = Mockito.mock(SchemaManager.class);

            Mockito.when(ctx.getRuleManager()).thenReturn(ruleManager);
            Mockito.when(ctx.getLatestSchemaManager()).thenReturn(schemaManager);
            mockedOptCtx.when(() -> OptimizerContext.getContext("test_db")).thenReturn(ctx);

            // 准备：设置 TableRule 为 null（触发分区表逻辑）
            Mockito.when(ruleManager.getTableRule("test_table")).thenReturn(null);

            // 准备：Mock TableMeta 和 PartitionInfo（tb != null 的情况）
            TableMeta tableMeta = Mockito.mock(TableMeta.class);
            PartitionInfo partitionInfo = Mockito.mock(PartitionInfo.class, Mockito.RETURNS_DEEP_STUBS);

            Mockito.when(schemaManager.getTableWithNull("test_table")).thenReturn(tableMeta);
            Mockito.when(tableMeta.getPartitionInfo()).thenReturn(partitionInfo);

            // 准备：设置为广播表场景
            Mockito.when(partitionInfo.isGsiBroadcastOrBroadcast()).thenReturn(true);
            Mockito.when(partitionInfo.defaultDbIndex()).thenReturn("group_000");

            // Mock 物理分区拓扑
            PhysicalPartitionInfo physicalPartInfo = Mockito.mock(PhysicalPartitionInfo.class);
            Mockito.when(physicalPartInfo.getPhyTable()).thenReturn("test_table_phy");
            List<PhysicalPartitionInfo> partInfoList = Collections.singletonList(physicalPartInfo);
            Map<String, List<PhysicalPartitionInfo>> topology = Collections.singletonMap("group_000", partInfoList);
            Mockito.when(partitionInfo.getPhysicalPartitionTopology(null)).thenReturn(topology);

            // 执行
            TestableVisitor visitor = new TestableVisitor("test_db", ec, false);
            SqlIdentifier input = new SqlIdentifier("test_table", SqlParserPos.ZERO);
            SqlNode result = visitor.callBuildSth(input);

            // 验证：应返回物理表名
            Assert.assertTrue("Result should be a SqlIdentifier", result instanceof SqlIdentifier);
            SqlIdentifier output = (SqlIdentifier) result;
            Assert.assertEquals("Should return physical table name", "test_table_phy", output.names.get(0));
        }
    }

    /**
     * 测试用例3：当使用分区提示且为限定表名（schema.table）且 TableMeta 为 null
     * 场景：使用 db.table 格式的表名，且表元数据为 null
     * 预期：抛出 TableNotFoundException，错误码为 ERR_TABLE_NOT_EXIST
     * <p>
     * 覆盖代码行：验证限定表名场景下的 tb == null 分支
     */
    @Test
    public void testPartitionHintWithQualifiedTableNameAndNullMeta_shouldThrowTableNotFoundException() {
        try (MockedStatic<ConfigDataMode> mockedConfig = Mockito.mockStatic(ConfigDataMode.class);
            MockedStatic<OptimizerContext> mockedOptCtx = Mockito.mockStatic(OptimizerContext.class)) {

            // 准备：设置非 FastMock 模式
            mockedConfig.when(ConfigDataMode::isFastMock).thenReturn(false);

            // 准备：创建 ExecutionContext 并设置分区提示
            ExecutionContext ec = new ExecutionContext();
            ec.setPartitionHint("p0");

            // 准备：Mock OptimizerContext 相关对象
            OptimizerContext ctx = Mockito.mock(OptimizerContext.class);
            TddlRuleManager ruleManager = Mockito.mock(TddlRuleManager.class);
            SchemaManager schemaManager = Mockito.mock(SchemaManager.class);

            Mockito.when(ctx.getRuleManager()).thenReturn(ruleManager);
            Mockito.when(ctx.getLatestSchemaManager()).thenReturn(schemaManager);
            mockedOptCtx.when(() -> OptimizerContext.getContext("test_db")).thenReturn(ctx);

            // 准备：设置 TableRule 为 null
            Mockito.when(ruleManager.getTableRule("test_table")).thenReturn(null);

            // 准备：设置 TableMeta 为 null（覆盖选中的代码）
            Mockito.when(schemaManager.getTableWithNull("test_table")).thenReturn(null);

            // 执行：使用限定表名
            TestableVisitor visitor = new TestableVisitor("test_db", ec, false);
            SqlIdentifier input = new SqlIdentifier(ImmutableList.of("test_db", "test_table"), SqlParserPos.ZERO);
            try {
                visitor.callBuildSth(input);
                Assert.fail("Expected TableNotFoundException to be thrown");
            } catch (TableNotFoundException e) {
                // 验证：应抛出包含 ERR_TABLE_NOT_EXIST 的异常
                Assert.assertTrue(e.getMessage().contains("ERR_TABLE_NOT_EXIST"));
            }
        }
    }

    /**
     * 测试用例3b：当使用分区提示且表名为 CTE 名称时，应透传原节点不抛异常
     * 场景：执行上下文中包含分区提示，且表名是 WITH 子句中定义的 CTE 名称
     * 预期：返回原始 sqlNode，不抛出 TableNotFoundException
     * <p>
     * 覆盖代码行：验证 cteNames 检查分支
     */
    @Test
    public void testPartitionHintWithCteName_shouldPassThroughWithoutException() {
        try (MockedStatic<ConfigDataMode> mockedConfig = Mockito.mockStatic(ConfigDataMode.class);
            MockedStatic<OptimizerContext> mockedOptCtx = Mockito.mockStatic(OptimizerContext.class)) {

            // 准备：设置非 FastMock 模式
            mockedConfig.when(ConfigDataMode::isFastMock).thenReturn(false);

            // 准备：创建 ExecutionContext 并设置分区提示
            ExecutionContext ec = new ExecutionContext();
            ec.setPartitionHint("p0");

            // 准备：Mock OptimizerContext 相关对象
            OptimizerContext ctx = Mockito.mock(OptimizerContext.class);
            TddlRuleManager ruleManager = Mockito.mock(TddlRuleManager.class);

            Mockito.when(ctx.getRuleManager()).thenReturn(ruleManager);
            mockedOptCtx.when(() -> OptimizerContext.getContext("test_db")).thenReturn(ctx);

            // 准备：设置 TableRule 为 null
            Mockito.when(ruleManager.getTableRule("my_cte")).thenReturn(null);

            // 执行：创建 visitor 并手动设置 cteNames（模拟 WITH 子句解析结果）
            TestableVisitor visitor = new TestableVisitor("test_db", ec, false);
            visitor.cteNames.add("my_cte");
            SqlIdentifier input = new SqlIdentifier("my_cte", SqlParserPos.ZERO);
            SqlNode result = visitor.callBuildSth(input);

            // 验证：应返回原始节点（透传）
            Assert.assertSame("CTE name should pass through without transformation", input, result);
        }
    }

    /**
     * 测试用例4：当没有分区提示时，不应触发该代码路径
     * 场景：ExecutionContext 中没有分区提示
     * 预期：不会进入分区提示处理逻辑，直接返回原节点或走其他路径
     */
    @Test
    public void testWithoutPartitionHint_shouldNotCheckTableMeta() {
        try (MockedStatic<ConfigDataMode> mockedConfig = Mockito.mockStatic(ConfigDataMode.class);
            MockedStatic<OptimizerContext> mockedOptCtx = Mockito.mockStatic(OptimizerContext.class)) {

            // 准备：设置非 FastMock 模式
            mockedConfig.when(ConfigDataMode::isFastMock).thenReturn(false);

            // 准备：创建 ExecutionContext，不设置分区提示
            ExecutionContext ec = new ExecutionContext();

            // 准备：Mock OptimizerContext 相关对象
            OptimizerContext ctx = Mockito.mock(OptimizerContext.class);
            TddlRuleManager ruleManager = Mockito.mock(TddlRuleManager.class);

            Mockito.when(ctx.getRuleManager()).thenReturn(ruleManager);
            mockedOptCtx.when(() -> OptimizerContext.getContext("test_db")).thenReturn(ctx);

            // 准备：设置 TableRule 为 null
            Mockito.when(ruleManager.getTableRule("test_table")).thenReturn(null);

            // 执行
            TestableVisitor visitor = new TestableVisitor("test_db", ec, false);
            SqlIdentifier input = new SqlIdentifier("test_table", SqlParserPos.ZERO);
            SqlNode result = visitor.callBuildSth(input);

            // 验证：应返回原始节点（因为没有 partition hint 且 tableRule 为 null）
            Assert.assertTrue("Result should be a SqlIdentifier", result instanceof SqlIdentifier);
            SqlIdentifier output = (SqlIdentifier) result;
            Assert.assertEquals("Should return original table name", "test_table", output.names.get(0));
        }
    }

    /**
     * 测试用例5：当使用分区提示且表存在且为分区表时（非广播表）
     * 场景：执行上下文中包含分区提示，表存在且有具体的分区信息
     * 预期：根据分区提示返回对应的物理表名
     * <p>
     * 覆盖代码行：验证 tb != null 且为分区表的分支
     */
    @Test
    public void testPartitionHintWithPartitionedTable_shouldReturnPhysicalTableNameForPartition() {
        try (MockedStatic<ConfigDataMode> mockedConfig = Mockito.mockStatic(ConfigDataMode.class);
            MockedStatic<OptimizerContext> mockedOptCtx = Mockito.mockStatic(OptimizerContext.class)) {

            // 准备：设置非 FastMock 模式
            mockedConfig.when(ConfigDataMode::isFastMock).thenReturn(false);

            // 准备：创建 ExecutionContext 并设置分区提示
            ExecutionContext ec = new ExecutionContext();
            ec.setPartitionHint("p0");

            // 准备：Mock OptimizerContext 相关对象
            OptimizerContext ctx = Mockito.mock(OptimizerContext.class);
            TddlRuleManager ruleManager = Mockito.mock(TddlRuleManager.class);
            SchemaManager schemaManager = Mockito.mock(SchemaManager.class);

            Mockito.when(ctx.getRuleManager()).thenReturn(ruleManager);
            Mockito.when(ctx.getLatestSchemaManager()).thenReturn(schemaManager);
            mockedOptCtx.when(() -> OptimizerContext.getContext("test_db")).thenReturn(ctx);

            // 准备：设置 TableRule 为 null（触发分区表逻辑）
            Mockito.when(ruleManager.getTableRule("test_table")).thenReturn(null);

            // 准备：Mock TableMeta 和 PartitionInfo（tb != null）
            TableMeta tableMeta = Mockito.mock(TableMeta.class);
            PartitionInfo partitionInfo = Mockito.mock(PartitionInfo.class);

            Mockito.when(schemaManager.getTableWithNull("test_table")).thenReturn(tableMeta);
            Mockito.when(tableMeta.getPartitionInfo()).thenReturn(partitionInfo);

            // 准备：设置为非广播表
            Mockito.when(partitionInfo.isGsiBroadcastOrBroadcast()).thenReturn(false);
            Mockito.when(partitionInfo.getTableGroupId()).thenReturn(1000L);

            // Mock 物理分区拓扑
            PhysicalPartitionInfo physicalPartInfo = Mockito.mock(PhysicalPartitionInfo.class);
            Mockito.when(physicalPartInfo.getPhyTable()).thenReturn("test_table_p0");
            List<PhysicalPartitionInfo> partInfoList = Collections.singletonList(physicalPartInfo);
            Map<String, List<PhysicalPartitionInfo>> topology = Collections.singletonMap("group_000", partInfoList);
            Mockito.when(partitionInfo.getPhysicalPartitionTopology(Mockito.anyList())).thenReturn(topology);

            // 执行
            TestableVisitor visitor = new TestableVisitor("test_db", ec, false);
            SqlIdentifier input = new SqlIdentifier("test_table", SqlParserPos.ZERO);
            SqlNode result = visitor.callBuildSth(input);

            // 验证：应返回对应分区的物理表名
            Assert.assertTrue("Result should be a SqlIdentifier", result instanceof SqlIdentifier);
            SqlIdentifier output = (SqlIdentifier) result;
            Assert.assertEquals("Should return physical table name for partition", "test_table_p0",
                output.names.get(0));
        }
    }

    /**
     * 测试用例6：FastMock 模式下应直接返回原节点
     * 场景：配置为 FastMock 模式
     * 预期：直接返回输入的 SqlNode，不做任何转换，不会执行到选中的代码
     */
    @Test
    public void testFastMockMode_shouldReturnOriginalNode() {
        try (MockedStatic<ConfigDataMode> mockedConfig = Mockito.mockStatic(ConfigDataMode.class)) {

            // 准备：设置 FastMock 模式
            mockedConfig.when(ConfigDataMode::isFastMock).thenReturn(true);

            // 准备：创建 ExecutionContext
            ExecutionContext ec = new ExecutionContext();

            // 执行
            TestableVisitor visitor = new TestableVisitor("test_db", ec, false);
            SqlIdentifier input = new SqlIdentifier("test_table", SqlParserPos.ZERO);
            SqlNode result = visitor.callBuildSth(input);

            // 验证：应返回原始节点
            Assert.assertSame("Should return same instance in FastMock mode", input, result);
        }
    }
}
