package com.alibaba.polardbx.optimizer.ttl;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.gms.topology.DbInfoManager;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import com.alibaba.polardbx.gms.ttl.TtlInfoRecord;

import static org.mockito.Mockito.*;

public class TtlUtilTest {

    @Mock
    private DbInfoManager dbInfoManager;

    @Mock
    private OptimizerContext optimizerContext;

    @Before
    public void setUp() {
        MockitoAnnotations.initMocks(this);
    }

    /**
     * 测试当 targetPartInfo 为 null 时，方法返回 true 的情况
     * 覆盖 TtlUtil.java 的第435行～442行代码
     */
    @Test
    public void testCheckIfAllowedDropTableOperationWhenTargetPartInfoIsNull() {
        String targetTableSchema = "test_schema";
        String targetTableName = "test_table";

        try (MockedStatic<DbInfoManager> dbInfoManagerMockedStatic = mockStatic(DbInfoManager.class)) {

            dbInfoManagerMockedStatic.when(() -> DbInfoManager.getInstance()).thenReturn(dbInfoManager);

            try (MockedStatic<OptimizerContext> optimizerContextMockedStatic = mockStatic(OptimizerContext.class)) {

                when(dbInfoManager.isNewPartitionDb(targetTableSchema)).thenReturn(true);

                // 设置 OptimizerContext
                optimizerContextMockedStatic.when(() -> OptimizerContext.getContext(targetTableSchema))
                    .thenReturn(optimizerContext);

                when(optimizerContext.getViewManager()).thenReturn(null);

                // 设置 SchemaManager 和 TableMeta
                SchemaManager schemaManager = mock(SchemaManager.class);

                TableMeta tableMeta = mock(TableMeta.class);
                when(tableMeta.getPartitionInfo()).thenReturn(null);
                when(tableMeta.getTtlDefinitionInfo()).thenReturn(null);

                ParamManager mockedParamManager = mock(ParamManager.class);
                when(mockedParamManager.getBoolean(ConnectionParams.TTL_DEBUG_USE_GSI_FOR_COLUMNAR_ARC_TBL)).thenReturn(
                    false);
                when(mockedParamManager.getBoolean(ConnectionParams.TTL_FORBID_DROP_TTL_TBL_WITH_ARC_CCI)).thenReturn(
                    true);

                ExecutionContext executionContext1 = mock(ExecutionContext.class);
                when(executionContext1.getSchemaManager(targetTableSchema)).thenReturn(schemaManager);
                when(
                    executionContext1.getSchemaManager(targetTableSchema).getTableWithNull(targetTableName)).thenReturn(
                    tableMeta);
                when(executionContext1.getParamManager()).thenReturn(mockedParamManager);

                TableMeta tableMeta2 = mock(TableMeta.class);
                ExecutionContext executionContext2 = mock(ExecutionContext.class);
                when(executionContext2.getSchemaManager(targetTableSchema)).thenReturn(schemaManager);
                when(
                    executionContext2.getSchemaManager(targetTableSchema).getTableWithNull(targetTableName)).thenReturn(
                    tableMeta2);
                when(executionContext2.getParamManager()).thenReturn(mockedParamManager);

                // 调用方法
                boolean result =
                    TtlUtil.checkIfAllowedDropTableOperation(targetTableSchema, targetTableName, executionContext1);
                // 验证结果为 true（覆盖第435行～442行代码）
                Assert.assertTrue("当targetPartInfo为null时，方法应该返回true", result);

            }
        }
    }

    /**
     * B2 fix: arcTmpTblName=null should not NPE
     */
    @Test
    public void testCheckIfDropCciOfArcTblView_arcTmpTblNameNull_shouldNotNPE() {
        TtlDefinitionInfo ttlInfo = mock(TtlDefinitionInfo.class);
        when(ttlInfo.needPerformExpiredDataArchiving()).thenReturn(true);
        when(ttlInfo.getTmpTableName()).thenReturn(null);
        TableMeta tableMeta = mock(TableMeta.class);
        when(tableMeta.getTtlDefinitionInfo()).thenReturn(ttlInfo);
        SchemaManager schemaManager = mock(SchemaManager.class);
        when(schemaManager.getTableWithNull("t1")).thenReturn(tableMeta);
        ExecutionContext ec = mock(ExecutionContext.class);
        when(ec.getSchemaManager("db1")).thenReturn(schemaManager);
        boolean result = TtlUtil.checkIfDropCciOfArcTblView("db1", "t1", "idx1", ec);
        Assert.assertFalse(result);
    }

    /**
     * B2 fix: arcTmpTblName="" should return false
     */
    @Test
    public void testCheckIfDropCciOfArcTblView_arcTmpTblNameEmpty_shouldReturnFalse() {
        TtlDefinitionInfo ttlInfo = mock(TtlDefinitionInfo.class);
        when(ttlInfo.needPerformExpiredDataArchiving()).thenReturn(true);
        when(ttlInfo.getTmpTableName()).thenReturn("");
        TableMeta tableMeta = mock(TableMeta.class);
        when(tableMeta.getTtlDefinitionInfo()).thenReturn(ttlInfo);
        SchemaManager schemaManager = mock(SchemaManager.class);
        when(schemaManager.getTableWithNull("t1")).thenReturn(tableMeta);
        ExecutionContext ec = mock(ExecutionContext.class);
        when(ec.getSchemaManager("db1")).thenReturn(schemaManager);
        boolean result = TtlUtil.checkIfDropCciOfArcTblView("db1", "t1", "idx1", ec);
        Assert.assertFalse(result);
    }

    /**
     * B2 fix: normal case - valid arcTmpTblName
     */
    @Test
    public void testCheckIfDropCciOfArcTblView_arcTmpTblNameValid() {
        TtlDefinitionInfo ttlInfo = mock(TtlDefinitionInfo.class);
        when(ttlInfo.needPerformExpiredDataArchiving()).thenReturn(true);
        when(ttlInfo.getTmpTableName()).thenReturn("arc_tmp_arc");
        TableMeta tableMeta = mock(TableMeta.class);
        when(tableMeta.getTtlDefinitionInfo()).thenReturn(ttlInfo);
        SchemaManager schemaManager = mock(SchemaManager.class);
        when(schemaManager.getTableWithNull("t1")).thenReturn(tableMeta);
        ExecutionContext ec = mock(ExecutionContext.class);
        when(ec.getSchemaManager("db1")).thenReturn(schemaManager);
        boolean result = TtlUtil.checkIfDropCciOfArcTblView("db1", "t1", "arc_tmp_arc_12345", ec);
        Assert.assertTrue(result);
    }

}
