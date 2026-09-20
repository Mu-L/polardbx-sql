package com.alibaba.polardbx.optimizer.utils;

import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.gms.metadb.table.IndexVisibility;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.planner.rule.util.CBOUtil;
import org.junit.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ColumnarIndexNamesWithoutArchiveTest {

    @Test
    public void testGetColumnarIndexNamesWithoutArchive_MixedArchiveAndNonArchive() {
        // Arrange
        String schema = "test";
        String table = "t1";

        Map<String, GsiMetaManager.GsiIndexMetaBean> indexMap = new HashMap<>();

        GsiMetaManager.GsiIndexMetaBean beanMock1 = mock(GsiMetaManager.GsiIndexMetaBean.class);
        when(beanMock1.getVisibility()).thenReturn(IndexVisibility.VISIBLE);
        indexMap.put("cci1", beanMock1);

        GsiMetaManager.GsiIndexMetaBean beanMock2 = mock(GsiMetaManager.GsiIndexMetaBean.class);
        when(beanMock2.getVisibility()).thenReturn(IndexVisibility.VISIBLE);
        indexMap.put("cci2", beanMock2);

        GsiMetaManager.GsiIndexMetaBean beanMock3 = mock(GsiMetaManager.GsiIndexMetaBean.class);
        when(beanMock3.getVisibility()).thenReturn(IndexVisibility.VISIBLE);
        indexMap.put("cci3", beanMock1);

        GsiMetaManager.GsiIndexMetaBean beanMock4 = mock(GsiMetaManager.GsiIndexMetaBean.class);
        when(beanMock4.getVisibility()).thenReturn(IndexVisibility.VISIBLE);
        indexMap.put("cci4", beanMock1);

        TableMeta mockTableMeta = mock(TableMeta.class);
        when(mockTableMeta.getColumnarIndexPublished()).thenReturn(indexMap);
        TableMeta mockTableMetaCci1 = mock(TableMeta.class);
        when(mockTableMetaCci1.isColumnar()).thenReturn(true);
        when(mockTableMetaCci1.isColumnarArchive()).thenReturn(true);

        // 模拟部分索引是归档
        TableMeta mockTableMetaCci2 = mock(TableMeta.class);
        when(mockTableMetaCci2.getId()).thenReturn(5L);
        when(mockTableMetaCci2.isColumnar()).thenReturn(true);
        when(mockTableMetaCci2.isColumnarArchive()).thenReturn(false);
        TableMeta mockTableMetaCci4 = mock(TableMeta.class);
        when(mockTableMetaCci4.getId()).thenReturn(4L);
        when(mockTableMetaCci4.isColumnar()).thenReturn(true);
        when(mockTableMetaCci4.isColumnarArchive()).thenReturn(false);
        SchemaManager mockSchemaManager = mock(SchemaManager.class);
        when(mockSchemaManager.getTableWithNull(table)).thenReturn(mockTableMeta);
        when(mockSchemaManager.getTableWithNull("cci1")).thenReturn(mockTableMetaCci1);
        when(mockSchemaManager.getTableWithNull("cci2")).thenReturn(mockTableMetaCci2);
        when(mockSchemaManager.getTableWithNull("cci4")).thenReturn(mockTableMetaCci4);

        OptimizerContext mockOptimizerContext = mock(OptimizerContext.class);
        when(mockOptimizerContext.getLatestSchemaManager()).thenReturn(mockSchemaManager);

        try (MockedStatic<OptimizerContext> optimizerContextMockedStatic = Mockito.mockStatic(OptimizerContext.class);
            MockedStatic<CBOUtil> cboUtilMockedStatic = Mockito.mockStatic(CBOUtil.class);
            MockedStatic<ConfigDataMode> configDataModeMockedStatic = Mockito.mockStatic(ConfigDataMode.class)) {
            optimizerContextMockedStatic.when(() -> OptimizerContext.getContext(schema))
                .thenReturn(mockOptimizerContext);
            cboUtilMockedStatic.when(
                    () -> CBOUtil.getColumnarIndexNamesWithArchiveInCol(anyString(), anyString(), any()))
                .thenCallRealMethod();
            cboUtilMockedStatic.when(() -> CBOUtil.getColumnarIndexNames(anyString(), anyString(), any()))
                .thenCallRealMethod();
            cboUtilMockedStatic.when(() -> CBOUtil.getColumnarIndexNamesWithoutArchive(anyString(), anyString(), any()))
                .thenCallRealMethod();

            // Act
            List<String> result = CBOUtil.getColumnarIndexNamesWithoutArchive(table, schema, null);
            assertEquals(2, result.size());
            assertEquals("cci4", result.get(0));
            assertEquals("cci2", result.get(1));

            ExecutionContext executionContext = mock(ExecutionContext.class);
            when(executionContext.getSchemaManager(schema)).thenReturn(mockSchemaManager);
            result = CBOUtil.getColumnarIndexNamesWithoutArchive(table, schema, executionContext);
            assertEquals(2, result.size());
            assertEquals("cci4", result.get(0));
            assertEquals("cci2", result.get(1));

            configDataModeMockedStatic.when(ConfigDataMode::isColumnarMode).thenReturn(false);
            result = CBOUtil.getColumnarIndexNamesWithArchiveInCol(table, schema, executionContext);
            assertEquals(2, result.size());
            assertEquals("cci4", result.get(0));
            assertEquals("cci2", result.get(1));

            configDataModeMockedStatic.when(ConfigDataMode::isColumnarMode).thenReturn(true);
            result = CBOUtil.getColumnarIndexNamesWithArchiveInCol(table, schema, executionContext);
            assertEquals(2, result.size());
            assertEquals("cci4", result.get(0));
            assertEquals("cci2", result.get(1));

        }
    }

    @Test
    public void testGetColumnarIndexNamesWithArchiveInCol_OnlyArchive() {
        // Arrange
        String schema = "test";
        String table = "t1";

        Map<String, GsiMetaManager.GsiIndexMetaBean> indexMap = new HashMap<>();

        GsiMetaManager.GsiIndexMetaBean beanMock1 = mock(GsiMetaManager.GsiIndexMetaBean.class);
        when(beanMock1.getVisibility()).thenReturn(IndexVisibility.VISIBLE);
        indexMap.put("cci1", beanMock1);

        GsiMetaManager.GsiIndexMetaBean beanMock2 = mock(GsiMetaManager.GsiIndexMetaBean.class);
        when(beanMock2.getVisibility()).thenReturn(IndexVisibility.INVISIBLE);
        indexMap.put("cci2", beanMock2);

        GsiMetaManager.GsiIndexMetaBean beanMock3 = mock(GsiMetaManager.GsiIndexMetaBean.class);
        when(beanMock3.getVisibility()).thenReturn(IndexVisibility.VISIBLE);
        indexMap.put("cci3", beanMock1);

        GsiMetaManager.GsiIndexMetaBean beanMock4 = mock(GsiMetaManager.GsiIndexMetaBean.class);
        when(beanMock4.getVisibility()).thenReturn(IndexVisibility.VISIBLE);
        indexMap.put("cci4", beanMock1);

        TableMeta mockTableMeta = mock(TableMeta.class);
        when(mockTableMeta.getColumnarIndexPublished()).thenReturn(indexMap);
        TableMeta mockTableMetaCci1 = mock(TableMeta.class);
        when(mockTableMetaCci1.getId()).thenReturn(3L);
        when(mockTableMetaCci1.isColumnar()).thenReturn(true);
        when(mockTableMetaCci1.isColumnarArchive()).thenReturn(true);
        // 模拟部分索引是归档
        TableMeta mockTableMetaCci2 = mock(TableMeta.class);
        when(mockTableMetaCci2.getId()).thenReturn(5L);
        when(mockTableMetaCci2.isColumnar()).thenReturn(true);
        when(mockTableMetaCci2.isColumnarArchive()).thenReturn(true);
        TableMeta mockTableMetaCci4 = mock(TableMeta.class);
        when(mockTableMetaCci4.getId()).thenReturn(4L);
        when(mockTableMetaCci4.isColumnar()).thenReturn(true);
        when(mockTableMetaCci4.isColumnarArchive()).thenReturn(true);
        SchemaManager mockSchemaManager = mock(SchemaManager.class);
        when(mockSchemaManager.getTableWithNull(table)).thenReturn(mockTableMeta);
        when(mockSchemaManager.getTableWithNull("cci1")).thenReturn(mockTableMetaCci1);
        when(mockSchemaManager.getTableWithNull("cci2")).thenReturn(mockTableMetaCci2);
        when(mockSchemaManager.getTableWithNull("cci4")).thenReturn(mockTableMetaCci4);

        OptimizerContext mockOptimizerContext = mock(OptimizerContext.class);
        when(mockOptimizerContext.getLatestSchemaManager()).thenReturn(mockSchemaManager);

        try (MockedStatic<OptimizerContext> optimizerContextMockedStatic = Mockito.mockStatic(OptimizerContext.class);
            MockedStatic<CBOUtil> cboUtilMockedStatic = Mockito.mockStatic(CBOUtil.class);
            MockedStatic<ConfigDataMode> configDataModeMockedStatic = Mockito.mockStatic(ConfigDataMode.class)) {
            optimizerContextMockedStatic.when(() -> OptimizerContext.getContext(schema))
                .thenReturn(mockOptimizerContext);
            cboUtilMockedStatic.when(
                    () -> CBOUtil.getColumnarIndexNamesWithArchiveInCol(anyString(), anyString(), any()))
                .thenCallRealMethod();
            cboUtilMockedStatic.when(() -> CBOUtil.getColumnarIndexNames(anyString(), anyString(), any()))
                .thenCallRealMethod();
            cboUtilMockedStatic.when(() -> CBOUtil.getColumnarIndexNamesWithoutArchive(anyString(), anyString(), any()))
                .thenCallRealMethod();
            configDataModeMockedStatic.when(ConfigDataMode::isColumnarMode).thenReturn(false);

            // Act
            List<String> result = CBOUtil.getColumnarIndexNamesWithArchiveInCol(table, schema, null);
            assertEquals(0, result.size());

            ExecutionContext executionContext = mock(ExecutionContext.class);
            when(executionContext.getSchemaManager(schema)).thenReturn(mockSchemaManager);
            result = CBOUtil.getColumnarIndexNamesWithArchiveInCol(table, schema, executionContext);
            assertEquals(0, result.size());

            result = CBOUtil.getColumnarIndexNames(table, schema, executionContext);
            assertEquals(2, result.size());
            assertEquals("cci1", result.get(0));
            assertEquals("cci4", result.get(1));

            configDataModeMockedStatic.when(ConfigDataMode::isColumnarMode).thenReturn(true);
            result = CBOUtil.getColumnarIndexNamesWithoutArchive(table, schema, executionContext);
            assertEquals(0, result.size());

            result = CBOUtil.getColumnarIndexNamesWithArchiveInCol(table, schema, executionContext);
            assertEquals(2, result.size());
            assertEquals("cci1", result.get(0));
            assertEquals("cci4", result.get(1));
        }
    }
}
