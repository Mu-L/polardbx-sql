package com.alibaba.polardbx.executor.handler;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.gms.metadb.MetaDbDataSource;
import com.alibaba.polardbx.gms.metadb.table.IndexesInfoSchemaRecord;
import com.alibaba.polardbx.gms.util.MetaDbUtil;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.dal.BaseDalOperation;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlShowIndex;
import org.junit.Test;
import org.mockito.MockedStatic;

import javax.sql.DataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

public class LogicalShowIndexHandlerTest {
    @Test
    public void testShowSingleTableStatusWithColumnarMode()
        throws IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        LogicalShowIndexHandler logicalShowIndexHandler =
            spy(new LogicalShowIndexHandler(mock(IRepository.class)));
        //input parameter
        BaseDalOperation logicalPlan = mock(BaseDalOperation.class);
        ExecutionContext ec = mock(ExecutionContext.class);
        String schemaName = "test";
        String tableName = "test_table";
        SqlShowIndex sqlShowIndex = mock(SqlShowIndex.class);
        when(logicalPlan.getNativeSqlNode()).thenReturn(sqlShowIndex);
        SqlNode sqlNode = mock(SqlNode.class);
        when(sqlShowIndex.getTableName()).thenReturn(sqlNode);
        try (MockedStatic<RelUtils> mockedRelUtils = mockStatic(RelUtils.class)) {
            mockedRelUtils.when(() -> RelUtils.lastStringValue(any())).thenReturn(tableName);
            //mock DataSourcek
            DataSource mockDataSource = mock(DataSource.class);
            MetaDbDataSource metaDbDataSource = mock(MetaDbDataSource.class);
            try (MockedStatic<MetaDbDataSource> mockedMetaDBDataSource = mockStatic(MetaDbDataSource.class)) {
                mockedMetaDBDataSource.when(() -> MetaDbDataSource.getInstance()).thenReturn(metaDbDataSource);
                when(metaDbDataSource.getDataSource()).thenReturn(mockDataSource);
                //mock Table MetaDB Utils
                try (MockedStatic<MetaDbUtil> mockedMetaDbUtil = mockStatic(MetaDbUtil.class)) {
                    List<IndexesInfoSchemaRecord> tables = new ArrayList<>();
                    IndexesInfoSchemaRecord ordinaryIndex = new IndexesInfoSchemaRecord();
                    ordinaryIndex.tableName = tableName;
                    ordinaryIndex.indexName = "idx_normal";
                    ordinaryIndex.indexType = "BTREE";
                    ordinaryIndex.comment = "ordinary-comment";
                    ordinaryIndex.indexComment = "ordinary-index-comment";
                    tables.add(ordinaryIndex);
                    IndexesInfoSchemaRecord vectorIndex = new IndexesInfoSchemaRecord();
                    vectorIndex.tableName = tableName;
                    vectorIndex.indexName = "idx_vector";
                    vectorIndex.indexType = "VECTOR";
                    vectorIndex.comment = "";
                    vectorIndex.indexComment = "M=6, DISTANCE=EUCLIDEAN, EF_CONSTRUCTION=40, DIM=3";
                    tables.add(vectorIndex);
                    mockedMetaDbUtil.when(() -> MetaDbUtil.query(anyString(), any(), any(), any()))
                        .thenReturn(tables);
                    //verify
                    Method method = logicalShowIndexHandler.getClass()
                        .getDeclaredMethod("handleForColumnarMode", RelNode.class, ExecutionContext.class,
                            String.class);
                    method.setAccessible(true);
                    Cursor result = (Cursor) method.invoke(logicalShowIndexHandler, logicalPlan, ec, schemaName);
                    Assert.assertTrue(
                        result instanceof ArrayResultCursor && ((ArrayResultCursor) result).getRows().size() == 2);
                    ArrayResultCursor resultCursor = (ArrayResultCursor) result;
                    Object[] ordinaryRow = resultCursor.getRows().get(0).getValues().toArray();
                    org.junit.Assert.assertEquals("ordinary-comment", ordinaryRow[11]);
                    org.junit.Assert.assertEquals("ordinary-index-comment", ordinaryRow[12]);
                    Object[] vectorRow = resultCursor.getRows().get(1).getValues().toArray();
                    org.junit.Assert.assertEquals("", vectorRow[11]);
                    org.junit.Assert.assertEquals(vectorIndex.indexComment, vectorRow[12]);
                }
            }
        }
    }

}
