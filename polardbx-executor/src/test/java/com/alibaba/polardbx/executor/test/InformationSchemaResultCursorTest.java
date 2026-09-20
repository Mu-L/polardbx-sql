package com.alibaba.polardbx.executor.test;

import com.alibaba.polardbx.common.utils.Assert;
import com.alibaba.polardbx.executor.PlanExecutor;
import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.cursor.impl.InformationSchemaResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaColumnarWarmupHandler;
import com.alibaba.polardbx.executor.handler.subhandler.InformationSchemaTablesHandler;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.view.InformationSchemaTables;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import com.alibaba.polardbx.optimizer.view.VirtualViewType;
import org.apache.calcite.rel.type.RelDataType;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;

import static org.mockito.Mockito.when;

public class InformationSchemaResultCursorTest {
    ExecutionContext executionContext;
    private ArrayResultCursor resultCursor;

    @Before
    public void setupCursor() {
        resultCursor = new ArrayResultCursor("TEST");
        resultCursor.addColumn("id", DataTypes.LongType);
        resultCursor.initMeta();
    }


    @Before
    public void setupExecutionContext() {
        executionContext = new ExecutionContext();
        executionContext.setSchemaName("test_db");
    }

    @Test
    public void test() {
        VirtualViewHandler handler =
            new VirtualViewHandler();

        InformationSchemaTables mockedVirtualView = Mockito.mock(InformationSchemaTables.class);
        when(mockedVirtualView.getVirtualViewType()).thenReturn(VirtualViewType.INFORMATION_SCHEMA_TABLES);
        RelDataType mockedRelDataType = Mockito.mock(RelDataType.class);
        when(mockedVirtualView.getRowType()).thenReturn(mockedRelDataType);
        when(mockedRelDataType.getFieldList()).thenReturn(new ArrayList<>());

        Cursor cursor = handler.handle(mockedVirtualView, executionContext);
        Assert.assertTrue(cursor instanceof InformationSchemaResultCursor);


    }
}
