package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.executor.cursor.Cursor;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.handler.VirtualViewHandler;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.metadb.ccl.DnCclRecord;
import com.alibaba.polardbx.gms.node.CCLDetectDnActor;
import com.alibaba.polardbx.gms.node.CCLDetectManager;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.view.InformationSchemaDnCcl;
import com.alibaba.polardbx.optimizer.view.VirtualView;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * Unit test for InformationSchemaDnCclHandler
 *
 * @author liugaoji
 */
@RunWith(MockitoJUnitRunner.class)
public class InformationSchemaDnCclHandlerTest {

    private InformationSchemaDnCclHandler target;

    @Mock
    private VirtualViewHandler mockVirtualViewHandler;

    @Mock
    private InformationSchemaDnCcl mockInformationSchemaDnCcl;

    @Mock
    private VirtualView mockOtherVirtualView;

    @Mock
    private ExecutionContext mockExecutionContext;

    @Mock
    private ArrayResultCursor mockCursor;

    @Mock
    private StorageInstHaContext mockStorageInstHaContext;

    @Before
    public void setUp() {
        // Initialize the target with mocked VirtualViewHandler
        target = new InformationSchemaDnCclHandler(mockVirtualViewHandler);
    }

    @Test
    public void testHandleSuccess() {
        try (MockedStatic<CCLDetectManager> mockedCCLDetectManager = Mockito.mockStatic(CCLDetectManager.class);
            MockedStatic<CCLDetectDnActor> mockedCCLDetectDnActor = Mockito.mockStatic(CCLDetectDnActor.class)) {

            // Mock StorageInstHaContext
            Mockito.when(mockStorageInstHaContext.getInstId()).thenReturn("inst1");
            Mockito.when(mockStorageInstHaContext.getStorageInstId()).thenReturn("storage1");

            // Create mock iterator with the storage instance
            Iterator<StorageInstHaContext> mockIterator = Arrays.asList(mockStorageInstHaContext).iterator();

            // Mock CCLDetectManager.getDnMasterIterator
            mockedCCLDetectManager.when(CCLDetectManager::getDnMasterIterator).thenReturn(mockIterator);

            // Create mock DnCclRecord instances
            DnCclRecord record1 = new DnCclRecord();
            record1.storageId = "storage1";
            record1.inst = "inst1";
            record1.id = "1";
            record1.type = "SELECT";
            record1.schema = "test_schema";
            record1.table = "test_table";
            record1.state = "ACTIVE";
            record1.order = "1";
            record1.concurrencyCount = "10";
            record1.matched = "5";
            record1.running = "3";
            record1.waiting = "2";
            record1.keywords = "test_keyword";

            DnCclRecord record2 = new DnCclRecord();
            record2.storageId = "storage1";
            record2.inst = "inst1";
            record2.id = "2";
            record2.type = "SELECT";
            record2.schema = "test_schema2";
            record2.table = "test_table2";
            record2.state = "ACTIVE";
            record2.order = "2";
            record2.concurrencyCount = "5";
            record2.matched = "3";
            record2.running = "1";
            record2.waiting = "2";
            record2.keywords = "test_keyword2";

            List<DnCclRecord> mockRecords = Arrays.asList(record1, record2);

            // Mock CCLDetectDnActor.getDnCclRules
            mockedCCLDetectDnActor.when(() -> CCLDetectDnActor.getDnCclRules(
                    Mockito.any(), Mockito.eq("inst1"), Mockito.eq("storage1")))
                .thenReturn(mockRecords);

            // Execute the handle method
            Cursor result = target.handle(mockInformationSchemaDnCcl, mockExecutionContext, mockCursor);

            // Verify the result
            Assert.assertNotNull("Result should not be null", result);
            Assert.assertTrue("Result should be ArrayResultCursor", result instanceof ArrayResultCursor);

            // Verify CCLDetectManager.getDnMasterIterator() was called
            mockedCCLDetectManager.verify(CCLDetectManager::getDnMasterIterator);

            // Verify CCLDetectDnActor.getDnCclRules was called
            mockedCCLDetectDnActor.verify(() -> CCLDetectDnActor.getDnCclRules(
                Mockito.any(), Mockito.eq("inst1"), Mockito.eq("storage1")));

            // Verify the result cursor contains expected data
            ArrayResultCursor arrayResult = (ArrayResultCursor) result;
            Assert.assertEquals("Should have 14 columns", 14, arrayResult.getReturnColumns().size());

            // Verify column names
            Assert.assertEquals("STORAGE_INST_ID", arrayResult.getReturnColumns().get(0).getName());
            Assert.assertEquals("INST_ID", arrayResult.getReturnColumns().get(1).getName());
            Assert.assertEquals("ID", arrayResult.getReturnColumns().get(2).getName());
            Assert.assertEquals("TYPE", arrayResult.getReturnColumns().get(3).getName());
            Assert.assertEquals("SCHEMA", arrayResult.getReturnColumns().get(4).getName());
            Assert.assertEquals("TABLE", arrayResult.getReturnColumns().get(5).getName());
            Assert.assertEquals("STATE", arrayResult.getReturnColumns().get(6).getName());
            Assert.assertEquals("ORDER", arrayResult.getReturnColumns().get(7).getName());
            Assert.assertEquals("CONCURRENCY_COUNT", arrayResult.getReturnColumns().get(8).getName());
            Assert.assertEquals("MATCHED", arrayResult.getReturnColumns().get(9).getName());
            Assert.assertEquals("RUNNING", arrayResult.getReturnColumns().get(10).getName());
            Assert.assertEquals("WAITTING", arrayResult.getReturnColumns().get(11).getName());
            Assert.assertEquals("KEYWORDS", arrayResult.getReturnColumns().get(12).getName());
        }
    }

    @Test
    public void testHandleWithException() {
        try (MockedStatic<CCLDetectManager> mockedCCLDetectManager = Mockito.mockStatic(CCLDetectManager.class)) {

            // Mock CCLDetectManager to throw exception
            mockedCCLDetectManager.when(CCLDetectManager::getDnMasterIterator)
                .thenThrow(new RuntimeException("CCLDetectManager initialization failed"));

            // Execute the handle method
            Cursor result = target.handle(mockInformationSchemaDnCcl, mockExecutionContext, mockCursor);

            // Verify the result - should still return a result cursor with empty records due to exception handling
            Assert.assertNotNull("Result should not be null even when exception occurs", result);
            Assert.assertTrue("Result should be ArrayResultCursor", result instanceof ArrayResultCursor);

            // Verify CCLDetectManager.getDnMasterIterator() was called
            mockedCCLDetectManager.verify(CCLDetectManager::getDnMasterIterator);

            // Verify the result cursor is properly initialized with columns but no data
            ArrayResultCursor arrayResult = (ArrayResultCursor) result;
            Assert.assertEquals("Should have 14 columns", 14, arrayResult.getReturnColumns().size());

            // Verify column names are correctly set even in exception case
            Assert.assertEquals("STORAGE_INST_ID", arrayResult.getReturnColumns().get(0).getName());
            Assert.assertEquals("INST_ID", arrayResult.getReturnColumns().get(1).getName());
            Assert.assertEquals("ID", arrayResult.getReturnColumns().get(2).getName());
        }
    }

    @Test
    public void testIsSupport() {
        // Test with supported VirtualView
        boolean supportResult = target.isSupport(mockInformationSchemaDnCcl);
        Assert.assertTrue("Should support InformationSchemaDnCcl", supportResult);

        // Test with unsupported VirtualView
        boolean unsupportResult = target.isSupport(mockOtherVirtualView);
        Assert.assertFalse("Should not support other VirtualView types", unsupportResult);
    }
}