package com.alibaba.polardbx.server.handler.pl;

import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.druid.sql.ast.statement.SQLCallStatement;
import com.alibaba.polardbx.druid.sql.parser.SQLParserFeature;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.gms.ha.impl.StorageHaManager;
import com.alibaba.polardbx.gms.ha.impl.StorageInstHaContext;
import com.alibaba.polardbx.gms.node.CCLDetectDnActor;
import com.alibaba.polardbx.gms.node.CCLDetectManager;
import com.alibaba.polardbx.gms.node.LeaderStatusBridge;
import com.alibaba.polardbx.gms.topology.DbTopologyManager;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.parse.FastsqlUtils;
import com.alibaba.polardbx.server.ServerConnection;
import com.alibaba.polardbx.server.handler.pl.inner.ClearAllDnCclProcedure;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Unit test for ClearAllDnCclProcedure
 *
 * @author liugaoji
 */
@RunWith(MockitoJUnitRunner.class)
public class ClearAllDnCclProcedureTest {

    @InjectMocks
    private ClearAllDnCclProcedure target;

    @Mock
    private ServerConnection mockConnection;

    @Mock
    private StorageHaManager mockStorageHaManager;

    @Mock
    private CCLDetectManager mockCclDetectManager;

    @Mock
    private StorageInstHaContext mockStorageInstHaContext;

    @Mock
    private Connection mockDbConnection;

    @Mock
    private LeaderStatusBridge mockLeaderStatusBridge;

    private SQLCallStatement statement;
    private ArrayResultCursor cursor;

    @Before
    public void setUp() {
        // Parse SQL statement
        statement = (SQLCallStatement) FastsqlUtils.parseSql("call polardbx.clear_all_dn_ccl_rules()",
            SQLParserFeature.IgnoreNameQuotes).get(0);

        // Create cursor
        cursor = new ArrayResultCursor("clear_all_dn_ccl_rules");

        // Setup mock storage context
        Map<String, StorageInstHaContext> storageStatusMap = new HashMap<>();
        storageStatusMap.put("storage1", mockStorageInstHaContext);
    }

    @Test
    public void testClearAllDnCclRulesSuccess() {
        try (MockedStatic<ConfigDataMode> mockedConfigDataMode = Mockito.mockStatic(ConfigDataMode.class);
            MockedStatic<LeaderStatusBridge> mockedLeaderStatusBridge = Mockito.mockStatic(LeaderStatusBridge.class);
            MockedStatic<CCLDetectManager> mockedCclDetectManager = Mockito.mockStatic(CCLDetectManager.class);
            MockedStatic<CCLDetectDnActor> mockedCclDetectDnActor = Mockito.mockStatic(CCLDetectDnActor.class);
            MockedStatic<DbTopologyManager> mockedDbTopologyManager = Mockito.mockStatic(DbTopologyManager.class)) {

            // Mock master mode and leadership checks
            mockedConfigDataMode.when(ConfigDataMode::isMasterMode).thenReturn(true);
            mockedLeaderStatusBridge.when(LeaderStatusBridge::getInstance).thenReturn(mockLeaderStatusBridge);
            Mockito.when(mockLeaderStatusBridge.hasLeadership()).thenReturn(true);

            // Mock CCLDetectManager.getDnMasterIterator()
            Iterator<StorageInstHaContext> mockIterator = Mockito.mock(Iterator.class);
            Mockito.when(mockIterator.hasNext()).thenReturn(true, false);
            Mockito.when(mockIterator.next()).thenReturn(mockStorageInstHaContext);
            mockedCclDetectManager.when(CCLDetectManager::getDnMasterIterator).thenReturn(mockIterator);

            // Mock static method calls
            mockedCclDetectManager.when(CCLDetectManager::getInstance).thenReturn(mockCclDetectManager);
            mockedDbTopologyManager.when(() -> DbTopologyManager.getConnectionForStorage(mockStorageInstHaContext))
                .thenReturn(mockDbConnection);

            // Mock successful operations
            Mockito.doNothing().when(mockCclDetectManager).clearGlobalGenKey();
            mockedCclDetectDnActor.when(() -> CCLDetectDnActor.deleteAllDnCCL(Mockito.any(Supplier.class)))
                .thenReturn(true);

            // Execute the procedure
            target.execute(mockConnection, statement, cursor);

            // Verify the result
            Assert.assertEquals(1, cursor.getRows().size());
            Row row = cursor.getRows().get(0);
            Assert.assertEquals("Success", row.getObject(0));

            // Verify interactions
            Mockito.verify(mockCclDetectManager).clearGlobalGenKey();
        }
    }

    @Test
    public void testClearAllDnCclRulesFailureOnClearGlobalGenKey() {
        try (MockedStatic<ConfigDataMode> mockedConfigDataMode = Mockito.mockStatic(ConfigDataMode.class);
            MockedStatic<LeaderStatusBridge> mockedLeaderStatusBridge = Mockito.mockStatic(LeaderStatusBridge.class);
            MockedStatic<CCLDetectManager> mockedCclDetectManager = Mockito.mockStatic(CCLDetectManager.class);
            MockedStatic<CCLDetectDnActor> mockedCclDetectDnActor = Mockito.mockStatic(CCLDetectDnActor.class);
            MockedStatic<DbTopologyManager> mockedDbTopologyManager = Mockito.mockStatic(DbTopologyManager.class)) {

            // Mock master mode and leadership checks
            mockedConfigDataMode.when(ConfigDataMode::isMasterMode).thenReturn(true);
            mockedLeaderStatusBridge.when(LeaderStatusBridge::getInstance).thenReturn(mockLeaderStatusBridge);
            Mockito.when(mockLeaderStatusBridge.hasLeadership()).thenReturn(true);

            // Mock CCLDetectManager.getDnMasterIterator()
            Iterator<StorageInstHaContext> mockIterator = Mockito.mock(Iterator.class);
            Mockito.when(mockIterator.hasNext()).thenReturn(true, false);
            Mockito.when(mockIterator.next()).thenReturn(mockStorageInstHaContext);
            mockedCclDetectManager.when(CCLDetectManager::getDnMasterIterator).thenReturn(mockIterator);

            // Mock static method calls
            mockedCclDetectManager.when(CCLDetectManager::getInstance).thenReturn(mockCclDetectManager);
            mockedDbTopologyManager.when(() -> DbTopologyManager.getConnectionForStorage(mockStorageInstHaContext))
                .thenReturn(mockDbConnection);

            // Mock exception on clearGlobalGenKey
            Mockito.doThrow(new RuntimeException("Clear global key failed"))
                .when(mockCclDetectManager).clearGlobalGenKey();
            mockedCclDetectDnActor.when(() -> CCLDetectDnActor.deleteAllDnCCL(Mockito.any(Supplier.class)))
                .thenReturn(true);

            // Execute the procedure
            target.execute(mockConnection, statement, cursor);

            // Verify the result shows failure
            Assert.assertEquals(1, cursor.getRows().size());
            Row row = cursor.getRows().get(0);
            String result = (String) row.getObject(0);
            Assert.assertTrue("Result should start with 'Fail, Please Check Log'",
                result.startsWith("Fail, Please Check Log"));
            Assert.assertTrue("Result should contain error details",
                result.contains("Details:"));

            // Verify interactions
            Mockito.verify(mockCclDetectManager).clearGlobalGenKey();
        }
    }

    @Test
    public void testClearAllDnCclRulesFailureOnDeleteDnCcl() {
        try (MockedStatic<ConfigDataMode> mockedConfigDataMode = Mockito.mockStatic(ConfigDataMode.class);
            MockedStatic<LeaderStatusBridge> mockedLeaderStatusBridge = Mockito.mockStatic(LeaderStatusBridge.class);
            MockedStatic<CCLDetectManager> mockedCclDetectManager = Mockito.mockStatic(CCLDetectManager.class);
            MockedStatic<CCLDetectDnActor> mockedCclDetectDnActor = Mockito.mockStatic(CCLDetectDnActor.class);
            MockedStatic<DbTopologyManager> mockedDbTopologyManager = Mockito.mockStatic(DbTopologyManager.class)) {

            // Mock master mode and leadership checks
            mockedConfigDataMode.when(ConfigDataMode::isMasterMode).thenReturn(true);
            mockedLeaderStatusBridge.when(LeaderStatusBridge::getInstance).thenReturn(mockLeaderStatusBridge);
            Mockito.when(mockLeaderStatusBridge.hasLeadership()).thenReturn(true);

            // Mock CCLDetectManager.getDnMasterIterator()
            Iterator<StorageInstHaContext> mockIterator = Mockito.mock(Iterator.class);
            Mockito.when(mockIterator.hasNext()).thenReturn(true, false);
            Mockito.when(mockIterator.next()).thenReturn(mockStorageInstHaContext);
            mockedCclDetectManager.when(CCLDetectManager::getDnMasterIterator).thenReturn(mockIterator);

            // Mock static method calls
            mockedCclDetectManager.when(CCLDetectManager::getInstance).thenReturn(mockCclDetectManager);
            mockedDbTopologyManager.when(() -> DbTopologyManager.getConnectionForStorage(mockStorageInstHaContext))
                .thenReturn(mockDbConnection);

            // Mock successful clearGlobalGenKey but failed deleteAllDnCCL
            Mockito.doNothing().when(mockCclDetectManager).clearGlobalGenKey();
            mockedCclDetectDnActor.when(() -> CCLDetectDnActor.deleteAllDnCCL(Mockito.any(Supplier.class)))
                .thenReturn(false);

            // Execute the procedure
            target.execute(mockConnection, statement, cursor);

            // Verify the result shows failure
            Assert.assertEquals(1, cursor.getRows().size());
            Row row = cursor.getRows().get(0);
            String result = (String) row.getObject(0);
            Assert.assertTrue("Result should start with 'Fail, Please Check Log'",
                result.startsWith("Fail, Please Check Log"));
            Assert.assertTrue("Result should contain error details",
                result.contains("Details:"));

            // Verify interactions
            Mockito.verify(mockCclDetectManager).clearGlobalGenKey();
        }
    }

    @Test
    public void testClearAllDnCclRulesWithMultipleStorageInstances() {
        try (MockedStatic<ConfigDataMode> mockedConfigDataMode = Mockito.mockStatic(ConfigDataMode.class);
            MockedStatic<LeaderStatusBridge> mockedLeaderStatusBridge = Mockito.mockStatic(LeaderStatusBridge.class);
            MockedStatic<CCLDetectManager> mockedCclDetectManager = Mockito.mockStatic(CCLDetectManager.class);
            MockedStatic<CCLDetectDnActor> mockedCclDetectDnActor = Mockito.mockStatic(CCLDetectDnActor.class);
            MockedStatic<DbTopologyManager> mockedDbTopologyManager = Mockito.mockStatic(DbTopologyManager.class)) {

            // Mock master mode and leadership checks
            mockedConfigDataMode.when(ConfigDataMode::isMasterMode).thenReturn(true);
            mockedLeaderStatusBridge.when(LeaderStatusBridge::getInstance).thenReturn(mockLeaderStatusBridge);
            Mockito.when(mockLeaderStatusBridge.hasLeadership()).thenReturn(true);

            // Setup multiple storage instances
            StorageInstHaContext mockStorageInstHaContext2 = Mockito.mock(StorageInstHaContext.class);

            // Mock CCLDetectManager.getDnMasterIterator() with multiple instances
            Iterator<StorageInstHaContext> mockIterator = Mockito.mock(Iterator.class);
            Mockito.when(mockIterator.hasNext()).thenReturn(true, true, false);
            Mockito.when(mockIterator.next()).thenReturn(mockStorageInstHaContext, mockStorageInstHaContext2);
            mockedCclDetectManager.when(CCLDetectManager::getDnMasterIterator).thenReturn(mockIterator);

            // Mock static method calls
            mockedCclDetectManager.when(CCLDetectManager::getInstance).thenReturn(mockCclDetectManager);
            mockedDbTopologyManager.when(
                    () -> DbTopologyManager.getConnectionForStorage(Mockito.any(StorageInstHaContext.class)))
                .thenReturn(mockDbConnection);

            // Mock successful operations
            Mockito.doNothing().when(mockCclDetectManager).clearGlobalGenKey();
            mockedCclDetectDnActor.when(() -> CCLDetectDnActor.deleteAllDnCCL(Mockito.any(Supplier.class)))
                .thenReturn(true);

            // Execute the procedure
            target.execute(mockConnection, statement, cursor);

            // Verify the result
            Assert.assertEquals(1, cursor.getRows().size());
            Row row = cursor.getRows().get(0);
            Assert.assertEquals("Success", row.getObject(0));

            // Verify interactions
            Mockito.verify(mockCclDetectManager).clearGlobalGenKey();
        }
    }

    @Test
    public void testClearAllDnCclRulesWithEmptyStorageMap() {
        try (MockedStatic<ConfigDataMode> mockedConfigDataMode = Mockito.mockStatic(ConfigDataMode.class);
            MockedStatic<LeaderStatusBridge> mockedLeaderStatusBridge = Mockito.mockStatic(LeaderStatusBridge.class);
            MockedStatic<CCLDetectManager> mockedCclDetectManager = Mockito.mockStatic(CCLDetectManager.class)) {

            // Mock master mode and leadership checks
            mockedConfigDataMode.when(ConfigDataMode::isMasterMode).thenReturn(true);
            mockedLeaderStatusBridge.when(LeaderStatusBridge::getInstance).thenReturn(mockLeaderStatusBridge);
            Mockito.when(mockLeaderStatusBridge.hasLeadership()).thenReturn(true);

            // Mock CCLDetectManager.getDnMasterIterator() with empty iterator
            Iterator<StorageInstHaContext> mockIterator = Mockito.mock(Iterator.class);
            Mockito.when(mockIterator.hasNext()).thenReturn(false);
            mockedCclDetectManager.when(CCLDetectManager::getDnMasterIterator).thenReturn(mockIterator);

            // Mock static method calls
            mockedCclDetectManager.when(CCLDetectManager::getInstance).thenReturn(mockCclDetectManager);

            // Mock successful clearGlobalGenKey
            Mockito.doNothing().when(mockCclDetectManager).clearGlobalGenKey();

            // Execute the procedure
            target.execute(mockConnection, statement, cursor);

            // Verify the result - should still be success even with empty storage map
            Assert.assertEquals(1, cursor.getRows().size());
            Row row = cursor.getRows().get(0);
            Assert.assertEquals("Success", row.getObject(0));

            // Verify interactions
            Mockito.verify(mockCclDetectManager).clearGlobalGenKey();
        }
    }
}