package com.alibaba.polardbx.executor.handler.ddl;

import com.alibaba.polardbx.common.properties.ConnectionParams;
import com.alibaba.polardbx.executor.common.RecycleBin;
import com.alibaba.polardbx.executor.common.RecycleBinManager;
import com.alibaba.polardbx.executor.ddl.newengine.job.DdlJob;
import com.alibaba.polardbx.executor.spi.IRepository;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.ddl.BaseDdlOperation;
import com.alibaba.polardbx.optimizer.utils.PlannerUtils;
import org.apache.calcite.rel.RelNode;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.MockitoAnnotations;

import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * Unit tests for LogicalCommonDdlHandler
 */
public class LogicalCommonDdlHandlerTest {

    @Mock
    private IRepository repo;

    @Mock
    private RelNode logicalPlan;

    @Mock
    private ExecutionContext executionContext;

    // Concrete implementation for testing abstract class
    private static class TestableLogicalCommonDdlHandler extends LogicalCommonDdlHandler {
        public TestableLogicalCommonDdlHandler(IRepository repo) {
            super(repo);
        }

        @Override
        protected DdlJob buildDdlJob(BaseDdlOperation logicalDdlPlan, ExecutionContext executionContext) {
            return mock(DdlJob.class);
        }
    }

    @Before
    public void setUp() {
        MockitoAnnotations.openMocks(this);
    }

    @Test
    public void testIsAvailableForRecycleBin_ReturnsTrue() {
        // Arrange
        String schemaName = "test_schema";
        String tableName = "test_table";

        // Mock RecycleBinManager
        try (MockedStatic<RecycleBinManager> mockedRecycleBinManager = mockStatic(RecycleBinManager.class);
            MockedStatic<RecycleBin> mockedRecycleBin = mockStatic(RecycleBin.class);
            MockedStatic<PlannerUtils> mockedPlannerUtils = mockStatic(PlannerUtils.class)) {

            RecycleBin recycleBin = mock(RecycleBin.class);
            RecycleBinManager manager = mock(RecycleBinManager.class);
            com.alibaba.polardbx.common.properties.ParamManager paramManager =
                mock(com.alibaba.polardbx.common.properties.ParamManager.class);

            mockedRecycleBinManager.when(RecycleBinManager::getInstance).thenReturn(manager);
            when(manager.getByAppName(anyString())).thenReturn(recycleBin);
            when(recycleBin.hasForeignConstraint(anyString(), anyString())).thenReturn(false);

            mockedRecycleBin.when(() -> RecycleBin.isRecyclebinTable(anyString())).thenReturn(false);

            when(executionContext.getParamManager()).thenReturn(paramManager);
            when(executionContext.getAppName()).thenReturn("test_app");
            when(executionContext.getSchemaName()).thenReturn(schemaName);

            when(paramManager.getBoolean(ConnectionParams.ENABLE_RECYCLEBIN)).thenReturn(true);

            mockedPlannerUtils.when(() -> PlannerUtils.checkIfUseSchemaUsePhyDbConfigs(anyString())).thenReturn(false);

            // Act
            boolean result = LogicalCommonDdlHandler.isAvailableForRecycleBin(schemaName, tableName, executionContext);

            // Assert
            Assert.assertTrue(result);
        }
    }

    @Test
    public void testIsAvailableForRecycleBin_ReturnsFalse_WhenRecycleBinNull() {
        // Arrange
        String schemaName = "test_schema";
        String tableName = "test_table";

        // Mock RecycleBinManager
        try (MockedStatic<RecycleBinManager> mockedRecycleBinManager = mockStatic(RecycleBinManager.class);
            MockedStatic<RecycleBin> mockedRecycleBin = mockStatic(RecycleBin.class)) {

            RecycleBinManager manager = mock(RecycleBinManager.class);
            com.alibaba.polardbx.common.properties.ParamManager paramManager =
                mock(com.alibaba.polardbx.common.properties.ParamManager.class);

            mockedRecycleBinManager.when(RecycleBinManager::getInstance).thenReturn(manager);
            when(manager.getByAppName(anyString())).thenReturn(null); // Return null recycleBin

            mockedRecycleBin.when(() -> RecycleBin.isRecyclebinTable(anyString())).thenReturn(false);

            when(executionContext.getParamManager()).thenReturn(paramManager);
            when(executionContext.getAppName()).thenReturn("test_app");
            when(executionContext.getSchemaName()).thenReturn(schemaName);

            when(paramManager.getBoolean(ConnectionParams.ENABLE_RECYCLEBIN)).thenReturn(true);

            // Act
            boolean result = LogicalCommonDdlHandler.isAvailableForRecycleBin(schemaName, tableName, executionContext);

            // Assert
            Assert.assertFalse(result);
        }
    }

    @Test
    public void testIsAvailableForRecycleBin_ReturnsFalse_WhenRecycleBinTable() {
        // Arrange
        String schemaName = "test_schema";
        String tableName = "test_table";

        // Mock RecycleBinManager
        try (MockedStatic<RecycleBinManager> mockedRecycleBinManager = mockStatic(RecycleBinManager.class);
            MockedStatic<RecycleBin> mockedRecycleBin = mockStatic(RecycleBin.class)) {

            RecycleBin recycleBin = mock(RecycleBin.class);
            RecycleBinManager manager = mock(RecycleBinManager.class);
            com.alibaba.polardbx.common.properties.ParamManager paramManager =
                mock(com.alibaba.polardbx.common.properties.ParamManager.class);

            mockedRecycleBinManager.when(RecycleBinManager::getInstance).thenReturn(manager);
            when(manager.getByAppName(anyString())).thenReturn(recycleBin);
            when(recycleBin.hasForeignConstraint(anyString(), anyString())).thenReturn(false);

            mockedRecycleBin.when(() -> RecycleBin.isRecyclebinTable(anyString()))
                .thenReturn(true); // Return true for recyclebin table

            when(executionContext.getParamManager()).thenReturn(paramManager);
            when(executionContext.getAppName()).thenReturn("test_app");
            when(executionContext.getSchemaName()).thenReturn(schemaName);

            when(paramManager.getBoolean(ConnectionParams.ENABLE_RECYCLEBIN)).thenReturn(true);

            // Act
            boolean result = LogicalCommonDdlHandler.isAvailableForRecycleBin(schemaName, tableName, executionContext);

            // Assert
            Assert.assertFalse(result);
        }
    }

    @Test
    public void testIsAvailableForRecycleBin_ReturnsFalse_WhenHasForeignConstraint() {
        // Arrange
        String schemaName = "test_schema";
        String tableName = "test_table";

        // Mock RecycleBinManager
        try (MockedStatic<RecycleBinManager> mockedRecycleBinManager = mockStatic(RecycleBinManager.class);
            MockedStatic<RecycleBin> mockedRecycleBin = mockStatic(RecycleBin.class)) {

            RecycleBin recycleBin = mock(RecycleBin.class);
            RecycleBinManager manager = mock(RecycleBinManager.class);
            com.alibaba.polardbx.common.properties.ParamManager paramManager =
                mock(com.alibaba.polardbx.common.properties.ParamManager.class);

            mockedRecycleBinManager.when(RecycleBinManager::getInstance).thenReturn(manager);
            when(manager.getByAppName(anyString())).thenReturn(recycleBin);
            when(recycleBin.hasForeignConstraint(anyString(), anyString())).thenReturn(
                true); // Return true for foreign constraint

            mockedRecycleBin.when(() -> RecycleBin.isRecyclebinTable(anyString())).thenReturn(false);

            when(executionContext.getParamManager()).thenReturn(paramManager);
            when(executionContext.getAppName()).thenReturn("test_app");
            when(executionContext.getSchemaName()).thenReturn(schemaName);

            when(paramManager.getBoolean(ConnectionParams.ENABLE_RECYCLEBIN)).thenReturn(true);

            // Act
            boolean result = LogicalCommonDdlHandler.isAvailableForRecycleBin(schemaName, tableName, executionContext);

            // Assert
            Assert.assertFalse(result);
        }
    }

    @Test
    public void testIsAvailableForRecycleBin_ReturnsFalse_WhenRecycleBinDisabled() {
        // Arrange
        String schemaName = "test_schema";
        String tableName = "test_table";

        // Mock RecycleBinManager
        try (MockedStatic<RecycleBinManager> mockedRecycleBinManager = mockStatic(RecycleBinManager.class);
            MockedStatic<RecycleBin> mockedRecycleBin = mockStatic(RecycleBin.class)) {

            RecycleBin recycleBin = mock(RecycleBin.class);
            RecycleBinManager manager = mock(RecycleBinManager.class);
            com.alibaba.polardbx.common.properties.ParamManager paramManager =
                mock(com.alibaba.polardbx.common.properties.ParamManager.class);

            mockedRecycleBinManager.when(RecycleBinManager::getInstance).thenReturn(manager);
            when(manager.getByAppName(anyString())).thenReturn(recycleBin);
            when(recycleBin.hasForeignConstraint(anyString(), anyString())).thenReturn(false);

            mockedRecycleBin.when(() -> RecycleBin.isRecyclebinTable(anyString())).thenReturn(false);

            when(executionContext.getParamManager()).thenReturn(paramManager);
            when(executionContext.getAppName()).thenReturn("test_app");
            when(executionContext.getSchemaName()).thenReturn(schemaName);

            when(paramManager.getBoolean(ConnectionParams.ENABLE_RECYCLEBIN)).thenReturn(
                false); // Return false for recyclebin enabled

            // Act
            boolean result = LogicalCommonDdlHandler.isAvailableForRecycleBin(schemaName, tableName, executionContext);

            // Assert
            Assert.assertFalse(result);
        }
    }

    @Test
    public void testIsAvailableForRecycleBin_ReturnsFalse_WhenSchemaIsNull() {
        // Arrange
        String schemaName = null;
        String tableName = "test_table";

        // Mock RecycleBinManager
        try (MockedStatic<RecycleBinManager> mockedRecycleBinManager = mockStatic(RecycleBinManager.class);
            MockedStatic<RecycleBin> mockedRecycleBin = mockStatic(RecycleBin.class);
            MockedStatic<PlannerUtils> mockedPlannerUtils = mockStatic(PlannerUtils.class)) {

            RecycleBin recycleBin = mock(RecycleBin.class);
            RecycleBinManager manager = mock(RecycleBinManager.class);
            com.alibaba.polardbx.common.properties.ParamManager paramManager =
                mock(com.alibaba.polardbx.common.properties.ParamManager.class);

            mockedRecycleBinManager.when(RecycleBinManager::getInstance).thenReturn(manager);
            when(manager.getByAppName(anyString())).thenReturn(recycleBin);
            when(recycleBin.hasForeignConstraint(anyString(), anyString())).thenReturn(false);

            mockedRecycleBin.when(() -> RecycleBin.isRecyclebinTable(anyString())).thenReturn(false);

            when(executionContext.getParamManager()).thenReturn(paramManager);
            when(executionContext.getAppName()).thenReturn("test_app");
            when(executionContext.getSchemaName()).thenReturn(null);

            when(paramManager.getBoolean(ConnectionParams.ENABLE_RECYCLEBIN)).thenReturn(true);

            mockedPlannerUtils.when(() -> PlannerUtils.checkIfUseSchemaUsePhyDbConfigs(anyString())).thenReturn(false);

            // Act
            boolean result = LogicalCommonDdlHandler.isAvailableForRecycleBin(schemaName, tableName, executionContext);

            // Assert
            Assert.assertFalse(result);
        }
    }

    @Test
    public void testIsAvailableForRecycleBin_ReturnsFalse_WhenUseSchemaUsePhyDbConfigs() {
        // Arrange
        String schemaName = "test_schema";
        String tableName = "test_table";

        // Mock RecycleBinManager
        try (MockedStatic<RecycleBinManager> mockedRecycleBinManager = mockStatic(RecycleBinManager.class);
            MockedStatic<RecycleBin> mockedRecycleBin = mockStatic(RecycleBin.class);
            MockedStatic<PlannerUtils> mockedPlannerUtils = mockStatic(PlannerUtils.class)) {

            RecycleBin recycleBin = mock(RecycleBin.class);
            RecycleBinManager manager = mock(RecycleBinManager.class);
            com.alibaba.polardbx.common.properties.ParamManager paramManager =
                mock(com.alibaba.polardbx.common.properties.ParamManager.class);

            mockedRecycleBinManager.when(RecycleBinManager::getInstance).thenReturn(manager);
            when(manager.getByAppName(anyString())).thenReturn(recycleBin);
            when(recycleBin.hasForeignConstraint(anyString(), anyString())).thenReturn(false);

            mockedRecycleBin.when(() -> RecycleBin.isRecyclebinTable(anyString())).thenReturn(false);

            when(executionContext.getParamManager()).thenReturn(paramManager);
            when(executionContext.getAppName()).thenReturn("test_app");
            when(executionContext.getSchemaName()).thenReturn(schemaName);

            when(paramManager.getBoolean(ConnectionParams.ENABLE_RECYCLEBIN)).thenReturn(true);

            mockedPlannerUtils.when(() -> PlannerUtils.checkIfUseSchemaUsePhyDbConfigs(anyString()))
                .thenReturn(true); // Return true for use schema use phy db configs

            // Act
            boolean result = LogicalCommonDdlHandler.isAvailableForRecycleBin(schemaName, tableName, executionContext);

            // Assert
            Assert.assertFalse(result);
        }
    }

}