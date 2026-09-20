/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the License);
 * you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.matrix.config;

import com.alibaba.polardbx.common.ddl.Job;
import com.alibaba.polardbx.common.logical.ITPrepareStatement;
import com.alibaba.polardbx.common.utils.InstanceRole;
import com.alibaba.polardbx.config.ConfigDataMode;
import com.alibaba.polardbx.executor.columnar.ExtColumnMappingManager;
import com.alibaba.polardbx.executor.common.ExecutorContext;
import com.alibaba.polardbx.executor.ddl.sync.JobRequest;
import com.alibaba.polardbx.executor.utils.SchemaMetaUtil;
import com.alibaba.polardbx.gms.listener.impl.MetaDbConfigManager;
import com.alibaba.polardbx.gms.listener.impl.MetaDbDataIdBuilder;
import com.alibaba.polardbx.gms.metadb.table.TableInfoManager;
import com.alibaba.polardbx.gms.topology.DbGroupInfoManager;
import com.alibaba.polardbx.matrix.jdbc.TConnection;
import com.alibaba.polardbx.matrix.jdbc.TDataSource;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.context.DdlContext;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.planmanager.PlanManager;
import com.alibaba.polardbx.transaction.TransactionManager;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.MockitoJUnitRunner;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class MatrixConfigHolderTest {

    @Mock
    private Connection metaDbConn;

    @InjectMocks
    private SchemaMetaUtil.PolarDbXSchemaMetaCleaner polarDbXSchemaMetaCleaner =
        new SchemaMetaUtil.PolarDbXSchemaMetaCleaner();

    @Before
    public void setUp() {
        // Setup procedures if necessary
    }

    @After
    public void tearDown() {
        // Cleanup resources or reset mocks
//        mockReset(metaDbConn);
    }

    @Test
    public void testClearSchemaMetaInvokesCleanupSchemaMeta() throws Exception {
        // Given
        final String schemaName = "test_schema";
        final long versionId = 123L;

        MetaDbConfigManager metaDbConfigManager = Mockito.mock(MetaDbConfigManager.class);
        ExtColumnMappingManager extColumnMappingManager = Mockito.mock(ExtColumnMappingManager.class);
        doNothing().when(metaDbConfigManager).unregister(anyString(), any());
        try (
            MockedConstruction<TableInfoManager> mockTableInfoManager =
                Mockito.mockConstruction(TableInfoManager.class, (mock, context) -> {
                    when(mock.hasExternalizedColumn(schemaName)).thenReturn(true);
                    when(mock.queryTables(schemaName)).thenReturn(Collections.emptyList());
                });
            MockedStatic<MetaDbDataIdBuilder> mockMetaDbDataIdBuilder = Mockito.mockStatic(MetaDbDataIdBuilder.class);
            MockedStatic<ExtColumnMappingManager> mockExtColumnMappingManager =
                Mockito.mockStatic(ExtColumnMappingManager.class)) {
            try (MockedStatic<MetaDbConfigManager> mockMetaDbConfigManager = Mockito.mockStatic(
                MetaDbConfigManager.class)) {
                mockMetaDbDataIdBuilder.when(() -> MetaDbDataIdBuilder.getTableListDataId(anyString()))
                    .thenReturn("table");
                mockMetaDbConfigManager.when(() -> MetaDbConfigManager.getInstance())
                    .thenAnswer(invocation -> metaDbConfigManager);
                mockExtColumnMappingManager.when(ExtColumnMappingManager::getInstance)
                    .thenReturn(extColumnMappingManager);

                polarDbXSchemaMetaCleaner.clearSchemaMeta(schemaName, metaDbConn, versionId);
                Mockito.verify(extColumnMappingManager).markDropBySchema(metaDbConn, schemaName);
            }
        }
    }

    @Test
    public void testExecutionContextSetTime() throws SQLException {
        MatrixConfigHolder matrixConfigHolder = new MatrixConfigHolder();
        TDataSource dataSource = Mockito.mock(TDataSource.class);
        TConnection tConnection = Mockito.mock(TConnection.class);
        Mockito.when(dataSource.getConnection()).thenReturn(tConnection);
        Mockito.when(tConnection.prepareStatement(anyString())).thenReturn(Mockito.mock(ITPrepareStatement.class));
        matrixConfigHolder.setDataSource(dataSource);
        ExecutorContext executorContext = Mockito.mock(ExecutorContext.class);
        matrixConfigHolder.setExecutorContext(executorContext);
        Mockito.when(executorContext.getTransactionManager()).thenReturn(Mockito.mock(TransactionManager.class));
        ExecutionContext executionContext = new ExecutionContext();
        Mockito.when(tConnection.getExecutionContext()).thenReturn(executionContext);

        // performAsyncDDLJob
        executionContext.setLogicalSqlStartTime(-1);
        executionContext.setLogicalSqlStartTimeInMs(-1);
        Job job = new Job();
        job.setId(100);
        job.setParentId(1);
        job.setDdlStmt("test");
        JobRequest jobRequest = Mockito.mock(JobRequest.class);
        matrixConfigHolder.performAsyncDDLJob(job, "test_schema", jobRequest);
        Assert.assertTrue(executionContext.getLogicalSqlStartTime() > 0);
        Assert.assertTrue(executionContext.getLogicalSqlStartTimeInMs() > 0);

        // restoreDDL
        executionContext.setLogicalSqlStartTime(-1);
        executionContext.setLogicalSqlStartTimeInMs(-1);
        try {
            matrixConfigHolder.restoreDDL("test_schema", 100L);
        } catch (Exception e) {
            // ignore
        }
        Assert.assertTrue(executionContext.getLogicalSqlStartTime() > 0);
        Assert.assertTrue(executionContext.getLogicalSqlStartTimeInMs() > 0);

        // remoteExecuteDdlTask
        executionContext.setLogicalSqlStartTime(-1);
        executionContext.setLogicalSqlStartTimeInMs(-1);
        try {
            matrixConfigHolder.remoteExecuteDdlTask("test_schema", 100L, 100L);
        } catch (Exception e) {
            // ignore
        }
        Assert.assertTrue(executionContext.getLogicalSqlStartTime() > 0);
        Assert.assertTrue(executionContext.getLogicalSqlStartTimeInMs() > 0);

        // executeBackgroundSql
        executionContext.setLogicalSqlStartTime(-1);
        executionContext.setLogicalSqlStartTimeInMs(-1);
        try {
            matrixConfigHolder.executeBackgroundSql("sql", "test_schema", null);
        } catch (Exception e) {
            // ignore
        }
        Assert.assertTrue(executionContext.getLogicalSqlStartTime() > 0);
        Assert.assertTrue(executionContext.getLogicalSqlStartTimeInMs() > 0);

        // submitRebalanceDDL
        executionContext.setLogicalSqlStartTime(-1);
        executionContext.setLogicalSqlStartTimeInMs(-1);
        try {
            matrixConfigHolder.submitRebalanceDDL("test_schema", "sql");
        } catch (Exception e) {
            // ignore
        }
        Assert.assertTrue(executionContext.getLogicalSqlStartTime() > 0);
        Assert.assertTrue(executionContext.getLogicalSqlStartTimeInMs() > 0);

        // executeQuerySql
        executionContext.setLogicalSqlStartTime(-1);
        executionContext.setLogicalSqlStartTimeInMs(-1);
        try {
            matrixConfigHolder.executeQuerySql("sql", "test_schema", null);
        } catch (Exception e) {
            // ignore
        }
        Assert.assertTrue(executionContext.getLogicalSqlStartTime() > 0);
        Assert.assertTrue(executionContext.getLogicalSqlStartTimeInMs() > 0);

        // submitSubDDL
        executionContext.setLogicalSqlStartTime(-1);
        executionContext.setLogicalSqlStartTimeInMs(-1);
        try {
            matrixConfigHolder.submitSubDDL("test_schema", new DdlContext(), 100L, 100L, false, "sql");
        } catch (Exception e) {
            // ignore
        }
        Assert.assertTrue(executionContext.getLogicalSqlStartTime() > 0);
        Assert.assertTrue(executionContext.getLogicalSqlStartTimeInMs() > 0);

    }

    @Test
    public void testInit() {
        // Mock DbGroupInfoManager.getInstance() 返回的对象
        DbGroupInfoManager spyDbGroupInfoManager = Mockito.spy(DbGroupInfoManager.getInstance());

        // 使用 Mockito.mockStatic 来 mock 静态方法 getInstance()
        try (MockedStatic<DbGroupInfoManager> mockedDbGroupInfoManager = Mockito.mockStatic(DbGroupInfoManager.class)) {
            mockedDbGroupInfoManager.when(DbGroupInfoManager::getInstance).thenReturn(spyDbGroupInfoManager);

            doThrow(new RuntimeException("Should not be called")).when(spyDbGroupInfoManager)
                .reloadGroupsOfDb(anyString());

            // 执行你的测试逻辑
            ConfigDataMode.setInstanceRole(InstanceRole.FAST_MOCK);

            ExecutorContext executorContext = Mockito.mock(ExecutorContext.class);
            TDataSource dataSource = Mockito.mock(TDataSource.class);

            when(dataSource.getConnectionProperties()).thenReturn(new java.util.HashMap<>());

            MatrixConfigHolder matrixConfigHolder = new MatrixConfigHolder();
            matrixConfigHolder.setAppName("test_app");
            matrixConfigHolder.setUnitName("test_unit");
            matrixConfigHolder.setSchemaName("test_schema");
            matrixConfigHolder.setExecutorContext(executorContext);
            matrixConfigHolder.setDataSource(dataSource);

            // 验证init()调用会抛出异常
            try (MockedStatic<PlanManager> mockedStatic = Mockito.mockStatic(PlanManager.class)) {
                mockedStatic.when(PlanManager::getInstance).thenReturn(Mockito.mock(PlanManager.class));

                matrixConfigHolder.init();
                Assert.fail("Expected exception was not thrown");
            } catch (Exception e) {
                // 验证确实抛出了异常
                Assert.assertNotNull("Exception should not be null", e);
                // 可以进一步验证异常类型或消息内容
                Assert.assertTrue("Exception should be of type RuntimeException",
                    e instanceof RuntimeException);
                Assert.assertEquals("Exception message should match", "Should not be called", e.getMessage());
            }

            OptimizerContext oc = matrixConfigHolder.getOptimizerContext();
            Assert.assertNotNull(oc);
            Assert.assertFalse(oc.isFinishInit());

            doNothing().when(spyDbGroupInfoManager).reloadGroupsOfDb(anyString());

            matrixConfigHolder.init();

            oc = matrixConfigHolder.getOptimizerContext();
            Assert.assertNotNull(oc);
            Assert.assertTrue(oc.isFinishInit());
        } finally {
            ConfigDataMode.setInstanceRole(InstanceRole.MASTER);
        }
    }
}
