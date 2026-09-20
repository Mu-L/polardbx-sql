/*
 * Copyright [1999-2024] Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.polardbx.repo.mysql.handler;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalDynamicValues;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsertIgnore;
import com.alibaba.polardbx.optimizer.core.rel.UkCheckEntry;
import com.alibaba.polardbx.optimizer.utils.RelUtils;
import com.google.common.collect.ImmutableList;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexNode;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class LogicalInsertIgnoreHandlerLocalUkTest {

    private static final String SCHEMA_NAME = "test_schema";
    private static final String PRIMARY_TABLE = "job_runs";
    private static final String GSI_TABLE = "g_i_job_time";
    private static final List<String> ID_PK = Collections.singletonList("id");
    private static final List<String> RUN_ID_UK = Collections.singletonList("run_id");

    @Test
    public void testSingleRowCandidateAccepted() {
        final TestLogicalInsertIgnoreHandler handler = new TestLogicalInsertIgnoreHandler(true);
        final TestContext context = testContext(true, false, false);

        Assert.assertTrue(useCandidate(handler, context, primaryCandidate()));
    }

    @Test
    public void testSwitchOffSkipsCandidateBeforeReadingInput() {
        final TestLogicalInsertIgnoreHandler handler = new TestLogicalInsertIgnoreHandler(true);
        final TestContext context = testContext(false, false, false);

        try (MockedStatic<RelUtils> relUtilsStatic = mockStatic(RelUtils.class)) {
            Assert.assertFalse(handler.canUsePartitionLocalUniqueCandidate(
                context.insert, context.executionContext, primaryCandidate(), singleValue(), insertColumns()));
            relUtilsStatic.verifyNoInteractions();
        }
    }

    @Test
    public void testFullTableScanForLocalUkKeepsLegacyPlan() {
        final TestLogicalInsertIgnoreHandler handler = new TestLogicalInsertIgnoreHandler(true);
        final TestContext context = testContext(true, true, false, false);

        try (MockedStatic<RelUtils> relUtilsStatic = mockStatic(RelUtils.class)) {
            Assert.assertFalse(handler.canUsePartitionLocalUniqueCandidate(
                context.insert, context.executionContext, primaryCandidate(), singleValue(), insertColumns()));
            relUtilsStatic.verifyNoInteractions();
        }
    }

    @Test
    public void testLocalPkSwitchIsIndependentFromLocalUkSwitch() {
        final TestLogicalInsertIgnoreHandler handler = new TestLogicalInsertIgnoreHandler(true);
        final Map<String, List<UkCheckEntry>> legacy = mixedLegacyPlan();
        final Map<String, List<UkCheckEntry>> candidate = mixedCandidate();

        final TestContext ukOnly = testContext(true, false, false, false, false, false);
        final Map<String, List<UkCheckEntry>> ukOnlyPlan = handler.buildEnabledPartitionLocalUniquePlan(
            ukOnly.insert, ukOnly.executionContext, legacy, candidate);
        assertPartitionLocal(ukOnlyPlan, ID_PK, false);
        assertPartitionLocal(ukOnlyPlan, RUN_ID_UK, true);

        final TestContext pkOnly = testContext(false, true, false, false, false, false);
        final Map<String, List<UkCheckEntry>> pkOnlyPlan = handler.buildEnabledPartitionLocalUniquePlan(
            pkOnly.insert, pkOnly.executionContext, legacy, candidate);
        assertPartitionLocal(pkOnlyPlan, ID_PK, true);
        assertPartitionLocal(pkOnlyPlan, RUN_ID_UK, false);
    }

    @Test
    public void testLocalUkFullScanFallbackDoesNotDisableLocalPk() {
        final TestLogicalInsertIgnoreHandler handler = new TestLogicalInsertIgnoreHandler(true);
        final TestContext context = testContext(true, true, true, false, false, false);

        final Map<String, List<UkCheckEntry>> plan = handler.buildEnabledPartitionLocalUniquePlan(
            context.insert, context.executionContext, mixedLegacyPlan(), mixedCandidate());
        assertPartitionLocal(plan, ID_PK, true);
        assertPartitionLocal(plan, RUN_ID_UK, false);
        Assert.assertTrue(useCandidate(handler, context, plan));
    }

    @Test
    public void testPrimaryOnlySettingRejectsPkCandidateWithGsiScope() {
        final TestLogicalInsertIgnoreHandler handler = new TestLogicalInsertIgnoreHandler(true);
        final TestContext context = testContext(false, true, false, true, false, false);
        final Map<String, List<UkCheckEntry>> legacy = new LinkedHashMap<>();
        legacy.put(PRIMARY_TABLE, Collections.singletonList(new UkCheckEntry(ID_PK, "PRIMARY", false)));

        Assert.assertTrue(handler.buildEnabledPartitionLocalUniquePlan(
            context.insert, context.executionContext, legacy, pkGsiCandidate()).isEmpty());
    }

    @Test
    public void testPrimaryOnlySettingAllowsPrimaryPkScope() {
        final TestLogicalInsertIgnoreHandler handler = new TestLogicalInsertIgnoreHandler(true);
        final TestContext context = testContext(false, true, false, true, false, false);
        final Map<String, List<UkCheckEntry>> legacy = new LinkedHashMap<>();
        legacy.put(PRIMARY_TABLE, Collections.singletonList(new UkCheckEntry(ID_PK, "PRIMARY", false)));

        final Map<String, List<UkCheckEntry>> plan = handler.buildEnabledPartitionLocalUniquePlan(
            context.insert, context.executionContext, legacy, pkCandidate());
        assertPartitionLocal(plan, ID_PK, true);
    }

    @Test
    public void testJdbcBatchAndMultiRowKeepLegacyPlan() {
        final TestLogicalInsertIgnoreHandler handler = new TestLogicalInsertIgnoreHandler(true);

        final TestContext batchContext = testContext(true, true, false);
        Assert.assertFalse(useCandidate(handler, batchContext, primaryCandidate()));

        final TestContext multiRowContext = testContext(true, false, true);
        Assert.assertFalse(useCandidate(handler, multiRowContext, primaryCandidate()));
    }

    @Test
    public void testInsertSelectKeepsLegacyPlan() {
        final TestLogicalInsertIgnoreHandler handler = new TestLogicalInsertIgnoreHandler(true);
        final TestContext context = testContext(true, false, false);
        final RelNode input = mock(RelNode.class);
        try (MockedStatic<RelUtils> relUtilsStatic = mockStatic(RelUtils.class)) {
            relUtilsStatic.when(() -> RelUtils.getRelInput(context.insert)).thenReturn(input);
            Assert.assertFalse(handler.canUsePartitionLocalUniqueCandidate(
                context.insert, context.executionContext, primaryCandidate(), singleValue(), insertColumns()));
        }
    }

    @Test
    public void testUnstableRouteOrNonPublicGsiKeepsLegacyPlan() {
        final TestLogicalInsertIgnoreHandler unroutableHandler = new TestLogicalInsertIgnoreHandler(false);
        final TestContext primaryContext = testContext(true, false, false);
        Assert.assertFalse(useCandidate(unroutableHandler, primaryContext, primaryCandidate()));

        final TestLogicalInsertIgnoreHandler handler = new TestLogicalInsertIgnoreHandler(true);
        final TestContext gsiContext = testContext(true, false, false);
        final TableMeta gsiMeta = mock(TableMeta.class);
        when(gsiContext.schemaManager.getTable(GSI_TABLE)).thenReturn(gsiMeta);
        try (MockedStatic<GlobalIndexMeta> globalIndexMetaStatic = mockStatic(GlobalIndexMeta.class)) {
            globalIndexMetaStatic.when(
                () -> GlobalIndexMeta.isPublished(gsiContext.executionContext, gsiMeta)).thenReturn(false);
            Assert.assertFalse(useCandidate(handler, gsiContext, gsiCandidate()));
        }
    }

    private static boolean useCandidate(TestLogicalInsertIgnoreHandler handler, TestContext context,
                                        Map<String, List<UkCheckEntry>> candidate) {
        final LogicalDynamicValues input = mock(LogicalDynamicValues.class);
        final ImmutableList<ImmutableList<RexNode>> tuples = context.multiRow
            ? ImmutableList.of(ImmutableList.of(), ImmutableList.of())
            : ImmutableList.of(ImmutableList.of());
        when(input.getTuples()).thenReturn(tuples);
        try (MockedStatic<RelUtils> relUtilsStatic = mockStatic(RelUtils.class)) {
            relUtilsStatic.when(() -> RelUtils.getRelInput(context.insert)).thenReturn(input);
            return handler.canUsePartitionLocalUniqueCandidate(
                context.insert, context.executionContext, candidate, singleValue(), insertColumns());
        }
    }

    private static TestContext testContext(boolean enable, boolean batch, boolean multiRow) {
        return testContext(enable, false, batch, multiRow);
    }

    private static TestContext testContext(boolean enable, boolean fullTableScan, boolean batch, boolean multiRow) {
        return testContext(enable, false, fullTableScan, false, batch, multiRow);
    }

    private static TestContext testContext(boolean ukEnable, boolean pkEnable, boolean fullTableScan,
                                           boolean primaryOnly, boolean batch, boolean multiRow) {
        final Map<String, String> properties = new HashMap<>();
        properties.put(ConnectionProperties.DML_PARTITION_LOCAL_UK_DUP_CHECK, Boolean.toString(ukEnable));
        properties.put(ConnectionProperties.DML_PARTITION_LOCAL_PK_DUP_CHECK, Boolean.toString(pkEnable));
        properties.put(ConnectionProperties.DML_GET_DUP_FOR_LOCAL_UK_WITH_FULL_TABLE_SCAN,
            Boolean.toString(fullTableScan));
        properties.put(ConnectionProperties.DML_GET_DUP_USING_GSI, Boolean.TRUE.toString());
        properties.put(ConnectionProperties.DML_GET_DUP_FOR_PK_FROM_PRIMARY_ONLY, Boolean.toString(primaryOnly));

        final ExecutionContext executionContext = new ExecutionContext();
        executionContext.setParamManager(new ParamManager(properties));
        executionContext.setParams(batch
            ? new Parameters(Collections.singletonList(Collections.emptyMap()))
            : new Parameters(Collections.emptyMap()));

        final SchemaManager schemaManager = mock(SchemaManager.class);
        final TableMeta primaryMeta = mock(TableMeta.class);
        final ColumnMeta idColumn = mock(ColumnMeta.class);
        when(idColumn.getName()).thenReturn("id");
        final IndexMeta primaryIndex = mock(IndexMeta.class);
        when(primaryIndex.getKeyColumns()).thenReturn(Collections.singletonList(idColumn));
        when(primaryMeta.isHasPrimaryKey()).thenReturn(true);
        when(primaryMeta.getPrimaryKey()).thenReturn(Collections.singletonList(idColumn));
        when(primaryMeta.getPrimaryIndex()).thenReturn(primaryIndex);
        when(schemaManager.getTable(PRIMARY_TABLE)).thenReturn(primaryMeta);
        executionContext.setSchemaManager(SCHEMA_NAME, schemaManager);

        final LogicalInsertIgnore insert = mock(LogicalInsertIgnore.class);
        when(insert.getSchemaName()).thenReturn(SCHEMA_NAME);
        when(insert.getLogicalTableName()).thenReturn(PRIMARY_TABLE);
        return new TestContext(executionContext, schemaManager, insert, multiRow);
    }

    private static Map<String, List<UkCheckEntry>> primaryCandidate() {
        final Map<String, List<UkCheckEntry>> candidate = new LinkedHashMap<>();
        candidate.put(PRIMARY_TABLE, Collections.singletonList(
            new UkCheckEntry(Collections.singletonList("run_id"), "uk_run_id", true)));
        return candidate;
    }

    private static Map<String, List<UkCheckEntry>> gsiCandidate() {
        final Map<String, List<UkCheckEntry>> candidate = primaryCandidate();
        candidate.put(GSI_TABLE, Collections.singletonList(
            new UkCheckEntry(Collections.singletonList("run_id"), "uk_run_id", true)));
        return candidate;
    }

    private static Map<String, List<UkCheckEntry>> pkCandidate() {
        final Map<String, List<UkCheckEntry>> candidate = new LinkedHashMap<>();
        candidate.put(PRIMARY_TABLE, Collections.singletonList(new UkCheckEntry(ID_PK, "PRIMARY", true)));
        return candidate;
    }

    private static Map<String, List<UkCheckEntry>> pkGsiCandidate() {
        final Map<String, List<UkCheckEntry>> candidate = pkCandidate();
        candidate.put(GSI_TABLE, Collections.singletonList(new UkCheckEntry(ID_PK, "PRIMARY", true)));
        return candidate;
    }

    private static Map<String, List<UkCheckEntry>> mixedLegacyPlan() {
        final Map<String, List<UkCheckEntry>> legacy = new LinkedHashMap<>();
        legacy.put(PRIMARY_TABLE, ImmutableList.of(
            new UkCheckEntry(ID_PK, "PRIMARY", false),
            new UkCheckEntry(RUN_ID_UK, "uk_run_id", false)));
        return legacy;
    }

    private static Map<String, List<UkCheckEntry>> mixedCandidate() {
        final Map<String, List<UkCheckEntry>> candidate = new LinkedHashMap<>();
        candidate.put(PRIMARY_TABLE, ImmutableList.of(
            new UkCheckEntry(ID_PK, "PRIMARY", true),
            new UkCheckEntry(RUN_ID_UK, "uk_run_id", true)));
        return candidate;
    }

    private static void assertPartitionLocal(Map<String, List<UkCheckEntry>> plan, List<String> columns,
                                             boolean expected) {
        final UkCheckEntry entry = plan.values().stream()
            .flatMap(List::stream)
            .filter(candidate -> candidate.getUkColumns().equals(columns))
            .findFirst()
            .orElseThrow(AssertionError::new);
        Assert.assertEquals(expected, entry.isPartitionLocal());
    }

    private static List<List<Object>> singleValue() {
        return Collections.singletonList(Collections.singletonList("value"));
    }

    private static List<String> insertColumns() {
        return Collections.singletonList("job_id");
    }

    private static class TestLogicalInsertIgnoreHandler extends LogicalInsertIgnoreHandler {

        private final boolean routeResult;

        private TestLogicalInsertIgnoreHandler(boolean routeResult) {
            super(null);
            this.routeResult = routeResult;
        }

        @Override
        protected boolean canRouteToOnePartition(String schemaName, TableMeta tableMeta, List<List<Object>> values,
                                                 List<String> insertColumns, ExecutionContext executionContext) {
            return routeResult;
        }
    }

    private static class TestContext {

        private final ExecutionContext executionContext;
        private final SchemaManager schemaManager;
        private final LogicalInsertIgnore insert;
        private final boolean multiRow;

        private TestContext(ExecutionContext executionContext, SchemaManager schemaManager,
                            LogicalInsertIgnore insert, boolean multiRow) {
            this.executionContext = executionContext;
            this.schemaManager = schemaManager;
            this.insert = insert;
            this.multiRow = multiRow;
        }
    }
}
