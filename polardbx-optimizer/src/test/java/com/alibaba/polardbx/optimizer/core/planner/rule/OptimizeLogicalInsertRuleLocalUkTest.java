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

package com.alibaba.polardbx.optimizer.core.planner.rule;

import com.alibaba.polardbx.common.properties.ConnectionProperties;
import com.alibaba.polardbx.common.properties.ParamManager;
import com.alibaba.polardbx.gms.metadb.table.IndexStatus;
import com.alibaba.polardbx.gms.metadb.table.IndexVisibility;
import com.alibaba.polardbx.gms.metadb.table.LackLocalIndexStatus;
import com.alibaba.polardbx.optimizer.OptimizerContext;
import com.alibaba.polardbx.optimizer.config.table.ColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.GlobalIndexMeta;
import com.alibaba.polardbx.optimizer.config.table.GsiMetaManager;
import com.alibaba.polardbx.optimizer.config.table.IndexColumnMeta;
import com.alibaba.polardbx.optimizer.config.table.IndexMeta;
import com.alibaba.polardbx.optimizer.config.table.SchemaManager;
import com.alibaba.polardbx.optimizer.config.table.TableMeta;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.rel.LogicalInsertIgnore;
import com.alibaba.polardbx.optimizer.core.rel.UkCheckEntry;
import com.alibaba.polardbx.optimizer.rule.TddlRuleManager;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.MockedStatic;

import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

public class OptimizeLogicalInsertRuleLocalUkTest {

    private static final String SCHEMA_NAME = "test_schema";
    private static final String PRIMARY_TABLE = "job_runs";
    private static final String GSI_TABLE = "g_i_job_time";
    private static final String LOCAL_UK = "uk_run_id";
    private static final List<String> ID_PK = Collections.singletonList("id");
    private static final List<String> RUN_ID_UK = Collections.singletonList("run_id");

    @Test
    public void testPrimaryLocalUkBecomesPartitionLocalCandidate() {
        final Map<String, List<UkCheckEntry>> candidate =
            buildCandidate(localUk(LOCAL_UK, 0), null);

        Assert.assertEquals(1, candidate.size());
        Assert.assertEquals(PRIMARY_TABLE, candidate.keySet().iterator().next());
        assertPartitionLocal(candidate.get(PRIMARY_TABLE), LOCAL_UK);
    }

    @Test
    public void testMatchingLocalUkOnNormalGsiAddsOneScope() {
        final TableMeta gsiMeta = gsiMeta(true, localUk(LOCAL_UK, 0));
        final Map<String, List<UkCheckEntry>> candidate =
            buildCandidate(localUk(LOCAL_UK, 0), gsiMeta);

        Assert.assertEquals(2, candidate.size());
        assertPartitionLocal(candidate.get(PRIMARY_TABLE), LOCAL_UK);
        assertPartitionLocal(candidate.get(GSI_TABLE), LOCAL_UK);
    }

    @Test
    public void testPrimaryLocalPkBecomesPartitionLocalCandidate() {
        final Map<String, List<UkCheckEntry>> candidate =
            buildCandidate(localUk(LOCAL_UK, 0), null, Collections.singletonList(ID_PK),
                Collections.singletonList("PRIMARY"));

        Assert.assertEquals(1, candidate.size());
        assertPartitionLocal(candidate.get(PRIMARY_TABLE), ID_PK, "PRIMARY");
    }

    @Test
    public void testMissingLogicalPrimaryKeyDoesNotBuildLocalPkCandidate() {
        final Map<String, List<List<String>>> legacyUkChecks = new LinkedHashMap<>();
        legacyUkChecks.put(PRIMARY_TABLE, Collections.singletonList(RUN_ID_UK));
        final Map<String, List<String>> localIndexNames = new LinkedHashMap<>();
        localIndexNames.put(PRIMARY_TABLE, Collections.singletonList(LOCAL_UK));

        final Map<String, List<UkCheckEntry>> candidate =
            buildCandidate(localUk(LOCAL_UK, 0), null, legacyUkChecks, localIndexNames,
                Collections.singletonList("job_id"), Collections.emptyList());

        assertPartitionLocal(candidate.get(PRIMARY_TABLE), RUN_ID_UK, LOCAL_UK);
        Assert.assertFalse(candidate.values().stream()
            .flatMap(List::stream)
            .anyMatch(entry -> entry.getUkColumns().equals(ID_PK)));
    }

    @Test
    public void testMatchingPhysicalPkOnNormalGsiAddsOneScope() {
        final TableMeta gsiMeta = gsiMeta(true, null, physicalIndex("PRIMARY", "id", 0));
        final Map<String, List<UkCheckEntry>> candidate =
            buildCandidate(localUk(LOCAL_UK, 0), gsiMeta, Collections.singletonList(ID_PK),
                Collections.singletonList("PRIMARY"));

        Assert.assertEquals(2, candidate.size());
        assertPartitionLocal(candidate.get(PRIMARY_TABLE), ID_PK, "PRIMARY");
        assertPartitionLocal(candidate.get(GSI_TABLE), ID_PK, "PRIMARY");
    }

    @Test
    public void testNonUniqueImplicitPkIndexOnGsiIsNotPhysicalScope() {
        final TableMeta gsiMeta = gsiMeta(true, null, null);
        final Map<String, List<UkCheckEntry>> candidate =
            buildCandidate(localUk(LOCAL_UK, 0), gsiMeta, Collections.singletonList(ID_PK),
                Collections.singletonList("PRIMARY"));

        Assert.assertEquals(1, candidate.size());
        assertPartitionLocal(candidate.get(PRIMARY_TABLE), ID_PK, "PRIMARY");
        Assert.assertNull(candidate.get(GSI_TABLE));
    }

    @Test
    public void testSameColumnsUgsiKeepsLegacyPkPlan() {
        final TableMeta ugsiMeta = gsiMeta(false, "id", null, null);
        final Map<String, List<UkCheckEntry>> candidate =
            buildCandidate(localUk(LOCAL_UK, 0), ugsiMeta, Collections.singletonList(ID_PK),
                Collections.singletonList("PRIMARY"));

        Assert.assertTrue(candidate.isEmpty());
    }

    @Test
    public void testPkAlreadyRoutedByGsiKeepsLegacyPlan() {
        final TableMeta gsiMeta = gsiMeta(true, null, physicalIndex("PRIMARY", "id", 0));
        final Map<String, List<List<String>>> legacyUkChecks = new LinkedHashMap<>();
        legacyUkChecks.put(GSI_TABLE, Collections.singletonList(ID_PK));
        final Map<String, List<String>> localIndexNames = new LinkedHashMap<>();
        localIndexNames.put(GSI_TABLE, Collections.singletonList("PRIMARY"));

        final Map<String, List<UkCheckEntry>> candidate =
            buildCandidate(localUk(LOCAL_UK, 0), gsiMeta, legacyUkChecks, localIndexNames,
                Collections.singletonList("id"));

        Assert.assertTrue(candidate.isEmpty());
    }

    @Test
    public void testSameColumnsUgsiKeepsLegacyPlan() {
        final TableMeta ugsiMeta = gsiMeta(false, null);
        final Map<String, List<UkCheckEntry>> candidate =
            buildCandidate(localUk(LOCAL_UK, 0), ugsiMeta);

        Assert.assertTrue(candidate.isEmpty());
    }

    @Test
    public void testPrefixLocalUkKeepsLegacyPlan() {
        final Map<String, List<UkCheckEntry>> candidate =
            buildCandidate(localUk(LOCAL_UK, 16), null);

        Assert.assertTrue(candidate.isEmpty());
    }

    @Test
    public void testPrefixLocalUkOnGsiKeepsLegacyPlan() {
        final TableMeta gsiMeta = gsiMeta(true, localUk(LOCAL_UK, 16));
        final Map<String, List<UkCheckEntry>> candidate =
            buildCandidate(localUk(LOCAL_UK, 0), gsiMeta);

        Assert.assertTrue(candidate.isEmpty());
    }

    @Test
    public void testFunctionLocalUkKeepsLegacyPlan() {
        final IndexMeta functionIndex = localUk(LOCAL_UK, 0);
        when(functionIndex.isFunctionIndex()).thenReturn(true);

        Assert.assertTrue(buildCandidate(functionIndex, null).isEmpty());
    }

    @Test
    public void testCandidateKeepsLegacyUkOrder() {
        final List<List<String>> uniqueKeys = Arrays.asList(
            ID_PK,
            RUN_ID_UK,
            Collections.singletonList("email"));
        final List<String> indexNames = Arrays.asList("PRIMARY", LOCAL_UK, "uk_email");
        final Map<String, List<UkCheckEntry>> candidate =
            buildCandidate(localUk(LOCAL_UK, 0), null, uniqueKeys, indexNames);

        final List<UkCheckEntry> entries = candidate.get(PRIMARY_TABLE);
        Assert.assertEquals(uniqueKeys, entries.stream().map(UkCheckEntry::getUkColumns)
            .collect(java.util.stream.Collectors.toList()));
        Assert.assertTrue(entries.get(0).isPartitionLocal());
        Assert.assertTrue(entries.get(1).isPartitionLocal());
        Assert.assertFalse(entries.get(2).isPartitionLocal());
    }

    private static Map<String, List<UkCheckEntry>> buildCandidate(IndexMeta primaryLocalUk, TableMeta gsiMeta) {
        return buildCandidate(primaryLocalUk, gsiMeta, Collections.singletonList(RUN_ID_UK),
            Collections.singletonList(LOCAL_UK));
    }

    private static Map<String, List<UkCheckEntry>> buildCandidate(IndexMeta primaryLocalUk, TableMeta gsiMeta,
                                                                  List<List<String>> uniqueKeys,
                                                                  List<String> indexNames) {
        final Map<String, List<List<String>>> legacyUkChecks = new LinkedHashMap<>();
        legacyUkChecks.put(PRIMARY_TABLE, uniqueKeys);
        final Map<String, List<String>> localIndexNames = new LinkedHashMap<>();
        localIndexNames.put(PRIMARY_TABLE, indexNames);
        return buildCandidate(primaryLocalUk, gsiMeta, legacyUkChecks, localIndexNames,
            Collections.singletonList("job_id"));
    }

    private static Map<String, List<UkCheckEntry>> buildCandidate(IndexMeta primaryLocalUk, TableMeta gsiMeta,
                                                                  Map<String, List<List<String>>> legacyUkChecks,
                                                                  Map<String, List<String>> localIndexNames,
                                                                  List<String> gsiPartitionColumns) {
        return buildCandidate(primaryLocalUk, gsiMeta, legacyUkChecks, localIndexNames, gsiPartitionColumns,
            Collections.singletonList("id"));
    }

    private static Map<String, List<UkCheckEntry>> buildCandidate(IndexMeta primaryLocalUk, TableMeta gsiMeta,
                                                                  Map<String, List<List<String>>> legacyUkChecks,
                                                                  Map<String, List<String>> localIndexNames,
                                                                  List<String> gsiPartitionColumns,
                                                                  List<String> primaryKeys) {
        final LogicalInsertIgnore insert = mock(LogicalInsertIgnore.class);
        when(insert.getSchemaName()).thenReturn(SCHEMA_NAME);
        when(insert.getLogicalTableName()).thenReturn(PRIMARY_TABLE);

        final TableMeta primaryMeta = mock(TableMeta.class);
        final IndexMeta primaryIndex = physicalIndex("PRIMARY", "id", 0);
        when(primaryMeta.getPrimaryIndex()).thenReturn(primaryIndex);
        when(primaryMeta.getUniqueIndexes(false)).thenReturn(Collections.singletonList(primaryLocalUk));
        when(primaryMeta.getUniqueIndexes(true)).thenReturn(Arrays.asList(primaryIndex, primaryLocalUk));

        final SchemaManager schemaManager = mock(SchemaManager.class);
        when(schemaManager.getTable(PRIMARY_TABLE)).thenReturn(primaryMeta);

        final Map<String, String> properties = new HashMap<>();
        properties.put(ConnectionProperties.DML_GET_DUP_USING_GSI, Boolean.TRUE.toString());
        final ExecutionContext executionContext = new ExecutionContext();
        executionContext.setParamManager(new ParamManager(properties));
        executionContext.setSchemaManager(SCHEMA_NAME, schemaManager);

        final OptimizerContext optimizerContext = mock(OptimizerContext.class);
        final TddlRuleManager ruleManager = mock(TddlRuleManager.class);
        when(optimizerContext.getRuleManager()).thenReturn(ruleManager);
        when(ruleManager.getSharedColumns(PRIMARY_TABLE)).thenReturn(Collections.singletonList("job_id"));
        when(ruleManager.getSharedColumns(GSI_TABLE)).thenReturn(gsiPartitionColumns);

        final List<TableMeta> gsiMetas =
            gsiMeta == null ? Collections.emptyList() : Collections.singletonList(gsiMeta);
        try (MockedStatic<OptimizerContext> optimizerContextStatic = mockStatic(OptimizerContext.class);
            MockedStatic<GlobalIndexMeta> globalIndexMetaStatic = mockStatic(GlobalIndexMeta.class)) {
            optimizerContextStatic.when(() -> OptimizerContext.getContext(SCHEMA_NAME)).thenReturn(optimizerContext);
            globalIndexMetaStatic.when(() -> GlobalIndexMeta.getPrimaryKeys(primaryMeta))
                .thenReturn(primaryKeys);
            globalIndexMetaStatic.when(
                () -> GlobalIndexMeta.getIndex(PRIMARY_TABLE, SCHEMA_NAME, executionContext)).thenReturn(gsiMetas);
            if (gsiMeta != null) {
                globalIndexMetaStatic.when(() -> GlobalIndexMeta.canWrite(executionContext, gsiMeta)).thenReturn(true);
                globalIndexMetaStatic.when(() -> GlobalIndexMeta.isPublished(executionContext, gsiMeta))
                    .thenReturn(true);
            }

            return OptimizeLogicalInsertRule.INSTANCE.buildPartitionLocalUniqueCandidate(
                insert, legacyUkChecks, localIndexNames, executionContext);
        }
    }

    private static IndexMeta localUk(String indexName, long subPart) {
        return physicalIndex(indexName, "run_id", subPart);
    }

    private static IndexMeta physicalIndex(String indexName, String columnName, long subPart) {
        final ColumnMeta columnMeta = mock(ColumnMeta.class);
        when(columnMeta.getName()).thenReturn(columnName);

        final IndexColumnMeta indexColumnMeta = mock(IndexColumnMeta.class);
        when(indexColumnMeta.hasColumn()).thenReturn(true);
        when(indexColumnMeta.getSubPart()).thenReturn(subPart);

        final IndexMeta indexMeta = mock(IndexMeta.class);
        when(indexMeta.getKeyColumns()).thenReturn(Collections.singletonList(columnMeta));
        when(indexMeta.getKeyColumnsExt()).thenReturn(Collections.singletonList(indexColumnMeta));
        when(indexMeta.getPhysicalIndexName()).thenReturn(indexName);
        when(indexMeta.isFunctionIndex()).thenReturn(false);
        return indexMeta;
    }

    private static TableMeta gsiMeta(boolean nonUnique, IndexMeta localUk) {
        return gsiMeta(nonUnique, "run_id", localUk, physicalIndex("PRIMARY", "id", 0));
    }

    private static TableMeta gsiMeta(boolean nonUnique, IndexMeta localUk, IndexMeta physicalPrimary) {
        return gsiMeta(nonUnique, "run_id", localUk, physicalPrimary);
    }

    private static TableMeta gsiMeta(boolean nonUnique, String logicalIndexColumn, IndexMeta localUk,
                                     IndexMeta physicalPrimary) {
        final GsiMetaManager.GsiIndexColumnMetaBean runIdColumn =
            new GsiMetaManager.GsiIndexColumnMetaBean(1, logicalIndexColumn, null, 0, null, null, null, nonUnique);
        final GsiMetaManager.GsiIndexMetaBean indexMetaBean =
            new GsiMetaManager.GsiIndexMetaBean(null, SCHEMA_NAME, PRIMARY_TABLE, nonUnique, SCHEMA_NAME,
                "gsi_run_id", Collections.singletonList(runIdColumn), Collections.emptyList(), null, null,
                null, null, GSI_TABLE, IndexStatus.PUBLIC, 1, false, false, IndexVisibility.VISIBLE,
                LackLocalIndexStatus.NO_LACKIING);
        final GsiMetaManager.GsiTableMetaBean tableMetaBean =
            new GsiMetaManager.GsiTableMetaBean(null, SCHEMA_NAME, GSI_TABLE, GsiMetaManager.TableType.GSI,
                null, null, null, null, null, null, Collections.emptyMap(), null, indexMetaBean);

        final TableMeta tableMeta = mock(TableMeta.class);
        when(tableMeta.getTableName()).thenReturn(GSI_TABLE);
        when(tableMeta.isColumnar()).thenReturn(false);
        when(tableMeta.getGsiTableMetaBean()).thenReturn(tableMetaBean);
        when(tableMeta.getUniqueIndexes(false)).thenReturn(
            localUk == null ? Collections.emptyList() : Collections.singletonList(localUk));
        final List<IndexMeta> allUniqueIndexes = new java.util.ArrayList<>();
        if (physicalPrimary != null) {
            allUniqueIndexes.add(physicalPrimary);
        }
        if (localUk != null) {
            allUniqueIndexes.add(localUk);
        }
        when(tableMeta.getUniqueIndexes(true)).thenReturn(allUniqueIndexes);
        return tableMeta;
    }

    private static void assertPartitionLocal(List<UkCheckEntry> entries, String indexName) {
        assertPartitionLocal(entries, RUN_ID_UK, indexName);
    }

    private static void assertPartitionLocal(List<UkCheckEntry> entries, List<String> columns, String indexName) {
        Assert.assertNotNull(entries);
        Assert.assertEquals(1, entries.size());
        Assert.assertEquals(columns, entries.get(0).getUkColumns());
        Assert.assertEquals(indexName, entries.get(0).getLocalIndexName());
        Assert.assertTrue(entries.get(0).isPartitionLocal());
    }
}
