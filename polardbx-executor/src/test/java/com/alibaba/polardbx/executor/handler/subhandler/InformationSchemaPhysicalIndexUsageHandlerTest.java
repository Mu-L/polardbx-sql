package com.alibaba.polardbx.executor.handler.subhandler;

import com.alibaba.polardbx.common.jdbc.Parameters;
import com.alibaba.polardbx.executor.cursor.impl.ArrayResultCursor;
import com.alibaba.polardbx.executor.sync.FetchIndexUsageSyncAction;
import com.alibaba.polardbx.optimizer.context.ExecutionContext;
import com.alibaba.polardbx.optimizer.core.datatype.DataTypes;
import com.alibaba.polardbx.optimizer.core.row.Row;
import com.alibaba.polardbx.optimizer.view.InformationSchemaPhysicalIndexUsage;
import org.junit.Test;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

/**
 * Unit tests for InformationSchemaPhysicalIndexUsageHandler.
 * <p>
 * Coverage targets:
 * - handle() with no filter: all rows pass through (including new LOGICAL_SCHEMA/LOGICAL_TABLE)
 * - handle() with OBJECT_SCHEMA filter: only matching physical schema rows pass
 * - handle() with OBJECT_NAME filter: only matching physical table rows pass
 * - handle() with both filters simultaneously
 * - handle() with empty sync result
 * - isSupport(): verifies only InformationSchemaPhysicalIndexUsage is handled
 * - LOGICAL_SCHEMA / LOGICAL_TABLE columns correctly derived from physical names
 */
public class InformationSchemaPhysicalIndexUsageHandlerTest {

    // ==================== isSupport() ====================

    @Test
    public void testIsSupport_PhysicalIndexUsage_ReturnsTrue() {
        InformationSchemaPhysicalIndexUsageHandler handler =
            new InformationSchemaPhysicalIndexUsageHandler(null);

        InformationSchemaPhysicalIndexUsage mockView = mock(InformationSchemaPhysicalIndexUsage.class);
        assertTrue(handler.isSupport(mockView));
    }
    // ==================== handle() — filter tests ====================

    @Test
    public void testHandle_EmptyResult() {
        InformationSchemaPhysicalIndexUsageHandler handler =
            new InformationSchemaPhysicalIndexUsageHandler(null);

        ArrayResultCursor syncCursor = buildSyncResultCursor(/* no rows */);

        try (MockedStatic<PhysicalToLogicalTableMapping> mappingMock =
            mockStatic(PhysicalToLogicalTableMapping.class);
            MockedConstruction<FetchIndexUsageSyncAction> mocked =
                mockConstruction(FetchIndexUsageSyncAction.class,
                    (mock, ctx) -> when(mock.sync()).thenReturn(syncCursor))) {

            mappingMock.when(PhysicalToLogicalTableMapping::buildForAllSchemas)
                .thenReturn(buildTestMapping());

            ArrayResultCursor output = buildOutputCursor();
            handler.handle(buildMockView(null, null), buildMockCtx("mydb"), output);

            assertTrue(output.getRows().isEmpty());
        }
    }

    @Test
    public void testHandle_NoFilter_AllRowsPassThrough() {
        InformationSchemaPhysicalIndexUsageHandler handler =
            new InformationSchemaPhysicalIndexUsageHandler(null);

        ArrayResultCursor syncCursor = buildSyncResultCursor(
            new Object[] {"mydb_p00000", "orders_v3gg_00001", "idx_user", 100L, 10L, 1000L, 500L},
            new Object[] {"mydb_p00001", "orders_v3gg_00002", "idx_user", 200L, 20L, 2000L, 800L},
            new Object[] {"other_p00000", "users_abc_00001", "idx_email", 50L, 5L, 500L, 300L}
        );

        try (MockedStatic<PhysicalToLogicalTableMapping> mappingMock =
            mockStatic(PhysicalToLogicalTableMapping.class);
            MockedConstruction<FetchIndexUsageSyncAction> mocked =
                mockConstruction(FetchIndexUsageSyncAction.class,
                    (mock, ctx) -> when(mock.sync()).thenReturn(syncCursor))) {

            mappingMock.when(PhysicalToLogicalTableMapping::buildForAllSchemas)
                .thenReturn(buildTestMapping());

            ArrayResultCursor output = buildOutputCursor();
            handler.handle(buildMockView(null, null), buildMockCtx("mydb"), output);

            // No filter -> all 3 rows pass
            assertEquals(3, output.getRows().size());
        }
    }

    @Test
    public void testHandle_FilterBySchemaName() {
        InformationSchemaPhysicalIndexUsageHandler handler =
            new InformationSchemaPhysicalIndexUsageHandler(null);

        ArrayResultCursor syncCursor = buildSyncResultCursor(
            new Object[] {"mydb_p00000", "orders_v3gg_00001", "idx_user", 100L, 10L, 1000L, 500L},
            new Object[] {"other_p00000", "users_abc_00001", "idx_email", 50L, 5L, 500L, 300L}
        );

        Set<String> schemaFilter = new HashSet<>(Collections.singletonList("mydb_p00000"));

        try (MockedStatic<PhysicalToLogicalTableMapping> mappingMock =
            mockStatic(PhysicalToLogicalTableMapping.class);
            MockedConstruction<FetchIndexUsageSyncAction> mocked =
                mockConstruction(FetchIndexUsageSyncAction.class,
                    (mock, ctx) -> when(mock.sync()).thenReturn(syncCursor))) {

            mappingMock.when(PhysicalToLogicalTableMapping::buildForAllSchemas)
                .thenReturn(buildTestMapping());

            ArrayResultCursor output = buildOutputCursor();
            handler.handle(buildMockView(schemaFilter, null), buildMockCtx("mydb"), output);

            List<Row> rows = output.getRows();
            assertEquals(1, rows.size());
            assertEquals("mydb_p00000", rows.get(0).getString(0));
            assertEquals("orders_v3gg_00001", rows.get(0).getString(1));
        }
    }

    @Test
    public void testHandle_FilterByTableName() {
        InformationSchemaPhysicalIndexUsageHandler handler =
            new InformationSchemaPhysicalIndexUsageHandler(null);

        ArrayResultCursor syncCursor = buildSyncResultCursor(
            new Object[] {"mydb_p00000", "orders_v3gg_00001", "idx_user", 100L, 10L, 1000L, 500L},
            new Object[] {"mydb_p00000", "users_abc_00001", "idx_email", 50L, 5L, 500L, 300L}
        );

        Set<String> tableFilter = new HashSet<>(Collections.singletonList("orders_v3gg_00001"));

        try (MockedStatic<PhysicalToLogicalTableMapping> mappingMock =
            mockStatic(PhysicalToLogicalTableMapping.class);
            MockedConstruction<FetchIndexUsageSyncAction> mocked =
                mockConstruction(FetchIndexUsageSyncAction.class,
                    (mock, ctx) -> when(mock.sync()).thenReturn(syncCursor))) {

            mappingMock.when(PhysicalToLogicalTableMapping::buildForAllSchemas)
                .thenReturn(buildTestMapping());

            ArrayResultCursor output = buildOutputCursor();
            handler.handle(buildMockView(null, tableFilter), buildMockCtx("mydb"), output);

            List<Row> rows = output.getRows();
            assertEquals(1, rows.size());
            assertEquals("orders_v3gg_00001", rows.get(0).getString(1));
        }
    }

    // ==================== handle() — LOGICAL_SCHEMA / LOGICAL_TABLE column tests ====================

    @Test
    public void testHandle_LogicalColumnsCorrectlyDerived() {
        // Verify the two new columns (index 4=LOGICAL_SCHEMA, 5=LOGICAL_TABLE) are populated
        InformationSchemaPhysicalIndexUsageHandler handler =
            new InformationSchemaPhysicalIndexUsageHandler(null);

        ArrayResultCursor syncCursor = buildSyncResultCursor(
            new Object[] {"mydb_p00000", "orders_v3gg_00001", "idx_user", 100L, 10L, 1000L, 500L}
        );

        try (MockedStatic<PhysicalToLogicalTableMapping> mappingMock =
            mockStatic(PhysicalToLogicalTableMapping.class);
            MockedConstruction<FetchIndexUsageSyncAction> mocked =
                mockConstruction(FetchIndexUsageSyncAction.class,
                    (mock, ctx) -> when(mock.sync()).thenReturn(syncCursor))) {

            mappingMock.when(PhysicalToLogicalTableMapping::buildForAllSchemas)
                .thenReturn(buildTestMapping());

            ArrayResultCursor output = buildOutputCursor();
            handler.handle(buildMockView(null, null), buildMockCtx("mydb"), output);

            List<Row> rows = output.getRows();
            assertEquals(1, rows.size());
            Row row = rows.get(0);
            // Original physical columns intact
            assertEquals("mydb_p00000", row.getString(0));    // OBJECT_SCHEMA (physical)
            assertEquals("orders_v3gg_00001", row.getString(1)); // OBJECT_NAME (physical)
            assertEquals("idx_user", row.getString(2));         // INDEX_NAME
            assertEquals(Long.valueOf(100L), row.getObject(3)); // COUNT_STAR
            // Derived logical columns (from metadata mapping)
            assertEquals("mydb", row.getString(4));            // LOGICAL_SCHEMA
            assertEquals("orders", row.getString(5));          // LOGICAL_TABLE
        }
    }

    @Test
    public void testHandle_RandomSuffix_ResolvedByMetadataMapping() {
        // Verify that random suffixes like _8htn (without trailing digits) are correctly
        // resolved via metadata mapping, which was impossible with PhysicalNameExtractor alone
        InformationSchemaPhysicalIndexUsageHandler handler =
            new InformationSchemaPhysicalIndexUsageHandler(null);

        ArrayResultCursor syncCursor = buildSyncResultCursor(
            new Object[] {
                "mydb_p00000", "web3_control_address_tag_model_category_pre_8htn", "idx_tag", 42L, 4L, 400L, 200L}
        );

        // Build a mapping that includes the random-suffix physical table
        Map<String, String> tableMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        tableMap.put("web3_control_address_tag_model_category_pre_8htn",
            "web3_control_address_tag_model_category_pre");
        Map<String, String> schemaMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        schemaMap.put("mydb_p00000", "mydb");
        PhysicalToLogicalTableMapping mapping = new PhysicalToLogicalTableMapping(tableMap, schemaMap);

        try (MockedStatic<PhysicalToLogicalTableMapping> mappingMock =
            mockStatic(PhysicalToLogicalTableMapping.class);
            MockedConstruction<FetchIndexUsageSyncAction> mocked =
                mockConstruction(FetchIndexUsageSyncAction.class,
                    (mock, ctx) -> when(mock.sync()).thenReturn(syncCursor))) {

            mappingMock.when(PhysicalToLogicalTableMapping::buildForAllSchemas)
                .thenReturn(mapping);

            ArrayResultCursor output = buildOutputCursor();
            handler.handle(buildMockView(null, null), buildMockCtx("mydb"), output);

            List<Row> rows = output.getRows();
            assertEquals(1, rows.size());
            Row row = rows.get(0);
            assertEquals("mydb", row.getString(4));  // LOGICAL_SCHEMA
            assertEquals("web3_control_address_tag_model_category_pre", row.getString(5));  // LOGICAL_TABLE
        }
    }

    // ==================== Helpers ====================

    /**
     * Build a test mapping that covers the physical names used in test data.
     * Maps are populated with entries matching the test fixtures so that
     * metadata-based lookup works without requiring real CN metadata.
     * Physical names not in the map will fall back to PhysicalNameExtractor.
     */
    private PhysicalToLogicalTableMapping buildTestMapping() {
        Map<String, String> tableMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        tableMap.put("orders_v3gg_00001", "orders");
        tableMap.put("orders_v3gg_00002", "orders");
        tableMap.put("users_abc_00001", "users");

        Map<String, String> schemaMap = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        schemaMap.put("mydb_p00000", "mydb");
        schemaMap.put("mydb_p00001", "mydb");
        schemaMap.put("other_p00000", "other");

        return new PhysicalToLogicalTableMapping(tableMap, schemaMap);
    }

    private ArrayResultCursor buildSyncResultCursor(Object[]... rows) {
        ArrayResultCursor cursor = new ArrayResultCursor("index_usage");
        cursor.addColumn("OBJECT_SCHEMA", DataTypes.StringType);
        cursor.addColumn("OBJECT_NAME", DataTypes.StringType);
        cursor.addColumn("INDEX_NAME", DataTypes.StringType);
        cursor.addColumn("COUNT_STAR", DataTypes.LongType);
        cursor.addColumn("COUNT_FETCH", DataTypes.LongType);
        cursor.addColumn("SUM_TIMER_WAIT", DataTypes.LongType);
        cursor.addColumn("MAX_TIMER_WAIT", DataTypes.LongType);
        cursor.initMeta();
        for (Object[] row : rows) {
            cursor.addRow(row);
        }
        return cursor;
    }

    /**
     * Output cursor mirrors the full 9-column schema of physical_index_usage:
     * OBJECT_SCHEMA, OBJECT_NAME, INDEX_NAME, COUNT_STAR, LOGICAL_SCHEMA, LOGICAL_TABLE,
     * COUNT_FETCH, SUM_TIMER_WAIT, MAX_TIMER_WAIT
     */
    private ArrayResultCursor buildOutputCursor() {
        ArrayResultCursor cursor = new ArrayResultCursor("physical_index_usage");
        cursor.addColumn("OBJECT_SCHEMA", DataTypes.StringType);
        cursor.addColumn("OBJECT_NAME", DataTypes.StringType);
        cursor.addColumn("INDEX_NAME", DataTypes.StringType);
        cursor.addColumn("COUNT_STAR", DataTypes.LongType);
        cursor.addColumn("LOGICAL_SCHEMA", DataTypes.StringType);
        cursor.addColumn("LOGICAL_TABLE", DataTypes.StringType);
        cursor.addColumn("COUNT_FETCH", DataTypes.LongType);
        cursor.addColumn("SUM_TIMER_WAIT", DataTypes.LongType);
        cursor.addColumn("MAX_TIMER_WAIT", DataTypes.LongType);
        cursor.initMeta();
        return cursor;
    }

    private InformationSchemaPhysicalIndexUsage buildMockView(Set<String> schemaFilter,
                                                              Set<String> tableFilter) {
        InformationSchemaPhysicalIndexUsage mockView =
            mock(InformationSchemaPhysicalIndexUsage.class);
        when(mockView.getEqualsFilterValues(anyInt(), any())).thenReturn(null);

        if (schemaFilter != null) {
            when(mockView.getEqualsFilterValues(
                eq(InformationSchemaPhysicalIndexUsage.getObjectSchemaIndex()), any()))
                .thenReturn(schemaFilter);
        }
        if (tableFilter != null) {
            when(mockView.getEqualsFilterValues(
                eq(InformationSchemaPhysicalIndexUsage.getObjectNameIndex()), any()))
                .thenReturn(tableFilter);
        }
        return mockView;
    }

    private ExecutionContext buildMockCtx(String schemaName) {
        ExecutionContext mockCtx = mock(ExecutionContext.class);
        Parameters mockParams = mock(Parameters.class);
        when(mockCtx.getParams()).thenReturn(mockParams);
        when(mockCtx.getSchemaName()).thenReturn(schemaName);
        when(mockParams.getCurrentParameter()).thenReturn(Collections.emptyMap());
        return mockCtx;
    }
}
